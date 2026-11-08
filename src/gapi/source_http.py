"""Public-source HTTP: bounded reads, no auth bypass, robots and finite retries."""
from __future__ import annotations

import hashlib
import ipaddress
import json
import math
import re
import time
import zlib
from contextlib import contextmanager
from email.utils import parsedate_to_datetime
from http.client import HTTPException
from urllib.error import HTTPError, URLError
from urllib.parse import urlsplit, parse_qs
from urllib.request import Request, urlopen, build_opener, HTTPRedirectHandler
from urllib.robotparser import RobotFileParser

from .persistent_http import read_chunk

USER_AGENT = "GAPI/0.2 (+https://github.com/Northern-Summer/global-academic-prestige-index; research)"
# Official anonymous quota, checked 2026-10-29:
# https://info.orcid.org/ufaqs/what-are-the-api-limits/
ORCID_ANONYMOUS_DAILY_READS = 25000
# Official wbgetentities paraminfo anonymous ids limit, checked 2026-10-30.
WIKIDATA_BATCH_LIMIT = 50
HOSTS = {"orcid.figshare.com", "ndownloader.figshare.com", "api.figshare.com", "s3-eu-west-1.amazonaws.com",
         "s3.amazonaws.com", "pub.orcid.org", "www.wikidata.org", "query.wikidata.org", "dumps.wikimedia.org",
         "www.tamucc.edu", "experts.colorado.edu", "api.openalex.org", "api.crossref.org", "api.nobelprize.org", "zenodo.org"}


class SourcePause(RuntimeError):
    """A server backoff belongs to the whole source, across member/run boundaries."""
    def __init__(self,code,retry_at,reason):
        self.code,self.retry_at=code,retry_at
        super().__init__(reason)


class StageDeadline(TimeoutError):
    """The caller's work window ended; this is not a source/member failure."""


def entity_batch_route(parsed):
    """Permit only the documented read-only claims/info Action API contract."""
    if parsed.hostname != 'www.wikidata.org' or parsed.path != '/w/api.php':
        return False
    query = parse_qs(parsed.query, keep_blank_values=True)
    expected = {'action':['wbgetentities'], 'props':['claims|info'],
                'format':['json'], 'maxlag':['5']}
    if set(query) != {*expected, 'ids'} or any(query.get(k) != v for k,v in expected.items()):
        return False
    import re
    ids = query.get('ids', [])
    return (len(ids) == 1 and 1 <= len(ids[0].split('|')) <= WIKIDATA_BATCH_LIMIT
            and all(re.fullmatch(r'Q[1-9]\d*', q) for q in ids[0].split('|')))


class SourceRedirect(RuntimeError):
    """ORCID identity redirects require candidate review before another request.

    Keep URLs available to the ingestion owner, without putting identifiers in
    ordinary error messages. A redirect is not a degree, job or person merge.
    """
    def __init__(self, source_url, target_url, code):
        self.source_url, self.target_url, self.code = source_url, target_url, code
        super().__init__('ORCID API identity redirect requires separate candidate validation')


def retry_time(value,now):
    if not value:return now+60
    if value.isdigit():return now+int(value)
    try:
        date=parsedate_to_datetime(value)
        if date.tzinfo is None:raise ValueError('timezone missing')
        return max(now,date.timestamp())
    except (ValueError,TypeError,OverflowError):
        # Invalid server instructions do not license immediate requests for
        # another person. This is an explicit conservative policy, not its delay.
        return now+21600


class SourceResponse:
    """Normalize mid-body transport failures at the shared HTTP boundary."""
    def __init__(self,response,check_deadline=lambda:None):
        self.response=response
        self.headers=response.headers
        self.check_deadline=check_deadline

    def read(self,size):
        if size<0:raise ValueError('unbounded source read forbidden')
        self.check_deadline()
        try:
            return read_chunk(self.response,min(size,65536))
        except (TimeoutError, URLError, ConnectionError):
            # A socket uses the remaining stage time as its timeout. Preserve a
            # genuine early transport failure; classify expiration at the owner.
            self.check_deadline()
            raise
        except HTTPException as exc:
            raise OSError('source body transport failed: '+type(exc).__name__) from exc

    def geturl(self):return self.response.geturl()
    def close(self):self.response.close()
    def __enter__(self):return self
    def __exit__(self,*args):self.close()


class XMLResponse:
    """Decode one HTTP gzip member with bounded input/output, never an XML tree.

    Hashing and SAX consumers receive the exact decoded representation as before.
    CRC/trailer validation completes before EOF; truncation, trailing members and
    expansion beyond the member limit fail the caller's existing transaction.
    """
    def __init__(self, response, limit, observe_wire):
        self.response, self.headers = response, response.headers
        self.limit, self.observe_wire = limit, observe_wire
        encoding = self.headers.get('Content-Encoding', 'identity').strip().lower()
        if encoding not in ('identity', 'gzip'):
            response.close()
            raise ValueError('unsupported XML content encoding')
        self.decoder = zlib.decompressobj(31) if encoding == 'gzip' else None
        self.pending = b''
        self.wire_size = self.decoded_size = 0
        self.finished = False

    def read(self, size):
        if type(size) is not int or size < 0:
            raise ValueError('unbounded XML read forbidden')
        size = min(size, 65536)
        if size == 0 or self.finished:
            return b''
        while True:
            self.response.check_deadline()
            chunk = self.pending
            self.pending = b''
            if not chunk:
                chunk = self.response.read(65536)
                self.wire_size += len(chunk)
                self.observe_wire(len(chunk))
                if self.wire_size > self.limit:
                    raise ValueError('XML wire response exceeds member bound')
                if not chunk:
                    if self.decoder is not None and not self.decoder.eof:
                        raise ValueError('truncated XML gzip response')
                    self.finished = True
                    return b''
            if self.decoder is not None:
                try:
                    output = self.decoder.decompress(chunk, min(size, self.limit + 1 - self.decoded_size))
                except zlib.error as exc:
                    raise ValueError('invalid XML gzip response') from exc
                if self.decoder.unused_data:
                    raise ValueError('trailing XML gzip data')
                self.pending = self.decoder.unconsumed_tail
            else:
                output, self.pending = chunk[:size], chunk[size:]
            self.decoded_size += len(output)
            if self.decoded_size > self.limit:
                raise ValueError('decoded XML response exceeds member bound')
            if output:
                return output

    def geturl(self):return self.response.geturl()
    def close(self):self.response.close()
    def __enter__(self):return self
    def __exit__(self,*args):self.close()


def safe_url(url: str):
    p = urlsplit(url)
    if p.scheme != "https" or p.username or p.password or p.port not in (None, 443):
        raise ValueError("only public HTTPS source URLs are allowed")
    if p.hostname not in HOSTS and not (p.hostname or "").endswith(".s3.amazonaws.com"):
        raise ValueError(f"source host is not approved: {p.hostname}")
    try:
        if not ipaddress.ip_address(p.hostname).is_global:
            raise ValueError("private address rejected")
    except ValueError as exc:
        if str(exc) == "private address rejected":
            raise


class ArchiveSeekReader:
    """Optional bounded seek adapter for imported gzip indexes.

    New 64 KiB blocks advance process-restorable whole-source hashes. Replayed
    blocks must match retained SHA-256 digests before bytes reach the decoder.
    A sparse index that needs more than 4 MiB of old compressed data fails
    closed instead of silently replaying the entire source. No raw disk cache.
    This adapter is not enabled by the production CLI yet.
    """
    BLOCK = 65536
    RETAIN_BLOCKS = 64

    def __init__(self, http, spec, *, state=None, max_reconnects=256):
        from .archive_hash import ArchiveHash, verify_native_hashes
        from .task import digest
        checksum = spec.get('checksum','')
        self.md5_expected = checksum.removeprefix('md5:') if checksum.startswith('md5:') else None
        self.url, self.total = spec['url'], spec.get('size_bytes')
        if not http._frozen_orcid_range(self.url,self.total,self.md5_expected):
            raise ValueError('seek requires exact frozen official annual source')
        if type(max_reconnects) is not int or not 1 <= max_reconnects <= 4096:
            raise ValueError('invalid archive seek reconnect bound')
        self.http, self.identity = http, digest(spec)
        self.response, self.wire_position = None, None
        self.position = self.highwater = self.reconnects = 0
        self.max_reconnects = max_reconnects
        self.closed, self.eof_checked = False, False
        self.cache_start, self.cache = None, b''
        self.headers, self.blocks = {}, {}
        verify_native_hashes()
        self.md5, self.sha256 = ArchiveHash('md5'), ArchiveHash('sha256')
        if state is not None:
            keys = {'schema','source','highwater','headers','blocks','md5','sha256','checksum'}
            if (not isinstance(state,dict) or set(state) != keys
                    or state['schema'] != 'archive-seek-state/v1'
                    or digest({k:v for k,v in state.items() if k != 'checksum'}) != state['checksum']
                    or state['source'] != self.identity):
                raise ValueError('archive seek state identity/checksum mismatch')
            high = state['highwater']
            if (type(high) is not int or not 0 < high <= self.total
                    or (high != self.total and high % self.BLOCK)):
                raise ValueError('archive seek highwater invalid')
            headers = state['headers']
            if (not isinstance(headers,dict) or set(headers)-{'etag','last-modified'}
                    or any(not isinstance(v,str) or len(v)>1024 for v in headers.values())):
                raise ValueError('archive seek validators invalid')
            blocks = state['blocks']
            last_start = ((high-1)//self.BLOCK)*self.BLOCK
            first_start = max(0,last_start-(self.RETAIN_BLOCKS-1)*self.BLOCK)
            starts = list(range(first_start,last_start+1,self.BLOCK))
            if not isinstance(blocks,list) or len(blocks) != len(starts):
                raise ValueError('archive seek replay window invalid')
            for start, row in zip(starts,blocks):
                if (not isinstance(row,list) or len(row)!=3 or row[0]!=start
                        or type(row[0]) is not int or type(row[1]) is not int
                        or row[1] != min(self.BLOCK,high-start)
                        or not isinstance(row[2],str) or not re.fullmatch('[0-9a-f]{64}',row[2])):
                    raise ValueError('archive seek replay block invalid')
                self.blocks[start] = (row[1],row[2])
            for name in ('md5','sha256'):
                restored = ArchiveHash.restore(state[name])
                if restored.algorithm != name or restored.size != high:
                    raise ValueError('archive seek hash position mismatch')
                setattr(self,name,restored)
            self.highwater, self.headers = high, dict(headers)

    def readable(self): return not self.closed
    def seekable(self): return not self.closed
    def tell(self): return self.position

    def seek(self, offset, whence=0):
        if self.closed: raise ValueError('archive seek reader closed')
        if type(offset) is not int or whence not in (0,1,2):
            raise ValueError('invalid archive seek')
        target = offset + (self.position if whence==1 else self.total if whence==2 else 0)
        if not 0 <= target <= self.total:
            raise ValueError('archive seek outside frozen length')
        self.position = target
        return target

    def _open_at(self, offset):
        if self.response is not None:
            self.response.close()
        self.response = None
        if offset:
            raw = self.http.open_range(self.url,offset,self.total,self.headers,
                                       expected_md5=self.md5_expected)
        else:
            raw = self.http.open(self.url,'application/octet-stream',headers={'Accept-Encoding':'identity'})
            try:
                if (int(raw.headers.get('Content-Length','-1')) != self.total
                        or raw.headers.get('Content-Encoding','identity') != 'identity'):
                    raise ValueError('archive seek initial representation mismatch')
                headers = {k.lower():v for k,v in raw.headers.items()
                           if k.lower() in ('etag','last-modified')}
                if self.highwater and any(headers.get(k)!=v for k,v in self.headers.items()):
                    raise ValueError('archive seek source validator changed')
                self.headers = headers
            except BaseException:
                raw.close()
                raise
        self.response, self.wire_position = raw, offset

    def _block(self, start):
        if start > self.highwater:
            raise ValueError('archive seek cannot skip unhashed bytes')
        if start < self.highwater and start not in self.blocks:
            raise ValueError('gzip index requires replay beyond bounded retained window')
        if self.response is None or self.wire_position != start:
            self._open_at(start)
        wanted = min(self.BLOCK,self.total-start)
        pending, stalled = bytearray(), 0
        while len(pending) < wanted:
            self.http.remaining()
            self.http.task.check_memory()
            try:
                chunk = self.response.read(wanted-len(pending))
                if not chunk:
                    raise EOFError('archive seek truncated block')
            except (OSError,EOFError):
                if self.reconnects >= self.max_reconnects or stalled >= 8:
                    raise
                self.reconnects += 1
                stalled += 1
                self._open_at(start+len(pending))
                continue
            if len(chunk)>wanted-len(pending):
                raise ValueError('archive seek oversized transport read')
            pending.extend(chunk)
            self.wire_position += len(chunk)
            stalled = 0
        block = bytes(pending)
        checksum = hashlib.sha256(block).hexdigest()
        if start < self.highwater:
            if self.blocks[start] != (len(block),checksum):
                raise ValueError('archive seek replay bytes changed')
        else:
            self.md5.update(block); self.sha256.update(block)
            self.highwater += len(block)
            self.blocks[start] = (len(block),checksum)
            while len(self.blocks)>self.RETAIN_BLOCKS:
                del self.blocks[min(self.blocks)]
        self.cache_start,self.cache = start,block

    def read(self, size=-1):
        if self.closed: raise ValueError('archive seek reader closed')
        if type(size) is not int or size < 0:
            raise ValueError('unbounded archive seek read forbidden')
        self.http.remaining()
        if size==0: return b''
        if self.position == self.total:
            if self.highwater != self.total:
                raise ValueError('archive seek EOF before complete checksum')
            if not self.eof_checked:
                if self.response is None or self.wire_position != self.total:
                    raise ValueError('archive seek EOF requires complete final block read')
                if self.response.read(1):
                    raise ValueError('archive seek source exceeds frozen length')
                self.eof_checked = True
            return b''
        start = self.position//self.BLOCK*self.BLOCK
        if self.cache_start != start:
            self._block(start)
        offset = self.position-start
        data = self.cache[offset:offset+min(size,self.BLOCK)]
        self.position += len(data)
        return data

    def snapshot(self):
        from .task import digest
        if not self.highwater:
            raise ValueError('empty archive seek checkpoint')
        value = {'schema':'archive-seek-state/v1','source':self.identity,
                 'highwater':self.highwater,'headers':dict(self.headers),
                 'blocks':[[k,*v] for k,v in sorted(self.blocks.items())],
                 'md5':self.md5.snapshot(),'sha256':self.sha256.snapshot()}
        return {**value,'checksum':digest(value)}

    def close(self):
        if self.response is not None: self.response.close()
        self.closed = True
        self.cache = b''


class PublicHTTP:
    def __init__(self, task, *, opener=None, sleep=time.sleep,reuse_connections=False):
        class PublicRedirect(HTTPRedirectHandler):
            def redirect_request(self, req, fp, code, msg, headers, newurl):
                try:
                    safe_url(newurl)
                except ValueError:
                    fp.close()
                    raise
                if urlsplit(req.full_url).hostname == 'pub.orcid.org':
                    # Do not silently follow another person's record or hide
                    # extra requests from the per-source quota reservation.
                    fp.close()
                    raise SourceRedirect(req.full_url, newurl, code)
                return super().redirect_request(req, fp, code, msg, headers, newurl)
        self.transport=None
        if opener is None and reuse_connections:
            from .persistent_http import SingleHTTPSHandler
            self.transport=SingleHTTPSHandler()
            opener=build_opener(PublicRedirect(),self.transport).open
        self.task, self.opener, self.sleep = task, opener or build_opener(PublicRedirect()).open, sleep
        self.robots = {}
        self.last_request = 0.0
        self.bytes_received = 0
        self.requests_count = 0
        self.deadline = None

    def close(self):
        if self.transport is not None:self.transport.close()

    @contextmanager
    def within(self, seconds):
        """Bound request/retry admission; an active socket still has its read timeout."""
        old=self.deadline
        proposed=time.monotonic()+max(0,seconds)
        self.deadline=min(old,proposed) if old is not None else proposed
        try:yield
        finally:self.deadline=old

    def remaining(self):
        if self.deadline is None:return self.task.config['http_timeout']
        remaining=self.deadline-time.monotonic()
        if remaining<=0:raise StageDeadline('source stage time exhausted; work remains pending')
        return min(self.task.config['http_timeout'],remaining)

    def wait(self,seconds):
        if self.deadline is not None and time.monotonic()+seconds>=self.deadline:
            raise StageDeadline('retry/rate-limit delay exceeds source stage budget')
        self.sleep(seconds)

    def _open(self, url, accept, *, headers=None, expected_status=200):
        safe_url(url)
        for attempt in range(self.task.config["max_retries"] + 1):
            self.remaining()
            wait = self.task.config["request_interval"] - (time.monotonic() - self.last_request)
            if wait > 0:
                self.wait(wait)
            self.last_request = time.monotonic()
            try:
                self.requests_count+=1
                r = self.opener(Request(url, headers={"User-Agent": USER_AGENT, "Accept": accept, **(headers or {})}), timeout=self.remaining())
                if r.status != expected_status:
                    code = r.status
                    r.close()
                    raise RuntimeError(f"source returned HTTP {code}; no usable body")
                safe_url(r.geturl())
                return r
            except HTTPError as error:
                code = error.code
                retry = error.headers.get("Retry-After", "")
                error.close()
                until=retry_time(retry,time.time())
                if code not in (429, 500, 502, 503, 504) or attempt == self.task.config["max_retries"]:
                    if code==429 or (retry and code in (500,502,503,504)):
                        raise SourcePause(code,until,'HTTP 429 retries exhausted; source paused') from error
                    raise
                delay = (int(retry) if retry.isdigit() else math.ceil(max(0,until-time.time()))) if retry else 2**attempt
                if delay>60 or (self.deadline is not None and time.monotonic()+delay>=self.deadline):
                    raise SourcePause(code,until,'server Retry-After exceeds source stage; source paused') from error
                self.wait(delay)
            except StageDeadline:
                raise
            except (TimeoutError, URLError, ConnectionError):
                self.remaining()
                if attempt == self.task.config['max_retries']:
                    raise
                self.wait(2**attempt)
        raise RuntimeError("source attempts exhausted")

    def allowed(self, url):
        p = urlsplit(url)
        origin = f"{p.scheme}://{p.netloc}"
        if origin not in self.robots:
            robot_url = origin + "/robots.txt"
            try:
                with SourceResponse(self._open(robot_url, "text/plain"), self.remaining) as r:
                    chunks, size = [], 0
                    while chunk := r.read(min(65536,262145-size)):
                        chunks.append(chunk)
                        size += len(chunk)
                        if size > 262144:break
                    body = b''.join(chunks)
                    if len(body) > 262144:
                        raise ValueError("robots file exceeds limit")
                    if "html" in r.headers.get("Content-Type", ""):
                        raise ValueError("robots returned HTML; access rules unresolved")
                rules = RobotFileParser()
                rules.parse(body.decode("utf-8", errors="replace").splitlines())
            except HTTPError as exc:
                # Figshare documents these frozen public download URLs as its
                # bulk-data interface. A 403 for robots.txt is an unavailable
                # robots resource (RFC 9309 2.3.1.3), not a file refusal. Keep
                # this permission URL-scoped: never cache an origin-wide allow.
                # A real file 401/403 still fails in _open without a retry.
                if exc.code == 403:
                    for spec in self.task.config.get('sources', {}).values():
                        if spec.get('kind') != 'orcid-summary-tar-gzip' or spec.get('url') != url:
                            continue
                        checksum = spec.get('checksum') or ''
                        md5 = checksum[4:] if isinstance(checksum, str) and checksum.startswith('md5:') else None
                        if self._frozen_orcid_range(url, spec.get('size_bytes'), md5):
                            return
                if exc.code != 404:
                    raise
                rules = RobotFileParser()
                rules.parse([])
            self.robots[origin] = rules
        rule = self.robots[origin]
        if not rule.can_fetch(USER_AGENT, url):
            raise PermissionError(f"robots disallows {url}")
        delay = rule.crawl_delay(USER_AGENT) or rule.crawl_delay("*") or 0
        if delay > 60:
            raise PermissionError("crawl delay exceeds bounded request schedule")
        if delay:
            self.wait(delay)

    def open(self, url, accept="application/octet-stream", *, headers=None):
        # These are documented programmatic interfaces, not website crawling.
        # Their API terms explicitly permit automated reads (ORCID: noncommercial).
        # Keep exact routes; never use this for pages, Figshare or auth/403 recovery.
        # https://info.orcid.org/public-client-terms-of-service/
        # https://www.wikidata.org/wiki/Wikidata:Data_access
        import re
        p = urlsplit(url)
        documented_api = (
            p.hostname == "pub.orcid.org" and re.fullmatch(r"/v3\.0/\d{4}-\d{4}-\d{4}-\d{3}[\dX]/(record|educations|employments)", p.path)
            or p.hostname == "www.wikidata.org" and re.fullmatch(r"/wiki/Special:EntityData/Q[1-9]\d*\.json", p.path)
            # Official Wikibase API: read-only bounded IDs, no search/write/auth.
            or entity_batch_route(p)
            or p.hostname == "query.wikidata.org" and p.path == "/sparql"
            # The official developer zone explicitly documents this free public API.
            or p.hostname == "api.nobelprize.org" and p.path == "/2.1/laureates"
        )
        if not documented_api:
            self.allowed(url)
        xml = documented_api and p.hostname == 'pub.orcid.org' and accept == 'application/vnd.orcid+xml'
        if xml:
            # Only negotiate HTTP encoding on the existing XML API route.
            # Archive/range bytes and JSON's independently bounded owner stay intact.
            headers = {'Accept-Encoding': 'gzip', **(headers or {})}
        response = SourceResponse(self._open(url, accept, headers=headers),self.remaining)
        if xml:
            def observe_wire(size):self.bytes_received += size
            return XMLResponse(response, self.task.config['max_member_bytes'], observe_wire)
        return response

    def _frozen_orcid_range(self, url, total, expected_md5):
        """Only the exact frozen official summary object may use this route."""
        p = urlsplit(url)
        official = (p.scheme == 'https' and not p.query and not p.fragment
                    and ((p.hostname == 'ndownloader.figshare.com' and re.fullmatch(r'/files/\d+', p.path))
                         or (p.hostname == 'orcid.figshare.com' and re.fullmatch(r'/ndownloader/files/\d+', p.path))))
        if (not official or type(total) is not int or total <= 0
                or not isinstance(expected_md5, str) or not re.fullmatch('[a-f0-9]{32}', expected_md5)):
            return False
        for spec in self.task.config.get('sources', {}).values():
            if (spec.get('kind') == 'orcid-summary-tar-gzip' and spec.get('url') == url
                    and spec.get('size_bytes') == total and spec.get('checksum') == 'md5:'+expected_md5):
                self.task.require_frozen()
                return True
        return False

    @contextmanager
    def open_archive(self, source, *, max_reconnects=256):
        """Keep one decoder/hash stream alive across bounded HTTP reconnects.

        This is not a persisted gzip seek index. Process exit still loses the
        decoder state. Non-pinned archive contracts retain their original path.
        """
        if type(max_reconnects) is not int or not 1 <= max_reconnects <= 4096:
            raise ValueError('total reconnect limit must be between 1 and 4096')
        spec = self.task.config['sources'][source]
        url, total = spec['url'], spec.get('size_bytes')
        checksum = spec.get('checksum') or ''
        md5 = checksum.removeprefix('md5:') if checksum.startswith('md5:') else None
        if not self._frozen_orcid_range(url, total, md5):
            with self.open(url) as raw:
                yield raw
            return
        from .dump_discovery import FeedReader
        raw = self.open(url, 'application/octet-stream', headers={'Accept-Encoding':'identity'})
        try:
            if (int(raw.headers.get('Content-Length', '-1')) != total
                    or raw.headers.get('Content-Encoding', 'identity') != 'identity'):
                raise ValueError('annual archive initial representation/length mismatch')
            initial = dict(raw.headers)
        except BaseException:
            raw.close()
            raise
        reader = FeedReader(raw, self.task, expected_bytes=total,
            resume=lambda offset:self.open_range(url, offset, total, initial, expected_md5=md5),
            max_reconnects=max_reconnects, verify_eof=True)
        try:
            yield reader
        finally:
            reader.close()

    def open_range(self, url, offset, total, initial_headers, *, expected_md5=None):
        """Resume a fixed official dump inside the SAME decompression process.

        A validator and strict Content-Range prevent concatenating a changed or
        ignored representation. Full upstream checksum remains mandatory.
        """
        p = urlsplit(url)
        from .easy_orcid_census import ARCHIVES
        fixed_career=(url,total,expected_md5) in ARCHIVES.values()
        fixed_annual=self._frozen_orcid_range(url,total,expected_md5)
        fixed_dump=(p.hostname == 'dumps.wikimedia.org' and not p.query and not p.fragment
                    and re.fullmatch(r'/wikidatawiki/entities/(\d{8})/wikidata-\1-all\.json\.(bz2|gz)', p.path))
        if not (fixed_dump or fixed_career or fixed_annual) or not 0 < offset < total:
            raise ValueError('range is restricted to fixed official dump offsets')
        normalized = {key.lower():value for key,value in initial_headers.items()}
        etag = normalized.get('etag', '')
        modified = normalized.get('last-modified', '')
        validator = etag if etag.startswith('"') and etag.endswith('"') else modified
        if not validator and not (fixed_career or fixed_annual):
            raise ValueError('dump resume requires a source identity validator')
        if validator and validator == modified:
            try:
                if parsedate_to_datetime(modified).tzinfo is None:
                    raise ValueError('missing timezone')
            except (ValueError, TypeError, OverflowError) as exc:
                raise ValueError('invalid dump Last-Modified validator') from exc
        self.allowed(url)
        # Zenodo exposes no ETag/Last-Modified here. Only the exact registered
        # URL, length and checksum may use this path. The census must verify
        # the WHOLE checksum before any provisional projections are promoted.
        headers={'Range':f'bytes={offset}-','Accept-Encoding':'identity'}
        if validator:headers['If-Range']=validator
        response = self._open(url, 'application/octet-stream', headers=headers, expected_status=206)
        try:
            match = re.fullmatch(r'bytes (\d+)-(\d+)/(\d+)', response.headers.get('Content-Range',''))
            if (not match or tuple(map(int,match.groups())) != (offset,total-1,total)
                    or int(response.headers.get('Content-Length','-1')) != total-offset):
                raise ValueError('dump resume Content-Range/length mismatch')
            if ((not fixed_annual and response.geturl() != url)
                    or response.headers.get('Content-Encoding','identity') != 'identity'):
                raise ValueError('dump resume representation changed')
            key = 'ETag' if validator == etag else 'Last-Modified'
            if validator and response.headers.get(key) != validator:
                raise ValueError('dump resume source validator changed')
            return SourceResponse(response,self.remaining)
        except BaseException:
            response.close()
            raise

    def json(self, url, limit=2*1024**2):
        if type(limit) is not int or not 0 < limit <= 2*1024**2:
            raise ValueError("invalid structured response bound")
        with self.open(url, "application/json", headers={'Accept-Encoding':'gzip'}) as response:
            encoding=response.headers.get('Content-Encoding','identity').strip().lower()
            if encoding not in ('identity','gzip'):
                raise ValueError('unsupported structured content encoding')
            decoder=zlib.decompressobj(31) if encoding=='gzip' else None
            chunks, size, wire_size = [], 0, 0
            while chunk := response.read(min(65536, limit + 1 - wire_size)):
                wire_size += len(chunk)
                self.bytes_received += len(chunk)
                if wire_size > limit:
                    raise ValueError("structured response exceeds bound")
                if decoder:
                    try:
                        chunk=decoder.decompress(chunk,limit+1-size)
                    except zlib.error as exc:
                        raise ValueError('invalid structured gzip response') from exc
                    if decoder.unused_data:
                        raise ValueError('trailing structured gzip data')
                chunks.append(chunk)
                size += len(chunk)
                if size > limit:
                    raise ValueError("structured response exceeds bound")
            if decoder and not decoder.eof:
                raise ValueError('truncated structured gzip response')
            body = b"".join(chunks)
        return json.loads(body), hashlib.sha256(body).hexdigest()
