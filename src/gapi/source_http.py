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
            return self.response.read(min(size,65536))
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
                with self._open(robot_url, "text/plain") as r:
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

    def open_range(self, url, offset, total, initial_headers, *, expected_md5=None):
        """Resume a fixed official dump inside the SAME decompression process.

        A validator and strict Content-Range prevent concatenating a changed or
        ignored representation. Full upstream checksum remains mandatory.
        """
        p = urlsplit(url)
        from .easy_orcid_census import ARCHIVES
        fixed_career=(url,total,expected_md5) in ARCHIVES.values()
        fixed_dump=(p.hostname == 'dumps.wikimedia.org' and not p.query and not p.fragment
                    and re.fullmatch(r'/wikidatawiki/entities/(\d{8})/wikidata-\1-all\.json\.(bz2|gz)', p.path))
        if not (fixed_dump or fixed_career) or not 0 < offset < total:
            raise ValueError('range is restricted to fixed official dump offsets')
        etag = initial_headers.get('ETag', '')
        modified = initial_headers.get('Last-Modified', '')
        validator = etag if etag.startswith('"') and etag.endswith('"') else modified
        if not validator and not fixed_career:
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
            if response.geturl() != url or response.headers.get('Content-Encoding','identity') != 'identity':
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
