"""Public-source HTTP: bounded reads, no auth bypass, robots and finite retries."""
from __future__ import annotations

import hashlib
import ipaddress
import json
import math
import time
from contextlib import contextmanager
from email.utils import parsedate_to_datetime
from http.client import HTTPException
from urllib.error import HTTPError, URLError
from urllib.parse import urlsplit
from urllib.request import Request, urlopen, build_opener, HTTPRedirectHandler
from urllib.robotparser import RobotFileParser

USER_AGENT = "GAPI/0.2 (+https://github.com/Northern-Summer/global-academic-prestige-index; research)"
# Official anonymous quota, checked 2026-10-29:
# https://info.orcid.org/ufaqs/what-are-the-api-limits/
ORCID_ANONYMOUS_DAILY_READS = 25000
HOSTS = {"orcid.figshare.com", "ndownloader.figshare.com", "api.figshare.com", "s3-eu-west-1.amazonaws.com",
         "s3.amazonaws.com", "pub.orcid.org", "www.wikidata.org", "query.wikidata.org", "dumps.wikimedia.org",
         "www.tamucc.edu", "api.openalex.org", "api.crossref.org"}


class SourcePause(RuntimeError):
    """A server backoff belongs to the whole source, across member/run boundaries."""
    def __init__(self,code,retry_at,reason):
        self.code,self.retry_at=code,retry_at
        super().__init__(reason)


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
        except HTTPException as exc:
            raise OSError('source body transport failed: '+type(exc).__name__) from exc

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
    def __init__(self, task, *, opener=None, sleep=time.sleep):
        class PublicRedirect(HTTPRedirectHandler):
            def redirect_request(self, req, fp, code, msg, headers, newurl):
                safe_url(newurl)
                return super().redirect_request(req, fp, code, msg, headers, newurl)
        self.task, self.opener, self.sleep = task, opener or build_opener(PublicRedirect()).open, sleep
        self.robots = {}
        self.last_request = 0.0
        self.bytes_received = 0
        self.requests_count = 0
        self.deadline = None

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
        if remaining<=0:raise TimeoutError('source stage time exhausted; work remains pending')
        return min(self.task.config['http_timeout'],remaining)

    def wait(self,seconds):
        if self.deadline is not None and time.monotonic()+seconds>=self.deadline:
            raise TimeoutError('retry/rate-limit delay exceeds source stage budget')
        self.sleep(seconds)

    def _open(self, url, accept):
        safe_url(url)
        for attempt in range(self.task.config["max_retries"] + 1):
            self.remaining()
            wait = self.task.config["request_interval"] - (time.monotonic() - self.last_request)
            if wait > 0:
                self.wait(wait)
            self.last_request = time.monotonic()
            try:
                self.requests_count+=1
                r = self.opener(Request(url, headers={"User-Agent": USER_AGENT, "Accept": accept}), timeout=self.remaining())
                if r.status != 200:
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
            except (TimeoutError, URLError, ConnectionError):
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
                    body = r.read(262145)
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

    def open(self, url, accept="application/octet-stream"):
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
            or p.hostname == "query.wikidata.org" and p.path == "/sparql"
        )
        if not documented_api:
            self.allowed(url)
        return SourceResponse(self._open(url, accept),self.remaining)

    def json(self, url, limit=2*1024**2):
        with self.open(url, "application/json") as response:
            chunks, size = [], 0
            while chunk := response.read(min(65536, limit + 1 - size)):
                chunks.append(chunk)
                size += len(chunk)
                self.bytes_received += len(chunk)
                if size > limit:
                    raise ValueError("structured response exceeds bound")
            body = b"".join(chunks)
        return json.loads(body), hashlib.sha256(body).hexdigest()
