"""Public-source HTTP: bounded reads, no auth bypass, robots and finite retries."""
from __future__ import annotations

import hashlib
import ipaddress
import json
import time
from urllib.error import HTTPError, URLError
from urllib.parse import urlsplit
from urllib.request import Request, urlopen, build_opener, HTTPRedirectHandler
from urllib.robotparser import RobotFileParser

USER_AGENT = "GAPI/0.2 (+https://github.com/global-academic-prestige-index; research)"
HOSTS = {"orcid.figshare.com", "ndownloader.figshare.com", "api.figshare.com", "s3-eu-west-1.amazonaws.com",
         "s3.amazonaws.com", "pub.orcid.org", "www.wikidata.org", "query.wikidata.org"}


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

    def _open(self, url, accept):
        safe_url(url)
        for attempt in range(self.task.config["max_retries"] + 1):
            wait = self.task.config["request_interval"] - (time.monotonic() - self.last_request)
            if wait > 0:
                self.sleep(wait)
            self.last_request = time.monotonic()
            try:
                r = self.opener(Request(url, headers={"User-Agent": USER_AGENT, "Accept": accept}), timeout=self.task.config["http_timeout"])
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
                if code not in (429, 500, 502, 503, 504) or attempt == self.task.config["max_retries"]:
                    raise
                if retry and not retry.isdigit():
                    raise RuntimeError("non-numeric Retry-After: pause instead of guessing")
                delay = int(retry) if retry else 2**attempt
                if delay > 60:
                    raise RuntimeError(f"source requested Retry-After={delay}; paused")
                self.sleep(delay)
            except (TimeoutError, URLError, ConnectionError):
                if attempt == self.task.config['max_retries']:
                    raise
                self.sleep(2**attempt)
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
            self.sleep(delay)

    def open(self, url, accept="application/octet-stream"):
        # These are documented programmatic interfaces, not website crawling.
        # Their API terms explicitly permit automated reads (ORCID: noncommercial).
        # Keep exact routes; never use this for pages, Figshare or auth/403 recovery.
        # https://info.orcid.org/public-client-terms-of-service/
        # https://www.wikidata.org/wiki/Wikidata:Data_access
        import re
        p = urlsplit(url)
        documented_api = (
            p.hostname == "pub.orcid.org" and re.fullmatch(r"/v3\.0/\d{4}-\d{4}-\d{4}-\d{3}[\dX]/record", p.path)
            or p.hostname == "www.wikidata.org" and re.fullmatch(r"/wiki/Special:EntityData/Q[1-9]\d*\.json", p.path)
            or p.hostname == "query.wikidata.org" and p.path == "/sparql"
        )
        if not documented_api:
            self.allowed(url)
        return self._open(url, accept)

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
