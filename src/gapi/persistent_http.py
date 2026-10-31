"""One reusable HTTPS connection, underneath urllib's existing safety handlers.

No hidden retry, parallel stream, alternate proxy or TLS exception. Unread bodies
discard their connection so the next response cannot consume predecessor bytes.
"""
import http.client
from urllib.error import URLError
from urllib.request import HTTPSHandler


def read_chunk(response, size):
    """Return available HTTP body bytes without filling the requested buffer.

    HTTPResponse.read(size) can perform many socket reads before returning,
    hiding a slow trickle from the caller's deadline check. read1 returns after
    one buffered body read; protocol framing and a blocked socket still obey
    the transport timeout, so this is not a hard total wall-clock deadline.
    Keep ordinary file-like/custom adapters on their existing read contract.
    """
    if isinstance(response, http.client.HTTPResponse):
        return response.read1(size)
    return response.read(size)


class ManagedResponse:
    def __init__(self,response,owner):
        self.response,self.owner=response,owner
        self.closed=False

    def __getattr__(self,name):return getattr(self.response,name)

    def read(self,size=-1):
        if self.closed:raise ValueError('source response is closed')
        # urllib consumes redirect/error bodies with read() before close().
        # It does not need an unbounded body to process the status/Location.
        size=65536 if size is None or size<0 else min(size,65536)
        try:return read_chunk(self.response,size)
        except BaseException:
            self.owner.close();raise

    def close(self):
        if self.closed:return
        self.closed=True
        try:
            if self.response.fp is not None or self.response.will_close:
                self.owner.close()
        finally:self.response.close()

    def __enter__(self):return self
    def __exit__(self,*args):self.close()


class SingleHTTPSHandler(HTTPSHandler):
    def __init__(self,*,connection_factory=http.client.HTTPSConnection):
        super().__init__()
        self.factory=connection_factory
        self.connection=None;self.key=None;self.response=None
        self.opened_connections=0;self.reused_connections=0

    def close(self):
        connection=self.connection
        self.connection=None;self.key=None;self.response=None
        if connection is not None:connection.close()

    def https_open(self,req):
        if req.get_method()!='GET' or req.data is not None:
            raise ValueError('public persistent transport permits GET only')
        if self.response is not None and not self.response.closed:
            raise RuntimeError('previous source response must be closed before another request')
        headers=dict(req.unredirected_hdrs)
        headers.update({k:v for k,v in req.headers.items() if k not in headers})
        headers={k.title():v for k,v in headers.items()}
        tunnel_headers={}
        if req._tunnel_host and 'Proxy-Authorization' in headers:
            tunnel_headers['Proxy-Authorization']=headers.pop('Proxy-Authorization')
        # Proxy identity and tunnel target are part of the reuse boundary.
        key=(req.host,req._tunnel_host,tuple(sorted(tunnel_headers.items())))
        if self.key!=key:self.close()
        if self.connection is None:
            self.connection=self.factory(req.host,timeout=req.timeout,context=self._context)
            if req._tunnel_host:self.connection.set_tunnel(req._tunnel_host,headers=tunnel_headers)
            self.key=key;self.opened_connections+=1
        else:
            self.reused_connections+=1
            self.connection.timeout=req.timeout
            if self.connection.sock is not None:self.connection.sock.settimeout(req.timeout)
        headers['Connection']='keep-alive'
        try:
            self.connection.request('GET',req.selector,None,headers)
            response=self.connection.getresponse()
        except (OSError,http.client.HTTPException) as exc:
            self.close();raise URLError(exc) from exc
        response.url=req.get_full_url();response.msg=response.reason
        self.response=ManagedResponse(response,self)
        return self.response
