import json
import threading
import unittest
from concurrent.futures import ThreadPoolExecutor
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from massive import RESTClient
from massive.rest.models.request import RequestOptionBuilder, X_MASSIVE_EDGE_ID


class EchoServer(ThreadingHTTPServer):
    "Echoes each request back, and records the most requests in flight at once."

    daemon_threads = True

    def __init__(self):
        super().__init__(("127.0.0.1", 0), EchoHandler)
        self.lock = threading.Lock()
        self.in_flight = 0
        self.peak = 0


class EchoHandler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"  # keep-alive, so pooled connections are reused
    server: EchoServer

    def do_GET(self):
        with self.server.lock:
            self.server.in_flight += 1
            self.server.peak = max(self.server.peak, self.server.in_flight)
        body = json.dumps([self.path, self.headers.get(X_MASSIVE_EDGE_ID)]).encode()
        self.send_response(200)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)
        with self.server.lock:
            self.server.in_flight -= 1

    def log_message(self, format, *args):
        pass


class ConcurrencyTest(unittest.TestCase):
    threads = 16
    calls = 64

    def test_shared_client(self):
        httpd = EchoServer()
        threading.Thread(target=httpd.serve_forever, daemon=True).start()
        self.addCleanup(httpd.server_close)
        self.addCleanup(httpd.shutdown)
        host, port = httpd.server_address[:2]
        c = RESTClient("key", base=f"http://{host!s}:{port}", maxsize=self.threads)
        headers_before = list(c.headers.items())
        barrier = threading.Barrier(self.threads, timeout=10)

        def run(t):
            barrier.wait()  # start together, so the requests overlap
            for i in range(self.calls):
                path = f"/v3/reference/tickers/T{t}N{i}"
                # odd calls send per-request headers, even calls send none
                edge_id = f"edge-{t}-{i}" if i % 2 else None
                options = RequestOptionBuilder(edge_id, "10.0.0.1") if edge_id else None
                # _get, which every endpoint calls; endpoint methods currently
                # also copy `options` into the query string
                resp = c._get(path=path, params={"n": i}, raw=True, options=options)
                # the response answers this call's request, not another thread's
                self.assertEqual(json.loads(resp.data), [f"{path}?n={i}", edge_id])

        with ThreadPoolExecutor(self.threads) as pool:
            for f in [pool.submit(run, t) for t in range(self.threads)]:
                f.result()

        self.assertGreater(httpd.peak, 1)  # the threads really overlapped
        # per-request headers never leak into the client's default headers
        self.assertEqual(list(c.headers.items()), headers_before)
