import json
import threading
import unittest
from concurrent.futures import ThreadPoolExecutor
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import List, Tuple
from urllib.parse import parse_qsl, urlparse

import pook  # type: ignore

from massive import RESTClient
from massive.rest.models.request import RequestOptionBuilder, X_MASSIVE_EDGE_ID


class EchoHandler(BaseHTTPRequestHandler):
    # keep-alive, so the client reuses pooled connections across threads
    protocol_version = "HTTP/1.1"

    def do_GET(self):
        url = urlparse(self.path)
        body = json.dumps(
            {
                "results": {
                    "path": url.path,
                    "query": dict(parse_qsl(url.query)),
                    "edge_id": self.headers.get(X_MASSIVE_EDGE_ID),
                    "authorization": self.headers.get("Authorization"),
                }
            }
        ).encode()
        with self.server.lock:  # type: ignore[attr-defined]
            self.server.count += 1  # type: ignore[attr-defined]
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format, *args):
        pass


class ConcurrencyTest(unittest.TestCase):
    """One RESTClient shared by many threads against a local HTTP server."""

    threads = 16
    calls = 200

    @classmethod
    def setUpClass(cls):
        # other suites leave pook intercepting urllib3; let requests through
        cls.pook_was_active = pook.isactive()
        pook.disable()

    @classmethod
    def tearDownClass(cls):
        if cls.pook_was_active:
            pook.activate()

    def setUp(self):
        httpd = ThreadingHTTPServer(("127.0.0.1", 0), EchoHandler)
        httpd.daemon_threads = True
        httpd.lock = threading.Lock()  # type: ignore[attr-defined]
        httpd.count = 0  # type: ignore[attr-defined]
        thread = threading.Thread(target=httpd.serve_forever, daemon=True)
        thread.start()
        self.addCleanup(thread.join)
        self.addCleanup(httpd.server_close)
        self.addCleanup(httpd.shutdown)
        self.httpd = httpd
        host, port = httpd.server_address[:2]
        self.base = f"http://{host}:{port}"

    def test_shared_client(self):
        c = RESTClient(
            "key", base=self.base, retries=0, maxsize=self.threads, pagination=False
        )
        headers_before = dict(c.headers)
        barrier = threading.Barrier(self.threads, timeout=10)

        def run(t: int) -> List[Tuple[dict, dict]]:
            barrier.wait()
            results = []
            for i in range(self.calls):
                expected = {
                    "path": f"/v1/thread/{t}/call/{i}",
                    "query": {"t": str(t), "i": str(i)},
                    "edge_id": f"edge-{t}-{i}" if i % 2 else None,
                    "authorization": "Bearer key",
                }
                options = (
                    RequestOptionBuilder(edge_id=f"edge-{t}-{i}", edge_ip_address="ip")
                    if i % 2
                    else None
                )
                got = c._get(
                    path=expected["path"],
                    params={"t": t, "i": i},
                    result_key="results",
                    options=options,
                )
                results.append((expected, got))
            return results

        with ThreadPoolExecutor(self.threads) as pool:
            futures = [pool.submit(run, t) for t in range(self.threads)]
            results = [r for f in futures for r in f.result()]

        self.assertEqual(len(results), self.threads * self.calls)
        for expected, got in results:
            self.assertEqual(got, expected)
        self.assertEqual(self.httpd.count, self.threads * self.calls)  # type: ignore[attr-defined]
        # per-request options must not leak into the shared default headers
        self.assertEqual(c.headers, headers_before)


if __name__ == "__main__":
    unittest.main()
