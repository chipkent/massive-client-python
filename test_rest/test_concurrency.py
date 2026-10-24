import json
import threading
import unittest
from concurrent.futures import ThreadPoolExecutor
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import parse_qsl, urlparse

from massive import RESTClient
from massive.rest.models.request import (
    RequestOptionBuilder,
    X_MASSIVE_EDGE_ID,
    X_MASSIVE_EDGE_IP_ADDRESS,
)


class EchoServer(ThreadingHTTPServer):
    """Answers every GET with what it received.

    It counts the requests and records the most requests it had in flight at
    once, which shows whether the client's threads really overlapped.
    """

    daemon_threads = True

    def __init__(self):
        super().__init__(("127.0.0.1", 0), EchoHandler)
        self.lock = threading.Lock()
        self.count = 0
        self.in_flight = 0
        self.peak_in_flight = 0


class EchoHandler(BaseHTTPRequestHandler):
    # keep-alive, so the client reuses pooled connections across threads
    protocol_version = "HTTP/1.1"
    server: EchoServer

    def do_GET(self):
        with self.server.lock:
            self.server.count += 1
            self.server.in_flight += 1
            self.server.peak_in_flight = max(
                self.server.peak_in_flight, self.server.in_flight
            )
        try:
            url = urlparse(self.path)
            body = json.dumps(
                {
                    "path": url.path,
                    "query": dict(parse_qsl(url.query)),
                    "edge_id": self.headers.get(X_MASSIVE_EDGE_ID),
                    "edge_ip": self.headers.get(X_MASSIVE_EDGE_IP_ADDRESS),
                    "authorization": self.headers.get("Authorization"),
                }
            ).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        finally:
            with self.server.lock:
                self.server.in_flight -= 1

    def log_message(self, format, *args):
        pass


class ConcurrencyTest(unittest.TestCase):
    """One RESTClient shared by many threads against a local HTTP server."""

    threads = 16
    calls = 200

    def setUp(self):
        self.httpd = EchoServer()
        thread = threading.Thread(target=self.httpd.serve_forever, daemon=True)
        thread.start()
        self.addCleanup(thread.join)
        self.addCleanup(self.httpd.server_close)
        self.addCleanup(self.httpd.shutdown)
        host, port = self.httpd.server_address[:2]
        self.base = f"http://{host!s}:{port}"

    def test_shared_client(self):
        c = RESTClient("key", base=self.base, retries=0, maxsize=self.threads)
        headers_before = list(c.headers.items())
        barrier = threading.Barrier(self.threads, timeout=10)

        def run(t: int) -> List[Tuple[Dict[str, Any], Dict[str, Any]]]:
            # start every thread together, so their requests overlap
            barrier.wait()
            results = []
            for i in range(self.calls):
                ticker = f"T{t}N{i}"
                date = f"2024-01-{t + 1:02}"
                edge_id: Optional[str] = None
                edge_ip: Optional[str] = None
                if i % 2:
                    # Odd calls send per-request headers. They go through _get,
                    # the method every endpoint calls, because the endpoint
                    # methods also copy `options` into the query string.
                    edge_id, edge_ip = f"edge-{t}-{i}", f"10.0.{t}.{i % 256}"
                    resp = c._get(
                        path=f"/v3/reference/tickers/{ticker}",
                        params={"ticker": ticker, "date": date},
                        raw=True,
                        options=RequestOptionBuilder(
                            edge_id=edge_id, edge_ip_address=edge_ip
                        ),
                    )
                else:
                    # raw=True returns the response, so the echoed request can
                    # be checked instead of a deserialized TickerDetails
                    resp = c.get_ticker_details(ticker, date=date, raw=True)
                expected = {
                    "path": f"/v3/reference/tickers/{ticker}",
                    "query": {"ticker": ticker, "date": date},
                    "edge_id": edge_id,
                    "edge_ip": edge_ip,
                    "authorization": "Bearer key",
                }
                results.append((expected, json.loads(resp.data)))
            return results

        with ThreadPoolExecutor(self.threads) as pool:
            futures = [pool.submit(run, t) for t in range(self.threads)]
            results = [r for f in futures for r in f.result()]

        # every response answers its own request: the path and query name the
        # thread and call, and the edge headers are the ones that call sent
        self.assertEqual(len(results), self.threads * self.calls)
        for expected, got in results:
            self.assertEqual(got, expected)
        self.assertEqual(self.httpd.count, self.threads * self.calls)
        # the threads really shared the client at the same time
        self.assertGreater(self.httpd.peak_in_flight, 1)
        # per-request headers must not leak into the client's default headers:
        # same keys, same values, same order
        self.assertEqual(list(c.headers.items()), headers_before)


if __name__ == "__main__":
    unittest.main()
