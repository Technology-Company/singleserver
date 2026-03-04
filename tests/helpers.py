"""
Helper utilities for tests.
"""

import http.server
import json
import os
import socketserver
import sys
import time


class SimpleTestHandler(http.server.BaseHTTPRequestHandler):
    """Simple HTTP handler for testing."""

    def log_message(self, format, *args):
        # Suppress logging in tests
        pass

    def do_GET(self):
        if self.path == "/":
            self.send_response(200)
            self.send_header("Content-Type", "text/plain")
            self.end_headers()
            self.wfile.write(b"OK")
        elif self.path == "/health":
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps({"status": "healthy", "pid": os.getpid()}).encode())
        elif self.path == "/slow":
            time.sleep(2)
            self.send_response(200)
            self.send_header("Content-Type", "text/plain")
            self.end_headers()
            self.wfile.write(b"SLOW OK")
        elif self.path == "/pid":
            self.send_response(200)
            self.send_header("Content-Type", "text/plain")
            self.end_headers()
            self.wfile.write(str(os.getpid()).encode())
        elif self.path == "/shutdown":
            self.send_response(200)
            self.send_header("Content-Type", "text/plain")
            self.end_headers()
            self.wfile.write(b"Shutting down")
            # Schedule shutdown after response
            import threading

            threading.Thread(target=lambda: (time.sleep(0.1), os._exit(0)), daemon=True).start()
        else:
            self.send_response(404)
            self.send_header("Content-Type", "text/plain")
            self.end_headers()
            self.wfile.write(b"Not Found")


def run_test_server(port: int, startup_delay: float = 0):
    """Run a simple test HTTP server."""
    if startup_delay > 0:
        time.sleep(startup_delay)

    socketserver.TCPServer.allow_reuse_address = True
    with socketserver.TCPServer(("127.0.0.1", port), SimpleTestHandler) as httpd:
        httpd.serve_forever()


if __name__ == "__main__":
    # Usage: python helpers.py <port> [startup_delay]
    port = int(sys.argv[1])
    startup_delay = float(sys.argv[2]) if len(sys.argv) > 2 else 0
    run_test_server(port, startup_delay)
