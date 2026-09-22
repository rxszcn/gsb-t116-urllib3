"""同一份响应在不同管道下 geturl() 给什么，把服务器上真正收到的请求行也记下来。

跑法：env -u http_proxy -u https_proxy -u no_proxy PYTHONPATH=src .venv/bin/python repro/url-provenance.py
"""

from __future__ import annotations

import socket
import threading
import urllib.parse

from urllib3 import HTTPConnectionPool, PoolManager
from urllib3.connection import HTTPConnection
from urllib3.util.retry import Retry

socket.setdefaulttimeout(10.0)
SEP = b"\r\n\r\n"


def ok(note: str = "") -> bytes:
    body = note.encode()
    return (
        b"HTTP/1.1 200 OK\r\ncontent-length: "
        + str(len(body)).encode()
        + b"\r\nconnection: keep-alive\r\n\r\n"
        + body
    )


def redirect(status: int, location: str) -> bytes:
    return (
        f"HTTP/1.1 {status} X\r\nlocation: {location}\r\ncontent-length: 0\r\n"
        "connection: keep-alive\r\n\r\n"
    ).encode()


def nostatus(status: int) -> bytes:
    return (
        f"HTTP/1.1 {status} X\r\ncontent-length: 0\r\nconnection: keep-alive\r\n\r\n"
    ).encode()


class Server:
    def __init__(self) -> None:
        self.lsock = socket.socket()
        self.lsock.bind(("127.0.0.1", 0))
        self.lsock.listen(16)
        self.port = self.lsock.getsockname()[1]
        self.hits: dict[str, int] = {}
        self.seen: list[str] = []
        self.drop_next = False

    def start(self) -> "Server":
        threading.Thread(target=self.serve, daemon=True).start()
        return self

    def reply(self, path: str) -> bytes:
        self.hits[path] = self.hits.get(path, 0) + 1
        n = self.hits[path]
        return {
            "/direct": lambda: ok("direct"),
            "/flaky": lambda: ok("flaky") if n >= 2 else nostatus(503),
            "/rel": lambda: redirect(302, "/direct"),
            "/abs": lambda: redirect(302, f"http://127.0.0.1:{self.port}/direct"),
            "/qrel": lambda: redirect(302, "/direct?x=1&y=2"),
            "/frag": lambda: redirect(302, "/direct#top"),
            "/chain1": lambda: redirect(302, "/rel"),
            "/to303": lambda: redirect(303, "/direct"),
            "/fq": lambda: redirect(302, "/direct?q=1#frag"),
            # announces 10 bytes, sends 5, hangs up: forces a read-error retry
            "/trunc": lambda: (
                b"HTTP/1.1 200 OK\r\ncontent-length: 10\r\n"
                b"connection: keep-alive\r\n\r\nabcde"
                if n == 1
                else ok("whole")
            ),
        }.get(path, lambda: ok("fallback"))()

    def serve(self) -> None:
        while True:
            try:
                conn, _ = self.lsock.accept()
            except OSError:
                return
            threading.Thread(target=self.conn, args=(conn,), daemon=True).start()

    def conn(self, conn: socket.socket) -> None:
        conn.settimeout(8.0)
        buf = b""
        with conn:
            if self.drop_next:
                self.drop_next = False
                conn.sendall(b"HTTP/1.1 200 OK\r\n")
                return
            while True:
                try:
                    while SEP not in buf:
                        data = conn.recv(65536)
                        if not data:
                            return
                        buf += data
                except OSError:
                    return
                head, _, buf = buf.partition(SEP)
                request_line = head.split(b"\r\n")[0].decode("latin-1")
                self.seen.append(request_line.split(" ")[1])
                path = urllib.parse.urlsplit(request_line.split(" ")[1]).path
                reply = self.reply(path)
                conn.sendall(reply)
                if path == "/trunc" and b"content-length: 10" in reply:
                    return  # hang up mid-body so the client must retry


def hist_last(resp) -> str:
    retries = getattr(resp, "retries", None)
    if not retries or not retries.history:
        return "history=none"
    h = retries.history[-1]
    return f"history[-1]=(status={h.status},loc={h.redirect_location!r})"


def main() -> None:
    srv = Server().start()
    base = f"http://127.0.0.1:{srv.port}"
    results: list[tuple[str, object]] = []

    def row(label: str, want: str | None, fn) -> None:
        srv.seen.clear()
        srv.hits.clear()
        try:
            status, got, url, last = fn()
        except Exception as e:  # noqa: BLE001
            print(f"  {label:24} RAISED {type(e).__name__}: {e}")
            return
        mark = "MATCH" if got == want else "DIFFER"
        results.append((label, got))
        print(f"  {label:24} status={status} geturl={got!r} url={url!r}")
        print(f"  {'':24} want={want!r} -> {mark}")
        print(f"  {'':24} wire={srv.seen}")
        print(f"  {'':24} {last}")

    def l1(path: str) -> tuple:
        conn = HTTPConnection("127.0.0.1", srv.port)
        conn.request("GET", path)
        resp = conn.getresponse()
        resp.read()
        out = (resp.status, resp.geturl(), resp.url, hist_last(resp))
        conn.close()
        return out

    def l2(path: str, retries=None, redirect_: bool = True) -> tuple:
        with HTTPConnectionPool("127.0.0.1", srv.port) as pool:
            resp = pool.request("GET", path, retries=retries, redirect=redirect_)
        return (resp.status, resp.geturl(), resp.url, hist_last(resp))

    def l3(path: str, retries=None, redirect_: bool = True) -> tuple:
        http = PoolManager()
        resp = http.request(
            "GET",
            f"{base}{path}",
            retries=retries,
            redirect=redirect_,
        )
        out = (resp.status, resp.geturl(), resp.url, hist_last(resp))
        resp.release_conn()
        http.clear()
        return out

    print("== L1 HTTPConnection ==")
    row("L1 direct", "/direct", lambda: l1("/direct"))
    row("L1 redir_rel", "/direct", lambda: l1("/rel"))

    print("== L2 HTTPConnectionPool ==")
    row("L2 direct", "/direct", lambda: l2("/direct"))
    row("L2 status_retry", f"{base}/flaky", lambda: l2("/flaky", Retry(total=3)))
    row(
        "L2 status_forcelist",
        "/flaky",
        lambda: l2(
            "/flaky",
            Retry(total=3, status_forcelist=[503], backoff_factor=0.0),
        ),
    )
    row("L2 read_retry", "/direct", lambda: l2("/trunc", Retry(total=3)))
    row("L2 redir_frag_query", "/direct?q=1", lambda: l2("/fq"))
    srv.drop_next = True
    row("L2 conn_retry", f"{base}/direct", lambda: l2("/direct", Retry(total=3)))
    row("L2 redir_rel", "/direct", lambda: l2("/rel"))
    row("L2 redir_abs", f"{base}/direct", lambda: l2("/abs"))
    row("L2 redir_query", "/direct?x=1&y=2", lambda: l2("/qrel"))
    row("L2 redir_frag", "/direct", lambda: l2("/frag"))
    row("L2 redir_chain", "/direct", lambda: l2("/chain1"))
    row("L2 303", "/direct", lambda: l2("/to303"))
    row("L2 no_redirect(302)", "/rel", lambda: l2("/rel", redirect_=False))
    row("L2 redir_after_retry", "/direct", lambda: l2("/rel", Retry(total=3)))

    print("== L3 PoolManager ==")
    row("L3 direct", f"{base}/direct", lambda: l3("/direct"))
    row("L3 redir_rel", f"{base}/direct", lambda: l3("/rel"))
    row("L3 redir_abs", f"{base}/direct", lambda: l3("/abs"))
    row("L3 redir_query", f"{base}/direct?x=1&y=2", lambda: l3("/qrel"))
    row("L3 no_redirect(302)", f"{base}/rel", lambda: l3("/rel", redirect_=False))
    row("L3 status_retry", f"{base}/flaky", lambda: l3("/flaky", Retry(total=3)))

    print("== summary of geturl() values ==")
    for label, got in results:
        print(f"  {label:24} {got!r}")


if __name__ == "__main__":
    main()
