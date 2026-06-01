#!/usr/bin/env python3
"""
TCP Server for Text Reverse Service
==============================
Protocol:
  - Initialization (Type=1): client->server, carries N (number of chunks)
  - agree (Type=2):           server->client, acknowledges initialization
  - reverseRequest (Type=3):  client->server, carries data to reverse
  - reverseAnswer (Type=4):   server->client, carries reversed data

Usage:
  python reversetcpserver.py <port>
"""

import socket
import struct
import threading
import sys
from datetime import datetime

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
_log_lock = threading.Lock()


def _log(msg: str) -> None:
    """Thread-safe logging to both console and run_log.txt."""
    ts = datetime.now().strftime('%H:%M:%S.%f')
    with _log_lock:
        print(f'[{ts}] {msg}')
        with open('run_log.txt', 'a', encoding='utf-8') as f:
            f.write(f'[{ts}] {msg}\n')


# ---------------------------------------------------------------------------
# TCP helpers
# ---------------------------------------------------------------------------
def recv_exactly(conn: socket.socket, size: int) -> bytes:
    """Read exactly *size* bytes from the TCP stream (handles fragmentation)."""
    buf = bytearray()
    while len(buf) < size:
        chunk = conn.recv(size - len(buf))
        if not chunk:
            raise ConnectionError('Client disconnected (received 0 bytes)')
        buf.extend(chunk)
    return bytes(buf)


# ---------------------------------------------------------------------------
# Client handler (runs in a dedicated thread)
# ---------------------------------------------------------------------------
def handle_client(conn: socket.socket, addr: tuple) -> None:
    """Process a single client: read chunks, reverse, send back."""
    cid = f'{addr[0]}:{addr[1]}'
    _log(f'[{cid}] New connection')

    try:
        # ---- 1. Receive Initialization (Type=1, N=4B) --------------------
        raw = recv_exactly(conn, 6)          # Type(2) + N(4)
        ptype, n = struct.unpack('!HI', raw)
        if ptype != 1:
            _log(f'[{cid}] Expected Initialization(Type=1), got Type={ptype}, closing')
            return
        _log(f'[{cid}] [RECV] Initialization: N={n}')

        # ---- 2. Send agree (Type=2) ------------------------------------
        conn.sendall(struct.pack('!H', 2))
        _log(f'[{cid}] [SEND] agree')

        # ---- 3. Process N reverse-request chunks ------------------------
        for i in range(n):
            # Receive reverseRequest header: Type(2) + Length(4)
            header = recv_exactly(conn, 6)
            ptype, length = struct.unpack('!HI', header)
            if ptype != 3:
                _log(f'[{cid}] Expected reverseRequest(Type=3), got Type={ptype}, aborting')
                break

            # Receive actual data
            data = recv_exactly(conn, length)
            _log(f'[{cid}] [RECV] reverseRequest chunk={i+1}, length={length}')

            # Reverse
            revdata = data[::-1]

            # Send reverseAnswer: Type(4) + Length(4) + reversed data
            conn.sendall(struct.pack('!HI', 4, len(revdata)) + revdata)
            _log(f'[{cid}] [SEND] reverseAnswer chunk={i+1}, length={len(revdata)}')

    except ConnectionError as e:
        _log(f'[{cid}] Connection lost: {e}')
    except Exception as e:
        _log(f'[{cid}] Unhandled error: {e}')
    finally:
        conn.close()
        _log(f'[{cid}] Connection closed')


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
def main() -> None:
    if len(sys.argv) != 2:
        print('Usage: python reversetcpserver.py <port>')
        sys.exit(1)

    port = int(sys.argv[1])

    # Fresh log file
    with open('run_log.txt', 'w', encoding='utf-8') as f:
        f.write('')

    server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server_sock.bind(('0.0.0.0', port))
    server_sock.listen(5)
    _log(f'Server listening on 0.0.0.0:{port}')

    try:
        while True:
            conn, addr = server_sock.accept()
            t = threading.Thread(target=handle_client, args=(conn, addr), daemon=True)
            t.start()
    except KeyboardInterrupt:
        _log('Server shutting down (Ctrl+C)')
    finally:
        server_sock.close()
        _log('Server socket closed')


if __name__ == '__main__':
    main()
