#!/usr/bin/env python3
"""
TCP Client for Text Reverse Service
==============================
Usage:
  python reversetcpclient.py <serverIP> <serverPort> <file> <Lmin> <Lmax> <chunk_seed>

Example:
  python reversetcpclient.py 127.0.0.1 8888 input.txt 50 100 42
"""

import socket
import struct
import random
import sys
from datetime import datetime

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
def _log(msg: str) -> None:
    """Log to both console and run_log.txt."""
    ts = datetime.now().strftime('%H:%M:%S.%f')
    print(f'[{ts}] {msg}')
    with open('run_log.txt', 'a', encoding='utf-8') as f:
        f.write(f'[{ts}] {msg}\n')


# ---------------------------------------------------------------------------
# TCP helpers
# ---------------------------------------------------------------------------
def recv_exactly(sock: socket.socket, size: int) -> bytes:
    """Read exactly *size* bytes from the TCP stream (handles fragmentation)."""
    buf = bytearray()
    while len(buf) < size:
        chunk = sock.recv(size - len(buf))
        if not chunk:
            raise ConnectionError('Server disconnected (received 0 bytes)')
        buf.extend(chunk)
    return bytes(buf)


# ---------------------------------------------------------------------------
# Chunking logic  ←  Pay attention – this is what the oral exam will ask about
# ---------------------------------------------------------------------------
def chunk_file(content: bytes, lmin: int, lmax: int, seed: int) -> list[bytes]:
    """
    Split *content* into variable-length chunks using a seeded PRNG.

    Algorithm
    ---------
    1. seed(seed) so the random sequence is reproducible.
    2. Repeatedly draw chunk_len ∈ [lmin, lmax].
    3. When remaining < lmin, the last chunk takes whatever is left
       (no random draw for the final piece).
    4. Return the list of chunks and set N = len(chunks).
    """
    random.seed(seed)
    chunks: list[bytes] = []
    pos = 0
    total = len(content)

    while pos < total:
        remaining = total - pos
        if remaining < lmin:
            # Last chunk – take the tail without randomizing
            chunk_len = remaining
        else:
            chunk_len = random.randint(lmin, lmax)
            if chunk_len > remaining:
                chunk_len = remaining
        chunks.append(content[pos:pos + chunk_len])
        pos += chunk_len

    return chunks


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
def main() -> None:
    if len(sys.argv) != 7:
        print('Usage: python reversetcpclient.py <serverIP> <serverPort> <file> <Lmin> <Lmax> <chunk_seed>')
        print('Example: python reversetcpclient.py 127.0.0.1 8888 input.txt 50 100 42')
        sys.exit(1)

    server_ip = sys.argv[1]
    server_port = int(sys.argv[2])
    file_path = sys.argv[3]
    lmin = int(sys.argv[4])
    lmax = int(sys.argv[5])
    seed = int(sys.argv[6])

    # Fresh log file
    with open('run_log.txt', 'w', encoding='utf-8') as f:
        f.write('')

    # ---- Read input file (binary – file is pure ASCII) ------------------
    with open(file_path, 'rb') as f:
        content = f.read()
    total_bytes = len(content)
    _log(f'Read "{file_path}": {total_bytes} bytes')

    # ---- Split into random-sized chunks ---------------------------------
    chunks = chunk_file(content, lmin, lmax, seed)
    n = len(chunks)

    _log(f'Parameters: Lmin={lmin}, Lmax={lmax}, seed={seed}')
    offset = 0
    for i, chunk in enumerate(chunks):
        _log(f'  Chunk {i+1}: offset={offset}, length={len(chunk)} (bytes {offset}–{offset+len(chunk)-1})')
        offset += len(chunk)
    _log(f'Total chunks (N) = {n}')

    # ---- Connect to server ------------------------------------------------
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(None)          # blocking mode
    try:
        sock.connect((server_ip, server_port))
        _log(f'Connected to {server_ip}:{server_port}')
    except Exception as e:
        _log(f'[ERROR] Connection failed: {e}')
        sys.exit(1)

    try:
        # ---- 1. Send Initialization: Type(2B)=1, N(4B) --------------------
        sock.sendall(struct.pack('!HI', 1, n))
        _log(f'[SEND] Initialization: N={n}')

        # ---- 2. Receive agree: Type(2B)=2 ---------------------------------
        raw = recv_exactly(sock, 2)
        ptype = struct.unpack('!H', raw)[0]
        if ptype != 2:
            _log(f'[ERROR] Expected agree(Type=2), got Type={ptype}')
            return
        _log(f'[RECV] agree')

        # ---- 3. Exchange reverseRequest / reverseAnswer for each chunk ----
        output_parts: list[bytes] = []

        for i, chunk in enumerate(chunks):
            chunk_num = i + 1

            # -- Send reverseRequest: Type(3) + Length(4) + Data --
            sock.sendall(struct.pack('!HI', 3, len(chunk)) + chunk)
            _log(f'[SEND] reverseRequest chunk={chunk_num}, length={len(chunk)}')

            # -- Receive reverseAnswer: Type(4) + Length(4) + reverseData --
            header = recv_exactly(sock, 6)
            ptype, length = struct.unpack('!HI', header)
            if ptype != 4:
                _log(f'[ERROR] Expected reverseAnswer(Type=4), got Type={ptype}')
                break

            revdata = recv_exactly(sock, length)
            revtext = revdata.decode('ascii')
            _log(f'[RECV] reverseAnswer chunk={chunk_num}, length={length}')

            # Console output required by spec (point 8)
            print(f'{chunk_num}: {revtext}')

            output_parts.append(revdata)

        # ---- Write final output -------------------------------------------
        out_path = 'output.txt'
        with open(out_path, 'wb') as f:
            for part in output_parts:
                f.write(part)
        total_out = sum(len(p) for p in output_parts)
        _log(f'Written "{out_path}": {total_out} bytes')

    except ConnectionError as e:
        _log(f'[ERROR] Connection lost: {e}')
    except Exception as e:
        _log(f'[ERROR] {e}')
    finally:
        sock.close()
        _log('Connection closed')


if __name__ == '__main__':
    main()
