#!/usr/bin/env python3
"""
UDP Server — Reliable Data Transfer Simulation (GBN-style)
===========================================================
Simulates a server that:
  1. Validates a StudentID during connection establishment.
  2. Randomly drops DATA packets (simulating network loss).
  3. Uses cumulative ACK (Go-Back-N receiver logic).

Custom protocol:  SRUDP (Simple Reliable UDP)
  Header (12 bytes):
    hdr_tag(2B) + proto_ver(1B) + pkt_type(1B) +
    data_A(2B) + data_B(2B) + time_val(4B)

Usage:
  python udpserver.py <port>
"""

import socket
import struct
import random
import sys
import threading
import time
from datetime import datetime

# ---------------------------------------------------------------------------
# Protocol constants
# ---------------------------------------------------------------------------
HDR_FMT = '!H B B H H I'
HDR_SZ = struct.calcsize(HDR_FMT)        # 12
MAGIC = 0x5CA3
VER = 0x01

MSG_SYN = 0x21       # Connection request
MSG_SYN_ACK = 0x22   # Connection acknowledgment
MSG_DAT = 0x31       # Data packet
MSG_DAT_ACK = 0x32   # Data acknowledgment (cumulative)
MSG_FIN = 0x41       # Finish

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
LOSS_RATE = 0.20                # 20 % simulated loss on DATA packets
STUDENT_LAST4 = 2103            # Last 4 digits of student number
STUDENT_MASK = 0x5A3C

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
_log_lock = threading.Lock()


def _log(msg: str) -> None:
    """Thread-safe logging — console + run_log.txt."""
    ts = datetime.now().strftime('%H:%M:%S.%f')
    with _log_lock:
        print(f'[{ts}] {msg}')
        with open('run_log.txt', 'a', encoding='utf-8') as f:
            f.write(f'[{ts}] {msg}\n')


# ---------------------------------------------------------------------------
# Protocol helpers
# ---------------------------------------------------------------------------
def build_hdr(pkt_type: int, field_a: int = 0,
              field_b: int = 0, time_val: int = 0) -> bytes:
    """Build a 12-byte SRUDP header."""
    return struct.pack(HDR_FMT, MAGIC, VER, pkt_type, field_a, field_b, time_val)


def parse_hdr(raw: bytes) -> tuple:
    """Unpack the 12-byte SRUDP header.  Returns (magic, ver, type, a, b, ts)."""
    return struct.unpack(HDR_FMT, raw[:HDR_SZ])


def valid_student_id(sid: int) -> bool:
    """Check if *sid* XOR MASK yields a value in [0, 9999]."""
    return 0 <= (sid ^ STUDENT_MASK) <= 9999


# ---------------------------------------------------------------------------
# Per-client session state
# ---------------------------------------------------------------------------
class ClientSession:
    """Tracks GBN receiver state for a single client."""

    def __init__(self) -> None:
        self.expected_seq: int = 1          # next in-order seq we expect
        self.packets: dict[int, bytes] = {} # seq -> payload (for possible later use)
        self.total_ok: int = 0              # count of accepted in-order packets


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main() -> None:
    if len(sys.argv) != 2:
        print('Usage: python udpserver.py <port>')
        sys.exit(1)

    port = int(sys.argv[1])

    # Fresh log
    with open('run_log.txt', 'w', encoding='utf-8') as f:
        f.write('')

    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind(('0.0.0.0', port))
    _log(f'Server listening on 0.0.0.0:{port}   (loss rate = {LOSS_RATE:.0%})')

    sessions: dict[tuple, ClientSession] = {}

    try:
        while True:
            raw, addr = sock.recvfrom(65535)
            if len(raw) < HDR_SZ:
                continue

            magic, ver, ptype, fa, fb, ts_val = parse_hdr(raw)

            if magic != MAGIC:
                _log(f'[DROP] Bad magic 0x{magic:04X} from {addr[0]}:{addr[1]}')
                continue

            cid = f'{addr[0]}:{addr[1]}'

            # ---- 1) Connection request ------------------------------------
            if ptype == MSG_SYN:
                if not valid_student_id(fa):
                    _log(f'[{cid}] [REJECT] SYN  StudentID={fa} (INVALID)')
                    continue

                _log(f'[{cid}] [RECV] SYN  StudentID={fa} (valid)')
                sessions[addr] = ClientSession()

                resp = build_hdr(MSG_SYN_ACK, fa, 0, int(time.time()))
                sock.sendto(resp, addr)
                _log(f'[{cid}] [SEND] SYN_ACK  StudentID={fa}')

            # ---- 2) Data packet -------------------------------------------
            elif ptype == MSG_DAT:
                seq = fa
                pay_len = fb
                payload = raw[HDR_SZ:HDR_SZ + pay_len]

                session = sessions.get(addr)
                if session is None:
                    _log(f'[{cid}] [WARN] DAT from unknown client, ignored')
                    continue

                # --- Simulate random loss (do NOT respond) -----------------
                if random.random() < LOSS_RATE:
                    _log(f'[{cid}] [LOST]  DAT seq={seq}  (simulated drop, no ACK)')
                    continue

                # --- GBN receiver logic ------------------------------------
                if seq == session.expected_seq:
                    # In-order — accept
                    session.packets[seq] = payload
                    session.expected_seq += 1
                    session.total_ok += 1
                    _log(f'[{cid}] [RECV]  DAT seq={seq}  len={pay_len}  ACCEPTED (in-order)')

                    # Send cumulative ACK
                    ack = session.expected_seq - 1
                    resp = build_hdr(MSG_DAT_ACK, ack, 0, int(time.time()))
                    sock.sendto(resp, addr)
                    _log(f'[{cid}] [SEND]  DAT_ACK  ack={ack}  (cumulative)')

                elif seq < session.expected_seq:
                    # Duplicate — re-ACK the last in-order packet
                    ack = session.expected_seq - 1
                    resp = build_hdr(MSG_DAT_ACK, ack, 0, int(time.time()))
                    sock.sendto(resp, addr)
                    _log(f'[{cid}] [SEND]  DAT_ACK  ack={ack}  (duplicate)')

                else:
                    # Out-of-order (seq > expected) — GBN discards;
                    # send duplicate ACK so sender knows where we are.
                    ack = session.expected_seq - 1
                    resp = build_hdr(MSG_DAT_ACK, ack, 0, int(time.time()))
                    sock.sendto(resp, addr)
                    _log(f'[{cid}] [SEND]  DAT_ACK  ack={ack}  (GBN keep-alive, '
                         f'received seq={seq} but expected {session.expected_seq})')

            # ---- 3) Finish ------------------------------------------------
            elif ptype == MSG_FIN:
                _log(f'[{cid}] [RECV]  FIN')
                if addr in sessions:
                    s = sessions.pop(addr)
                    _log(f'[{cid}] Session closed.  In-order packets accepted: {s.total_ok}')
                resp = build_hdr(MSG_FIN, 0, 0, 0)
                sock.sendto(resp, addr)
                _log(f'[{cid}] [SEND]  FIN_ACK')

            else:
                _log(f'[{cid}] [WARN] Unknown packet type 0x{ptype:02X}')

    except KeyboardInterrupt:
        _log('Server shutting down (Ctrl+C)')
    finally:
        sock.close()
        _log('Server socket closed')


if __name__ == '__main__':
    main()
