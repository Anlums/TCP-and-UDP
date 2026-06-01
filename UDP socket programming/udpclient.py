#!/usr/bin/env python3
"""
UDP Client — Reliable Data Transfer using GBN-style protocol
=============================================================
Simulates a sender that:
  1. Establishes a logical connection over UDP (SYN / SYN_ACK).
  2. Sends 30 data packets using a Go-Back-N sliding window (400 bytes).
  3. Handles timeouts and retransmission.
  4. Collects RTT statistics and prints a summary (using pandas).

Custom protocol:  SRUDP (Simple Reliable UDP)
  Header (12 bytes):
    hdr_tag(2B) + proto_ver(1B) + pkt_type(1B) +
    data_A(2B) + data_B(2B) + time_val(4B)

Usage:
  python udpclient.py <serverIP> <serverPort>
"""

import socket
import struct
import random
import sys
import time
from datetime import datetime

# ---------------------------------------------------------------------------
# Protocol constants  (must match server)
# ---------------------------------------------------------------------------
HDR_FMT = '!H B B H H I'
HDR_SZ = struct.calcsize(HDR_FMT)          # 12
MAGIC = 0x5CA3
VER = 0x01

MSG_SYN = 0x21       # Connection request
MSG_SYN_ACK = 0x22   # Connection acknowledgment
MSG_DAT = 0x31       # Data packet
MSG_DAT_ACK = 0x32   # Data acknowledgment (cumulative)
MSG_FIN = 0x41       # Finish

# ---------------------------------------------------------------------------
# GBN / simulation parameters
# ---------------------------------------------------------------------------
STUDENT_LAST4 = 2103
STUDENT_MASK = 0x5A3C
STUDENT_ID = STUDENT_LAST4 ^ STUDENT_MASK   # 2103 ^ 0x5A3C = 21003

WIN_MAX_BYTES = 400     # Maximum in-flight payload bytes
TIMEOUT_SEC = 0.3       # 300 ms default timeout
TOTAL_PKTS = 30         # Number of data packets to send

MIN_DATA = 40           # Minimum payload bytes per packet
MAX_DATA = 80           # Maximum payload bytes per packet

PKT_SIZE_SEED = 42      # Seed for reproducible packet-size sequence

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
def _log(msg: str) -> None:
    """Log to console and run_log.txt with microsecond timestamp."""
    ts = datetime.now().strftime('%H:%M:%S.%f')
    print(f'[{ts}] {msg}')
    with open('run_log.txt', 'a', encoding='utf-8') as f:
        f.write(f'[{ts}] {msg}\n')


# ---------------------------------------------------------------------------
# Protocol helpers
# ---------------------------------------------------------------------------
def build_hdr(pkt_type: int, field_a: int = 0,
              field_b: int = 0, time_val: int = 0) -> bytes:
    return struct.pack(HDR_FMT, MAGIC, VER, pkt_type, field_a, field_b, time_val)


def parse_hdr(raw: bytes) -> tuple:
    return struct.unpack(HDR_FMT, raw[:HDR_SZ])


# ---------------------------------------------------------------------------
# Packet-data generator
# ---------------------------------------------------------------------------
def _make_payload(seq: int, byte_start: int, byte_end: int,
                  total_size: int) -> bytes:
    """Build a fixed-size payload for packet *seq*."""
    text = f'PKT#{seq:03d}:[{byte_start}-{byte_end}]'.encode('ascii')
    if len(text) < total_size:
        text = text + b'~' * (total_size - len(text))
    else:
        text = text[:total_size]
    return text


# ---------------------------------------------------------------------------
# Helper: try to receive one valid SRUDP packet
# ---------------------------------------------------------------------------
def _recv_srudp(sock: socket.socket,
                expected_type: int | None = None) -> tuple | None:
    """
    Receive one valid SRUDP packet.

    Loops until a packet matching *expected_type* arrives *or* the
    socket-level timeout fires.  This discards stale / non-SRUDP data
    that may be lingering in the kernel receive buffer.
    """
    deadline = time.time() + sock.gettimeout()
    while time.time() < deadline:
        remaining = deadline - time.time()
        if remaining <= 0:
            break
        try:
            sock.settimeout(remaining)
            raw, _ = sock.recvfrom(65535)
        except socket.timeout:
            break

        if len(raw) < HDR_SZ:
            continue
        hdr = parse_hdr(raw)
        if hdr[0] != MAGIC or hdr[1] != VER:
            continue                     # not our protocol, keep waiting
        if expected_type is not None and hdr[2] != expected_type:
            continue                     # wrong packet type, keep waiting
        return hdr
    return None                          # genuine timeout


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main() -> None:
    if len(sys.argv) != 3:
        print('Usage: python udpclient.py <serverIP> <serverPort>')
        sys.exit(1)

    server_addr = (sys.argv[1], int(sys.argv[2]))

    # Fresh log
    with open('run_log.txt', 'w', encoding='utf-8') as f:
        f.write('')

    # ---- Pre-compute packet sizes & offsets -------------------------------
    random.seed(PKT_SIZE_SEED)
    pkt_sizes = [random.randint(MIN_DATA, MAX_DATA) for _ in range(TOTAL_PKTS)]

    offsets: list[int] = []
    cum = 0
    for sz in pkt_sizes:
        offsets.append(cum)
        cum += sz
    total_bytes = cum

    # Build payloads
    payloads: dict[int, bytes] = {}
    for i in range(TOTAL_PKTS):
        seq = i + 1
        payloads[seq] = _make_payload(seq, offsets[i],
                                      offsets[i] + pkt_sizes[i] - 1,
                                      pkt_sizes[i])

    _log(f'Prepared {TOTAL_PKTS} packets, total payload = {total_bytes} bytes')
    _log(f'  Window = {WIN_MAX_BYTES} bytes,  Timeout = {TIMEOUT_SEC*1000:.0f} ms')
    _log(f'  Packet-size seed = {PKT_SIZE_SEED}')

    # ---- Socket -----------------------------------------------------------
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.settimeout(TIMEOUT_SEC)

    # ---- Stats ------------------------------------------------------------
    rtt_vals: list[float] = []          # RTT in ms
    sent_times: dict[int, float] = {}   # seq -> time.time()
    total_sent = 0                       # includes retransmissions
    retx_count = 0
    bytes_sent = 0

    # ======================================================================
    # Phase 1 — Connection Establishment
    # ======================================================================
    _log('=== Phase 1: Connection Establishment ===')
    connected = False
    for attempt in range(1, 4):          # up to 3 retries
        sock.sendto(build_hdr(MSG_SYN, STUDENT_ID, 0, 0), server_addr)
        total_sent += 1
        _log(f'[SEND] SYN  (attempt {attempt})')

        hdr = _recv_srudp(sock, MSG_SYN_ACK)
        if hdr is not None:
            _log(f'[RECV] SYN_ACK  StudentID={hdr[3]}  server_time_utc={hdr[5]}')
            connected = True
            break
        _log(f'[TIMEOUT] SYN attempt {attempt}')

    if not connected:
        _log('[FATAL] Could not establish connection after 3 attempts')
        sock.close()
        return

    # ======================================================================
    # Phase 2 — Data Transfer (Go-Back-N)
    # ======================================================================
    _log('=== Phase 2: Data Transfer (GBN) ===')

    base = 1          # oldest unacknowledged sequence number
    nxt = 1           # next sequence number to send

    # acked[i] == True means packet i (1-indexed) is cumulatively confirmed
    acked = [False] * (TOTAL_PKTS + 2)   # extra padding for safety

    while base <= TOTAL_PKTS:
        # ---- (a) Send all packets that fit in the window ------------------
        while nxt <= TOTAL_PKTS:
            window_payload = sum(pkt_sizes[s - 1]
                                 for s in range(base, nxt))
            add = pkt_sizes[nxt - 1]
            if window_payload + add > WIN_MAX_BYTES:
                break   # window full

            pkt = build_hdr(MSG_DAT, nxt, add, 0) + payloads[nxt]
            sock.sendto(pkt, server_addr)
            sent_times[nxt] = time.time()
            total_sent += 1
            bytes_sent += add

            b_lo = offsets[nxt - 1]
            b_hi = b_lo + add - 1
            _log(f'[SEND] DAT seq={nxt}  bytes=[{b_lo}-{b_hi}]  len={add}')
            print(f'第{nxt}个（第{b_lo}~{b_hi}字节）client端已经发送')

            nxt += 1

        # ---- (b) Wait for ACK (or timeout) --------------------------------
        hdr = _recv_srudp(sock, MSG_DAT_ACK)

        if hdr is not None:
            ack_num = hdr[3]          # data_A = cumulative ACK
            srv_time = hdr[5]         # time_val = server Unix seconds

            # Convert server time to local hh:mm:ss
            srv_local = time.localtime(srv_time)
            srv_hhmmss = f'{srv_local.tm_hour:02d}-{srv_local.tm_min:02d}-{srv_local.tm_sec:02d}'

            recv_ts = time.time()

            if base <= ack_num < nxt:
                # RTT for the packet that was just acknowledged
                if ack_num in sent_times:
                    rtt_ms = (recv_ts - sent_times[ack_num]) * 1000.0
                    rtt_vals.append(rtt_ms)
                else:
                    rtt_ms = 0.0

                # Mark cumulatively acknowledged
                for s in range(base, ack_num + 1):
                    acked[s] = True

                old_base = base
                while base <= TOTAL_PKTS and acked[base]:
                    base += 1

                _log(f'[RECV] DAT_ACK  ack={ack_num}  server_time={srv_hhmmss}  '
                     f'RTT≈{rtt_ms:.2f} ms  window=[{base}-{nxt-1}]')

                b_lo = offsets[ack_num - 1]
                b_hi = b_lo + pkt_sizes[ack_num - 1] - 1
                print(f'第{ack_num}个（第{b_lo}~{b_hi}字节）server端已经收到，RTT是{rtt_ms:.2f} ms')

        else:
            # ---- (c) Timeout — retransmit entire window -------------------
            _log(f'[TIMEOUT] base={base}  nxt={nxt}  window payloads '
                 f'({sum(pkt_sizes[s-1] for s in range(base, nxt))} B)')
            retx_count += 1

            for seq in range(base, nxt):
                if seq > TOTAL_PKTS:
                    break
                sz = pkt_sizes[seq - 1]
                pkt = build_hdr(MSG_DAT, seq, sz, 0) + payloads[seq]
                sock.sendto(pkt, server_addr)
                sent_times[seq] = time.time()
                total_sent += 1

                b_lo = offsets[seq - 1]
                b_hi = b_lo + sz - 1
                _log(f'[RETX]  DAT seq={seq}  bytes=[{b_lo}-{b_hi}]')
                print(f'重传第{seq}个（第{b_lo}~{b_hi}字节）数据包')

    # ======================================================================
    # Phase 3 — Finish
    # ======================================================================
    _log('=== Phase 3: Finish ===')
    sock.sendto(build_hdr(MSG_FIN, 0, 0, 0), server_addr)
    total_sent += 1
    _log('[SEND] FIN')

    # Try to receive FIN_ACK (non-critical)
    try:
        hdr = _recv_srudp(sock, MSG_FIN)
        if hdr:
            _log('[RECV] FIN_ACK')
    except Exception:
        pass

    # ======================================================================
    # Summary Statistics  (using pandas)
    # ======================================================================
    _log('=== Summary ===')

    # Formula from spec: loss_rate = TOTAL_PKTS / actual_sent * 100
    loss_display = TOTAL_PKTS / total_sent * 100.0

    # --- pandas-based RTT summary ---
    if rtt_vals:
        import pandas as pd
        ser = pd.Series(rtt_vals)
        rtt_max = ser.max()
        rtt_min = ser.min()
        rtt_avg = ser.mean()
        rtt_std = ser.std()
    else:
        rtt_max = rtt_min = rtt_avg = rtt_std = 0.0

    print()
    print('=' * 55)
    print('  汇总信息  (Summary)')
    print('=' * 55)
    print(f'  丢包率 (Loss ratio):           {loss_display:.2f} %')
    print(f'  目标包数 (Target packets):     {TOTAL_PKTS}')
    print(f'  实际发送 (Actual sends):       {total_sent}')
    print(f'  重传次数 (Retransmissions):    {retx_count}')
    print(f'  最大RTT (Max RTT):             {rtt_max:.2f} ms')
    print(f'  最小RTT (Min RTT):             {rtt_min:.2f} ms')
    print(f'  平均RTT (Avg RTT):             {rtt_avg:.2f} ms')
    print(f'  RTT标准差 (RTT std):           {rtt_std:.2f} ms')
    print('=' * 55)

    # Log summary to run_log.txt too
    _log(f'Summary: loss_rate={loss_display:.2f}%  '
         f'sent={total_sent}  retx={retx_count}  '
         f'RTT(avg)={rtt_avg:.2f}ms  RTT(std)={rtt_std:.2f}ms')

    sock.close()
    _log('Connection closed')


if __name__ == '__main__':
    main()
