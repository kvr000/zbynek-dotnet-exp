#!/usr/bin/env python3

# Test device for iot-kestrel: valid session, byte-by-byte session, tampered signature, unknown device.
# Usage: python3 test-client.py [appsettings.json]
import socket, struct, hmac, hashlib, base64, json, time, sys

cfg = json.load(open(sys.argv[1] if len(sys.argv) > 1 else "appsettings.json")); DEV = "sensor-001"; K = base64.b64decode(cfg["Devices"][DEV])
def varint(n):
    out = b""
    while n >= 0x80: out += bytes([(n & 0x7F) | 0x80]); n >>= 7
    return out + bytes([n])
def blob(b): return varint(len(b)) + b
def H(*parts): return hmac.new(K, b"".join(parts), hashlib.sha256).digest()
def recv_exact(s, n):
    b = b""
    while len(b) < n:
        c = s.recv(n - len(b)); assert c, "closed"; b += c
    return b
def session(dev=DEV, tamper=False, slow=False):
    s = socket.create_connection(("127.0.0.1", 5000))
    msg = blob(dev.encode())
    if slow:
        for ch in msg: s.send(bytes([ch])); time.sleep(0.01)
    else: s.send(msg)
    try:
        st = struct.unpack("<q", recv_exact(s, 8))[0]; ln = recv_exact(s, 1)[0]; ssig = recv_exact(s, ln)
    except AssertionError: print(dev, "-> rejected at login"); return
    assert ssig == H(b"S", blob(dev.encode()), struct.pack("<q", st)), "bad server sig"
    print("server sig OK, serverTime", st)
    dt = int(time.time()*1000) + 1234
    dsig = H(b"D", ssig, struct.pack("<q", dt)); s.send(struct.pack("<q", dt) + blob(dsig))
    prev = dsig
    for k in range(3):
        samples = [(dt + i*1000 + k*10000, 215 + i + k) for i in range(4)]
        body = b"T" + varint(len(samples)) + b"".join(struct.pack("<qh", t, v) for t, v in samples)
        sig = H(prev, body); prev = sig
        if tamper and k == 1: sig = bytes([sig[0] ^ 1]) + sig[1:]
        data = body + blob(sig)
        if slow:
            for i in range(0, len(data), 7): s.send(data[i:i+7]); time.sleep(0.01)
        else: s.send(data)
    time.sleep(0.3); s.close()
session(); session(slow=True); session(tamper=True); session(dev="nobody")
