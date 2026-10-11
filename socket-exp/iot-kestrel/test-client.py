#!/usr/bin/env python3

# Test device for iot-kestrel and iot-netty.
# Usage: python3 test-client.py [appsettings.json | devices.properties] [port]
#
# Sessions: valid, byte-by-byte, tampered data signature, bad login signature, unknown device, data before login.
import socket, struct, hmac, hashlib, base64, json, time, sys

CONFIG = sys.argv[1] if len(sys.argv) > 1 else "appsettings.json"
PORT = int(sys.argv[2]) if len(sys.argv) > 2 else 5000
DEV = "sensor-001"


def load_key(path, device):
    text = open(path, encoding="utf-8").read()
    if path.endswith(".json"):
        return base64.b64decode(json.loads(text)["Devices"][device])
    for line in text.splitlines():                       # devices.properties: deviceId=base64key
        if "=" in line and not line.lstrip().startswith("#"):
            name, value = line.split("=", 1)
            if name.strip() == device:
                return base64.b64decode(value.strip())
    raise KeyError(device)


K = load_key(CONFIG, DEV)


def varint(n):
    out = b""
    while n >= 0x80: out += bytes([(n & 0x7F) | 0x80]); n >>= 7
    return out + bytes([n])
def blob(b): return varint(len(b)) + b
def H(*parts): return hmac.new(K, b"".join(parts), hashlib.sha256).digest()


class Conn:
    def __init__(self):
        self.s = socket.create_connection(("127.0.0.1", PORT))
    def send(self, data, chunk=None):
        if chunk is None: self.s.send(data); return
        for i in range(0, len(data), chunk): self.s.send(data[i:i + chunk]); time.sleep(0.01)
    def exact(self, n):
        b = b""
        while len(b) < n:
            c = self.s.recv(n - len(b))
            if not c: raise EOFError("closed by server")
            b += c
        return b
    def varint(self):
        v, shift = 0, 0
        while True:
            b = self.exact(1)[0]; v |= (b & 0x7F) << shift; shift += 7
            if not b & 0x80: return v
    def closed(self):
        self.s.settimeout(2)
        try: return self.s.recv(1) == b""
        except ConnectionResetError: return True
    def close(self):
        time.sleep(0.3); self.s.close()


def login(c, dev=DEV, sign_ok=True, chunk=None):
    """Sends 'L', reads 'l'; returns (success, serverSig)."""
    dt = int(time.time() * 1000) + 1234
    body = b"L" + blob(dev.encode()) + struct.pack("<q", dt)
    dsig = H(body) if sign_ok else bytes(32)
    c.send(body + blob(dsig), chunk)
    assert c.exact(1) == b"l", "expected login response"
    rdev = c.exact(c.varint()).decode(); st = struct.unpack("<q", c.exact(8))[0]
    ok = c.exact(1)[0]; ssig = c.exact(c.varint())
    assert rdev == dev, "device id mismatch"
    if ssig:
        expected = H(dsig, b"l", blob(dev.encode()), struct.pack("<q", st), bytes([ok]))
        assert ssig == expected, "bad server signature"
    return bool(ok), ssig


def temperatures(c, prev, count=3, tamper_at=None, chunk=None):
    dt = int(time.time() * 1000)
    for k in range(count):
        samples = [(dt + i * 1000 + k * 10000, 215 + i + k) for i in range(4)]
        body = b"T" + varint(len(samples)) + b"".join(struct.pack("<qh", t, v) for t, v in samples)
        sig = H(prev, body); prev = sig
        if k == tamper_at: sig = bytes([sig[0] ^ 1]) + sig[1:]
        c.send(body + blob(sig), chunk)


def check(name, condition):
    print(("OK   " if condition else "FAIL ") + name)


c = Conn(); ok, ssig = login(c); temperatures(c, ssig); c.close()
check("valid session: login accepted, 3 messages sent", ok)

c = Conn(); ok, ssig = login(c, chunk=1); temperatures(c, ssig, chunk=7); c.close()
check("byte-by-byte session: login accepted", ok)

c = Conn(); ok, ssig = login(c); temperatures(c, ssig, tamper_at=1)
check("tampered data signature: connection closed", ok and c.closed()); c.close()

c = Conn(); ok, ssig = login(c, sign_ok=False)
check("bad login signature: rejected with signed response, closed", not ok and len(ssig) == 32 and c.closed()); c.close()

c = Conn(); ok, ssig = login(c, dev="nobody")
check("unknown device: rejected with empty signature, closed", not ok and ssig == b"" and c.closed()); c.close()

c = Conn(); temperatures(c, b"", count=1)
check("data before login: connection closed", c.closed()); c.close()
