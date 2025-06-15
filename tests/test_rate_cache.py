import asyncio
import time
import pytest

from modbus_proxy import ModBus


@pytest.mark.asyncio
async def test_caching_read(monkeypatch):
    # Setup a ModBus with small cache_ttl
    cfg = {
        "modbus": {"url": "127.0.0.1:502"},
        "listen": {"bind": "127.0.0.1:0"},
        "timeout": None,
        "rate_limit": 0,
        "cache_ttl": 0.5,
    }
    mb = ModBus(cfg)
    # Fake backend _write_read to record calls and return a stable reply
    calls = []
    async def fake_write_read(data):
        calls.append(bytes(data))
        # Build a minimal valid Modbus-TCP reply: echo register 0 as value 0x0001
        tid = data[0:2]
        proto = data[2:4]
        unit = data[6]
        func = data[7]
        # single register -> bytecount=2, data=0x0001
        bytecount = b"\x02"
        reg = b"\x00\x01"
        # length = unit(1)+func(1)+bytecount(1)+data(2) = 5
        length = (5).to_bytes(2, 'big')
        reply = tid + proto + length + bytes([unit, func]) + bytecount + reg
        return reply
    monkeypatch.setattr(mb, '_write_read', fake_write_read)
    # Prevent real network connect
    async def fake_connect():
        return
    monkeypatch.setattr(mb, 'connect', fake_connect)
    # Build a read-holding-registers PDU (start=2, count=1)
    pdu = bytearray(12)
    pdu[0:2] = (1).to_bytes(2, 'big')
    pdu[2:4] = (0).to_bytes(2, 'big')
    pdu[4:6] = (6).to_bytes(2, 'big')
    pdu[6] = 1
    pdu[7] = 3
    pdu[8:10] = (2).to_bytes(2, 'big')
    pdu[10:12] = (1).to_bytes(2, 'big')
    # First call should hit backend
    r1 = await mb.write_read(bytes(pdu))
    # Second immediate call should hit cache
    r2 = await mb.write_read(bytes(pdu))
    assert r1 == r2
    assert len(calls) == 1
    # After cache_ttl expires, next call should hit backend again
    await asyncio.sleep(0.6)
    r3 = await mb.write_read(bytes(pdu))
    assert r3 == r1
    assert len(calls) == 2

@pytest.mark.asyncio
async def test_rate_limit(monkeypatch):
    # Setup a ModBus with rate_limit=2 rps, no cache
    cfg = {
        "modbus": {"url": "127.0.0.1:502"},
        "listen": {"bind": "127.0.0.1:0"},
        "timeout": None,
        "rate_limit": 2,
        "cache_ttl": 0,
    }
    mb = ModBus(cfg)
    # Prevent real network connect
    monkeypatch.setattr(mb, 'connect', lambda: asyncio.sleep(0))
    # Fake backend _write_read to record call times
    calls = []
    async def fake_write_read(data):
        calls.append(asyncio.get_event_loop().time())
        # minimal valid reply echoing data
        return data[:]
    monkeypatch.setattr(mb, '_write_read', fake_write_read)
    # Build a read request PDU (small read)
    def make_pdu(tid):
        p = bytearray(12)
        p[0:2] = tid.to_bytes(2,'big')
        p[2:4] = (0).to_bytes(2,'big')
        p[4:6] = (6).to_bytes(2,'big')
        p[6] = 1; p[7] = 3
        p[8:10] = (0).to_bytes(2,'big')
        p[10:12] = (1).to_bytes(2,'big')
        return bytes(p)
    # Fire 4 concurrent requests
    pdus = [make_pdu(i) for i in range(4)]
    start = asyncio.get_event_loop().time()
    results = await asyncio.gather(*(mb.write_read(p) for p in pdus))
    # All replies should match input
    assert results == pdus
    # Check that backend _write_read was called 4 times
    assert len(calls) == 4
    # Compute intervals between calls
    intervals = [calls[i+1] - calls[i] for i in range(3)]
    # Given rate_limit=2 (interval=0.5s), each interval >= ~0.45s
    for interval in intervals:
        assert interval >= 0.45
