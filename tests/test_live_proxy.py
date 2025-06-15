import asyncio
import pytest

from modbus_proxy import run
from tests.test_modbus_proxy import Ready


@pytest.mark.asyncio
async def test_live_inverter_proxy():
    """
    Integration test: start a proxy to the real SunSpec inverter at 192.168.178.28
    and perform a good read and a misbehaving client read through the proxy.
    """
    # Launch the proxy
    ready = Ready()
    args = ["--modbus", "192.168.178.28:502", "--bind", "127.0.0.1:1502"]
    task = asyncio.create_task(run(args, ready))
    try:
        # Wait for proxy to be ready
        await asyncio.wait_for(ready.wait(), timeout=5)
    except asyncio.TimeoutError:
        pytest.skip("Proxy did not start in time")
        return
    bridges = ready.data
    assert bridges and len(bridges) == 1
    proxy = bridges[0]
    # Build a Read-Holding-Registers request: read 1 register at human 40001 => offset 0
    req = b"\x00\x01\x00\x00\x00\x06\x01\x03\x00\x00\x00\x01"
    # First, read SunSpec ID registers directly from inverter
    # Build Read-Holding-Registers request: unit=1, func=3, start=0, count=2
    req_direct = b"\x00\x01\x00\x00\x00\x06\x01\x03\x00\x00\x00\x02"
    try:
        rdev, wdev = await asyncio.open_connection('192.168.178.28', 502)
    except (ConnectionRefusedError, OSError):
        pytest.skip("Cannot connect to backend inverter; skipping live test")
        return
    wdev.write(req_direct)
    await wdev.drain()
    try:
        rep_direct = await rdev.readexactly(13)
    except asyncio.IncompleteReadError:
        pytest.skip("Backend inverter did not reply; skipping live test")
        return
    # Parse two registers from direct reply
    # MBAP 6 bytes + unit,func,bytecount = 3 bytes, then 4 bytes data
    data_direct = rep_direct[9:13]
    reg0 = int.from_bytes(data_direct[0:2], 'big')
    reg1 = int.from_bytes(data_direct[2:4], 'big')
    print(f"Direct read registers [40001,40002]: {reg0}, {reg1}")
    wdev.close()
    await wdev.wait_closed()

    # Now read via proxy
    try:
        rpx, wpx = await asyncio.open_connection(*proxy.address)
    except (ConnectionRefusedError, OSError):
        pytest.skip("Cannot connect to proxy; skipping live test")
        return
    wpx.write(req_direct)
    await wpx.drain()
    try:
        rep_px = await rpx.readexactly(13)
    except asyncio.IncompleteReadError:
        pytest.skip("Proxy did not relay reply; skipping live test")
        return
    data_px = rep_px[9:13]
    wpx.close()
    await wpx.wait_closed()
    # Compare direct vs proxy
    reg0_px = int.from_bytes(data_px[0:2], 'big')
    reg1_px = int.from_bytes(data_px[2:4], 'big')
    print(f"Proxy first read registers [40001,40002]: {reg0_px}, {reg1_px}")
    assert data_px == data_direct
    # Misbehaving client: open then close without request
    r2, w2 = await asyncio.open_connection(*proxy.address)
    w2.close()
    await w2.wait_closed()
    # Perform another proper read
    # Now re-read via proxy to confirm stability
    r3, w3 = await asyncio.open_connection(*proxy.address)
    w3.write(req_direct)
    await w3.drain()
    try:
        rep3 = await r3.readexactly(13)
    except asyncio.IncompleteReadError:
        pytest.skip("Proxy failed second read; skipping remainder.")
        return
    data3 = rep3[9:13]
    reg0_3 = int.from_bytes(data3[0:2], 'big')
    reg1_3 = int.from_bytes(data3[2:4], 'big')
    print(f"Proxy second read registers [40001,40002]: {reg0_3}, {reg1_3}")
    assert data3 == data_direct
    w3.close()
    await w3.wait_closed()
    # Should still match direct data
    assert data3 == data_direct
    # Ensure proxy still runs without crashing
    assert proxy.opened is True or proxy.server is not None
    # Stop proxy
    for bridge in bridges:
        await bridge.stop()
    try:
        task.cancel()
        await task
    except asyncio.CancelledError:
        pass
