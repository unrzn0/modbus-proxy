#!/usr/bin/env python3
"""
Example script to read holding registers via the proxy and display
the human register address, zero-based offset, and value.
Originally used to verify polarity inversion, but now serves as a general
register inspection tool.

Requires umodbus: install with `pip install umodbus`.
"""
import socket
import argparse
try:
    # Prefer pymodbus if installed
    from pymodbus.client.sync import ModbusTcpClient
    _USE_PYMODBUS = True
except ImportError:
    from umodbus.client import tcp
    _USE_PYMODBUS = False

def main():
    """
    Connect to the proxy and read holding registers, applying example polarity inversion.

    Example usage:
      python check_polarity.py --host 127.0.0.1 --port 1502 --reg 40206 --count 4
    """
    parser = argparse.ArgumentParser(
        description="Check inverted register values via modbus-proxy"
    )
    parser.add_argument(
        "--host", default="127.0.0.1", help="proxy host (default: %(default)s)"
    )
    parser.add_argument(
        "--port", type=int, default=1502, help="proxy port (default: %(default)s)"
    )
    parser.add_argument(
        "--reg", type=int, default=40206,
        help="first human register number to read (default: %(default)s)"
    )
    parser.add_argument(
        "--count", type=int, default=4,
        help="number of registers to read (default: %(default)s)"
    )
    parser.add_argument(
        "--type", dest="rtype", choices=["int16", "uint16"], default="int16",
        help="interpret registers as signed int16 or unsigned uint16 (default: %(default)s)"
    )
    args = parser.parse_args()

    # Read registers via proxy
    reg_start = args.reg
    count = args.count
    offset = reg_start - 40001
    if _USE_PYMODBUS:
        client = ModbusTcpClient(args.host, port=args.port)
        if not client.connect():
            print(f"Failed to connect to proxy at {args.host}:{args.port}")
            return
        rr = client.read_holding_registers(address=offset, count=count, unit=1)
        if not hasattr(rr, 'registers'):
            print(f"Error reading registers: {rr}")
            client.close()
            return
        regs = rr.registers
        client.close()
    else:
        addr = (args.host, args.port)
        try:
            sock = socket.create_connection(addr)
        except Exception as e:
            print(f"Failed to connect to proxy at {addr}: {e}")
            return
        with sock:
            adu = tcp.read_holding_registers(
                slave_id=1, starting_address=offset, quantity=count
            )
            try:
                regs = tcp.send_message(adu, sock)
            except ValueError:
                print("Failed to receive a complete response from proxy.")
                return

    # Interpret register values according to requested type
    if args.rtype == 'int16':
        def to_int16(v):
            return v - 0x10000 if v & 0x8000 else v
        regs = [to_int16(v) for v in regs]
    # else uint16: leave as-is
    # Print each register with its human address, base-0 offset, and value
    print(f"Read registers {reg_start}-{reg_start+count-1} ({args.rtype}):")
    print("addr\toffset\tvalue")
    for i, v in enumerate(regs):
        addr = reg_start + i
        print(f"{addr}\t{i}\t{v}")

if __name__ == "__main__":
    main()
