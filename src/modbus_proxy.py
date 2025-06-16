# -*- coding: utf-8 -*-
#
# This file is part of the modbus-proxy project
#
# Copyright (c) 2020-2021 Tiago Coutinho
# Distributed under the GPLv3 license. See LICENSE for more info.


import asyncio
import time
import pathlib
import argparse
import warnings
import contextlib
import logging.config
from urllib.parse import urlparse
import re
import ast

__version__ = "0.8.0"


DEFAULT_LOG_CONFIG = {
    "version": 1,
    "formatters": {
        "standard": {"format": "%(asctime)s %(levelname)8s %(name)s: %(message)s"}
    },
    "handlers": {
        "console": {"class": "logging.StreamHandler", "formatter": "standard"}
    },
    "root": {"handlers": ["console"], "level": "INFO"},
}

log = logging.getLogger("modbus-proxy")


def parse_url(url):
    if "://" not in url:
        url = f"tcp://{url}"
    result = urlparse(url)
    if not result.hostname:
        url = result.geturl().replace("://", "://0")
        result = urlparse(url)
    return result


class Connection:
    def __init__(self, name, reader, writer):
        self.name = name
        self.reader = reader
        self.writer = writer
        self.log = log.getChild(name)

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_value, tb):
        await self.close()

    @property
    def opened(self):
        return (
            self.writer is not None
            and not self.writer.is_closing()
            and not self.reader.at_eof()
        )

    async def close(self):
        if self.writer is not None:
            self.log.info("closing connection...")
            try:
                self.writer.close()
                await self.writer.wait_closed()
            except Exception as error:
                self.log.info("failed to close: %r", error)
            else:
                self.log.info("connection closed")
            finally:
                self.reader = None
                self.writer = None

    async def _write(self, data):
        self.log.debug("sending %r", data)
        self.writer.write(data)
        await self.writer.drain()

    async def write(self, data):
        try:
            await self._write(data)
        except Exception as error:
            self.log.error("writting error: %r", error)
            await self.close()
            return False
        return True

    async def _read(self):
        """Read ModBus TCP message"""
        # TODO: Handle Modbus RTU and ASCII
        header = await self.reader.readexactly(6)
        size = int.from_bytes(header[4:], "big")
        reply = header + await self.reader.readexactly(size)
        self.log.debug("received %r", reply)
        return reply

    async def read(self):
        try:
            return await self._read()
        except asyncio.IncompleteReadError as error:
            if error.partial:
                self.log.error("reading error: %r", error)
            else:
                self.log.info("client closed connection")
            await self.close()
        except Exception as error:
            self.log.error("reading error: %r", error)
            await self.close()


class Client(Connection):
    def __init__(self, reader, writer):
        peer = writer.get_extra_info("peername")
        super().__init__(f"Client({peer[0]}:{peer[1]})", reader, writer)
        self.log.info("new client connection")


class ModBus(Connection):
    def __init__(self, config):
        modbus = config["modbus"]
        url = parse_url(modbus["url"])
        bind = parse_url(config["listen"]["bind"])
        super().__init__(f"ModBus({url.hostname}:{url.port})", None, None)
        self.host = bind.hostname
        self.port = 502 if bind.port is None else bind.port
        self.modbus_host = url.hostname
        self.modbus_port = url.port
        self.timeout = modbus.get("timeout", None)
        self.connection_time = modbus.get("connection_time", 0)
        self.unit_id_remapping = config.get("unit_id_remapping") or {}
        # prepare register value transformations (holding registers)
        # config key 'register_transformations' maps register addresses or ranges to simple formulas
        transforms_cfg = config.get("register_transformations") or {}
        self._register_transforms = self._init_register_transforms(transforms_cfg)
        # track expanded read requests for cross-register transforms
        self._pending_reqs = {}
        self.server = None
        self.lock = asyncio.Lock()
        # optional rate-limit (requests per second) and cache TTL (seconds)
        self.rate_limit = config.get("rate_limit", 0)
        self.cache_ttl = config.get("cache_ttl", 0)
        # simple cache for small reads: map (func, start, count) -> (timestamp, reply_bytes)
        self._cache = {}
        # per-register cache: map (func, register_address) -> (timestamp, 2-byte raw value)
        self._reg_cache = {}
        loop = asyncio.get_event_loop()
        if self.rate_limit:
            self._rate_limit_interval = 1.0 / self.rate_limit
            # allow immediate first request
            self._last_rate_time = loop.time() - self._rate_limit_interval
        else:
            self._rate_limit_interval = 0
            self._last_rate_time = 0
    # maximum registers per read-holding or read-input request
    MAX_READ_REGISTERS = 125

    @property
    def address(self):
        if self.server is not None:
            return self.server.sockets[0].getsockname()

    async def open(self):
        self.log.info("connecting to modbus...")
        self.reader, self.writer = await asyncio.open_connection(
            self.modbus_host, self.modbus_port
        )
        self.log.info("connected!")

    async def connect(self):
        if not self.opened:
            await asyncio.wait_for(self.open(), self.timeout)
            if self.connection_time > 0:
                self.log.info("delay after connect: %s", self.connection_time)
                await asyncio.sleep(self.connection_time)

    async def write_read(self, data, attempts=2):
        """
        Send a Modbus request and read the reply, splitting large reads
        (function codes 3 & 4) into <= MAX_READ_REGISTERS chunks.
        """
        # detect small read-holding/input requests
        is_read = len(data) >= 12 and data[7] in (3, 4)
        if self.cache_ttl and is_read:
            start = int.from_bytes(data[8:10], 'big')
            count = int.from_bytes(data[10:12], 'big')
            if count <= self.MAX_READ_REGISTERS:
                func = data[7]
                # block-level cache check
                key = (func, start, count)
                ts, reply = self._cache.get(key, (0, None))
                if reply is not None and (time.time() - ts) < self.cache_ttl:
                    self.log.debug("cache hit for %s", key)
                    return reply
                # per-register cache check: return immediately if all regs are fresh
                now = time.time()
                fresh = {}
                missing = []
                for i in range(count):
                    reg = start + i
                    entry = self._reg_cache.get((func, reg))
                    if entry and (now - entry[0]) < self.cache_ttl:
                        fresh[i] = entry[1]
                    else:
                        missing.append(i)
                if not missing:
                    # assemble full reply from per-register cache
                    data_bytes = b''.join(fresh[i] for i in range(count))
                    tid = data[0:2]
                    proto = data[2:4]
                    unit = data[6]
                    bytecount = 2 * count
                    length = 3 + bytecount
                    reply = (
                        tid + proto + length.to_bytes(2, 'big')
                        + bytes([unit, func, bytecount])
                        + data_bytes
                    )
                    return reply
        async with self.lock:
            # double-check block-level cache after acquiring lock
            if self.cache_ttl and len(data) >= 12 and data[7] in (3, 4):
                start = int.from_bytes(data[8:10], 'big')
                count = int.from_bytes(data[10:12], 'big')
                func = data[7]
                if count <= self.MAX_READ_REGISTERS:
                    key = (func, start, count)
                    ts, reply = self._cache.get(key, (0, None))
                    if reply is not None and (time.time() - ts) < self.cache_ttl:
                        self.log.debug("cache hit for %s", key)
                        return reply
                    # per-register cache double-check
                    now = time.time()
                    fresh = {}
                    missing = []
                    for i in range(count):
                        reg = start + i
                        entry = self._reg_cache.get((func, reg))
                        if entry and (now - entry[0]) < self.cache_ttl:
                            fresh[i] = entry[1]
                        else:
                            missing.append(i)
                    if not missing:
                        # assemble full reply from per-register cache
                        data_bytes = b''.join(fresh[i] for i in range(count))
                        tid = data[0:2]
                        proto = data[2:4]
                        unit = data[6]
                        bytecount = 2 * count
                        length = 3 + bytecount
                        reply = (
                            tid + proto + length.to_bytes(2, 'big')
                            + bytes([unit, func, bytecount])
                            + data_bytes
                        )
                        # update block-level cache
                        self._cache[key] = (time.time(), reply)
                        return reply
                    # fetch only missing register segments
                    segments = []
                    prev = None
                    for idx in missing:
                        if prev is None or idx != prev + 1:
                            segments.append([idx])
                        else:
                            segments[-1].append(idx)
                        prev = idx
                    # fetch each missing contiguous segment
                    for seg in segments:
                        seg_start = seg[0]
                        seg_count = len(seg)
                        # build chunk request for this sub-range
                        chunk = bytearray(data)
                        chunk[8:10] = (start + seg_start).to_bytes(2, 'big')
                        chunk[10:12] = seg_count.to_bytes(2, 'big')
                        # perform backend fetch
                        await self.connect()
                        await self._apply_rate_limit()
                        reply_seg = await asyncio.wait_for(
                            self._write_read(chunk), self.timeout
                        )
                        if not reply_seg:
                            return None
                        bytecount = reply_seg[8]
                        seg_bytes = reply_seg[9:9 + bytecount]
                        # update per-register cache and collect data
                        for j in range(seg_count):
                            val_bytes = seg_bytes[2 * j:2 * j + 2]
                            regno = start + seg_start + j
                            self._reg_cache[(func, regno)] = (time.time(), val_bytes)
                            fresh[seg_start + j] = val_bytes
                    # assemble full data bytes
                    data_bytes = b''.join(fresh[i] for i in range(count))
                    tid = data[0:2]
                    proto = data[2:4]
                    unit = data[6]
                    bytecount = 2 * count
                    length = 3 + bytecount
                    reply = (
                        tid + proto + length.to_bytes(2, 'big')
                        + bytes([unit, func, bytecount])
                        + data_bytes
                    )
                    # update block-level cache
                    self._cache[key] = (time.time(), reply)
                    return reply
            for i in range(attempts):
                try:
                    await self.connect()
                    # rate limiting
                    await self._apply_rate_limit()
                    # detect oversized read-holding (3) or read-input (4)
                    if len(data) >= 12 and data[7] in (3, 4):
                        start = int.from_bytes(data[8:10], 'big')
                        count = int.from_bytes(data[10:12], 'big')
                        if count > self.MAX_READ_REGISTERS:
                            return await asyncio.wait_for(
                                self._batch_read(data),
                                self.timeout,
                            )
                    # normal request
                    coro = self._write_read(data)
                    reply = await asyncio.wait_for(coro, self.timeout)
                    # store in cache if applicable
                    if self.cache_ttl and len(data) >= 12 and data[7] in (3, 4):
                        start = int.from_bytes(data[8:10], 'big')
                        count = int.from_bytes(data[10:12], 'big')
                        if count <= self.MAX_READ_REGISTERS:
                            key = (data[7], start, count)
                            self._cache[key] = (time.time(), reply)
                    return reply
                except Exception as error:
                    # log the backend I/O error
                    self.log.error(
                        "write_read error [%s/%s]: %r", i + 1, attempts, error
                    )
                    # clear any cached data to avoid serving stale replies
                    if self.cache_ttl:
                        self._cache.clear()
                        self._reg_cache.clear()
                    # clear pending transform mappings to reset state
                    self._pending_reqs.clear()
                    # close the backend connection so next attempt reconnects
                    await self.close()

    async def _write_read(self, data):
        await self._write(data)
        return await self._read()
    
    async def _batch_read(self, request):
        """
        Split a large Read (FC3/4) into chunks of <= MAX_READ_REGISTERS,
        request each chunk, and stitch together a single combined reply.
        """
        # Parse MBAP header and PDU fields
        tid = request[0:2]
        proto = request[2:4]
        unit = request[6]
        func = request[7]
        start = int.from_bytes(request[8:10], 'big')
        count = int.from_bytes(request[10:12], 'big')
        blocks = []
        idx = 0
        # Issue chunked reads
        while idx < count:
            chunk_count = min(self.MAX_READ_REGISTERS, count - idx)
            chunk_start = start + idx
            # Build chunk request frame
            chunk = bytearray(12)
            chunk[0:2] = tid
            chunk[2:4] = proto
            # PDU length (unit+func+addr+count) = 6
            chunk[4:6] = (6).to_bytes(2, 'big')
            chunk[6] = unit
            chunk[7] = func
            chunk[8:10] = chunk_start.to_bytes(2, 'big')
            chunk[10:12] = chunk_count.to_bytes(2, 'big')
            # Send and await reply
            reply = await self._write_read(chunk)
            if not reply:
                return None
            # Extract bytecount and data
            bytecount = reply[8]
            data_bytes = reply[9:9 + bytecount]
            blocks.append(data_bytes)
            idx += chunk_count
        # Concatenate all data blocks
        full_data = b''.join(blocks)
        total_bytecount = len(full_data)
        # Build combined MBAP header
        length = 3 + total_bytecount  # unit + func + bytecount + data
        header = bytearray(6)
        header[0:2] = tid
        header[2:4] = proto
        header[4:6] = length.to_bytes(2, 'big')
        # Build payload
        payload = bytearray([unit, func, total_bytecount])
        # Return the assembled reply
        return bytes(header) + bytes(payload) + full_data
    
    async def _apply_rate_limit(self):
        """
        Enforce a simple fixed-interval rate limit between backend requests.
        """
        if self._rate_limit_interval <= 0:
            return
        loop = asyncio.get_event_loop()
        now = loop.time()
        elapsed = now - self._last_rate_time
        if elapsed < self._rate_limit_interval:
            await asyncio.sleep(self._rate_limit_interval - elapsed)
            now = loop.time()
        self._last_rate_time = now

    def _transform_request(self, request):
        """
        Transform outgoing request PDU: apply unit ID remapping,
        and expand read requests to fetch any registers needed by transformations.
        """
        # make mutable copy
        req = bytearray(request)
        # remap unit ID if configured
        uid = req[6]
        new_uid = self.unit_id_remapping.setdefault(uid, uid)
        if uid != new_uid:
            req[6] = new_uid
            self.log.debug("remapping unit ID %s to %s in request", uid, new_uid)
        # detect human-style addressing (absolute >=40001) and normalize to 0-based offset
        if len(req) >= 12 and req[7] in (3, 4):
            raw_start = int.from_bytes(req[8:10], 'big')
            if raw_start >= 40001:
                new_start = raw_start - 40001
                self.log.warning(
                    "human-style register request detected: remapping start %d to offset %d",
                    raw_start, new_start
                )
                req[8:10] = new_start.to_bytes(2, 'big')
        # expand read holding/input registers to cover transform dependencies
        if self._register_transforms:
            try:
                func = req[7]
                # only for function codes 3 (holding) and 4 (input)
                if func in (3, 4) and len(req) >= 12:
                    orig_start = int.from_bytes(req[8:10], 'big')
                    orig_count = int.from_bytes(req[10:12], 'big')
                    exp_start = orig_start
                    exp_count = orig_count
                    needed = []
                    # find any transforms targeting this block and collect deps
                    for dest_start, dest_end, fn, deps in self._register_transforms:
                        if dest_end < orig_start or dest_start > orig_start + orig_count - 1:
                            continue
                        for human in deps:
                            # convert human reg number to PDU address
                            needed.append(human - 40001)
                    if needed:
                        # combine original and needed addresses
                        all_addrs = list(range(orig_start, orig_start + orig_count)) + needed
                        new_start = min(all_addrs)
                        new_end = max(all_addrs)
                        exp_start = new_start
                        exp_count = new_end - new_start + 1
                        # modify PDU starting address and count
                        req[8:10] = exp_start.to_bytes(2, 'big')
                        req[10:12] = exp_count.to_bytes(2, 'big')
                        self.log.debug(
                            "expanded read from %d..%d (%d) to %d..%d (%d) for transforms",
                            orig_start, orig_start + orig_count - 1, orig_count,
                            exp_start, exp_start + exp_count - 1, exp_count,
                        )
                        # track original and expanded parameters by transaction ID
                        tid = bytes(req[0:2])
                        self._pending_reqs[tid] = (orig_start, orig_count, exp_start, exp_count)
            except Exception:
                pass
        return bytes(req)

    def _init_register_transforms(self, transforms_cfg):
        """
        Build transformation functions for specified registers.
        transforms_cfg maps destination register (or range) to a formula string.
        Formulas can reference other registers as $<regnum> (e.g. '$40210 * 0.5'),
        or be unary operations '* -1', '+ 5', etc., implying current register.
        Returns a list of (dest_start, dest_end, transform_fn).
        """
        transforms = []
        var_re = re.compile(r"\$(\d+)")
        unary_re = re.compile(r"^\s*(?P<op>[+\-*/])\s*(?P<val>-?\d+(?:\.\d*)?)\s*$")
        for key, formula in transforms_cfg.items():
            if isinstance(key, str) and '-' in key:
                dstart, dend = key.split('-', 1)
                dest_start = int(dstart)
                dest_end = int(dend)
            else:
                dest_start = dest_end = int(key)
            if dest_start >= 40000:
                dest_start -= 40001
            if dest_end >= 40000:
                dest_end -= 40001
            m_un = unary_re.match(formula)
            if m_un:
                op = m_un.group('op')
                val_str = m_un.group('val')
                try:
                    operand = int(val_str)
                except ValueError:
                    operand = float(val_str)
                def make_unary(op, operand):
                    def fn(raw, human, ctx):
                        v = raw - 0x10000 if (raw & 0x8000) else raw
                        if op == '*':
                            r = v * operand
                        elif op == '/':
                            r = int(v / operand)
                        elif op == '+':
                            r = v + operand
                        else:
                            r = v - operand
                        return max(min(int(r), 0x7FFF), -0x8000)
                    return fn
                fn = make_unary(op, operand)
                # no external dependencies for unary ops
                deps = []
                # register unary transform
                transforms.append((dest_start, dest_end, fn, deps))
            else:
                expr = var_re.sub(lambda m: f"r{m.group(1)}", formula)
                # capture referenced registers (human numbers)
                deps = [int(n) for n in var_re.findall(formula)]
                try:
                    tree = ast.parse(expr, mode='eval')
                except Exception as e:
                    raise ValueError(f"Invalid formula '{formula}' for register {key}: {e}")
                for node in ast.walk(tree):
                    if not isinstance(node, (
                        ast.Expression, ast.BinOp, ast.UnaryOp, ast.Constant,
                        ast.Name, ast.Load, ast.Add, ast.Sub, ast.Mult, ast.Div,
                        ast.UAdd, ast.USub, ast.Call
                    )):
                        raise ValueError(f"Unsupported AST node {node} in formula '{formula}'")
                    if isinstance(node, ast.Call):
                        if not (isinstance(node.func, ast.Name) and node.func.id in ('int', 'float')):
                            raise ValueError(f"Unsupported function {node.func.id} in formula '{formula}'")
                code = compile(tree, '<formula>', 'eval')
                def make_expr(code):
                    def fn(raw, human, ctx):
                        try:
                            v = raw - 0x10000 if (raw & 0x8000) else raw
                            ctx[f"r{human}"] = v
                            new_val = eval(code, {'__builtins__': {}}, ctx)
                        except Exception:
                            return v
                        try:
                            r = int(new_val)
                        except Exception:
                            r = 0
                        return max(min(r, 0x7FFF), -0x8000)
                    return fn
                fn = make_expr(code)
                transforms.append((dest_start, dest_end, fn, deps))
        return transforms

    def _transform_reply(self, reply, request):
        """
        Transform incoming reply PDU: apply unit ID inverse remapping,
        apply register value transformations, and trim any expanded data back
        to the original request size.
        """
        data = bytearray(reply)
        # inverse unit ID remapping
        uid = data[6]
        inverse_map = {v: k for k, v in self.unit_id_remapping.items()}
        new_uid = inverse_map.setdefault(uid, uid)
        if uid != new_uid:
            data[6] = new_uid
            self.log.debug("remapping unit ID %s to %s in reply", uid, new_uid)
        # apply register transformations for holding/input registers (function codes 3 and 4)
        if self._register_transforms and len(data) >= 9:
            func = data[7]
            if func in (3, 4):
                # determine original and expanded ranges
                tid = bytes(data[0:2])
                mapping = self._pending_reqs.pop(tid, None)
                if mapping:
                    orig_start, orig_count, exp_start, exp_count = mapping
                else:
                    orig_start = int.from_bytes(request[8:10], 'big')
                    orig_count = int.from_bytes(request[10:12], 'big')
                    exp_start, exp_count = orig_start, orig_count
                # build context for expression evaluation over expanded block
                ctx = {'int': int, 'float': float}
                for j in range(exp_count):
                    off = 9 + j * 2
                    raw = int.from_bytes(data[off:off+2], 'big')
                    val = raw - 0x10000 if (raw & 0x8000) else raw
                    human = exp_start + j + 40001
                    ctx[f'r{human}'] = val
                # apply each transformation function to the expanded block
                for dest_start, dest_end, fn, deps in self._register_transforms:
                    for i in range(exp_count):
                        reg_addr = exp_start + i
                        if dest_start <= reg_addr <= dest_end:
                            off = 9 + i * 2
                            raw = int.from_bytes(data[off:off+2], 'big')
                            # signed 16-bit value
                            signed = raw - 0x10000 if (raw & 0x8000) else raw
                            human = reg_addr + 40001
                            new_val = fn(raw, human, ctx)
                            # log transformation details
                            self.log.debug(
                                "transform register %d (human %d): %d -> %d",
                                reg_addr, human, signed, new_val
                            )
                            raw2 = new_val & 0xFFFF
                            data[off] = (raw2 >> 8) & 0xFF
                            data[off + 1] = raw2 & 0xFF
                # if we expanded beyond the original request, trim and re-align
                if exp_count != orig_count:
                    # calculate how many registers were prepended
                    skip = orig_start - exp_start
                    # update MBAP length: unit(1) + func(1) + bytecount(1) + 2*orig_count
                    new_len = 3 + 2 * orig_count
                    data[4:6] = new_len.to_bytes(2, 'big')
                    # update byte count at data[8]
                    bytecount = 2 * orig_count
                    data[8] = bytecount & 0xFF
                    # slice only the original requested registers
                    start = 9 + skip * 2
                    end = start + bytecount
                    data = data[:9] + data[start:end]
        return data

    async def handle_client(self, reader, writer):
        async with Client(reader, writer) as client:
            while True:
                request = await client.read()
                if not request:
                    break
                # reject client read requests > MAX_READ_REGISTERS per Modbus spec
                if len(request) >= 12 and request[7] in (3, 4):
                    start = int.from_bytes(request[8:10], 'big')
                    count = int.from_bytes(request[10:12], 'big')
                    if count > self.MAX_READ_REGISTERS:
                        # build Modbus exception: ILLEGAL_DATA_VALUE (code 3)
                        tid = request[0:2]
                        proto = request[2:4]
                        # PDU: unit(1) + func_error(1) + exception_code(1) = 3 bytes
                        length = (3).to_bytes(2, 'big')
                        unit = request[6]
                        func_err = request[7] | 0x80
                        ex_code = 3
                        reply = tid + proto + length + bytes([unit, func_err, ex_code])
                        await client.write(reply)
                        continue
                # send request to actual device (with unit ID remapping)
                # build and send request (with unit-ID remap and transform-driven expansions)
                transformed_req = self._transform_request(request)
                reply = await self.write_read(transformed_req)
                if not reply:
                    break
                # transform reply (unit ID and register values)
                reply = self._transform_reply(reply, request)
                # send final reply back to client
                result = await client.write(reply)
                if not result:
                    break

    async def start(self):
        self.server = await asyncio.start_server(
            self.handle_client, self.host, self.port, start_serving=True
        )

    async def stop(self):
        if self.server is not None:
            self.server.close()
            await self.server.wait_closed()
        await self.close()

    async def serve_forever(self):
        if self.server is None:
            await self.start()
        async with self.server:
            self.log.info("Ready to accept requests on %s:%d", self.host, self.port)
            await self.server.serve_forever()


def load_config(file_name):
    file_name = pathlib.Path(file_name)
    ext = file_name.suffix
    if ext.endswith("toml"):
        from toml import load
    elif ext.endswith("yml") or ext.endswith("yaml"):
        import yaml

        def load(fobj):
            return yaml.load(fobj, Loader=yaml.Loader)

    elif ext.endswith("json"):
        from json import load
    else:
        raise NotImplementedError
    with open(file_name) as fobj:
        return load(fobj)


def prepare_log(config):
    cfg = config.get("logging")
    if not cfg:
        cfg = DEFAULT_LOG_CONFIG
    if cfg:
        cfg.setdefault("version", 1)
        cfg.setdefault("disable_existing_loggers", False)
        logging.config.dictConfig(cfg)
    warnings.simplefilter("always", DeprecationWarning)
    logging.captureWarnings(True)
    return log


def parse_args(args=None):
    parser = argparse.ArgumentParser(
        description="ModBus proxy",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "-c", "--config-file", default=None, type=str, help="config file"
    )
    parser.add_argument("-b", "--bind", default=None, type=str, help="listen address")
    parser.add_argument(
        "--modbus",
        default=None,
        type=str,
        help="modbus device address (ex: tcp://plc.acme.org:502)",
    )
    parser.add_argument(
        "--modbus-connection-time",
        type=float,
        default=0,
        help="delay after establishing connection with modbus before first request",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=10,
        help="modbus connection and request timeout in seconds",
    )
    parser.add_argument(
        "--export-log-config",
        action="store_true",
        help="Export default logging configuration as YAML and exit",
    )
    parser.add_argument(
        "--export-full-config",
        action="store_true",
        help="Export full configuration template with defaults as YAML and exit",
    )
    options = parser.parse_args(args=args)
    # If exporting logging or full config, skip further argument validation
    if getattr(options, "export_log_config", False) or getattr(options, "export_full_config", False):
        return options
    if not options.config_file and not options.modbus:
        parser.exit(1, "must give a config-file or/and a --modbus")
    return options


def create_config(args):
    if args.config_file is None:
        assert args.modbus
    config = load_config(args.config_file) if args.config_file else {}
    prepare_log(config)
    log.info("Starting...")
    devices = config.setdefault("devices", [])
    if args.modbus:
        listen = {"bind": ":502" if args.bind is None else args.bind}
        devices.append(
            {
                "modbus": {
                    "url": args.modbus,
                    "timeout": args.timeout,
                    "connection_time": args.modbus_connection_time,
                },
                "listen": listen,
            }
        )
    return config


def create_bridges(config):
    return [ModBus(cfg) for cfg in config["devices"]]


async def start_bridges(bridges):
    coros = [bridge.start() for bridge in bridges]
    await asyncio.gather(*coros)


async def run_bridges(bridges, ready=None):
    async with contextlib.AsyncExitStack() as stack:
        coros = [stack.enter_async_context(bridge) for bridge in bridges]
        await asyncio.gather(*coros)
        await start_bridges(bridges)
        if ready is not None:
            ready.set(bridges)
        coros = [bridge.serve_forever() for bridge in bridges]
        await asyncio.gather(*coros)


async def run(args=None, ready=None):
    args = parse_args(args)
    # export full configuration template if requested
    if getattr(args, "export_full_config", False):
        # load any existing config file or start from empty
        config = load_config(args.config_file) if args.config_file else {}
        # merge CLI-specified device into devices list
        devices = config.get("devices", [])
        if getattr(args, "modbus", None):
            listen_bind = args.bind if args.bind is not None else ":502"
            devices.append({
                "modbus": {
                    "url": args.modbus,
                    "timeout": args.timeout,
                    "connection_time": args.modbus_connection_time,
                },
                "listen": {"bind": listen_bind},
            })
        config["devices"] = devices
        # fill defaults for each device entry
        for d in config.get("devices", []):
            mb = d.get("modbus", {})
            mb.setdefault("timeout", args.timeout)
            mb.setdefault("connection_time", args.modbus_connection_time)
            d["modbus"] = mb
            ln = d.get("listen", {})
            if "bind" not in ln:
                ln["bind"] = args.bind if args.bind is not None else ":502"
            d["listen"] = ln
            d.setdefault("unit_id_remapping", {})
            d.setdefault("register_transformations", {})
            d.setdefault("rate_limit", 0)
            d.setdefault("cache_ttl", 0)
        # prepare default logging block
        log_cfg = DEFAULT_LOG_CONFIG.copy()
        log_cfg.setdefault("disable_existing_loggers", False)
        # assemble export dict with ordered sections
        export = {
            "logging": log_cfg,
            "devices": config.get("devices", []),
        }
        # output as YAML (or JSON fallback)
        try:
            import yaml
            # preserve insertion order: sort_keys=False requires PyYAML>=5.1
            print(yaml.dump(export, default_flow_style=False, sort_keys=False))
        except ImportError:
            import json
            print(json.dumps(export, indent=2))
        return
    # export default logging configuration if requested
    if getattr(args, "export_log_config", False):
        # export default logging configuration
        # include disable_existing_loggers default
        cfg = DEFAULT_LOG_CONFIG.copy()
        cfg.setdefault("disable_existing_loggers", False)
        try:
            import yaml

            print(yaml.dump(cfg, default_flow_style=False))
        except ImportError:
            import json

            print(json.dumps(cfg, indent=2))
        return
    config = create_config(args)
    bridges = create_bridges(config)
    await run_bridges(bridges, ready=ready)


def main():
    try:
        asyncio.run(run())
    except KeyboardInterrupt:
        log.warning("Ctrl-C pressed. Bailing out!")


if __name__ == "__main__":
    main()
