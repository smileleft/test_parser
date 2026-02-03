import asyncio
import os
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Optional

import asyncpg
import orjson
import psutil
from pydantic import BaseModel, Field, ValidationError

from opentelemetry import metrics, trace
from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import OTLPMetricExporter
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor


# =========================
# Config
# =========================

def env_int(name: str, default: int) -> int:
    v = os.getenv(name)
    return default if v is None else int(v)

def env_float(name: str, default: float) -> float:
    v = os.getenv(name)
    return default if v is None else float(v)

UDP_HOST = os.getenv("UDP_HOST", "0.0.0.0")
UDP_PORT = env_int("UDP_PORT", 5000)

PG_DSN = os.getenv("PG_DSN", "postgresql://postgres:postgres@localhost:5432/postgres")
PG_POOL_MIN = env_int("PG_POOL_MIN", 1)
PG_POOL_MAX = env_int("PG_POOL_MAX", 10)

QUEUE_MAX = env_int("QUEUE_MAX", 50000)
WORKERS = env_int("WORKERS", 4)
BATCH_SIZE = env_int("BATCH_SIZE", 200)
BATCH_FLUSH_MS = env_int("BATCH_FLUSH_MS", 200)

OTEL_SERVICE_NAME = os.getenv("OTEL_SERVICE_NAME", "secs_udp_ingestor")
OTEL_EXPORT_INTERVAL_SEC = env_int("OTEL_EXPORT_INTERVAL_SEC", 5)


# =========================
# OpenTelemetry setup
# =========================

def setup_otel() -> None:
    resource = Resource.create({"service.name": OTEL_SERVICE_NAME})

    # Traces
    tp = TracerProvider(resource=resource)
    tp.add_span_processor(BatchSpanProcessor(OTLPSpanExporter()))
    trace.set_tracer_provider(tp)

    # Metrics
    reader = PeriodicExportingMetricReader(
        OTLPMetricExporter(),
        export_interval_millis=OTEL_EXPORT_INTERVAL_SEC * 1000,
    )
    mp = MeterProvider(resource=resource, metric_readers=[reader])
    metrics.set_meter_provider(mp)


# =========================
# Models
# =========================

class SecsMessage(BaseModel):
    timestamp: Optional[str] = None  # e.g. "2026-01-23T14:23:25.287"
    stream: int
    function: int
    wbit: bool = False
    deviceId: Optional[int] = None
    systemBytes: Optional[str] = None
    ptype: Optional[int] = None
    stype: Optional[int] = None
    body: Optional[dict] = None


# =========================
# SECS-II SML(JSON) parser
# =========================

def _as_scalar(value: Any) -> Any:
    # U2/U4 등 숫자 타입이 [55]처럼 배열로 오는 케이스가 많아서
    if isinstance(value, list) and len(value) == 1:
        return value[0]
    return value

def _looks_like_kv_pair_list_item(x: Any) -> bool:
    """
    x가 {"type":"L","value":[{"type":"A","value":"KEY"}, <something>]} 형태인지 판별
    """
    if not isinstance(x, dict):
        return False
    if x.get("type") != "L":
        return False
    v = x.get("value")
    if not (isinstance(v, list) and len(v) == 2):
        return False
    k = v[0]
    return isinstance(k, dict) and k.get("type") == "A" and isinstance(k.get("value"), str)

def _convert_list_to_obj(lst: list) -> Any:
    """
    리스트가 다음 중 어떤 구조인지에 따라 dict/list 로 변환
    1) 모든 원소가 KV pair list item => dict
    2) 그 외 => list
    """
    if lst and all(_looks_like_kv_pair_list_item(x) for x in lst):
        out: dict[str, Any] = {}
        for item in lst:
            key = item["value"][0]["value"]
            val = _parse_item(item["value"][1])
            # 중복 키가 나오면 list로 누적
            if key in out:
                if not isinstance(out[key], list):
                    out[key] = [out[key]]
                out[key].append(val)
            else:
                out[key] = val
        return out
    return [_parse_item(x) for x in lst]

def _parse_item(item: Any) -> Any:
    """
    {"type":"A","value":"..."} / {"type":"U2","value":[55]} / {"type":"L","value":[...]} 등을
    Python 객체로 변환
    """
    if item is None:
        return None
    if not isinstance(item, dict):
        return item

    t = item.get("type")
    v = item.get("value")

    if t == "L":
        if not isinstance(v, list):
            return []
        return _convert_list_to_obj(v)

    # 문자열/숫자/바이너리 표현(여기선 JSON이므로 문자열/숫자 위주)
    return _as_scalar(v)

def parse_secs_body_to_obj(body: Optional[dict]) -> Any:
    """
    body 최상단이 {"type":"L","value":[...]} 형태라고 가정하고 파싱
    """
    if not body:
        return None
    return _parse_item(body)


# =========================
# Message routing & extraction
# =========================

SUPPORTED = {
    (5, 1): "S5F1",
    (6, 11): "S6F11",
    (2, 33): "S2F33",
    (2, 35): "S2F35",
    (2, 37): "S2F37",
    (2, 41): "S2F41",
    (2, 49): "S2F49",
    (2, 5): "S2F5",
    (6, 15): "S6F15",
}

def parse_ts(ts: Optional[str]) -> Optional[datetime]:
    if not ts:
        return None
    # "2026-01-23T14:23:25.287" 같은 형태
    try:
        dt = datetime.fromisoformat(ts)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except ValueError:
        return None

@dataclass
class ExtractedFields:
    command_name: Optional[str] = None
    command_id: Optional[str] = None
    priority: Optional[int] = None
    carrier_id: Optional[str] = None
    source: Optional[str] = None
    dest: Optional[str] = None

def extract_fields(msg_type: str, parsed_obj: Any) -> ExtractedFields:
    """
    메시지 타입별로 대표 필드만 뽑아 column에 저장.
    parsed_obj 는 parse_secs_body_to_obj 결과(중첩 dict/list) 입니다.

    샘플 S2F49 기준:
      body.value = [ U2, A, A("TRANSFER"), L([COMMANDINFO..., TRANSFERINFO...]) ]
    """
    out = ExtractedFields()

    if msg_type == "S2F49":
        # 기대: parsed_obj = [0, "", "TRANSFER", { "COMMANDINFO": {...}, "TRANSFERINFO": {...}} ]
        if isinstance(parsed_obj, list) and len(parsed_obj) >= 4:
            out.command_name = parsed_obj[2] if isinstance(parsed_obj[2], str) else None
            info = parsed_obj[3]
            if isinstance(info, dict):
                cmdinfo = info.get("COMMANDINFO")
                trinfo = info.get("TRANSFERINFO")

                # COMMANDINFO: {"COMMANDID": "...", "PRIORITY": 55}
                if isinstance(cmdinfo, dict):
                    cid = cmdinfo.get("COMMANDID")
                    if isinstance(cid, str):
                        out.command_id = cid
                    pr = cmdinfo.get("PRIORITY")
                    if isinstance(pr, (int, float)):
                        out.priority = int(pr)

                # TRANSFERINFO: {"CARRIERID": "...", "SOURCE": "...", "DEST": "..."}
                if isinstance(trinfo, dict):
                    for k, attr in [("CARRIERID", "carrier_id"), ("SOURCE", "source"), ("DEST", "dest")]:
                        val = trinfo.get(k)
                        if isinstance(val, str):
                            setattr(out, attr, val)
        return out

    # 다른 타입들은 우선 raw/parsed_json 저장 위주.
    # 필요 시 여기에 S6F11(CEID/Report), S5F1(Alarm) 등 추출 규칙 추가.
    return out


# =========================
# DB
# =========================

INSERT_SQL = """
INSERT INTO secs_messages (
  msg_timestamp, stream, function, wbit, device_id, system_bytes, ptype, stype,
  msg_type, raw_json, parsed_json,
  command_name, command_id, priority, carrier_id, source, dest
)
VALUES (
  $1, $2, $3, $4, $5, $6, $7, $8,
  $9, $10::jsonb, $11::jsonb,
  $12, $13, $14, $15, $16, $17
)
"""

async def insert_batch(pool: asyncpg.Pool, rows: list[dict[str, Any]]) -> None:
    async with pool.acquire() as conn:
        async with conn.transaction():
            await conn.executemany(
                INSERT_SQL,
                [
                    (
                        r["msg_timestamp"], r["stream"], r["function"], r["wbit"], r["device_id"],
                        r["system_bytes"], r["ptype"], r["stype"],
                        r["msg_type"], r["raw_json"], r["parsed_json"],
                        r["command_name"], r["command_id"], r["priority"],
                        r["carrier_id"], r["source"], r["dest"],
                    )
                    for r in rows
                ],
            )


# =========================
# OTel instruments
# =========================

tracer = trace.get_tracer(__name__)
meter = metrics.get_meter(__name__)

m_udp_packets = meter.create_counter("udp_packets_total")
m_bytes_received = meter.create_counter("udp_bytes_received_total")
m_parse_errors = meter.create_counter("parse_errors_total")
m_unsupported = meter.create_counter("unsupported_messages_total")
m_db_inserts = meter.create_counter("db_inserts_total")
m_db_errors = meter.create_counter("db_errors_total")

m_processing_ms = meter.create_histogram("processing_latency_ms")
m_db_insert_ms = meter.create_histogram("db_insert_latency_ms")

# Gauges
_process = psutil.Process(os.getpid())
_last_net = psutil.net_io_counters()
_last_net_ts = time.time()

# throughput calc
_throughput_window = 5.0
_recent_done: list[float] = []  # timestamps of processed items


def _obs_cpu_percent(options):
    # psutil cpu_percent는 첫 호출은 0일 수 있어, 여기서는 process cpu_times 기반으로 단순화하기 어려움
    # 대신 시스템 전체 cpu% 사용 + process cpu% 둘 다 보고 싶다면 추가 가능
    return _process.cpu_percent(interval=None)

def _obs_rss_bytes(options):
    return _process.memory_info().rss

def _obs_net_recv_bps(options):
    global _last_net, _last_net_ts
    now = time.time()
    cur = psutil.net_io_counters()
    dt = max(1e-6, now - _last_net_ts)
    bps = (cur.bytes_recv - _last_net.bytes_recv) / dt
    _last_net, _last_net_ts = cur, now
    return bps

def _obs_net_sent_bps(options):
    # _obs_net_recv_bps에서 상태 갱신하므로, 여기서는 현재값 기준으로 계산을 피하기 위해 별도 계산
    # 간단히 동일 net_io 재사용하려면 위 방식 하나만 observable로 두고 recv/sent 둘 다 갱신하는 방식이 안전.
    # 여기서는 보수적으로 recv/sent를 분리하지 않고 recv만 제공하고, sent는 별도 콜백에서 갱신하도록 구현.
    return psutil.net_io_counters().bytes_sent  # 누적치(그래프에서 rate로 변환 가능)

def _obs_throughput_per_sec(options):
    now = time.time()
    cutoff = now - _throughput_window
    # 최근 window 내만 유지
    while _recent_done and _recent_done[0] < cutoff:
        _recent_done.pop(0)
    return len(_recent_done) / _throughput_window

meter.create_observable_gauge("process_cpu_percent", callbacks=[_obs_cpu_percent])
meter.create_observable_gauge("process_rss_bytes", callbacks=[_obs_rss_bytes])
meter.create_observable_gauge("net_recv_bytes_per_sec", callbacks=[_obs_net_recv_bps])
meter.create_observable_gauge("net_sent_bytes_total", callbacks=[_obs_net_sent_bps])
meter.create_observable_gauge("throughput_per_sec", callbacks=[_obs_throughput_per_sec])


# =========================
# UDP Server
# =========================

class UDPIngestProtocol(asyncio.DatagramProtocol):
    def __init__(self, queue: asyncio.Queue[bytes]):
        self.queue = queue

    def datagram_received(self, data: bytes, addr):
        m_udp_packets.add(1)
        m_bytes_received.add(len(data))
        # queue full이면 드랍(또는 블로킹 전략 변경 가능)
        try:
            self.queue.put_nowait(data)
        except asyncio.QueueFull:
            # 드랍도 지표로 보고 싶으면 counter 추가 가능
            pass


# =========================
# Worker
# =========================

async def worker_loop(
    worker_id: int,
    queue: asyncio.Queue[bytes],
    pool: asyncpg.Pool,
):
    buf: list[dict[str, Any]] = []
    last_flush = time.time()

    while True:
        timeout = max(0.0, (BATCH_FLUSH_MS / 1000.0) - (time.time() - last_flush))
        try:
            data = await asyncio.wait_for(queue.get(), timeout=timeout)
            queue.task_done()
        except asyncio.TimeoutError:
            data = None

        if data is not None:
            t0 = time.time()
            with tracer.start_as_current_span("udp_message_process") as span:
                try:
                    obj = orjson.loads(data)
                    msg = SecsMessage.model_validate(obj)
                except (orjson.JSONDecodeError, ValidationError) as e:
                    m_parse_errors.add(1)
                    span.record_exception(e)
                    span.set_attribute("error", True)
                    continue

                msg_type = SUPPORTED.get((msg.stream, msg.function))
                if not msg_type:
                    m_unsupported.add(1)
                    span.set_attribute("secs.stream", msg.stream)
                    span.set_attribute("secs.function", msg.function)
                    continue

                parsed_obj = None
                with tracer.start_as_current_span("secs_body_parse"):
                    try:
                        parsed_obj = parse_secs_body_to_obj(msg.body)
                    except Exception as e:
                        m_parse_errors.add(1)
                        span.record_exception(e)
                        span.set_attribute("error", True)

                extracted = extract_fields(msg_type, parsed_obj)

                row = {
                    "msg_timestamp": parse_ts(msg.timestamp),
                    "stream": msg.stream,
                    "function": msg.function,
                    "wbit": msg.wbit,
                    "device_id": msg.deviceId,
                    "system_bytes": msg.systemBytes,
                    "ptype": msg.ptype,
                    "stype": msg.stype,
                    "msg_type": msg_type,
                    "raw_json": orjson.dumps(obj).decode("utf-8"),
                    "parsed_json": (orjson.dumps(parsed_obj).decode("utf-8") if parsed_obj is not None else None),
                    "command_name": extracted.command_name,
                    "command_id": extracted.command_id,
                    "priority": extracted.priority,
                    "carrier_id": extracted.carrier_id,
                    "source": extracted.source,
                    "dest": extracted.dest,
                }

                buf.append(row)
                dt_ms = (time.time() - t0) * 1000.0
                m_processing_ms.record(dt_ms)
                _recent_done.append(time.time())

        # flush 조건
        now = time.time()
        if buf and (len(buf) >= BATCH_SIZE or (now - last_flush) * 1000.0 >= BATCH_FLUSH_MS):
            with tracer.start_as_current_span("db_insert_batch") as span:
                t1 = time.time()
                try:
                    await insert_batch(pool, buf)
                    m_db_inserts.add(len(buf))
                except Exception as e:
                    m_db_errors.add(1)
                    span.record_exception(e)
                    span.set_attribute("error", True)
                finally:
                    m_db_insert_ms.record((time.time() - t1) * 1000.0)
                    buf.clear()
                    last_flush = now


# =========================
# Main
# =========================

async def main():
    setup_otel()

    pool = await asyncpg.create_pool(
        dsn=PG_DSN,
        min_size=PG_POOL_MIN,
        max_size=PG_POOL_MAX,
    )

    queue: asyncio.Queue[bytes] = asyncio.Queue(maxsize=QUEUE_MAX)

    loop = asyncio.get_running_loop()
    transport, protocol = await loop.create_datagram_endpoint(
        lambda: UDPIngestProtocol(queue),
        local_addr=(UDP_HOST, UDP_PORT),
    )

    workers = [asyncio.create_task(worker_loop(i, queue, pool)) for i in range(WORKERS)]

    print(f"[OK] UDP listening on {UDP_HOST}:{UDP_PORT}")
    print(f"[OK] PostgreSQL DSN: {PG_DSN}")
    print("[OK] Press Ctrl+C to stop.")

    try:
        await asyncio.gather(*workers)
    except asyncio.CancelledError:
        pass
    finally:
        transport.close()
        await pool.close()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
