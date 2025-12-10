#!/usr/bin/env python3
import argparse
import os
import sys
import threading
import time
import math
import random
from datetime import datetime
from typing import List, Dict, Optional

try:
    import psutil  # type: ignore
except Exception:  # pragma: no cover
    psutil = None  # optional

try:
    import resource  # type: ignore
except Exception:  # pragma: no cover
    resource = None

import pyarrow as pa
import pyarrow.parquet as pq

from ticks_collector.parquet_utils import StreamingParquetWriter


def schema_sample() -> pa.Schema:
    return pa.schema(
        [
            pa.field("tradable", pa.bool_()),
            pa.field("mode", pa.string()),
            pa.field("instrument_token", pa.int64()),
            pa.field("last_price", pa.float64()),
            pa.field(
                "ohlc",
                pa.struct(
                    [
                        pa.field("close", pa.float64()),
                        pa.field("high", pa.float64()),
                        pa.field("low", pa.float64()),
                        pa.field("open", pa.float64()),
                    ]
                ),
            ),
            pa.field("change", pa.float64()),
            pa.field("exchange_timestamp", pa.timestamp("ns")),
            pa.field("last_traded_quantity", pa.float64()),
            pa.field("average_traded_price", pa.float64()),
            pa.field("volume_traded", pa.float64()),
            pa.field("total_buy_quantity", pa.float64()),
            pa.field("total_sell_quantity", pa.float64()),
            pa.field("last_trade_time", pa.timestamp("ns")),
            pa.field("oi", pa.float64()),
            pa.field("oi_day_high", pa.float64()),
            pa.field("oi_day_low", pa.float64()),
            pa.field(
                "depth",
                pa.struct(
                    [
                        pa.field(
                            "buy",
                            pa.list_(
                                pa.struct(
                                    [
                                        pa.field("orders", pa.int64()),
                                        pa.field("price", pa.float64()),
                                        pa.field("quantity", pa.int64()),
                                    ]
                                )
                            ),
                        ),
                        pa.field(
                            "sell",
                            pa.list_(
                                pa.struct(
                                    [
                                        pa.field("orders", pa.int64()),
                                        pa.field("price", pa.float64()),
                                        pa.field("quantity", pa.int64()),
                                    ]
                                )
                            ),
                        ),
                    ]
                ),
            ),
            pa.field("name", pa.string()),
        ]
    )


def make_rows(n: int, start: int = 0, name_len: int = 8) -> List[Dict]:
    now = datetime.now()
    rows: List[Dict] = []
    for i in range(start, start + n):
        rows.append(
            {
                "tradable": (i % 2 == 0),
                "mode": "full",
                "instrument_token": 100000 + i,
                "last_price": float(i) + 100.0,
                "ohlc": {
                    "close": float(i) + 1.0,
                    "high": float(i) + 2.0,
                    "low": float(i) - 1.0,
                    "open": float(i) + 0.5,
                },
                "change": 0.01 * i,
                "exchange_timestamp": now,
                "last_traded_quantity": float((i % 10) + 1),
                "average_traded_price": float(i) + 0.75,
                "volume_traded": float(i * 10),
                "total_buy_quantity": float(i * 5 + 10),
                "total_sell_quantity": float(i * 5 + 8),
                "last_trade_time": now,
                "oi": float(1000 + i),
                "oi_day_high": float(1100 + i),
                "oi_day_low": float(900 + i),
                "depth": {
                    "buy": [
                        {
                            "orders": 10 + i,
                            "price": float(i) + 99.5,
                            "quantity": 1 + (i % 3),
                        },
                        {
                            "orders": 11 + i,
                            "price": float(i) + 99.0,
                            "quantity": 2 + (i % 3),
                        },
                    ],
                    "sell": [
                        {
                            "orders": 12 + i,
                            "price": float(i) + 100.5,
                            "quantity": 1 + (i % 3),
                        },
                        {
                            "orders": 13 + i,
                            "price": float(i) + 101.0,
                            "quantity": 2 + (i % 3),
                        },
                    ],
                },
                "name": ("name-" + str(i) + "-" + ("x" * name_len))[:64],
            }
        )
    return rows


class Monitor:
    def __init__(self, pid: int, file_path_supplier, interval: float = 0.5):
        self.pid = pid
        self.file_path_supplier = file_path_supplier
        self.interval = interval
        self.samples = []  # (ts, rss_bytes, file_size_bytes)
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, daemon=True)

    def start(self):
        self._thread.start()

    def stop(self):
        self._stop.set()
        self._thread.join()

    def _get_rss(self) -> Optional[int]:
        try:
            if psutil:
                p = psutil.Process(self.pid)
                return p.memory_info().rss
            if resource:  # fallback
                usage = resource.getrusage(resource.RUSAGE_SELF)
                # macOS reports bytes, Linux usually KiB
                if sys.platform == "darwin":
                    return int(usage.ru_maxrss)
                else:
                    return int(usage.ru_maxrss * 1024)
        except Exception:
            return None
        return None

    def _get_file_size(self) -> int:
        try:
            path = self.file_path_supplier()
            if path and os.path.exists(path):
                return os.path.getsize(path)
        except Exception:
            pass
        return 0

    def _run(self):
        while not self._stop.is_set():
            rss = self._get_rss()
            fsz = self._get_file_size()
            self.samples.append((time.time(), rss, fsz))
            time.sleep(self.interval)

    def stats(self):
        rss_values = [s[1] for s in self.samples if s[1] is not None]
        file_sizes = [s[2] for s in self.samples]
        return {
            "rss_max": max(rss_values) if rss_values else None,
            "rss_avg": (sum(rss_values) / len(rss_values)) if rss_values else None,
            "file_size_last": file_sizes[-1] if file_sizes else 0,
        }


def ticks_file_path(partition_dir: str) -> str:
    return os.path.join(partition_dir, "ticks.parquet")


essential_compressions = {
    "ZSTD": "ZSTD",
    "SNAPPY": "SNAPPY",
    "GZIP": "GZIP",
    "NONE": None,
}


def run_benchmark(
    base_path: str,
    rows_per_sec: float,
    producers: int,
    batch_size: int,
    duration: Optional[float],
    total_rows: Optional[int],
    compression: str,
    name_len: int,
    sample_interval: float,
):
    assert (duration is None) ^ (total_rows is None), "Specify either duration or total_rows"

    schema = schema_sample()

    # Create unique run directory to avoid 'exist_ok=False' error
    stamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    run_base = os.path.join(base_path, f"perf-{stamp}")
    os.makedirs(run_base, exist_ok=True)

    writer = StreamingParquetWriter(
        base_path=run_base,
        schema=schema,
        compression=compression,
    )

    part_dir = writer.today_partition_dir
    parquet_path = ticks_file_path(part_dir)

    # Monitor process RSS and file size
    mon = Monitor(os.getpid(), file_path_supplier=lambda: parquet_path, interval=sample_interval)
    mon.start()

    # Additional consumer metrics sampled over time
    consumer_samples = []  # (ts, rows_written, backlog)

    start_ts = time.perf_counter()
    end_deadline = None
    if duration is not None:
        end_deadline = start_ts + duration

    target_rows = total_rows
    rows_written_attempted = 0

    # per-thread rate
    thread_rows_per_sec = rows_per_sec / max(1, producers)
    batches_per_sec = max(1e-6, thread_rows_per_sec / max(1, batch_size))
    period = 1.0 / batches_per_sec

    stop_event = threading.Event()

    def producer_loop(tid: int):
        nonlocal rows_written_attempted
        next_deadline = time.perf_counter()
        local_i = tid * 1_000_000
        # latency sampling (first few batches per thread)
        latency_samples_to_collect = 5
        collected = 0
        while not stop_event.is_set():
            if end_deadline is not None and time.perf_counter() >= end_deadline:
                break
            if target_rows is not None and rows_written_attempted >= target_rows:
                break

            # create batch
            batch = make_rows(batch_size, start=local_i, name_len=name_len)
            local_i += batch_size

            try:
                before = writer.rows_written
                writer.write_rows(batch)
                rows_written_attempted += len(batch)
                # Measure latency until consumer thread writes this batch
                if collected < latency_samples_to_collect:
                    t0 = time.perf_counter()
                    deadline = t0 + 5.0  # max wait
                    expected = before + len(batch)
                    while time.perf_counter() < deadline:
                        if writer.rows_written >= expected:
                            break
                        time.sleep(0.005)
                    t1 = time.perf_counter()
                    collected += 1
                    consumer_samples.append((t1, writer.rows_written, rows_written_attempted - writer.rows_written, t1 - t0))
            except Exception as e:
                # If closed or error, stop
                stop_event.set()
                break

            # sleep to maintain rate
            next_deadline += period
            now = time.perf_counter()
            sleep_for = next_deadline - now
            if sleep_for > 0:
                time.sleep(sleep_for)
            else:
                # if we're behind schedule, realign
                next_deadline = now

    threads = [threading.Thread(target=producer_loop, args=(i,), daemon=True) for i in range(producers)]
    for t in threads:
        t.start()

    # Join threads
    for t in threads:
        t.join()

    # Close writer and stop monitor
    writer.close()
    mon.stop()

    elapsed = time.perf_counter() - start_ts
    stats = mon.stats()

    # Derive results
    effective_rows = writer.rows_written
    rps = effective_rows / elapsed if elapsed > 0 else 0.0
    # Consumer throughput over samples
    consumer_rates = []
    backlog_sizes = []
    latencies = []
    # Build derived samples at monitor cadence
    if consumer_samples:
        # sort by timestamp
        consumer_samples.sort(key=lambda x: x[0])
        prev_ts = None
        prev_rows = None
        for s in consumer_samples:
            ts = s[0]
            rows = s[1]
            backlog = s[2]
            latency = s[3] if len(s) > 3 else None
            backlog_sizes.append(backlog)
            if latency is not None:
                latencies.append(latency)
            if prev_ts is not None:
                dt = ts - prev_ts
                dr = rows - prev_rows
                if dt > 0:
                    consumer_rates.append(dr / dt)
            prev_ts = ts
            prev_rows = rows

    def pct(values, p):
        if not values:
            return None
        arr = sorted(values)
        idx = max(0, min(len(arr) - 1, int(math.floor(p * (len(arr) - 1)))))
        return arr[idx]
    file_size = stats.get("file_size_last") or (os.path.getsize(parquet_path) if os.path.exists(parquet_path) else 0)
    mbps = (file_size / (1024 * 1024)) / elapsed if elapsed > 0 else 0.0

    def fmt_bytes(n):
        if n is None:
            return "n/a"
        return f"{n / (1024*1024):.2f} MiB"

    print("\n=== StreamingParquetWriter Performance Summary ===")
    print(f"Base path: {run_base}")
    print(f"Compression: {compression}")
    print(f"Producers: {producers}, Batch size: {batch_size}")
    if duration is not None:
        print(f"Duration: {duration:.2f} s")
    if total_rows is not None:
        print(f"Total rows target: {total_rows}")
    print(f"Attempted rows: {rows_written_attempted}")
    print(f"Written rows: {effective_rows}")
    print(f"Elapsed: {elapsed:.3f} s")
    print(f"Throughput: {rps:,.0f} rows/s")
    print(f"Parquet size: {file_size:,} bytes ({file_size/(1024*1024):.2f} MiB)")
    print(f"Write speed: {mbps:.2f} MiB/s")
    print(f"RSS max: {fmt_bytes(stats.get('rss_max'))}, RSS avg: {fmt_bytes(stats.get('rss_avg'))}")
    # Consumer-centric metrics
    if consumer_rates:
        print(f"Consumer rows/s avg: {sum(consumer_rates)/len(consumer_rates):.0f}")
        print(f"Consumer rows/s p50: {pct(consumer_rates, 0.5):.0f}, p95: {pct(consumer_rates, 0.95):.0f}")
    if backlog_sizes:
        print(f"Backlog max: {max(backlog_sizes)} rows, p95: {pct(backlog_sizes, 0.95)}")
    if latencies:
        print(f"Batch write latency p50: {pct(latencies, 0.5):.3f}s, p95: {pct(latencies, 0.95):.3f}s")


def main():
    parser = argparse.ArgumentParser(description="Benchmark StreamingParquetWriter throughput and memory usage")
    g = parser.add_mutually_exclusive_group(required=True)
    g.add_argument("--duration", type=float, help="Run time in seconds")
    g.add_argument("--total-rows", type=int, help="Total rows to send across all producers")

    parser.add_argument("--rows-per-sec", type=float, default=10000, help="Target aggregate rows per second")
    parser.add_argument("--producers", type=int, default=1, help="Number of concurrent producer threads")
    parser.add_argument("--batch-size", type=int, default=100, help="Rows per write_rows call")
    parser.add_argument("--compression", type=str, default="ZSTD", choices=["ZSTD", "SNAPPY", "GZIP", "NONE"], help="Parquet compression")
    parser.add_argument("--base-path", type=str, default=None, help="Base directory for parquet output (default: temp under ./bench_output)")
    parser.add_argument("--name-len", type=int, default=8, help="String length for 'name' field")
    parser.add_argument("--sample-interval", type=float, default=0.5, help="Monitor sampling interval in seconds")

    args = parser.parse_args()

    base_path = args.base_path
    if not base_path:
        base_path = os.path.join(os.getcwd(), "bench_output")
    os.makedirs(base_path, exist_ok=True)

    compression = args.compression
    if compression == "NONE":
        compression = None  # type: ignore

    run_benchmark(
        base_path=base_path,
        rows_per_sec=args.rows_per_sec,
        producers=args.producers,
        batch_size=args.batch_size,
        duration=args.duration,
        total_rows=args.total_rows,
        compression=compression,  # type: ignore
        name_len=args.name_len,
        sample_interval=args.sample_interval,
    )


if __name__ == "__main__":
    main()
