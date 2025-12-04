import os
import sys
import pathlib
from datetime import datetime
import pickle
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from ticks_collector.parquet_utils import StreamingParquetWriter  # noqa: E402


def today_partition_dir(base_path: str) -> str:
    today = datetime.now().strftime("%Y-%m-%d")
    return os.path.join(base_path, f"date={today}")


def schema_sample() -> pa.Schema:
    # Actual nested schema
    return pa.schema([
        pa.field("tradable", pa.bool_()),
        pa.field("mode", pa.string()),
        pa.field("instrument_token", pa.int64()),
        pa.field("last_price", pa.float64()),
        pa.field(
            "ohlc",
            pa.struct([
                pa.field("close", pa.float64()),
                pa.field("high", pa.float64()),
                pa.field("low", pa.float64()),
                pa.field("open", pa.float64()),
            ])
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
            pa.struct([
                pa.field(
                    "buy",
                    pa.list_(
                        pa.struct([
                            pa.field("orders", pa.int64()),
                            pa.field("price", pa.float64()),
                            pa.field("quantity", pa.int64()),
                        ])
                    )
                ),
                pa.field(
                    "sell",
                    pa.list_(
                        pa.struct([
                            pa.field("orders", pa.int64()),
                            pa.field("price", pa.float64()),
                            pa.field("quantity", pa.int64()),
                        ])
                    )
                ),
            ])
        ),
        pa.field("name", pa.string()),
    ])


def make_rows(n, start=0):
    from datetime import datetime
    rows = []
    now = datetime.now()
    for i in range(start, start + n):
        rows.append({
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
                    {"orders": 10 + i, "price": float(i) + 99.5, "quantity": 1 + (i % 3)},
                    {"orders": 11 + i, "price": float(i) + 99.0, "quantity": 2 + (i % 3)},
                ],
                "sell": [
                    {"orders": 12 + i, "price": float(i) + 100.5, "quantity": 1 + (i % 3)},
                    {"orders": 13 + i, "price": float(i) + 101.0, "quantity": 2 + (i % 3)},
                ],
            },
            "name": f"name-{i}",
        })
    return rows


def list_parts(partition_dir: str):
    if not os.path.exists(partition_dir):
        return []
    return sorted(
        [
            os.path.join(partition_dir, f)
            for f in os.listdir(partition_dir)
            if f.startswith("part-") and f.endswith(".parquet")
        ]
    )


def list_tmp_parts(partition_dir: str):
    if not os.path.exists(partition_dir):
        return []
    return sorted(
        [
            os.path.join(partition_dir, f)
            for f in os.listdir(partition_dir)
            if f.startswith("part-") and f.endswith(".parquet.tmp")
        ]
    )


def read_all_tables(partition_dir: str):
    tables = []
    for path in list_parts(partition_dir):
        tables.append(pq.read_table(path))
    return tables


def assert_tables_row_count(tables, expected):
    total = sum(t.num_rows for t in tables)
    assert total == expected, f"Expected {expected} rows, found {total}"


def assert_no_tmp_files(partition_dir: str):
    tmps = list_tmp_parts(partition_dir)
    assert not tmps, f"Unexpected tmp files remaining: {tmps}"


def assert_compression_is(path: str, expected: str):
    pf = pq.ParquetFile(path)
    # Check compression of first column in first row group
    comp = pf.metadata.row_group(0).column(0).compression
    assert comp.upper() == expected.upper(), f"Expected compression {expected}, got {comp}"


def test_partition_dir_created_and_no_files_before_flush(tmp_path):
    base = str(tmp_path)
    writer = StreamingParquetWriter(
        base_path=base, schema=schema_sample(), rows_per_part=10, flush_batch_rows=5, compression="ZSTD"
    )

    partition_dir = writer.today_partition_dir
    assert os.path.isdir(partition_dir), "Partition directory not created"

    # Add fewer rows than flush threshold: no file should be created yet
    writer.write_row(make_rows(1)[0])
    writer.write_row(make_rows(1, start=1)[0])
    assert list_parts(partition_dir) == []
    assert list_tmp_parts(partition_dir) == []

    writer.close()
    # close() flushes remaining buffer -> expect a single finalized part with 2 rows
    parts = list_parts(partition_dir)
    assert len(parts) == 1
    table = pq.read_table(parts[0])
    assert table.num_rows == 2
    assert_no_tmp_files(partition_dir)


def test_flush_and_close_writes_single_part(tmp_path):
    base = str(tmp_path)
    writer = StreamingParquetWriter(
        base_path=base, schema=schema_sample(), rows_per_part=100, flush_batch_rows=3, compression="ZSTD"
    )
    partition_dir = writer.today_partition_dir

    # Trigger a flush by reaching flush_batch_rows
    rows = make_rows(3)
    for r in rows:
        writer.write_row(r)

    # After automatic flush, tmp file should exist (writer not closed yet)
    tmps = list_tmp_parts(partition_dir)
    assert len(tmps) == 1, "Expected a single tmp part file after flush"
    assert os.path.exists(tmps[0])

    # Explicit flush again should be noop for file state
    writer.flush()
    assert len(list_tmp_parts(partition_dir)) == 1

    writer.close()

    parts = list_parts(partition_dir)
    assert len(parts) == 1, "Expected single part after close"
    assert_no_tmp_files(partition_dir)

    # Validate content
    table = pq.read_table(parts[0])
    assert table.num_rows == 3
    # Validate compression
    assert_compression_is(parts[0], "ZSTD")


def test_rollover_creates_multiple_parts(tmp_path):
    base = str(tmp_path)
    writer = StreamingParquetWriter(
        base_path=base, schema=schema_sample(), rows_per_part=5, flush_batch_rows=4, compression="SNAPPY"
    )
    partition_dir = writer.today_partition_dir

    # 12 rows with part size 5 => parts: 5, 5, 2
    rows = make_rows(12)
    writer.write_rows(rows[:4])   # no flush to disk yet
    writer.write_rows(rows[4:8])  # triggers flush (>=4), may open writer and write 8 rows -> 5 then 3
    writer.write_rows(rows[8:12]) # triggers another flush
    writer.close()

    parts = list_parts(partition_dir)
    assert len(parts) == 3, f"Expected 3 parts, found {len(parts)}"
    assert_no_tmp_files(partition_dir)

    tables = [pq.read_table(p) for p in parts]
    assert_tables_row_count(tables, 12)

    # Check the per-part sizes
    sizes = [t.num_rows for t in tables]
    assert sizes == [5, 5, 2], f"Expected [5, 5, 2], got {sizes}"

    # Validate compression on first part
    assert_compression_is(parts[0], "SNAPPY")


def test_resumes_part_index_from_existing_files(tmp_path):
    base = str(tmp_path)
    writer = StreamingParquetWriter(
        base_path=base, schema=schema_sample(), rows_per_part=10, flush_batch_rows=2, compression="ZSTD"
    )
    partition_dir = writer.today_partition_dir

    # Pre-create existing parts: 00000, 00002, 00007
    for idx in [0, 2, 7]:
        path = os.path.join(partition_dir, f"part-{idx:05d}.parquet")
        # Write a tiny valid parquet so pyarrow can open if needed
        pq.write_table(pa.Table.from_pydict({"x": [idx]}), path)
    
    writer.part_index = writer._calc_next_part_index()


    # Write a couple rows and close
    writer.write_rows(make_rows(2))
    writer.close()

    parts = list_parts(partition_dir)
    # Should have created part-00008 as the next index
    assert any(p.endswith("part-00008.parquet") for p in parts), f"Expected part-00008.parquet, got {parts}"
    assert_no_tmp_files(partition_dir)


def test_flush_noop_when_empty(tmp_path):
    base = str(tmp_path)
    writer = StreamingParquetWriter(
        base_path=base, schema=schema_sample(), rows_per_part=10, flush_batch_rows=5, compression="ZSTD"
    )
    partition_dir = writer.today_partition_dir

    # No data yet
    writer.flush()
    assert list_parts(partition_dir) == []
    assert_no_tmp_files(partition_dir)

    writer.close()
    assert list_parts(partition_dir) == []
    assert_no_tmp_files(partition_dir)


def test_exact_boundary_rollover(tmp_path):
    base = str(tmp_path)
    writer = StreamingParquetWriter(
        base_path=base, schema=schema_sample(), rows_per_part=5, flush_batch_rows=10, compression="ZSTD"
    )
    partition_dir = writer.today_partition_dir

    # Write exactly rows_per_part, then one more to force rollover to next part
    writer.write_rows(make_rows(5))
    writer.flush()  # forces write, writer closes and finalizes part at boundary
    tmps = list_tmp_parts(partition_dir)
    assert len(tmps) == 0, "No tmp should remain after flush at exact part boundary"
    parts = list_parts(partition_dir)
    assert len(parts) == 1, "Expected one finalized part after boundary flush"

    writer.write_row(make_rows(1, start=5)[0])
    writer.close()

    parts = list_parts(partition_dir)
    assert len(parts) == 2, "Expected rollover to second part"
    tables = [pq.read_table(p) for p in parts]
    sizes = [t.num_rows for t in tables]
    assert sizes == [5, 1], f"Expected [5,1], got {sizes}"
    assert_no_tmp_files(partition_dir)


def test_multiple_small_flushes_accumulate(tmp_path):
    base = str(tmp_path)
    writer = StreamingParquetWriter(
        base_path=base, schema=schema_sample(), rows_per_part=100, flush_batch_rows=3, compression="ZSTD"
    )
    partition_dir = writer.today_partition_dir

    # 5 small batches; automatic flush may happen when threshold reached across calls
    for i in range(5):
        writer.write_rows(make_rows(2, start=i * 2))
    assert list_parts(partition_dir) == []
    # After accumulation, threshold is crossed -> expect a tmp file
    tmps = list_tmp_parts(partition_dir)
    assert len(tmps) == 1, f"Expected one tmp part after automatic flush, got {tmps}"

    writer.close()

    parts = list_parts(partition_dir)
    assert len(parts) == 1
    table = pq.read_table(parts[0])
    assert table.num_rows == 10
    assert_no_tmp_files(partition_dir)


def test_idempotent_close_and_flush_after_close(tmp_path):
    base = str(tmp_path)
    writer = StreamingParquetWriter(
        base_path=base, schema=schema_sample(), rows_per_part=10, flush_batch_rows=2, compression="ZSTD"
    )
    partition_dir = writer.today_partition_dir

    writer.write_rows(make_rows(3))
    writer.close()
    # Calling close again should be safe and not create new files
    writer.close()
    # flush after close should be no-op
    writer.flush()

    parts = list_parts(partition_dir)
    assert len(parts) == 1
    assert_no_tmp_files(partition_dir)
    table = pq.read_table(parts[0])
    assert table.num_rows == 3


def test_empty_write_rows_is_noop(tmp_path):
    base = str(tmp_path)
    writer = StreamingParquetWriter(
        base_path=base, schema=schema_sample(), rows_per_part=10, flush_batch_rows=2, compression="ZSTD"
    )
    partition_dir = writer.today_partition_dir

    writer.write_rows([])
    writer.flush()
    writer.close()
    assert list_parts(partition_dir) == []
    assert_no_tmp_files(partition_dir)


def test_write_row_none_raises_on_flush(tmp_path):
    base = str(tmp_path)
    writer = StreamingParquetWriter(
        base_path=base, schema=schema_sample(), rows_per_part=10, flush_batch_rows=1, compression="ZSTD"
    )
    # Without input validation, pyarrow should raise during conversion on flush
    with pytest.raises(Exception):
        writer.write_row(None)  # triggers flush immediately due to threshold


def test_round_trip_field_values(tmp_path):
    base = str(tmp_path)
    rows = make_rows(7)
    writer = StreamingParquetWriter(
        base_path=base, schema=schema_sample(), rows_per_part=10, flush_batch_rows=4, compression="ZSTD"
    )
    writer.write_rows(rows[:4])
    writer.write_rows(rows[4:])
    writer.close()

    partition_dir = writer.today_partition_dir
    parts = list_parts(partition_dir)
    table = pq.read_table(parts[0])

    # Validate a subset of fields and nested structures via to_pylist for portability
    lp = table.column("last_price").to_pylist()
    assert lp[:3] == [rows[0]["last_price"], rows[1]["last_price"], rows[2]["last_price"]]

    ohlc = table.column("ohlc").to_pylist()
    assert ohlc[0]["high"] == rows[0]["ohlc"]["high"]

    depth = table.column("depth").to_pylist()
    assert depth[0]["buy"][0]["orders"] == rows[0]["depth"]["buy"][0]["orders"]


def test_large_batch_split_across_parts(tmp_path):
    base = str(tmp_path)
    writer = StreamingParquetWriter(
        base_path=base, schema=schema_sample(), rows_per_part=50, flush_batch_rows=1000, compression="SNAPPY"
    )
    partition_dir = writer.today_partition_dir

    rows = make_rows(135)
    writer.write_rows(rows)  # single large batch triggers internal chunking
    writer.close()

    parts = list_parts(partition_dir)
    assert len(parts) == 3
    tables = [pq.read_table(p) for p in parts]
    assert_tables_row_count(tables, 135)
    sizes = [t.num_rows for t in tables]
    assert sizes == [50, 50, 35]
    # Compression on each part
    for p in parts:
        assert_compression_is(p, "SNAPPY")


def test_part_index_ignores_tmp_files(tmp_path):
    base = str(tmp_path)
    writer = StreamingParquetWriter(
        base_path=base, schema=schema_sample(), rows_per_part=10, flush_batch_rows=2, compression="ZSTD"
    )
    partition_dir = writer.today_partition_dir

    # Create finalized parts and stray tmp files
    for idx in [0, 1]:
        pq.write_table(pa.Table.from_pydict({"x": [idx]}), os.path.join(partition_dir, f"part-{idx:05d}.parquet"))
    for stray in [3, 5]:
        open(os.path.join(partition_dir, f"part-{stray:05d}.parquet.tmp"), "w").close()
    
    writer.part_index = writer._calc_next_part_index()

    writer.write_rows(make_rows(2))
    assert len(writer.buffer) == 0
    writer.close()

    parts = list_parts(partition_dir)
    # Next index after 00001 should be 00002, ignoring tmp files
    assert any(p.endswith("part-00002.parquet") for p in parts), f"Expected part-00002.parquet, got {parts}"
    # Implementation does not clean pre-existing stray tmp files; they may remain
    tmps = list_tmp_parts(partition_dir)
    assert set(os.path.basename(p) for p in tmps) >= {"part-00003.parquet.tmp", "part-00005.parquet.tmp"}
