import os
from datetime import datetime
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from ticks_collector.parquet_utils import StreamingParquetWriter


def today_partition_dir(base_path: str) -> str:
    today = datetime.now().strftime("%Y-%m-%d")
    return os.path.join(base_path, f"date={today}")


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


def make_rows(n, start=0):
    now = datetime.now()
    rows = []
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
                "name": f"name-{i}",
            }
        )
    return rows


def ticks_file_path(partition_dir: str) -> str:
    return os.path.join(partition_dir, "ticks.parquet")


def assert_compression_is(path: str, expected: str):
    pf = pq.ParquetFile(path)
    comp = pf.metadata.row_group(0).column(0).compression
    assert comp.upper() == expected.upper(), f"Expected compression {expected}, got {comp}"


def test_partition_dir_created_and_writes_after_close(tmp_path):
    base = str(tmp_path)
    writer = StreamingParquetWriter(
        base_path=base,
        schema=schema_sample(),
        compression="ZSTD",
    )

    partition_dir = writer.today_partition_dir
    assert os.path.isdir(partition_dir), "Partition directory not created"

    writer.write_rows([make_rows(1)[0]])
    writer.write_rows([make_rows(1, start=1)[0]])
    writer.close()

    path = ticks_file_path(partition_dir)
    assert os.path.exists(path)
    table = pq.read_table(path)
    assert table.num_rows == 2
    assert_compression_is(path, "ZSTD")


def test_single_batch_writes_file(tmp_path):
    base = str(tmp_path)
    writer = StreamingParquetWriter(
        base_path=base,
        schema=schema_sample(),
        compression="ZSTD",
    )

    rows = make_rows(3)
    writer.write_rows(rows)
    writer.close()

    path = ticks_file_path(writer.today_partition_dir)
    table = pq.read_table(path)
    assert table.num_rows == 3
    assert_compression_is(path, "ZSTD")


def test_multiple_batches_accumulate(tmp_path):
    base = str(tmp_path)
    writer = StreamingParquetWriter(
        base_path=base,
        schema=schema_sample(),
        compression="ZSTD",
    )

    for i in range(5):
        writer.write_rows(make_rows(2, start=i * 2))
    writer.close()

    path = ticks_file_path(writer.today_partition_dir)
    table = pq.read_table(path)
    assert table.num_rows == 10


def test_idempotent_close(tmp_path):
    base = str(tmp_path)
    writer = StreamingParquetWriter(
        base_path=base,
        schema=schema_sample(),
        compression="ZSTD",
    )

    writer.write_rows(make_rows(3))
    writer.close()
    writer.close()  # should be safe

    path = ticks_file_path(writer.today_partition_dir)
    table = pq.read_table(path)
    assert table.num_rows == 3


def test_empty_write_rows_creates_empty_file(tmp_path):
    base = str(tmp_path)
    writer = StreamingParquetWriter(
        base_path=base,
        schema=schema_sample(),
        compression="ZSTD",
    )
    writer.write_rows([])
    writer.close()

    path = ticks_file_path(writer.today_partition_dir)
    table = pq.read_table(path)
    assert table.num_rows == 0


def test_round_trip_field_values(tmp_path):
    base = str(tmp_path)
    rows = make_rows(7)
    writer = StreamingParquetWriter(
        base_path=base,
        schema=schema_sample(),
        compression="ZSTD",
    )
    writer.write_rows(rows[:4])
    writer.write_rows(rows[4:])
    writer.close()

    path = ticks_file_path(writer.today_partition_dir)
    table = pq.read_table(path)

    lp = table.column("last_price").to_pylist()
    assert lp[:3] == [rows[0]["last_price"], rows[1]["last_price"], rows[2]["last_price"]]

    ohlc = table.column("ohlc").to_pylist()
    assert ohlc[0]["high"] == rows[0]["ohlc"]["high"]

    depth = table.column("depth").to_pylist()
    assert depth[0]["buy"][0]["orders"] == rows[0]["depth"]["buy"][0]["orders"]


def test_large_batch_single_file(tmp_path):
    base = str(tmp_path)
    writer = StreamingParquetWriter(
        base_path=base,
        schema=schema_sample(),
        compression="SNAPPY",
    )

    rows = make_rows(135)
    writer.write_rows(rows)
    writer.close()

    path = ticks_file_path(writer.today_partition_dir)
    table = pq.read_table(path)
    assert table.num_rows == 135
    assert_compression_is(path, "SNAPPY")


def test_writer_is_closed_after_close(tmp_path):
    base = str(tmp_path)
    writer = StreamingParquetWriter(
        base_path=base,
        schema=schema_sample(),
        compression="ZSTD",
    )

    # No writes necessary; just ensure resources close properly
    writer.close()

    # ParquetWriter exposes `is_open`; should be False after close
    assert hasattr(writer, "writer"), "Writer should have underlying ParquetWriter"
    assert not writer.writer.is_open, "Underlying ParquetWriter should be closed after close()"
