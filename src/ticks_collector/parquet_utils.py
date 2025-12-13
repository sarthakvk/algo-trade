import os
import pickle
import threading
from datetime import datetime, timezone
import queue

import pyarrow as pa
import pyarrow.parquet as pq
import logging
import uuid
from collections import deque
from typing import Deque

logger = logging.getLogger(__name__)


def get_tick_schema() -> pa.Schema:
    """Returns the schema for tick data."""
    schema_path = os.path.join(
        os.path.dirname(os.path.dirname(__file__)), "tick_schema.pkl"
    )
    with open(schema_path, "rb") as f:
        return pickle.load(f)


def get_today_ds(tz: timezone | None = None) -> str:
    """Returns today's date string in YYYY-MM-DD format."""
    return f"date={(datetime.now(tz=tz).strftime('%Y-%m-%d'))}"


class StreamingParquetWriter:
    """
    Simple streaming writer that accepts rows (dicts) and writes Parquet part files automatically.

    Note: This implementation only partitions by current date (i.e., all data goes to today's partition).
    """

    def __init__(
        self,
        base_path: str,
        schema: pa.Schema,
        compression: str = "ZSTD",
        batch_size: int = 50000,
    ):
        """
        Args:
            base_path: folder where `date=YYYY-MM-DD/` directories will be created.
            schema: optional pyarrow.Schema to enforce column order & types. If None, inferred from first batch.
            compression: e.g., "ZSTD", "SNAPPY", "GZIP"
        """
        self.base_path = base_path
        os.makedirs(self.base_path, exist_ok=True)
        self.today_partition_dir = os.path.join(
            self.base_path,
            get_today_ds(),
        )
        # Only 1 writer for a day's partition
        os.makedirs(self.today_partition_dir, exist_ok=False)

        self.schema = schema

        self.compression = compression

        self.writer: pq.ParquetWriter = pq.ParquetWriter(
            os.path.join(self.today_partition_dir, f"ticks.parquet"),
            self.schema,
            compression=self.compression,
        )
        self.is_open = True
        self.rows_written = 0
        self._batch_size = batch_size
        self._buffer: Deque[dict] = deque()
        
        # Add Backpressure queue to limit memory usage
        self._queue = queue.Queue(maxsize=batch_size * 5)
        self._thread = threading.Thread(target=self._write_worker, daemon=True)
        self._thread.start()

    # -----------------------
    # Public API
    # -----------------------
    def write_rows(self, rows: list[dict]):
        """Add multiple rows at once (list of dicts)."""
        if not self.is_open:
            raise RuntimeError("Parquet writer is not open.")
        if self._queue.full():
            logger.warning("Parquet writer queue is full; waiting to enqueue rows.")
        self._queue.put(rows)

    def close(self):
        """Flush remaining, close writer. After this you can optionally run a merge job."""
        self.is_open = False
        self._queue.put(None)  # Sentinel to stop the thread
        self._thread.join()
        self._close_writer()

    # -----------------------
    # Internal helpers
    # -----------------------

    def _close_writer(self):
        if self.writer.is_open:
            self.writer.close()
            logger.info("Parquet writer closed.")
        else:
            logger.warning("Parquet writer already closed.")

    def _write_worker(self):
        """
        Thread worker to consume rows from the queue and write to Parquet.
        """
        while True:
            rows = self._queue.get()
            try:
                # Sentinel to stop the thread: flush remaining buffer then exit
                if rows is None:
                    if self._buffer:
                        # Flush remaining buffer in final partial batch
                        table: pa.Table = pa.Table.from_pylist(
                            self._buffer, schema=self.schema
                        )
                        self.writer.write_table(table)
                        self.rows_written += table.num_rows
                        self._buffer.clear()
                    logger.info("Stopping Parquet writer thread.")
                    break

                # Accumulate rows and flush exactly batch_size at a time to keep memory bounded
                if rows is not None:
                    if len(rows) == 0:
                        # Write explicit empty batch to ensure empty file is readable
                        empty_table: pa.Table = pa.Table.from_pylist(
                            [], schema=self.schema
                        )
                        self.writer.write_table(empty_table)
                    else:
                        self._buffer.extend(rows)
                        if len(self._buffer) >= self._batch_size:
                            table: pa.Table = pa.Table.from_pylist(
                                self._buffer, schema=self.schema
                            )
                            self.writer.write_table(table)
                            self.rows_written += table.num_rows
                            self._buffer.clear()
                            logger.info(
                                f"Wrote {table.num_rows} rows to Parquet file."
                            )
            except Exception:
                logger.exception(
                    f"Error writing rows to Parquet (incoming_rows={len(rows) if rows is not None else 0}, buffer_size={len(self._buffer)})"
                )
            finally:
                self._queue.task_done()
