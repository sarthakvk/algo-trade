import os
import pickle
import re
import threading
import time
import math
import tempfile
from datetime import datetime, timezone
from typing import List, Dict, Optional

import pyarrow as pa
import pyarrow.parquet as pq
import logging

logger = logging.getLogger(__name__)

def get_tick_schema() -> pa.Schema:
    """Returns the schema for tick data."""
    return pickle.load(open("./tick_schema.pkl", "rb"))


class StreamingParquetWriter:
    """
    Simple streaming writer that accepts rows (dicts) and writes Parquet part files automatically.

    Note: This implementation only partitions by current date (i.e., all data goes to today's partition).
    """

    writer_counter = 0
    _lock = threading.Lock()

    def __init__(
        self,
        base_path: str,
        schema: pa.Schema,
        rows_per_part: int = 200_000,
        flush_batch_rows: int = 10_000,
        compression: str = "ZSTD",
    ):
        """
        Args:
            base_path: folder where `date=YYYY-MM-DD/` directories will be created.
            schema: optional pyarrow.Schema to enforce column order & types. If None, inferred from first batch.
            rows_per_part: how many rows per part file before starting a new part for the same date.
            flush_batch_rows: when buffer reaches this, it is flushed to disk (as a partial write).
            compression: e.g., "ZSTD", "SNAPPY", "GZIP"
        """
        self.base_path = base_path
        os.makedirs(self.base_path, exist_ok=True)

        with StreamingParquetWriter._lock:
            self.today_partition_dir = os.path.join(
                self.base_path,
                f"date={(datetime.now().strftime('%Y-%m-%d'))}",
                f"writer_id={StreamingParquetWriter.writer_counter}",
            )
            StreamingParquetWriter.writer_counter += 1
        os.makedirs(self.today_partition_dir, exist_ok=True)

        self.schema = schema
        self.rows_per_part = rows_per_part
        self.flush_batch_rows = flush_batch_rows

        self.compression = compression

        # runtime state
        self.buffer: List[Dict] = []
        self.writer: Optional[pq.ParquetWriter] = None
        self.rows_written = 0
        self.part_index = self._calc_next_part_index()

    # -----------------------
    # Public API
    # -----------------------
    def write_row(self, row: Dict):
        """Add single row (dict) to buffer and flush if needed."""
        self.buffer.append(row)
        if len(self.buffer) >= self.flush_batch_rows:
            self._flush_buffer()

    def write_rows(self, rows: List[Dict]):
        """Add multiple rows at once (list of dicts)."""
        self.buffer.extend(rows)
        if len(self.buffer) >= self.flush_batch_rows:
            logger.info(f"Flushing {len(self.buffer)} rows to Parquet.")
            self._flush_buffer()

    def flush(self):
        """Force flush buffer to disk (partial write)."""
        if self.buffer:
            self._flush_buffer()

    def close(self):
        """Flush remaining, close writer. After this you can optionally run a merge job."""
        self.flush()
        self._close_writer()
        logger.info("Parquet writer closed.")

    # -----------------------
    # Internal helpers
    # -----------------------

    def _calc_next_part_index(self) -> int:
        pat = re.compile(r"^part-(\d{5})\.parquet$")
        max_idx = -1
        try:
            for fname in os.listdir(self.today_partition_dir):
                m = pat.match(fname)
                if m:
                    idx = int(m.group(1))
                    if idx > max_idx:
                        max_idx = idx
        except FileNotFoundError:
            return 0
        return max_idx + 1

    def _next_part_filename(self):
        folder = self.today_partition_dir
        name = f"part-{self.part_index:05d}.parquet.tmp"
        return os.path.join(folder, name)

    def _init_writer(self):
        assert (
            self.schema is not None
        ), "Schema must be defined before initializing writer."
        temp_filename = self._next_part_filename()
        self.writer = pq.ParquetWriter(
            temp_filename,
            self.schema,
            compression=self.compression,
        )

    def _close_writer(self):
        if self.writer is not None:
            self.writer.close()

            temp_path = self._next_part_filename()
            os.replace(temp_path, temp_path.replace(".tmp", ""))

            self.writer = None
            self.rows_written = 0
            self.part_index += 1

            logger.info(f"Parquet writer closed for file: {temp_path.replace('.tmp', '')}")

    def _flush_buffer(self):
        if not self.buffer:
            return

        # convert buffer to Arrow Table
        table: pa.Table = pa.Table.from_pylist(self.buffer, schema=self.schema)
        self.buffer = []

        start_idx = 0
        while start_idx < table.num_rows:
            if self.writer is None:
                self._init_writer()

            capacity = self.rows_per_part - self.rows_written
            if capacity <= 0:
                # Close current part file
                self._close_writer()
                continue

            writable_rows = min(capacity, table.num_rows - start_idx)
            chunk = table.slice(start_idx, writable_rows)

            self.writer.write_table(chunk)
            self.rows_written += chunk.num_rows
            start_idx += chunk.num_rows

            if self.rows_written >= self.rows_per_part:
                # Close current part file
                self._close_writer()
