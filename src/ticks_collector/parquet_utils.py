import os
import pickle
import threading
from datetime import datetime, timezone
import queue

import pyarrow as pa
import pyarrow.parquet as pq
import logging

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
        os.makedirs(self.today_partition_dir, exist_ok=False)

        self.schema = schema

        self.compression = compression

        self.writer: pq.ParquetWriter = pq.ParquetWriter(
            os.path.join(self.today_partition_dir, "ticks.parquet"),
            self.schema,
            compression=self.compression,
        )
        self.is_open = True
        self.rows_written = 0

        self._queue = queue.Queue()
        self._thread = threading.Thread(target=self._write_worker, daemon=True)
        self._thread.start()

    # -----------------------
    # Public API
    # -----------------------
    def write_rows(self, rows: list[dict]):
        """Add multiple rows at once (list of dicts)."""
        if not self.is_open:
            raise RuntimeError("Parquet writer is not open.")
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

                # Sentinel to stop the thread
                if rows is None:
                    logger.info("Stopping Parquet writer thread.")
                    break

                table: pa.Table = pa.Table.from_pylist(rows, schema=self.schema)
                self.writer.write_table(table)
                self.rows_written += table.num_rows
                logger.info(f"Wrote {table.num_rows} rows to Parquet.")
            except Exception:
                logger.exception(f"Error writing rows to Parquet (batch_size={len(rows) if rows else 0})")
            finally:
                self._queue.task_done()
