# algo-trade

A real-time tick data collection and management system for the Indian stock market. It connects to [Zerodha's Kite](https://kite.zerodha.com/) trading API over WebSocket, streams live market ticks into date-partitioned [Apache Parquet](https://parquet.apache.org/) files, and uploads the collected data to AWS S3 for long-term storage and analysis.

## How It Works

### Architecture Overview

```
Zerodha Kite WebSocket ──► Ticker (3 connections) ──► StreamingParquetWriter ──► Parquet files ──► S3
         │                       │                            │
    NSE instruments        on_ticks callback            queue + worker thread
    (full-mode data)       distributes load             batched writes (ZSTD)
```

The system is driven by an **APScheduler** background scheduler that orchestrates a daily pipeline, exposed through a **FastAPI** REST API.

### Daily Pipeline

1. **9:10 AM IST** — The scheduler triggers the tick collection job.
2. The job checks whether today is a trading day (skips weekends and NSE holidays).
3. **9:14 AM IST** — Three parallel `KiteTicker` WebSocket connections start. All NSE instrument tokens are fetched, shuffled, and evenly distributed across the connections. Each subscribes in **full mode** (highest-frequency tick data).
4. Incoming ticks flow through an `on_ticks` callback into a **`StreamingParquetWriter`**, which uses a producer-consumer pattern with a bounded queue and a dedicated writer thread to stream rows into a single Parquet file per day.
5. **3:35 PM IST** — The connections close and the writer flushes remaining data.
6. The collected Parquet files are uploaded to S3 (`ticks-data-bucket`) with parallel `ThreadPoolExecutor` workers, verified via SHA-256 checksums, and the local copies are deleted.
7. The EC2 instance running the pipeline is stopped automatically to save costs.

### Key Components

| Component | File | Role |
|---|---|---|
| REST API | `src/server.py` | FastAPI app with endpoints to trigger jobs and inspect status |
| Scheduler | `src/scheduler.py` | APScheduler `BackgroundScheduler` configured for IST |
| Jobs | `src/jobs.py` | Tick collection job and S3 upload job definitions |
| Ticker | `src/ticks_collector/ticker.py` | Singleton managing three `KiteTicker` WebSocket connections |
| Parquet Writer | `src/ticks_collector/parquet_utils.py` | Thread-safe streaming writer with backpressure queue |
| S3 Utilities | `src/ticks_collector/s3_utils.py` | Parallel upload, SHA-256 verification, and cleanup |
| Kite Auth | `src/ticks_collector/kite_utils.py` | Zerodha login with TOTP two-factor authentication |

### Data Storage

Tick data is written in Hive-style date partitions:

```
ticks/
└── date=2025-03-15/
    └── ticks.parquet
```

The same structure is mirrored on S3 under `s3://ticks-data-bucket/ticks/`.

### API Endpoints

| Method | Path | Description |
|---|---|---|
| `POST` | `/tasks/trigger-ticks-collector` | Manually start tick collection |
| `POST` | `/tasks/trigger-s3-upload` | Upload collected data to S3 |
| `POST` | `/tasks/stop-ec2-instance` | Stop the EC2 instance |
| `GET`  | `/jobs` | List all scheduled jobs |
| `GET`  | `/jobs/{job_id}` | Get a specific job's status |

## Setup

### Prerequisites

- Python ≥ 3.9
- A [Zerodha Kite](https://kite.zerodha.com/) account with API access
- AWS credentials configured for S3 access

### Installation

```bash
pip install -e .

# For development (pytest, black)
pip install -e ".[dev]"
```

### Configuration

Create a `.env` file in the project root with your Zerodha and AWS credentials:

```
USER_ID=<zerodha_user_id>
PASSWORD=<zerodha_password>
API_KEY=<kite_api_key>
API_SECRET=<kite_api_secret>
TOTP_SECRET=<2fa_totp_secret>
```

AWS credentials should be configured through the standard AWS mechanisms (`~/.aws/credentials`, environment variables, or an IAM role).

### Running

```bash
# Start the scheduler (runs the daily pipeline automatically)
python src/scheduler.py

# Or start the FastAPI server (includes the scheduler)
uvicorn src.server:app --reload
```

### Running Tests

```bash
pytest tests/
```

## Project Structure

```
src/
├── server.py                  # FastAPI REST API
├── scheduler.py               # APScheduler setup
├── jobs.py                    # Job definitions
├── app_config.py              # Timezone and base directory config
├── logging_config.py          # Rotating file + console logging
├── utils.py                   # Historical data fetching helpers
├── tick_schema.pkl            # PyArrow schema for tick data
└── ticks_collector/
    ├── ticker.py              # WebSocket tick collection
    ├── parquet_utils.py       # Streaming Parquet writer
    ├── s3_utils.py            # S3 upload and verification
    └── kite_utils.py          # Zerodha authentication

tests/                         # pytest unit tests
scripts/                       # Performance benchmarks
notebooks/                     # Jupyter notebooks for analysis and backtesting
```
