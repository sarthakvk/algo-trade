import os
from unittest.mock import call, patch, MagicMock
from datetime import datetime
from zoneinfo import ZoneInfo
import pytest

# Import your module
import scheduler


@pytest.fixture
def mock_scheduler():
    """create a fake scheduler object"""
    mock = MagicMock()
    mock.add_job = MagicMock()
    return mock


def test_schedule_ticker_jobs_on_trading_day(mock_scheduler):
    with patch("scheduler.is_trading_day", return_value=True), \
         patch("scheduler.Ticker") as MockTicker:

        scheduler.schedule_ticker_jobs(mock_scheduler)

        MockTicker.assert_called_once()
        assert mock_scheduler.add_job.call_count == 2  # start & stop
        calls = [c.kwargs.get("id") for c in mock_scheduler.add_job.mock_calls]
        assert set(calls) == {"start_ticker", "stop_ticker"}


def test_schedule_ticker_jobs_not_trading_day(mock_scheduler):
    with patch("scheduler.is_trading_day", return_value=False), \
         patch("scheduler.Ticker") as MockTicker:

        scheduler.schedule_ticker_jobs(mock_scheduler)

        MockTicker.assert_not_called()
        mock_scheduler.add_job.assert_not_called()


def test_schedule_ticker_jobs_immediate(mock_scheduler):
    with patch("scheduler.is_trading_day", return_value=True), \
         patch("scheduler.Ticker") as MockTicker:

        inst = MockTicker.return_value

        scheduler.schedule_ticker_jobs_immediate(mock_scheduler)

        inst.start.assert_called_once()
        # stop ticker + upload job
        assert mock_scheduler.add_job.call_count == 2


def test_upload_ticks_to_s3_calls_correct_path(tmp_path):
    fake_dir = tmp_path / "ticks/date=2025-01-01"
    fake_dir.mkdir(parents=True)

    with patch("scheduler.TICKS_DIR", str(tmp_path / "ticks")), \
         patch("scheduler.upload_parquet_folder_to_s3") as up_mock:

        scheduler.upload_ticks_to_s3()
        up_mock.assert_called_once()
        # Confirm correct folder passed
        assert str(fake_dir).split("date")[0] in up_mock.call_args.args[0]


def test_schedule_ticker_jobs_time_validation(mock_scheduler):
    with patch("scheduler.is_trading_day", return_value=True), \
         patch("scheduler.Ticker") as MockTicker:

        scheduler.schedule_ticker_jobs(mock_scheduler)

        # validate individual job call arguments
        mock_scheduler.add_job.assert_has_calls([
            call(MockTicker.return_value.start, 'cron',
                 hour=9, minute=12, second=0,
                 timezone=scheduler.ZONEINFO, id='start_ticker'),
            call(MockTicker.return_value.stop, 'cron',
                 hour=15, minute=32, second=0,
                 timezone=scheduler.ZONEINFO, id='stop_ticker'),
        ], any_order=True)


def test_schedule_ticker_jobs_immediate_time_validation(mock_scheduler):
    with patch("scheduler.is_trading_day", return_value=True), \
         patch("scheduler.Ticker") as MockTicker:

        inst = MockTicker.return_value
        scheduler.schedule_ticker_jobs_immediate(mock_scheduler)

        inst.start.assert_called_once()

        mock_scheduler.add_job.assert_has_calls([
            call(inst.stop, 'cron',
                 hour=15, minute=32, second=0,
                 timezone=scheduler.ZONEINFO, id='stop_ticker'),
            call(scheduler.upload_ticks_to_s3, 'cron',
                 hour=15, minute=35, second=0,
                 timezone=scheduler.ZONEINFO, id='upload_ticks'),
        ], any_order=True)


@patch("scheduler.BlockingScheduler")
def test_main_schedules_daily_job_immediate(MockScheduler):
    mock_sched = MockScheduler.return_value
    mock_sched.add_job = MagicMock()
    mock_sched.start.side_effect = SystemExit  # to stop after scheduling

    fake_now = datetime(2025, 1, 1, 10, 0, 0, tzinfo=ZoneInfo("Asia/Kolkata"))  # after 9:11

    with patch("scheduler.datetime") as dt, \
         patch("scheduler.is_trading_day", return_value=True), \
         patch("scheduler.schedule_ticker_jobs_immediate") as mock_immediate:

        dt.now.return_value = fake_now
        dt.side_effect = lambda *args, **kwargs: datetime(*args, **kwargs)

        # Scheduler.start() never returns, so we stop after scheduling
        with pytest.raises(SystemExit):
            scheduler.main()

        # daily scheduler cron must still be set correctly
        mock_sched.add_job.assert_any_call(
            scheduler.schedule_ticker_jobs,
            'cron',
            args=[mock_sched],
            hour=9, minute=11, second=0,
            timezone=scheduler.ZONEINFO,
            id='daily_scheduler'
        )
        mock_immediate.assert_called_once()


@patch("scheduler.BlockingScheduler")
def test_main_schedules_daily_job_no_immediate(MockScheduler):
    mock_sched = MockScheduler.return_value
    mock_sched.add_job = MagicMock()
    mock_sched.start.side_effect = SystemExit  # to stop after scheduling

    fake_now = datetime(2025, 1, 1, 8, 30, 0, tzinfo=ZoneInfo("Asia/Kolkata"))  # before 9:11

    with patch("scheduler.datetime") as dt, \
         patch("scheduler.is_trading_day", return_value=True), \
         patch("scheduler.schedule_ticker_jobs_immediate") as mock_immediate:

        dt.now.return_value = fake_now
        dt.side_effect = lambda *args, **kwargs: datetime(*args, **kwargs)

        with pytest.raises(SystemExit):
            scheduler.main()

        mock_sched.add_job.assert_any_call(
            scheduler.schedule_ticker_jobs,
            'cron',
            args=[mock_sched],
            hour=9, minute=11, second=0,
            timezone=scheduler.ZONEINFO,
            id='daily_scheduler'
        )
        mock_immediate.assert_not_called()
