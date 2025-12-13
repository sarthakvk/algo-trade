import os
import tempfile
import pathlib
from unittest.mock import patch, MagicMock
from ticks_collector.s3_utils import (
    upload_file_to_s3,
    upload_parquet_folder_to_s3,
    verify_parquet_folder_uploaded_to_s3,
)
from ticks_collector.ticker import TICKS_DIR


@patch("boto3.client")
def test_upload_file_to_s3(mock_boto_client):
    mock_s3 = MagicMock()
    mock_boto_client.return_value = mock_s3

    upload_file_to_s3("sample.parquet", "test-bucket", "folder/sample.parquet")

    mock_s3.upload_file.assert_called_once_with(
        "sample.parquet", "test-bucket", "folder/sample.parquet"
    )


@patch("ticks_collector.s3_utils.upload_file_to_s3")
def test_upload_parquet_folder_to_s3(mock_upload):
    # Create temp directory under TICKS_DIR so relative_to works
    with tempfile.TemporaryDirectory(dir=TICKS_DIR) as tmpdir:
        tmp_path = pathlib.Path(tmpdir)

        # create fake parquet files
        (tmp_path / "a.parquet").write_text("test")
        (tmp_path / "b.parquet").write_text("test")
        sub = tmp_path / "nested"
        sub.mkdir()
        (sub / "c.parquet").write_text("test")

        upload_parquet_folder_to_s3(tmpdir, False)

        # Expect 3 uploads
        assert mock_upload.call_count == 3

        called_keys = [c.args[2] for c in mock_upload.call_args_list]
        # Ensure keys end with parquet file names
        assert any(k.endswith("a.parquet") for k in called_keys)
        assert any(k.endswith("b.parquet") for k in called_keys)
        assert any(k.endswith("c.parquet") for k in called_keys)


@patch("boto3.client")
def test_verify_streams_remote_and_succeeds(mock_boto_client):
    # Setup mock s3 client
    mock_s3 = MagicMock()
    mock_boto_client.return_value = mock_s3

    with tempfile.TemporaryDirectory(dir=TICKS_DIR) as tmpdir:
        tmp_path = pathlib.Path(tmpdir)
        fp = tmp_path / "d.parquet"
        fp.write_bytes(b"hello parquet")

        # Upload folder (parquet files only)
        upload_parquet_folder_to_s3(tmpdir, False)
        # Prepare verification: stream remote content in chunks matching local
        class _StreamingBody:
            def iter_chunks(self, chunk_size=1024 * 1024):
                data = b"hello parquet"
                yield data[:5]
                yield data[5:]
        mock_s3.get_object.return_value = {"Body": _StreamingBody()}

        assert verify_parquet_folder_uploaded_to_s3(tmpdir) is True


@patch("boto3.client")
def test_verify_fails_on_checksum_mismatch(mock_boto_client):
    mock_s3 = MagicMock()
    mock_boto_client.return_value = mock_s3

    with tempfile.TemporaryDirectory(dir=TICKS_DIR) as tmpdir:
        tmp_path = pathlib.Path(tmpdir)
        fp = tmp_path / "e.parquet"
        fp.write_bytes(b"different content")

        # Stream wrong remote content to cause mismatch
        class _StreamingBody:
            def iter_chunks(self, chunk_size=1024 * 1024):
                yield b"something else"
        mock_s3.get_object.return_value = {"Body": _StreamingBody()}

        assert verify_parquet_folder_uploaded_to_s3(tmpdir) is False
