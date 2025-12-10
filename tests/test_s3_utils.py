import os
import tempfile
import pathlib
from unittest.mock import patch, MagicMock
from ticks_collector.s3_utils import upload_file_to_s3, upload_parquet_folder_to_s3
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

        upload_parquet_folder_to_s3(tmpdir)

        # Expect 3 uploads
        assert mock_upload.call_count == 3

        called_keys = [c.args[2] for c in mock_upload.call_args_list]
        # Ensure keys end with parquet file names
        assert any(k.endswith("a.parquet") for k in called_keys)
