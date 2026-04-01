import os
import unittest
from unittest.mock import MagicMock, patch

import backup_client


class TestBackupClient(unittest.TestCase):
    def test_large_file_is_compressed_before_upload(self):
        response = MagicMock()
        response.__enter__.return_value = response
        response.__exit__.return_value = False
        response.getcode.return_value = 200

        large_bytes = b"a" * (10 * 1024 * 1024 + 1)

        with patch.dict(
            os.environ,
            {
                "FILE_BACKUP_ENABLED": "true",
                "FILE_STORAGE_API_KEY": "test-key",
                "FILE_BACKUP_PROJECT_ID": "bi-data",
                "FILE_API_BASE": "https://api.tstwg.cn/api",
            },
            clear=False,
        ), patch("backup_client.urllib.request.urlopen", return_value=response) as mocked_open:
            result = backup_client.backup_uploaded_file("big.xlsx", large_bytes, note="数据备份")

        self.assertTrue(result)
        req = mocked_open.call_args.args[0]
        body = req.data
        self.assertIn(b'filename="big.xlsx.gz"', body)
        self.assertIn(b"\x1f\x8b\x08", body)

    def test_upload_disabled_returns_false(self):
        with patch.dict(
            os.environ,
            {
                "FILE_BACKUP_ENABLED": "false",
                "FILE_STORAGE_API_KEY": "k",
                "FILE_BACKUP_PROJECT_ID": "bi-data",
            },
            clear=False,
        ):
            result = backup_client.backup_uploaded_file("a.xlsx", b"123")
        self.assertFalse(result)

    def test_upload_success_builds_request(self):
        response = MagicMock()
        response.__enter__.return_value = response
        response.__exit__.return_value = False
        response.getcode.return_value = 200

        with patch.dict(
            os.environ,
            {
                "FILE_BACKUP_ENABLED": "true",
                "FILE_STORAGE_API_KEY": "test-key",
                "FILE_BACKUP_PROJECT_ID": "bi-data",
                "FILE_API_BASE": "https://api.tstwg.cn/api",
                "FILE_BACKUP_TIMEOUT_SEC": "8",
            },
            clear=False,
        ), patch("backup_client.urllib.request.urlopen", return_value=response) as mocked_open:
            result = backup_client.backup_uploaded_file("源数据.xlsx", b"excel-bytes", note="数据备份")

        self.assertTrue(result)
        req = mocked_open.call_args.args[0]
        self.assertEqual(req.full_url, "https://api.tstwg.cn/api/upload/bi-data")
        self.assertEqual(req.get_method(), "POST")
        self.assertEqual(req.get_header("X-api-key"), "test-key")
        self.assertIn("multipart/form-data", req.get_header("Content-type"))
        body = req.data
        self.assertIn("name=\"keep_name\"".encode("utf-8"), body)
        self.assertIn("name=\"note\"".encode("utf-8"), body)
        self.assertIn("name=\"file\"; filename=\"".encode("utf-8"), body)

    def test_missing_api_key_returns_false(self):
        with patch.dict(
            os.environ,
            {
                "FILE_BACKUP_ENABLED": "true",
                "FILE_STORAGE_API_KEY": "",
                "FILE_BACKUP_PROJECT_ID": "bi-data",
            },
            clear=False,
        ):
            result = backup_client.backup_uploaded_file("a.xlsx", b"123")
        self.assertFalse(result)


if __name__ == "__main__":
    unittest.main()
