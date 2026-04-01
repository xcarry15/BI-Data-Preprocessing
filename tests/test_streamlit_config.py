from pathlib import Path
import unittest


class TestStreamlitUploadLimit(unittest.TestCase):
    def test_max_upload_size_is_20mb(self):
        config_path = Path(".streamlit/config.toml")
        self.assertTrue(config_path.exists(), "缺少 .streamlit/config.toml 配置文件")

        content = config_path.read_text(encoding="utf-8")
        self.assertIn("[server]", content)
        self.assertRegex(content, r"(?m)^\s*maxUploadSize\s*=\s*20\s*$")

