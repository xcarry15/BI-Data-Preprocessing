import copy
import unittest
from datetime import datetime

import pandas as pd

from process_big_file import DEFAULT_CONFIG
from web_runtime import build_output_filename, process_dataframe


class TestWebRuntime(unittest.TestCase):
    def test_process_dataframe_detail_mode_returns_rows(self):
        df = pd.DataFrame(
            {
                "门店编码": ["S1", "S1"],
                "开业时间": ["2026/01/01", "2026/01/01"],
                "监控日期": ["2026/01/02", "2026/01/03"],
                "审批状态": [1, 1],
                "盈利性判断": [0, 1],
            }
        )

        cfg = copy.deepcopy(DEFAULT_CONFIG)
        cfg["output_mode"] = "detail"
        cfg["aggregation"]["exclude_initial_days"] = 0
        result = process_dataframe(df, cfg)

        self.assertEqual(len(result), 2)
        self.assertIn("★50%盈利判断", result.columns)

    def test_process_dataframe_handles_empty_trend_sequence(self):
        df = pd.DataFrame(
            {
                "门店编码": ["S1"],
                "开业时间": ["2026/01/01"],
                "监控日期": ["2026/01/02"],
                "审批状态": [1],
                "盈利性判断": [1],
            }
        )

        cfg = copy.deepcopy(DEFAULT_CONFIG)
        cfg["output_mode"] = "detail"
        cfg["aggregation"]["exclude_based_on"] = "monitor_date"
        cfg["aggregation"]["exclude_initial_days"] = 10
        cfg["output_fields"]["profit_trend"] = True
        cfg["output_fields"]["profit_trend_slope"] = True
        cfg["output_fields"]["profit_monthly_change"] = True
        cfg["output_fields"]["profit_stability"] = True
        cfg["output_fields"]["data_observation_days"] = True

        result = process_dataframe(df, cfg)
        self.assertEqual(len(result), 0)


class TestOutputFilenameRule(unittest.TestCase):
    def test_detail_mode_filename_matches_legacy_pattern(self):
        cfg = copy.deepcopy(DEFAULT_CONFIG)
        cfg["output_mode"] = "detail"
        cfg["aggregation"]["trend_window"] = 15
        cfg["aggregation"]["exclude_based_on"] = "opening_date"
        cfg["aggregation"]["exclude_initial_days"] = 60
        dt = datetime(2026, 4, 1, 12, 34, 56)

        name = build_output_filename(cfg, dt)
        self.assertEqual(
            name,
            "BI_详细模式_趋势窗口15_依据开业日期_排除60_20260401_123456.csv",
        )

    def test_aggregated_mode_filename_matches_legacy_pattern(self):
        cfg = copy.deepcopy(DEFAULT_CONFIG)
        cfg["output_mode"] = "aggregated"
        cfg["aggregation"]["trend_window"] = 7
        cfg["aggregation"]["exclude_based_on"] = "monitor_date"
        cfg["aggregation"]["exclude_initial_days"] = 30
        dt = datetime(2026, 4, 1, 8, 0, 0)

        name = build_output_filename(cfg, dt)
        self.assertEqual(
            name,
            "BI_聚合模式_趋势窗口7_依据监控日期_排除30_20260401_080000.csv",
        )


if __name__ == "__main__":
    unittest.main()
