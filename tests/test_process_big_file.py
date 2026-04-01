import unittest
from unittest.mock import patch
import subprocess
import sys
from pathlib import Path

import pandas as pd

from process_big_file import (
    App,
    calc_trend_metrics,
    convert_date_series_with_cache,
    convert_number_date_to_standard,
    read_excel_columns,
    read_excel_text_table,
    resolve_group_column,
)


class TestDateConversion(unittest.TestCase):
    def test_excel_serial_as_string_is_converted(self):
        result = convert_number_date_to_standard("45292", "%Y/%m/%d")
        self.assertEqual(result, "2024/01/01")

    def test_yyyymmdd_string_is_converted(self):
        result = convert_number_date_to_standard("20260105", "%Y/%m/%d")
        self.assertEqual(result, "2026/01/05")


class TestGroupColumnResolution(unittest.TestCase):
    def test_uses_group_by_when_available(self):
        cfg = {"aggregation": {"group_by": "大区"}, "columns": {"store_code": "门店编码"}}
        cols = {"门店编码": 0, "大区": 1}
        self.assertEqual(resolve_group_column(cfg, cols), "大区")

    def test_falls_back_to_store_code_when_group_by_missing(self):
        cfg = {"aggregation": {"group_by": "不存在字段"}, "columns": {"store_code": "门店编码"}}
        cols = {"门店编码": 0, "大区": 1}
        self.assertEqual(resolve_group_column(cfg, cols), "门店编码")


class TestExcelReadOptimization(unittest.TestCase):
    @patch("process_big_file.pd.read_excel")
    def test_prefers_calamine_when_available(self, mock_read_excel):
        mock_read_excel.return_value = pd.DataFrame({"A": ["1"]})

        df, meta = read_excel_text_table("dummy.xlsx")

        self.assertEqual(df.shape, (1, 1))
        self.assertEqual(meta["strategy"], "calamine")
        mock_read_excel.assert_called_once()
        called_kwargs = mock_read_excel.call_args.kwargs
        self.assertEqual(called_kwargs["engine"], "calamine")
        self.assertEqual(called_kwargs["dtype"], str)
        self.assertEqual(called_kwargs["keep_default_na"], False)
        self.assertEqual(called_kwargs["na_filter"], False)

    @patch("process_big_file.pd.read_excel")
    def test_falls_back_to_read_only_openpyxl(self, mock_read_excel):
        expected = pd.DataFrame({"A": ["1"]})
        mock_read_excel.side_effect = [ValueError("unknown engine"), expected]

        df, meta = read_excel_text_table("dummy.xlsx")

        self.assertEqual(df.shape, (1, 1))
        self.assertEqual(meta["strategy"], "openpyxl_read_only")
        self.assertEqual(mock_read_excel.call_count, 2)
        second_kwargs = mock_read_excel.call_args_list[1].kwargs
        self.assertEqual(second_kwargs["engine"], "openpyxl")
        self.assertEqual(second_kwargs["engine_kwargs"]["read_only"], True)
        self.assertEqual(second_kwargs["engine_kwargs"]["data_only"], True)

    @patch("process_big_file.pd.read_excel")
    def test_falls_back_when_engine_kwargs_unsupported(self, mock_read_excel):
        expected = pd.DataFrame({"A": ["1"]})
        mock_read_excel.side_effect = [
            ValueError("unknown engine"),
            TypeError("bad kwargs"),
            expected,
        ]

        df, meta = read_excel_text_table("dummy.xlsx")

        self.assertEqual(df.shape, (1, 1))
        self.assertEqual(meta["strategy"], "openpyxl_basic")
        self.assertEqual(mock_read_excel.call_count, 3)
        first_kwargs = mock_read_excel.call_args_list[0].kwargs
        second_kwargs = mock_read_excel.call_args_list[1].kwargs
        third_kwargs = mock_read_excel.call_args_list[2].kwargs
        self.assertEqual(first_kwargs["engine"], "calamine")
        self.assertIn("engine_kwargs", second_kwargs)
        self.assertNotIn("engine_kwargs", third_kwargs)

    @patch("process_big_file.pd.read_excel")
    def test_xls_falls_back_to_xlrd_when_other_engines_unavailable(self, mock_read_excel):
        expected = pd.DataFrame({"A": ["1"]})
        mock_read_excel.side_effect = [
            ValueError("unknown engine calamine"),
            ImportError("openpyxl missing"),
            ImportError("openpyxl missing"),
            expected,
        ]

        df, meta = read_excel_text_table("dummy.xls")

        self.assertEqual(df.shape, (1, 1))
        self.assertEqual(meta["strategy"], "xlrd")
        self.assertEqual(mock_read_excel.call_count, 4)
        last_kwargs = mock_read_excel.call_args_list[3].kwargs
        self.assertEqual(last_kwargs["engine"], "xlrd")


class TestExcelColumnReadOptimization(unittest.TestCase):
    @patch("process_big_file.pd.read_excel")
    def test_column_read_prefers_calamine(self, mock_read_excel):
        mock_read_excel.return_value = pd.DataFrame(columns=["A", "B"])

        df, meta = read_excel_columns("dummy.xlsx")

        self.assertEqual(meta["strategy"], "calamine")
        self.assertEqual(list(df.columns), ["A", "B"])
        called_kwargs = mock_read_excel.call_args.kwargs
        self.assertEqual(called_kwargs["engine"], "calamine")
        self.assertEqual(called_kwargs["nrows"], 0)


class TestDateSeriesConversion(unittest.TestCase):
    def test_cached_series_conversion_keeps_results_identical(self):
        series = pd.Series(["20260105", "45292", "", "20260105", "45292", "abc"])
        expected = series.apply(
            lambda v: convert_number_date_to_standard(v, "%Y/%m/%d")
        ).tolist()

        actual = convert_date_series_with_cache(series, "%Y/%m/%d").tolist()

        self.assertEqual(actual, expected)


class TestTrendMetrics(unittest.TestCase):
    def test_empty_profit_values_returns_safe_defaults(self):
        metrics = calc_trend_metrics([], 7)
        self.assertEqual(metrics["trend"], "→平稳")
        self.assertEqual(metrics["slope"], 0.0)
        self.assertEqual(metrics["change_rate"], "0%")
        self.assertEqual(metrics["stability"], "低稳定(0分)")
        self.assertEqual(metrics["obs_days"], 0)


class TestPandas3StringDtypeCompatibility(unittest.TestCase):
    def test_ratio_columns_accept_float_assignment(self):
        df = pd.DataFrame({"门店编码": ["S1", "S2"]}, dtype=str)
        out_fields = {"dinein_ratio": True, "delivery_ratio": True}

        initialized = App._init_output_fields(None, df, out_fields)
        initialized.loc[initialized["门店编码"] == "S1", "★堂食预计占比"] = 0.451
        initialized.loc[initialized["门店编码"] == "S1", "★到店预计占比"] = 0.549

        self.assertAlmostEqual(float(initialized.loc[0, "★堂食预计占比"]), 0.451, places=6)
        self.assertAlmostEqual(float(initialized.loc[0, "★到店预计占比"]), 0.549, places=6)

    def test_trend_text_columns_accept_string_assignment(self):
        df = pd.DataFrame({"门店编码": ["S1", "S2"]}, dtype=str)
        out_fields = {"profit_monthly_change": True, "profit_stability": True}

        initialized = App._init_output_fields(None, df, out_fields)
        initialized.loc[initialized["门店编码"] == "S1", "★盈利环比变化"] = "+12%"
        initialized.loc[initialized["门店编码"] == "S1", "★盈利稳定性"] = "高稳定(88分)"

        self.assertEqual(initialized.loc[0, "★盈利环比变化"], "+12%")
        self.assertEqual(initialized.loc[0, "★盈利稳定性"], "高稳定(88分)")


class TestHeadlessImportCompatibility(unittest.TestCase):
    def test_imports_without_tkinter_available(self):
        project_root = Path(__file__).resolve().parents[1]
        script = """
import builtins
real_import = builtins.__import__

def blocked_import(name, globals=None, locals=None, fromlist=(), level=0):
    if name == "tkinter" or name.startswith("tkinter."):
        raise ImportError("tkinter unavailable in runtime")
    return real_import(name, globals, locals, fromlist, level)

builtins.__import__ = blocked_import
import process_big_file
assert "columns" in process_big_file.DEFAULT_CONFIG
"""
        result = subprocess.run(
            [sys.executable, "-c", script],
            cwd=str(project_root),
            capture_output=True,
            text=True,
        )
        self.assertEqual(
            result.returncode,
            0,
            msg=f"stdout={result.stdout}\nstderr={result.stderr}",
        )


if __name__ == "__main__":
    unittest.main()
