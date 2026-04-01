import copy
import io
from datetime import datetime

import pandas as pd

from process_big_file import (
    App,
    EXCLUDE_BASED_ON_VALUE_MAP,
    read_excel_text_table,
    resolve_group_column,
)


class _HeadlessApp:
    def __init__(self, logger=None, progress_callback=None):
        self._aggregated_result = None
        self._logger = logger
        self._progress_callback = progress_callback

    def log(self, message):
        if self._logger:
            self._logger(message)

    def update_progress(self, value, status=""):
        if self._progress_callback:
            self._progress_callback(value, status)


_HeadlessApp._preprocess_data = App._preprocess_data
_HeadlessApp._init_output_fields = App._init_output_fields
_HeadlessApp._process_detail_mode = App._process_detail_mode
_HeadlessApp._process_aggregated_mode = App._process_aggregated_mode


def _drop_disabled_detail_columns(df, out_fields):
    field_to_col = {
        "latest_approval": "★最新审批状态",
        "latest_profit": "★最新盈利判断",
        "profit_50": "★50%盈利判断",
        "total_profit_days": "★合计盈利天数",
        "total_unprofit_days": "★合计不盈利天数",
        "profit_ratio": "★盈利天数占比",
        "total_monitor_days": "★合计监控天数",
        "total_delivery_days": "★合计外卖天数",
        "dinein_ratio": "★堂食预计占比",
        "delivery_ratio": "★到店预计占比",
        "approval_consistency": "检查-审批一致性",
        "profit_consistency": "检查-盈利一致性",
        "profit_trend": "★盈利趋势",
        "profit_trend_slope": "★盈利趋势斜率",
        "profit_monthly_change": "★盈利环比变化",
        "profit_stability": "★盈利稳定性",
        "data_observation_days": "★数据观测期",
    }
    cols_to_drop = []
    for key, col_name in field_to_col.items():
        if col_name in df.columns and not out_fields.get(key, True):
            cols_to_drop.append(col_name)
    if cols_to_drop:
        df = df.drop(columns=cols_to_drop)
    return df.loc[:, (df != "").any(axis=0)]


def _round_float_columns(df, decimal_places):
    if decimal_places < 0:
        return df
    float_cols = df.select_dtypes(include=["float64"]).columns
    for col in float_cols:
        df[col] = df[col].round(decimal_places)
    return df


def process_dataframe(df, cfg, logger=None, progress_callback=None):
    worker = _HeadlessApp(logger=logger, progress_callback=progress_callback)
    cfg = copy.deepcopy(cfg)

    cols = {str(col).strip(): idx for idx, col in enumerate(df.columns)}
    group_col = resolve_group_column(cfg, cols)
    col_map = cfg["columns"]

    required = [
        col_map["store_code"],
        col_map["opening_date"],
        col_map["monitor_date"],
        col_map["approval_status"],
        col_map["profit_judgment"],
    ]
    missing = [c for c in required if c not in cols]
    if missing:
        raise ValueError(f"缺少必需列: {','.join(missing)}")

    processed = worker._preprocess_data(df.copy(), cfg, cols)
    out_fields = cfg["output_fields"]
    processed = worker._init_output_fields(processed, out_fields)

    output_mode = cfg.get("output_mode", cfg["aggregation"].get("output_mode", "detail"))
    is_aggregated = output_mode == "aggregated"

    if is_aggregated:
        worker._process_aggregated_mode(processed, cfg, cols)
        output_df = worker._aggregated_result
    else:
        output_df = worker._process_detail_mode(processed, cfg, cols)
        output_df = _drop_disabled_detail_columns(output_df, out_fields)

    decimal_places = cfg["format"].get("decimal_places", 2)
    return _round_float_columns(output_df, decimal_places)


def process_excel_file(path, cfg, logger=None, progress_callback=None):
    df, _ = read_excel_text_table(path)
    return process_dataframe(df, cfg, logger=logger, progress_callback=progress_callback)


def dataframe_to_csv_bytes(df, encoding="utf-8-sig"):
    buf = io.StringIO()
    df.to_csv(buf, index=False)
    return buf.getvalue().encode(encoding)


def build_output_filename(cfg, now=None):
    dt = now or datetime.now()
    output_mode = cfg.get("output_mode", cfg["aggregation"].get("output_mode", "detail"))
    output_mode_text = "聚合模式" if output_mode == "aggregated" else "详细模式"
    trend_window = cfg["aggregation"].get("trend_window", 7)
    exclude_days = cfg["aggregation"].get("exclude_initial_days", 0)
    exclude_based_on_display = EXCLUDE_BASED_ON_VALUE_MAP.get(
        cfg["aggregation"].get("exclude_based_on", "monitor_date"),
        "监控日期",
    )
    timestamp = dt.strftime("%Y%m%d_%H%M%S")
    return (
        f"BI_{output_mode_text}_趋势窗口{trend_window}_依据"
        f"{exclude_based_on_display}_排除{exclude_days}_{timestamp}.csv"
    )
