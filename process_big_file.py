# BI数据预处理工具 Python版 v2.0 (2026-03-26)
# 支持灵活配置的BI数据预处理工具
# 使用方法: python process_big_file.py

import sys
import os
from datetime import datetime, timedelta
import threading
import json
import copy
import warnings

warnings.filterwarnings("ignore")

try:
    import tkinter as tk
    from tkinter import ttk, filedialog, messagebox

    HAS_TKINTER = True
except ImportError:
    tk = None
    ttk = None
    filedialog = None
    messagebox = None
    HAS_TKINTER = False


def _require_tkinter():
    if not HAS_TKINTER:
        raise RuntimeError(
            "当前运行环境缺少 tkinter，无法启动桌面版 GUI。"
            "请安装 Tk 运行时，或改用 streamlit_app.py 在线版。"
        )


try:
    import pandas as pd

    HAS_PANDAS = True
except ImportError:
    HAS_PANDAS = False


def load_field_descriptions():
    """从txt文件加载字段说明"""
    txt_path = os.path.join(
        os.path.dirname(os.path.abspath(__file__)), "field_descriptions.txt"
    )
    if not os.path.exists(txt_path):
        return {}
    fields = {}
    current_key = None
    current_label = ""
    current_simple = ""
    current_formula = ""
    with open(txt_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.rstrip("\n")
            if line.startswith("[") and line.endswith("]"):
                if current_key:
                    fields[current_key] = {
                        "label": current_label,
                        "simple": current_simple,
                        "formula": current_formula,
                    }
                current_key = line[1:-1]
                current_label = ""
                current_simple = ""
                current_formula = ""
            elif current_key:
                if current_label == "":
                    current_label = line
                elif current_simple == "":
                    current_simple = line
                elif current_formula == "":
                    current_formula = line
    if current_key:
        fields[current_key] = {
            "label": current_label,
            "simple": current_simple,
            "formula": current_formula,
        }
    return fields


FIELD_DESCRIPTIONS = load_field_descriptions()


def calculate_consistency(values):
    """计算一致性"""
    values = [v for v in values if v in [0, 1]]
    if not values:
        return "混合"
    if all(v == 1 for v in values):
        return "全1"
    if all(v == 0 for v in values):
        return "全0"
    return "混合"


def calc_trend_metrics(profit_values, trend_window):
    """计算趋势指标：斜率、趋势标签、环比变化、稳定性"""
    n = len(profit_values)
    if n == 0:
        return {
            "trend": "→平稳",
            "slope": 0.0,
            "change_rate": "0%",
            "stability": "低稳定(0分)",
            "obs_days": 0,
        }

    obs_days = min(n, trend_window)
    obs_days = max(obs_days, 2)
    recent_profit = profit_values[-obs_days:]
    actual_len = len(recent_profit)
    x_vals = list(range(actual_len))
    y_vals = recent_profit

    x_mean = sum(x_vals) / actual_len
    y_mean = sum(y_vals) / actual_len

    numerator = sum(
        (x_vals[i] - x_mean) * (y_vals[i] - y_mean) for i in range(actual_len)
    )
    denominator = sum((x_vals[i] - x_mean) ** 2 for i in range(actual_len))

    slope = numerator / denominator if denominator != 0 else 0

    slope_threshold = 1.0 / obs_days
    if slope > slope_threshold:
        trend_label = "↑上升"
    elif slope < -slope_threshold:
        trend_label = "↓下降"
    else:
        trend_label = "→平稳"

    half = obs_days // 2
    change_rate = 0.0
    if half >= 1:
        current_period = sum(recent_profit[-half:])
        previous_period = sum(recent_profit[:half])
        if previous_period != 0:
            change_rate = (current_period - previous_period) / previous_period
        elif current_period != 0:
            change_rate = 1.0

    # 环比变化：转百分比字符串
    change_rate_percent = round(change_rate * 100, 0)
    if change_rate_percent > 0:
        change_rate_label = f"+{int(change_rate_percent)}%"
    elif change_rate_percent < 0:
        change_rate_label = f"{int(change_rate_percent)}%"
    else:
        change_rate_label = "0%"

    y_variance = sum((y_vals[i] - y_mean) ** 2 for i in range(actual_len)) / actual_len
    y_std = y_variance**0.5
    stability = max(0, min(100, (1 - y_std) * 100))

    # 稳定性：转等级+分数
    stability_int = round(stability)
    if stability_int >= 70:
        stability_label = f"高稳定({stability_int}分)"
    elif stability_int >= 40:
        stability_label = f"中稳定({stability_int}分)"
    else:
        stability_label = f"低稳定({stability_int}分)"

    return {
        "trend": trend_label,
        "slope": round(slope, 4),
        "change_rate": change_rate_label,
        "stability": stability_label,
        "obs_days": obs_days,
    }


def convert_number_date_to_standard(value, date_format="%Y/%m/%d"):
    if HAS_PANDAS:
        if pd.isna(value) or value == "" or value is None:
            return value
    elif value == "" or value is None:
        return value

    date_str = str(value).strip()
    if len(date_str) == 8 and date_str.isdigit():
        try:
            year, month, day = date_str[:4], date_str[4:6], date_str[6:8]
            date = datetime(int(year), int(month), int(day))
            return date.strftime(date_format)
        except (ValueError, IndexError):
            pass

    numeric_value = None
    if isinstance(value, (int, float)):
        numeric_value = float(value)
    elif date_str:
        try:
            numeric_value = float(date_str)
        except ValueError:
            pass

    if numeric_value is not None and numeric_value > 25569:
        try:
            excel_date = datetime(1899, 12, 30) + timedelta(days=int(numeric_value))
            return excel_date.strftime(date_format)
        except (ValueError, TypeError, OverflowError):
            pass

    return value


def convert_date_series_with_cache(series, date_format="%Y/%m/%d"):
    """批量转换日期，复用单值转换逻辑并利用缓存减少重复计算。"""
    cache = {}

    def _convert(value):
        try:
            if value in cache:
                return cache[value]
        except TypeError:
            return convert_number_date_to_standard(value, date_format)

        converted = convert_number_date_to_standard(value, date_format)
        cache[value] = converted
        return converted

    return series.map(_convert)


def _read_excel_with_fallback(path, base_kwargs):
    """按策略回退读取 Excel。"""
    if not HAS_PANDAS:
        raise RuntimeError("缺少 pandas 库，请运行: pip install pandas")

    ext = os.path.splitext(str(path))[1].lower()
    strategies = [
        ("calamine", {"engine": "calamine"}),
        (
            "openpyxl_read_only",
            {"engine": "openpyxl", "engine_kwargs": {"read_only": True, "data_only": True}},
        ),
        ("openpyxl_basic", {"engine": "openpyxl"}),
    ]
    if ext == ".xls":
        strategies.append(("xlrd", {"engine": "xlrd"}))
    strategies.append(("pandas_default", {}))
    errors = []

    for strategy, extra_kwargs in strategies:
        try:
            df = pd.read_excel(path, **base_kwargs, **extra_kwargs)
            return df, {"strategy": strategy}
        except (TypeError, ImportError, ValueError) as exc:
            errors.append(f"{strategy}: {exc}")
            continue

    raise RuntimeError("Excel 解析失败: " + " | ".join(errors))


def read_excel_text_table(path):
    """读取 Excel 首个工作表，尽量以更快参数解析并保证兼容回退。"""
    base_kwargs = {
        "sheet_name": 0,
        "header": 0,
        "dtype": str,
        "keep_default_na": False,
        "na_filter": False,
    }
    return _read_excel_with_fallback(path, base_kwargs)


def read_excel_columns(path):
    """仅读取列头，避免列检测受单一引擎版本限制。"""
    base_kwargs = {
        "sheet_name": 0,
        "header": 0,
        "nrows": 0,
    }
    return _read_excel_with_fallback(path, base_kwargs)


def resolve_group_column(cfg, cols):
    group_by = cfg["aggregation"].get("group_by")
    store_col = cfg["columns"]["store_code"]
    return group_by if group_by in cols else store_col


# 魔法字符串常量
EXCLUDE_BASED_ON_DISPLAY_MAP = {"监控日期": "monitor_date", "开业日期": "opening_date"}
EXCLUDE_BASED_ON_VALUE_MAP = {"monitor_date": "监控日期", "opening_date": "开业日期"}
OUTPUT_MODE_DISPLAY_MAP = {
    "详细模式(保留原始行)": "detail",
    "聚合模式(每组仅输出一行)": "aggregated",
}
OUTPUT_MODE_VALUE_MAP = {
    "detail": "详细模式(保留原始行)",
    "aggregated": "聚合模式(每组仅输出一行)",
}


DEFAULT_CONFIG = {
    "columns": {
        "store_code": "门店编码",
        "opening_date": "开业时间",
        "monitor_date": "监控日期",
        "approval_status": "审批状态",
        "profit_judgment": "盈利性判断",
        "delivery_days": "外卖营业天数",
        "daily_revenue": "日商_预计达成比例",
        "delivery_revenue": "外卖日商_预计达成比例",
        "dinein_revenue": "到店日商_预计达成比例",
    },
    "output_fields": {
        "latest_approval": True,
        "latest_profit": True,
        "profit_50": True,
        "total_profit_days": True,
        "total_unprofit_days": True,
        "profit_ratio": True,
        "total_monitor_days": True,
        "total_delivery_days": True,
        "dinein_ratio": True,
        "delivery_ratio": True,
        "approval_consistency": True,
        "profit_consistency": True,
        "profit_trend": True,
        "profit_trend_slope": True,
        "profit_monthly_change": True,
        "profit_stability": True,
        "data_observation_days": True,
    },
    "format": {
        "date_format": "%Y/%m/%d",
        "encoding": "utf-8-sig",
        "decimal_places": 4,
    },
    "output_mode": "aggregated",
    "aggregation": {
        "profit_threshold": 50,
        "exclude_initial_days": 60,
        "exclude_based_on": "opening_date",
        "group_by": "门店编码",
        "trend_window": 15,
    },
}


class ConfigManager:
    """配置管理器 - 统一处理配置的加载和保存"""

    CONFIG_FILE = "config.json"

    def __init__(self, default_config: dict) -> None:
        self.default_config = default_config
        self.config = copy.deepcopy(default_config)
        self._load()

    def _get_config_path(self) -> str:
        return os.path.join(
            os.path.dirname(os.path.abspath(__file__)), self.CONFIG_FILE
        )

    def _load(self) -> None:
        """加载配置文件"""
        config_path = self._get_config_path()
        if os.path.exists(config_path):
            try:
                with open(config_path, "r", encoding="utf-8") as f:
                    loaded = json.load(f)
                self._merge_config(loaded)
            except Exception as e:
                print(f"[ConfigManager] 加载配置文件失败: {e}")

    def _merge_config(self, loaded: dict) -> None:
        """合并加载的配置到默认配置"""

        def merge_dict(base: dict, override: dict) -> None:
            """递归合并override字典到base字典"""
            for key, value in override.items():
                if key in base:
                    if isinstance(value, dict) and isinstance(base[key], dict):
                        merge_dict(base[key], value)
                    else:
                        base[key] = value

        merge_dict(self.config, loaded)

    def save(self) -> None:
        """保存配置到文件"""
        try:
            with open(self._get_config_path(), "w", encoding="utf-8") as f:
                json.dump(self.config, f, ensure_ascii=False, indent=2)
        except Exception as e:
            print(f"[ConfigManager] 保存配置文件失败: {e}")


class App:
    def __init__(self, root):
        _require_tkinter()
        self.root = root
        self.style = ttk.Style()
        self.root.title("BI数据预处理工具 v2.0")
        self.root.geometry("600x700")
        self.root.minsize(600, 700)

        self.input_file = None
        self.processing = False
        self.detected_columns = []
        self.last_output_path = None
        self.config_manager = ConfigManager(DEFAULT_CONFIG)
        self.config = self.config_manager.config

        self._build_ui()
        self._refresh_all_ui()
        self.root.protocol("WM_DELETE_WINDOW", self._on_closing)

    def _build_ui(self):
        main_frame = ttk.Frame(self.root, padding="5")
        main_frame.pack(fill=tk.BOTH, expand=True)

        self.paned = ttk.PanedWindow(main_frame, orient=tk.HORIZONTAL)
        self.paned.pack(fill=tk.BOTH, expand=True)

        left_frame = ttk.Frame(self.paned, width=300)
        self.paned.add(left_frame, weight=0)

        right_frame = ttk.Frame(self.paned)
        self.paned.add(right_frame, weight=1)

        self._build_left_panel(left_frame)
        self._build_right_panel(right_frame)
        self._build_log_panel(main_frame)
        self._enable_drag_drop()

    def _build_left_panel(self, parent):
        file_frame = ttk.LabelFrame(parent, text="文件选择", padding="5")
        file_frame.pack(fill=tk.X, pady=(0, 8))

        self.file_var = tk.StringVar(value="未选择文件")
        ttk.Entry(file_frame, textvariable=self.file_var).pack(fill=tk.X, pady=(0, 4))
        ttk.Button(file_frame, text="浏览...", command=self.select_file).pack(fill=tk.X)

        action_frame = ttk.LabelFrame(parent, text="操作", padding="5")
        action_frame.pack(fill=tk.X, pady=(0, 8))

        self.start_btn = ttk.Button(
            action_frame, text="开始处理", command=self.start_process
        )
        self.start_btn.pack(fill=tk.X, pady=(0, 4))
        self.open_btn = ttk.Button(
            action_frame,
            text="打开表格",
            command=self.open_output_file,
            state=tk.DISABLED,
        )
        self.open_btn.pack(fill=tk.X, pady=(0, 4))
        ttk.Button(action_frame, text="清空日志", command=self.clear_log).pack(
            fill=tk.X
        )

        progress_frame = ttk.LabelFrame(parent, text="进度", padding="5")
        progress_frame.pack(fill=tk.X, pady=(0, 8))

        self.progress = ttk.Progressbar(progress_frame, mode="determinate")
        self.progress.pack(fill=tk.X, pady=(0, 5))
        self.status_var = tk.StringVar(value="就绪")
        ttk.Label(
            progress_frame, textvariable=self.status_var, font=("Microsoft YaHei", 8)
        ).pack()

    def select_file(self):
        path = filedialog.askopenfilename(
            title="选择Excel文件",
            filetypes=[("Excel文件", "*.xlsx *.xls"), ("所有文件", "*.*")],
        )
        if path:
            self.input_file = path
            self.file_var.set(path)
            self.log(f"已选择: {path}")
            self._detect_columns(path)

    def _detect_columns(self, path):
        try:
            if HAS_PANDAS:
                df, meta = read_excel_columns(path)
                self.detected_columns = list(df.columns)
                self.log(f"列检测解析策略: {meta['strategy']}")
                self.log(f"检测到 {len(self.detected_columns)} 列")
                self._auto_fill_config()
                self._set_column_mapping_state("normal")
            else:
                self.detected_columns = []
                self._set_column_mapping_state("disabled")
        except Exception as e:
            self.log(f"检测列失败: {e}")
            self.detected_columns = []
            self._set_column_mapping_state("disabled")

    def _auto_fill_config(self):
        if not self.detected_columns:
            return
        col_map = self.config["columns"]
        for key, default_val in col_map.items():
            best_match = default_val
            best_score = 0
            for col in self.detected_columns:
                if col == default_val:
                    best_match = col
                    best_score = 100
                    break
                elif default_val in col or col in default_val:
                    if len(col) > best_score:
                        best_match = col
                        best_score = len(col)
            if best_match:
                col_map[key] = best_match
        self._refresh_column_mappings()
        self._refresh_group_by_combo()
        self.log("已自动匹配列名，请核对配置")

    def _refresh_group_by_combo(self):
        if hasattr(self, "group_by_combo"):
            self.group_by_combo["values"] = (
                self.detected_columns if self.detected_columns else ["门店编码"]
            )

    def _set_column_mapping_state(self, state):
        if hasattr(self, "col_combos"):
            for combo in self.col_combos.values():
                combo["state"] = state

    def _build_right_panel(self, parent):
        notebook = ttk.Notebook(parent)
        notebook.pack(fill=tk.BOTH, expand=True)

        col_frame = ttk.Frame(notebook, padding="5")
        notebook.add(col_frame, text="列映射")
        self._build_column_mapping_tab(col_frame)

        field_frame = ttk.Frame(notebook, padding="5")
        notebook.add(field_frame, text="输出字段")
        self._build_output_fields_tab(field_frame)

        format_frame = ttk.Frame(notebook, padding="5")
        notebook.add(format_frame, text="格式设置")
        self._build_format_tab(format_frame)

    def _build_column_mapping_tab(self, parent):
        hint_label = ttk.Label(
            parent,
            text="请先选择Excel文件以加载列映射",
            font=("Microsoft YaHei", 8),
            foreground="#888",
        )
        hint_label.pack(pady=(0, 3))

        required_frame = ttk.LabelFrame(
            parent, text="必需字段（留空则使用默认值）", padding="5"
        )
        required_frame.pack(fill=tk.X, pady=(0, 8))

        self.col_vars = {}
        self.col_combos = {}
        required_fields = [
            ("store_code", "门店编码"),
            ("opening_date", "开业时间"),
            ("monitor_date", "监控日期"),
            ("approval_status", "审批状态"),
            ("profit_judgment", "盈利性判断"),
        ]

        for key, label in required_fields:
            frame = ttk.Frame(required_frame)
            frame.pack(fill=tk.X, pady=1)
            ttk.Label(frame, text=label, width=12).pack(side=tk.LEFT)
            var = tk.StringVar(value=self.config["columns"].get(key, ""))
            self.col_vars[key] = var
            combo = ttk.Combobox(frame, textvariable=var, width=25, state="disabled")
            combo["values"] = self.detected_columns if self.detected_columns else []
            combo.pack(side=tk.LEFT, fill=tk.X, expand=True)
            self.col_combos[key] = combo

        optional_frame = ttk.LabelFrame(parent, text="日商字段（可选）", padding="5")
        optional_frame.pack(fill=tk.X)

        optional_fields = [
            ("delivery_days", "外卖营业天数"),
            ("daily_revenue", "日商预计"),
            ("delivery_revenue", "外卖日商"),
            ("dinein_revenue", "到店日商"),
        ]

        for key, label in optional_fields:
            frame = ttk.Frame(optional_frame)
            frame.pack(fill=tk.X, pady=1)
            ttk.Label(frame, text=label, width=12).pack(side=tk.LEFT)
            var = tk.StringVar(value=self.config["columns"].get(key, ""))
            self.col_vars[key] = var
            combo = ttk.Combobox(frame, textvariable=var, width=25, state="disabled")
            combo["values"] = self.detected_columns if self.detected_columns else []
            combo.pack(side=tk.LEFT, fill=tk.X, expand=True)
            self.col_combos[key] = combo

    def _build_output_fields_tab(self, parent):
        btn_frame = ttk.Frame(parent)
        btn_frame.pack(fill=tk.X, pady=(0, 5))
        ttk.Button(
            btn_frame, text="全选", command=lambda: self._set_all_fields(True)
        ).pack(side=tk.LEFT, padx=(0, 5))
        ttk.Button(
            btn_frame, text="全不选", command=lambda: self._set_all_fields(False)
        ).pack(side=tk.LEFT)

        self.field_vars = {}

        canvas = tk.Canvas(parent, highlightthickness=0)
        scrollbar = ttk.Scrollbar(parent, orient="vertical", command=canvas.yview)
        self.fields_container = ttk.Frame(canvas)

        self.fields_container.bind(
            "<Configure>",
            lambda e: canvas.configure(scrollregion=canvas.bbox("all")),
        )

        canvas.create_window((0, 0), window=self.fields_container, anchor="nw")
        canvas.configure(yscrollcommand=scrollbar.set)

        def _on_mousewheel(event):
            canvas.yview_scroll(int(-1 * (event.delta / 120)), "units")

        canvas.bind("<MouseWheel>", _on_mousewheel)

        canvas.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        scrollbar.pack(side=tk.RIGHT, fill=tk.Y)

        self.style.configure("FieldName.TCheckbutton", font=("Microsoft YaHei", 9))

        # 字段键的顺序（与输出顺序一致）
        field_keys = [
            "latest_approval",
            "latest_profit",
            "profit_50",
            "total_profit_days",
            "total_unprofit_days",
            "profit_ratio",
            "total_monitor_days",
            "total_delivery_days",
            "dinein_ratio",
            "delivery_ratio",
            "approval_consistency",
            "profit_consistency",
            "profit_trend",
            "profit_trend_slope",
            "profit_monthly_change",
            "profit_stability",
            "data_observation_days",
        ]

        for key in field_keys:
            desc = FIELD_DESCRIPTIONS.get(key, {})
            label = desc.get("label", key)

            var = tk.BooleanVar(value=self.config["output_fields"].get(key, True))
            self.field_vars[key] = var

            row_frame = ttk.Frame(self.fields_container)
            row_frame.pack(fill=tk.X, padx=8, pady=2)

            cb = ttk.Checkbutton(
                row_frame, text=label, variable=var, style="FieldName.TCheckbutton"
            )
            cb.pack(side=tk.LEFT)

    def _build_format_tab(self, parent):
        format_frame = ttk.LabelFrame(parent, text="日期和编码", padding="5")
        format_frame.pack(fill=tk.X, pady=(0, 8))

        frame = ttk.Frame(format_frame)
        frame.pack(fill=tk.X, pady=1)
        ttk.Label(frame, text="日期格式", width=10).pack(side=tk.LEFT)
        self.date_format_var = tk.StringVar(value=self.config["format"]["date_format"])
        date_combo = ttk.Combobox(
            frame,
            textvariable=self.date_format_var,
            width=15,
            values=["%Y/%m/%d", "%Y-%m-%d", "%m/%d/%Y", "%Y%m%d"],
            state="readonly",
        )
        date_combo.pack(side=tk.LEFT)

        frame = ttk.Frame(format_frame)
        frame.pack(fill=tk.X, pady=1)
        ttk.Label(frame, text="编码方式", width=10).pack(side=tk.LEFT)
        self.encoding_var = tk.StringVar(value=self.config["format"]["encoding"])
        encoding_combo = ttk.Combobox(
            frame,
            textvariable=self.encoding_var,
            width=15,
            values=["utf-8-sig", "utf-8", "gbk"],
            state="readonly",
        )
        encoding_combo.pack(side=tk.LEFT)

        frame = ttk.Frame(format_frame)
        frame.pack(fill=tk.X, pady=1)
        ttk.Label(frame, text="小数位数", width=10).pack(side=tk.LEFT)
        self.decimal_var = tk.IntVar(
            value=self.config["format"].get("decimal_places", 2)
        )
        decimal_spin = ttk.Spinbox(
            frame, from_=0, to=6, textvariable=self.decimal_var, width=8
        )
        decimal_spin.pack(side=tk.LEFT)
        ttk.Label(frame, text="位").pack(side=tk.LEFT)

        agg_frame = ttk.LabelFrame(parent, text="聚合设置", padding="5")
        agg_frame.pack(fill=tk.X)

        frame = ttk.Frame(agg_frame)
        frame.pack(fill=tk.X, pady=1)
        ttk.Label(frame, text="盈利阈值", width=10).pack(side=tk.LEFT)
        self.threshold_var = tk.IntVar(
            value=self.config["aggregation"]["profit_threshold"]
        )
        threshold_spin = ttk.Spinbox(
            frame, from_=0, to=100, textvariable=self.threshold_var, width=8
        )
        threshold_spin.pack(side=tk.LEFT)

        ttk.Label(frame, text="%").pack(side=tk.LEFT)

        frame = ttk.Frame(agg_frame)
        frame.pack(fill=tk.X, pady=1)
        ttk.Label(frame, text="聚合字段", width=10).pack(side=tk.LEFT)
        self.group_by_var = tk.StringVar(value=self.config["aggregation"]["group_by"])
        self.group_by_combo = ttk.Combobox(
            frame,
            textvariable=self.group_by_var,
            width=15,
            values=self.detected_columns if self.detected_columns else ["门店编码"],
            state="readonly",
        )
        self.group_by_combo.pack(side=tk.LEFT)

        frame = ttk.Frame(agg_frame)
        frame.pack(fill=tk.X, pady=1)
        ttk.Label(frame, text="趋势窗口", width=10).pack(side=tk.LEFT)
        self.trend_window_var = tk.IntVar(
            value=self.config["aggregation"].get("trend_window", 7)
        )
        trend_spin = ttk.Spinbox(
            frame, from_=3, to=30, textvariable=self.trend_window_var, width=8
        )
        trend_spin.pack(side=tk.LEFT)

        ttk.Label(frame, text="天").pack(side=tk.LEFT)
        ttk.Label(
            frame,
            text="← 用近几天数据计算趋势（建议7-14天）",
            font=("Microsoft YaHei", 8),
            foreground="#888",
        ).pack(side=tk.LEFT, padx=(5, 0))

        frame = ttk.Frame(agg_frame)
        frame.pack(fill=tk.X, pady=1)
        ttk.Label(frame, text="排除天数", width=10).pack(side=tk.LEFT)
        self.exclude_initial_days_var = tk.IntVar(
            value=self.config["aggregation"].get("exclude_initial_days", 60)
        )
        exclude_spin = ttk.Spinbox(
            frame, from_=0, to=365, textvariable=self.exclude_initial_days_var, width=8
        )
        exclude_spin.pack(side=tk.LEFT)

        ttk.Label(frame, text="天  依据").pack(side=tk.LEFT)
        self.exclude_based_on_var = tk.StringVar(
            value=self.config["aggregation"].get("exclude_based_on", "monitor_date")
        )
        exclude_based_on_combo = ttk.Combobox(
            frame,
            textvariable=self.exclude_based_on_var,
            width=12,
            values=["监控日期", "开业日期"],
            state="readonly",
        )
        exclude_based_on_combo.pack(side=tk.LEFT, padx=(5, 0))

        frame = ttk.Frame(agg_frame)
        frame.pack(fill=tk.X, pady=(5, 1))
        ttk.Label(frame, text="输出模式", width=10).pack(side=tk.LEFT)
        self.output_mode_var = tk.StringVar(
            value=self.config.get("output_mode", "detail")
        )
        for text, value in [("详细模式", "detail"), ("聚合模式", "aggregated")]:
            ttk.Radiobutton(
                frame, text=text, variable=self.output_mode_var, value=value
            ).pack(side=tk.LEFT, padx=5)

    def _refresh_column_mappings(self):
        for key, var in self.col_vars.items():
            val = self.config["columns"].get(key, "")
            var.set(val)
            if (
                key in self.col_combos
                and hasattr(self, "detected_columns")
                and self.detected_columns
            ):
                self.col_combos[key]["values"] = self.detected_columns

    def _set_all_fields(self, value):
        for var in self.field_vars.values():
            var.set(value)

    def _collect_config(self):
        for key, var in self.col_vars.items():
            if var.get():
                self.config["columns"][key] = var.get()
        for key, var in self.field_vars.items():
            self.config["output_fields"][key] = var.get()
        self.config["format"]["date_format"] = self.date_format_var.get()
        self.config["format"]["encoding"] = self.encoding_var.get()
        try:
            self.config["format"]["decimal_places"] = int(self.decimal_var.get())
        except (ValueError, TypeError):
            self.config["format"]["decimal_places"] = 2
        try:
            self.config["aggregation"]["profit_threshold"] = (
                int(self.threshold_var.get()) if self.threshold_var.get() else 50
            )
        except (ValueError, TypeError):
            self.config["aggregation"]["profit_threshold"] = 50
        try:
            exclude_val = self.exclude_initial_days_var.get()
            self.config["aggregation"]["exclude_initial_days"] = (
                int(exclude_val) if exclude_val else 0
            )
        except (ValueError, TypeError):
            self.config["aggregation"]["exclude_initial_days"] = 0
        based_on_display = self.exclude_based_on_var.get()
        self.config["aggregation"]["exclude_based_on"] = (
            EXCLUDE_BASED_ON_DISPLAY_MAP.get(based_on_display, "monitor_date")
        )
        self.config["aggregation"]["group_by"] = self.group_by_var.get()
        try:
            trend_val = self.trend_window_var.get()
            self.config["aggregation"]["trend_window"] = (
                int(trend_val) if trend_val else 7
            )
        except (ValueError, TypeError):
            self.config["aggregation"]["trend_window"] = 7
        self.config["output_mode"] = self.output_mode_var.get()
        self.config["aggregation"]["output_mode"] = self.output_mode_var.get()

    def _build_log_panel(self, parent):
        log_frame = ttk.LabelFrame(parent, text="处理日志", padding="5")
        log_frame.pack(fill=tk.BOTH, expand=True, pady=(5, 0))

        self.log_text = tk.Text(log_frame, height=12, font=("Consolas", 8))
        self.log_text.pack(fill=tk.BOTH, expand=True, side=tk.LEFT)
        scrollbar = ttk.Scrollbar(log_frame, command=self.log_text.yview)
        scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        self.log_text.config(yscrollcommand=scrollbar.set)

    def log(self, message):
        def _do_log():
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            self.log_text.insert(tk.END, f"[{timestamp}] {message}\n")
            self.log_text.see(tk.END)
            self.root.update_idletasks()

        self._run_on_ui_thread(_do_log)

    def clear_log(self):
        self.log_text.delete(1.0, tk.END)

    def update_progress(self, value, status=""):
        def _do_update():
            self.progress["value"] = value
            if status:
                self.status_var.set(status)
            self.root.update_idletasks()

        self._run_on_ui_thread(_do_update)

    def _run_on_ui_thread(self, func):
        if threading.current_thread() is threading.main_thread():
            func()
        else:
            self.root.after(0, func)

    def _process_aggregated_mode(self, df, cfg, cols):
        """聚合模式处理：每个聚合仅输出一行汇总数据"""
        col_map = cfg["columns"]
        out_fields = cfg["output_fields"]
        threshold = cfg["aggregation"]["profit_threshold"] / 100.0
        date_format = cfg["format"]["date_format"]
        trend_window = cfg["aggregation"].get("trend_window", 7)
        group_col = resolve_group_column(cfg, cols)

        # 数据已通过 _preprocess_data 预处理，直接使用
        grouped = df.groupby(group_col, sort=False)

        self.log("正在聚合数据...")

        agg_result = pd.DataFrame()
        latest_data = df.loc[df.groupby(group_col)["★监控日期_排序"].idxmax()]
        latest_indexed = latest_data.set_index(group_col)

        agg_result["审批状态"] = latest_indexed["审批状态"]
        agg_result["盈利性判断"] = latest_indexed["盈利性判断"]
        agg_result["★开业日期"] = latest_indexed["★开业日期"]
        agg_result["★监控日期"] = (
            grouped["★监控日期_排序"].max().dt.strftime(date_format)
        )
        agg_result["★合计盈利天数"] = grouped["盈利性判断"].sum().astype(int)
        agg_result["★合计监控天数"] = grouped["盈利性判断"].count().astype(int)
        agg_result["★合计不盈利天数"] = (
            agg_result["★合计监控天数"] - agg_result["★合计盈利天数"]
        )

        daily_col = col_map.get("daily_revenue")
        delivery_col = col_map.get("delivery_revenue")
        dinein_col = col_map.get("dinein_revenue")

        latest_daily = latest_data.set_index(group_col)

        if daily_col and delivery_col and dinein_col:
            daily = pd.to_numeric(latest_daily[daily_col], errors="coerce")
            delivery = pd.to_numeric(latest_daily[delivery_col], errors="coerce")
            dine_in = pd.to_numeric(latest_daily[dinein_col], errors="coerce")
            valid_mask = daily.notna() & delivery.notna() & dine_in.notna()
            if valid_mask.any():
                denom = dine_in - delivery
                x = (daily - delivery) / denom.where(lambda d: abs(d) > 0.0001, other=1)
                x = x.clip(lower=0, upper=1)
                agg_result.loc[valid_mask, "★堂食预计占比"] = x[valid_mask]
                agg_result.loc[valid_mask, "★到店预计占比"] = (1 - x)[valid_mask]

        if "总营业天数" in cols:
            agg_result["总营业天数"] = latest_indexed["总营业天数"]
        delivery_days_col = col_map.get("delivery_days")
        if delivery_days_col and delivery_days_col in cols:
            agg_result["外卖营业天数"] = pd.to_numeric(
                latest_indexed[delivery_days_col], errors="coerce"
            ).fillna(0).astype(int)

        agg_result["★盈利天数占比"] = agg_result.apply(
            lambda row: row["★合计盈利天数"] / max(row["★合计监控天数"], 1), axis=1
        )
        agg_result["★50%盈利判断"] = (agg_result["★盈利天数占比"] >= threshold).astype(
            int
        )

        if out_fields.get("approval_consistency"):
            approval_lists = grouped["审批状态"].apply(list)
            agg_result["检查-审批一致性"] = approval_lists.apply(calculate_consistency)
        if out_fields.get("profit_consistency"):
            profit_lists = grouped["盈利性判断"].apply(list)
            agg_result["检查-盈利一致性"] = profit_lists.apply(calculate_consistency)

        if (
            out_fields.get("profit_trend")
            or out_fields.get("profit_trend_slope")
            or out_fields.get("profit_monthly_change")
            or out_fields.get("profit_stability")
            or out_fields.get("data_observation_days")
        ):
            # 简化版趋势计算：复用 calc_trend_metrics 保证结果一致
            exclude_initial_days = cfg["aggregation"].get("exclude_initial_days", 0)
            exclude_based_on = cfg["aggregation"].get(
                "exclude_based_on", "monitor_date"
            )

            # 准备数据：排序并添加辅助列
            df["_row_num"] = df.groupby(group_col).cumcount()
            df_sorted = df.sort_values([group_col, "★监控日期_排序"])

            # 按门店计算趋势指标
            store_codes = agg_result.index.tolist()
            trend_data = {
                col: []
                for col in [
                    "★盈利趋势",
                    "★盈利趋势斜率",
                    "★盈利环比变化",
                    "★盈利稳定性",
                    "★数据观测期",
                ]
            }

            for store_code in store_codes:
                group = df_sorted[df_sorted[group_col] == store_code].copy()

                # 应用排除初始天数的过滤（与详细模式一致）
                if exclude_initial_days > 0:
                    if exclude_based_on == "opening_date":
                        group["距开业天数"] = (
                            group["★监控日期_排序"]
                            - pd.to_datetime(
                                group["★开业日期"], format=date_format, errors="coerce"
                            )
                        ).dt.days
                        group = group[group["距开业天数"] >= exclude_initial_days]
                    else:
                        group = group[group["_row_num"] >= exclude_initial_days]

                # 调用与详细模式相同的函数
                profit_values = group["盈利性判断"].tolist()
                metrics = calc_trend_metrics(profit_values, trend_window)

                trend_data["★盈利趋势"].append(metrics["trend"])
                trend_data["★盈利趋势斜率"].append(metrics["slope"])
                trend_data["★盈利环比变化"].append(metrics["change_rate"])
                trend_data["★盈利稳定性"].append(metrics["stability"])
                trend_data["★数据观测期"].append(metrics["obs_days"])

            # 赋值到结果
            if out_fields.get("profit_trend"):
                agg_result["★盈利趋势"] = trend_data["★盈利趋势"]
            if out_fields.get("profit_trend_slope"):
                agg_result["★盈利趋势斜率"] = trend_data["★盈利趋势斜率"]
            if out_fields.get("profit_monthly_change"):
                agg_result["★盈利环比变化"] = trend_data["★盈利环比变化"]
            if out_fields.get("profit_stability"):
                agg_result["★盈利稳定性"] = trend_data["★盈利稳定性"]
            if out_fields.get("data_observation_days"):
                agg_result["★数据观测期"] = trend_data["★数据观测期"]

        agg_result["★最新审批状态"] = agg_result["审批状态"]
        agg_result["★最新盈利判断"] = agg_result["盈利性判断"]

        agg_result = agg_result.reset_index()

        # 数据驱动的输出列构建
        FIELD_COLUMN_MAP = [
            ("total_profit_days", "★合计盈利天数"),
            ("total_unprofit_days", "★合计不盈利天数"),
            ("total_monitor_days", "★合计监控天数"),
            ("profit_ratio", "★盈利天数占比"),
            ("profit_50", "★50%盈利判断"),
            ("approval_consistency", "检查-审批一致性"),
            ("profit_consistency", "检查-盈利一致性"),
            ("profit_trend", "★盈利趋势"),
            ("profit_trend_slope", "★盈利趋势斜率"),
            ("profit_monthly_change", "★盈利环比变化"),
            ("profit_stability", "★盈利稳定性"),
            ("data_observation_days", "★数据观测期"),
        ]

        CONDITIONAL_COLUMNS = [
            ("总营业天数", lambda: "总营业天数" in agg_result.columns),
            (
                "外卖营业天数",
                lambda: out_fields.get("total_delivery_days")
                and "外卖营业天数" in agg_result.columns,
            ),
            (
                "★堂食预计占比",
                lambda: out_fields.get("dinein_ratio")
                and "★堂食预计占比" in agg_result.columns,
            ),
            (
                "★到店预计占比",
                lambda: out_fields.get("delivery_ratio")
                and "★到店预计占比" in agg_result.columns,
            ),
        ]

        output_columns = [
            group_col,
            "★开业日期",
            "★监控日期",
            "★最新审批状态",
            "★最新盈利判断",
        ]
        output_columns.extend(
            col for key, col in FIELD_COLUMN_MAP if out_fields.get(key)
        )
        output_columns.extend(col for col, cond in CONDITIONAL_COLUMNS if cond())
        output_columns = [c for c in output_columns if c in agg_result.columns]

        agg_result = agg_result[output_columns]

        self._aggregated_result = agg_result

    def _preprocess_data(self, df, cfg, cols):
        """公共数据预处理：日期转换、状态转换、排除初始天数"""
        col_map = cfg["columns"]
        date_format = cfg["format"]["date_format"]
        exclude_initial_days = cfg["aggregation"].get("exclude_initial_days", 0)
        exclude_based_on = cfg["aggregation"].get("exclude_based_on", "monitor_date")
        group_col = resolve_group_column(cfg, cols)

        df["★开业日期"] = convert_date_series_with_cache(
            df[col_map["opening_date"]], date_format
        )
        df["★监控日期"] = convert_date_series_with_cache(
            df[col_map["monitor_date"]], date_format
        )

        df["审批状态"] = (
            pd.to_numeric(df[col_map["approval_status"]], errors="coerce")
            .fillna(0)
            .astype(int)
        )
        df["盈利性判断"] = (
            pd.to_numeric(df[col_map["profit_judgment"]], errors="coerce")
            .fillna(0)
            .astype(int)
        )

        df["★监控日期_排序"] = pd.to_datetime(
            df["★监控日期"], format=date_format, errors="coerce"
        )

        if exclude_initial_days > 0:
            if exclude_based_on == "opening_date":
                df["距开业天数"] = (
                    df["★监控日期_排序"]
                    - pd.to_datetime(
                        df["★开业日期"], format=date_format, errors="coerce"
                    )
                ).dt.days
                df = df[df["距开业天数"] >= exclude_initial_days].copy()
            else:
                df = df.sort_values(["★监控日期_排序"])
                df["天数序号"] = df.groupby(group_col).cumcount()
                df = df[df["天数序号"] >= exclude_initial_days].copy()

        return df

    def _init_output_fields(self, df, out_fields):
        """初始化所有输出字段为空值"""
        if out_fields.get("latest_approval"):
            df["★最新审批状态"] = 0
        if out_fields.get("latest_profit"):
            df["★最新盈利判断"] = 0
        if out_fields.get("profit_50"):
            df["★50%盈利判断"] = 0
        if out_fields.get("total_profit_days"):
            df["★合计盈利天数"] = 0
        if out_fields.get("total_unprofit_days"):
            df["★合计不盈利天数"] = 0
        if out_fields.get("profit_ratio"):
            df["★盈利天数占比"] = 0.0
        if out_fields.get("total_monitor_days"):
            df["★合计监控天数"] = 0
        if out_fields.get("total_delivery_days"):
            df["★合计外卖天数"] = 0
        if out_fields.get("dinein_ratio"):
            df["★堂食预计占比"] = float("nan")
        if out_fields.get("delivery_ratio"):
            df["★到店预计占比"] = float("nan")
        if out_fields.get("approval_consistency"):
            df["检查-审批一致性"] = "混合"
        if out_fields.get("profit_consistency"):
            df["检查-盈利一致性"] = "混合"
        if out_fields.get("profit_trend"):
            df["★盈利趋势"] = "平稳"
        if out_fields.get("profit_trend_slope"):
            df["★盈利趋势斜率"] = 0.0
        if out_fields.get("profit_monthly_change"):
            df["★盈利环比变化"] = "0%"
        if out_fields.get("profit_stability"):
            df["★盈利稳定性"] = "低稳定(0分)"
        if out_fields.get("data_observation_days"):
            df["★数据观测期"] = 0
        return df

    def _process_detail_mode(self, df, cfg, cols):
        """详细模式处理：保留所有原始数据行"""
        col_map = cfg["columns"]
        out_fields = cfg["output_fields"]
        threshold = cfg["aggregation"]["profit_threshold"] / 100.0
        exclude_initial_days = cfg["aggregation"].get("exclude_initial_days", 0)
        exclude_based_on = cfg["aggregation"].get("exclude_based_on", "monitor_date")
        trend_window = cfg["aggregation"].get("trend_window", 7)
        date_format = cfg["format"]["date_format"]
        group_col = resolve_group_column(cfg, cols)

        self.log("正在按门店聚合数据...")
        grouped = df.groupby(group_col, sort=False)
        total_stores = len(grouped)
        self.log(f"共 {total_stores} 个门店")

        store_count = 0
        for store_code, group in grouped:
            store_count += 1
            if store_count % 1000 == 0:
                progress = 25 + int((store_count / total_stores) * 65)
                self.update_progress(
                    progress, f"处理中... {store_count}/{total_stores}"
                )
                self.log(f"  处理进度: {store_count}/{total_stores} 门店")

            group_sorted = group.sort_values("★监控日期", ascending=False)
            latest = group_sorted.iloc[0]
            latest_approval = latest["审批状态"]
            latest_profit = latest["盈利性判断"]

            if exclude_based_on == "opening_date":
                group_for_count = group.copy()
                group_for_count["距开业天数"] = (
                    pd.to_datetime(
                        group["★监控日期"], format=date_format, errors="coerce"
                    )
                    - pd.to_datetime(
                        group["★开业日期"], format=date_format, errors="coerce"
                    )
                ).dt.days
                group_for_count = group_for_count[
                    group_for_count["距开业天数"] >= exclude_initial_days
                ]
                profit_values = group_for_count["盈利性判断"].tolist()
            else:
                group_for_count = group.sort_values("★监控日期", ascending=True)
                profit_values = group_for_count["盈利性判断"].tolist()
                if exclude_initial_days > 0:
                    profit_values = profit_values[exclude_initial_days:]

            approval_values = group["审批状态"].tolist()

            # 向量化计数
            profit_series = group_for_count["盈利性判断"]
            profitable_count = int((profit_series == 1).sum())
            unprofitable_count = int((profit_series == 0).sum())
            total_days = len(profit_series)

            profit_ratio = profitable_count / total_days if total_days > 0 else 0
            profit_50 = 1 if profit_ratio >= threshold else 0

            approval_consistency = calculate_consistency(approval_values)
            profit_consistency = calculate_consistency(profit_series.tolist())

            mask = df[group_col] == store_code

            if out_fields["latest_approval"]:
                df.loc[mask, "★最新审批状态"] = latest_approval
            if out_fields["latest_profit"]:
                df.loc[mask, "★最新盈利判断"] = latest_profit
            if out_fields["profit_50"]:
                df.loc[mask, "★50%盈利判断"] = profit_50
            if out_fields["total_profit_days"]:
                df.loc[mask, "★合计盈利天数"] = profitable_count
            if out_fields["total_unprofit_days"]:
                df.loc[mask, "★合计不盈利天数"] = unprofitable_count
            if out_fields["profit_ratio"]:
                df.loc[mask, "★盈利天数占比"] = round(profit_ratio, 4)
            if out_fields["total_monitor_days"]:
                df.loc[mask, "★合计监控天数"] = total_days
            if out_fields["approval_consistency"]:
                df.loc[mask, "检查-审批一致性"] = approval_consistency
            if out_fields["profit_consistency"]:
                df.loc[mask, "检查-盈利一致性"] = profit_consistency

            delivery_days_col = col_map.get("delivery_days")
            if delivery_days_col and delivery_days_col in cols:
                if out_fields["total_delivery_days"]:
                    dd = pd.to_numeric(group[delivery_days_col], errors="coerce")
                    delivery_days = int(dd.max()) if dd.notna().any() else 0
                    df.loc[mask, "★合计外卖天数"] = delivery_days

            daily_col = col_map.get("daily_revenue")
            delivery_col = col_map.get("delivery_revenue")
            dinein_col = col_map.get("dinein_revenue")
            if daily_col and delivery_col and dinein_col:
                if daily_col in cols and delivery_col in cols and dinein_col in cols:
                    if out_fields.get("dinein_ratio") or out_fields.get(
                        "delivery_ratio"
                    ):
                        daily = pd.to_numeric(latest[daily_col], errors="coerce")
                        delivery = pd.to_numeric(latest[delivery_col], errors="coerce")
                        dine_in = pd.to_numeric(latest[dinein_col], errors="coerce")
                        if pd.notna(daily) and pd.notna(delivery) and pd.notna(dine_in):
                            denom = dine_in - delivery
                            if abs(denom) > 0.0001:
                                x = (daily - delivery) / denom
                                x = max(0, min(1, x))
                                if out_fields["dinein_ratio"]:
                                    df.loc[mask, "★堂食预计占比"] = round(float(x), 4)
                                if out_fields["delivery_ratio"]:
                                    df.loc[mask, "★到店预计占比"] = round(
                                        float(1 - x), 4
                                    )

            # 趋势计算使用与计数相同的过滤后数据
            profit_seq = profit_values

            if (
                out_fields.get("profit_trend")
                or out_fields.get("profit_trend_slope")
                or out_fields.get("profit_monthly_change")
                or out_fields.get("profit_stability")
                or out_fields.get("data_observation_days")
            ):
                metrics = calc_trend_metrics(profit_seq, trend_window)
                if out_fields.get("profit_trend_slope"):
                    df.loc[mask, "★盈利趋势斜率"] = metrics["slope"]
                if out_fields.get("profit_trend"):
                    df.loc[mask, "★盈利趋势"] = metrics["trend"]
                if out_fields.get("profit_monthly_change"):
                    df.loc[mask, "★盈利环比变化"] = metrics["change_rate"]
                if out_fields.get("profit_stability"):
                    df.loc[mask, "★盈利稳定性"] = metrics["stability"]
                if out_fields.get("data_observation_days"):
                    df.loc[mask, "★数据观测期"] = metrics["obs_days"]

        return df

    def _on_closing(self):
        self._collect_config()
        self.config_manager.save()
        self.root.destroy()

    def _refresh_all_ui(self):
        self._refresh_column_mappings()
        for key, var in self.field_vars.items():
            var.set(self.config["output_fields"].get(key, True))
        self.date_format_var.set(self.config["format"]["date_format"])
        self.encoding_var.set(self.config["format"]["encoding"])
        self.decimal_var.set(self.config["format"].get("decimal_places", 2))
        self.threshold_var.set(self.config["aggregation"]["profit_threshold"])
        self.exclude_initial_days_var.set(
            self.config["aggregation"].get("exclude_initial_days", 60)
        )
        based_on = self.config["aggregation"].get("exclude_based_on", "monitor_date")
        self.exclude_based_on_var.set(
            EXCLUDE_BASED_ON_VALUE_MAP.get(based_on, "监控日期")
        )
        self.group_by_var.set(self.config["aggregation"]["group_by"])
        self.trend_window_var.set(self.config["aggregation"].get("trend_window", 7))
        self.output_mode_var.set(self.config.get("output_mode", "detail"))
        self._refresh_group_by_combo()
        self.log(
            f"UI已刷新: 阈值={self.config['aggregation']['profit_threshold']}, 趋势窗口={self.config['aggregation'].get('trend_window', 7)}"
        )

    def _enable_drag_drop(self):
        def handle_drop(event):
            files = event.data.split()
            if files:
                path = files[0]
                if os.path.isfile(path) and (
                    path.endswith(".xlsx") or path.endswith(".xls")
                ):
                    self.input_file = path
                    self.file_var.set(path)
                    self.log(f"已选择: {path}")
                    self._detect_columns(path)

        try:
            self.root.drop_target_register(tk.DND.Files)
            self.root.dnd_bind("<<Drop>>", handle_drop)
        except Exception:
            pass

    def open_output_file(self):
        if self.last_output_path and os.path.exists(self.last_output_path):
            os.startfile(self.last_output_path)
        else:
            messagebox.showwarning("提示", "文件不存在或路径无效")

    def start_process(self):
        if not HAS_PANDAS:
            messagebox.showerror("错误", "缺少 pandas 库，请运行: pip install pandas")
            return

        if not self.input_file:
            messagebox.showwarning("提示", "请先选择输入文件")
            return

        if not os.path.exists(self.input_file):
            messagebox.showerror("错误", f"文件不存在: {self.input_file}")
            return

        if self.processing:
            return

        self.processing = True
        self.start_btn.config(state=tk.DISABLED)
        self.update_progress(0, "正在读取文件...")

        thread = threading.Thread(target=self._process_file, daemon=True)
        thread.start()

    def _process_file(self):
        try:
            self._collect_config()
            input_path = self.input_file
            cfg = self.config

            output_mode = cfg.get(
                "output_mode", cfg["aggregation"].get("output_mode", "detail")
            )
            is_aggregated = output_mode == "aggregated"

            self.log(f"========== 开始处理 ==========")
            self.log(f"输入文件: {input_path}")
            self.log(f"文件大小: {os.path.getsize(input_path) / 1024 / 1024:.2f} MB")
            self.log(f"日期格式: {cfg['format']['date_format']}")
            self.log(f"编码: {cfg['format']['encoding']}")
            self.log(f"输出模式: {'聚合模式' if is_aggregated else '详细模式'}")

            self.update_progress(5, "正在解析Excel数据...")
            df, read_meta = read_excel_text_table(input_path)
            self.log(f"解析策略: {read_meta['strategy']}")
            self.log(f"读取完成: {len(df)} 行, {len(df.columns)} 列")

            cols = {col.strip(): idx for idx, col in enumerate(df.columns)}
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
                raise Exception(f"缺少必需列: {','.join(missing)}")

            self.log(
                f"必填列映射: 门店={col_map['store_code']}, 开业={col_map['opening_date']}, 监控={col_map['monitor_date']}"
            )
            self.log(f"盈利阈值: {cfg['aggregation']['profit_threshold']}%")
            self.log(
                f"排除开业前: {cfg['aggregation'].get('exclude_initial_days', 0)} 天"
            )

            self.update_progress(20, "正在预处理数据...")
            df = self._preprocess_data(df, cfg, cols)
            unique_stores = df[group_col].nunique()
            self.log(f"预处理完成: {unique_stores} 家门店, {len(df)} 条记录")

            out_fields = cfg["output_fields"]
            df = self._init_output_fields(df, out_fields)

            if is_aggregated:
                self.log("使用聚合模式处理...")
                self._process_aggregated_mode(df, cfg, cols)
            else:
                self.log("使用详细模式处理...")
                df = self._process_detail_mode(df, cfg, cols)

            self.update_progress(90, "正在导出CSV...")

            output_mode_text = "聚合模式" if is_aggregated else "详细模式"
            trend_window = cfg["aggregation"].get("trend_window", 7)
            exclude_days = cfg["aggregation"].get("exclude_initial_days", 0)
            exclude_based_on_display = EXCLUDE_BASED_ON_VALUE_MAP.get(
                cfg["aggregation"].get("exclude_based_on", "monitor_date"), "监控日期"
            )
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_path = os.path.join(
                os.path.dirname(input_path),
                f"BI_{output_mode_text}_趋势窗口{trend_window}_依据{exclude_based_on_display}_排除{exclude_days}_{timestamp}.csv",
            )
            counter = 1
            while os.path.exists(output_path):
                output_path = os.path.join(
                    os.path.dirname(input_path),
                    f"BI_{output_mode_text}_趋势窗口{trend_window}_依据{exclude_based_on_display}_排除{exclude_days}_{timestamp}_{counter}.csv",
                )
                counter += 1

            if is_aggregated:
                output_df = self._aggregated_result
            else:
                output_df = df
                cols_to_drop = []
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
                for key, col_name in field_to_col.items():
                    if col_name in output_df.columns and not out_fields.get(key, True):
                        cols_to_drop.append(col_name)
                if cols_to_drop:
                    output_df = output_df.drop(columns=cols_to_drop)

                output_df = output_df.loc[:, (output_df != "").any(axis=0)]

            decimal_places = cfg["format"].get("decimal_places", 2)
            if decimal_places >= 0:
                float_cols = output_df.select_dtypes(include=["float64"]).columns
                for col in float_cols:
                    output_df[col] = output_df[col].round(decimal_places)

            self.log(f"输出字段: {len(output_df.columns)} 列")
            self.log(f"输出行数: {len(output_df)} 行")
            self.log(f"小数位数: {decimal_places}")

            encoding = cfg["format"]["encoding"]
            output_df.to_csv(output_path, index=False, encoding=encoding)
            self.last_output_path = output_path
            self._run_on_ui_thread(lambda: self.open_btn.config(state=tk.NORMAL))
            self.update_progress(100, "处理完成!")
            self.log(f"导出完成: {output_path}")
            self.log(
                f"输出文件大小: {os.path.getsize(output_path) / 1024 / 1024:.2f} MB"
            )
            self.log("========== 处理完成 ==========")

            self._run_on_ui_thread(
                lambda: messagebox.showinfo("完成", f"处理成功!\n\n输出文件:\n{output_path}")
            )

        except Exception as e:
            self.log(f"错误: {e}")
            import traceback

            traceback.print_exc()
            self._run_on_ui_thread(lambda: messagebox.showerror("错误", str(e)))

        finally:
            self.processing = False
            self._run_on_ui_thread(lambda: self.start_btn.config(state=tk.NORMAL))
            self.update_progress(100, "就绪")


if __name__ == "__main__":
    if not HAS_TKINTER:
        print("错误: 当前运行环境缺少 tkinter，无法启动桌面版 GUI。")
        print("请安装 Tk 运行时，或改用 streamlit_app.py 在线版。")
        sys.exit(1)

    if not HAS_PANDAS:
        root = tk.Tk()
        root.withdraw()
        messagebox.showerror("错误", "缺少 pandas 库，请运行: pip install pandas")
        sys.exit(1)

    root = tk.Tk()
    app = App(root)
    root.mainloop()
