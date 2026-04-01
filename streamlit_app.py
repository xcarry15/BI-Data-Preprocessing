import copy
import os
import tempfile

import pandas as pd
import streamlit as st

from process_big_file import DEFAULT_CONFIG, read_excel_columns
from web_runtime import build_output_filename, dataframe_to_csv_bytes, process_excel_file

# Widget key prefixes
_COL_PREFIX = "col_"
_OUT_PREFIX = "out_"

st.set_page_config(page_title="BI数据预处理（在线版）", layout="wide")
st.markdown(
    """
<style>
:root {
    --bg: #f4f7fc;
    --panel: #ffffff;
    --line: #d5ddeb;
    --text: #14253f;
    --muted: #5d6f8f;
    --accent: #1a4e99;
    --accent-soft: #eaf1ff;
    --title: #10223c;
    --badge: #345d9f;
    --heading: #173157;
}
.stApp {
    background: linear-gradient(180deg, #f4f7fc 0%, #f1f5fb 100%);
    color: var(--text);
}
.block-container {
    padding-top: 0.55rem;
    padding-bottom: 0.8rem;
    max-width: 1520px;
}
.hero-shell {
    margin-bottom: 0.45rem;
    border-radius: 10px;
    border: none;
    background: transparent;
}
.hero-strip {
    display: none;
}
.hero-main {
    padding: 0.72rem 0.95rem 0.68rem 0.95rem;
}
.hero-title {
    margin: 0;
    font-size: 1.12rem;
    line-height: 1.22;
    letter-spacing: 0.1px;
    color: var(--title);
}
.hero-sub {
    color: var(--muted);
    font-size: 0.8rem;
    margin-top: 0.18rem;
}
.hero-meta {
    display: inline-block;
    margin-top: 0.45rem;
    padding: 0.16rem 0.5rem;
    border-radius: 999px;
    border: 1px solid #c3d1e9;
    background: var(--accent-soft);
    color: var(--badge);
    font-size: 0.7rem;
    font-weight: 600;
    letter-spacing: 0.2px;
}
h3 {
    font-size: 0.97rem;
    margin: 0 0 0.35rem 0;
    color: var(--heading);
}
p, label, span, div {
    font-size: 0.9rem;
}
div[data-testid="stVerticalBlock"] > div:has(> div > div > div > h3) {
    gap: 0.15rem;
}
div[data-testid="stFileUploaderDropzone"] {
    border-radius: 10px;
    border: 1px solid var(--line);
    background: #f6f9ff;
    padding: 0.46rem 0.54rem;
}
div[data-testid="stTextInput"] input,
div[data-testid="stNumberInput"] input,
div[data-testid="stSelectbox"] > div > div {
    min-height: 34px;
    border-radius: 8px;
}
div[data-testid="stTextInput"] label,
div[data-testid="stNumberInput"] label,
div[data-testid="stSelectbox"] label,
div[data-testid="stCheckbox"] label {
    font-size: 0.82rem;
    color: var(--muted);
}
button[kind="primary"] {
    background: var(--accent);
    border: 1px solid #164687;
}
div[data-baseweb="tab-list"] button {
    height: 33px;
    padding-top: 0;
    padding-bottom: 0;
    border-radius: 6px 6px 0 0;
}
div[data-testid="stExpander"] {
    border: 1px solid var(--line);
    border-radius: 10px;
    background: var(--panel);
}
@media (max-width: 768px) {
    .hero-main {
        padding: 0.62rem 0.72rem 0.6rem 0.72rem;
    }
    .hero-title {
        font-size: 1rem;
    }
    .hero-sub {
        font-size: 0.76rem;
    }
}
</style>
""",
    unsafe_allow_html=True,
)
st.markdown(
    """
<div class="hero-shell">
  <div class="hero-strip"></div>
  <div class="hero-main">
    <h1 class="hero-title">BI 数据预处理工具</h1>
    <div class="hero-sub">面向数据分析场景的批量清洗与聚合工作台</div>
    <div class="hero-meta">v2.0</div>
  </div>
</div>
""",
    unsafe_allow_html=True,
)

if "cfg" not in st.session_state:
    st.session_state["cfg"] = copy.deepcopy(DEFAULT_CONFIG)
if "result_df" not in st.session_state:
    st.session_state["result_df"] = None
if "download_name" not in st.session_state:
    st.session_state["download_name"] = ""


@st.cache_data(show_spinner=False)
def detect_columns(file_bytes, suffix):
    with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as temp:
        temp.write(file_bytes)
        temp_path = temp.name
    try:
        df, _ = read_excel_columns(temp_path)
        return [str(c) for c in df.columns]
    finally:
        if os.path.exists(temp_path):
            os.remove(temp_path)


def _compact_field_editor(cfg, detected_columns):
    st.subheader("列映射")
    fields = [
        ("store_code", "门店编码"),
        ("opening_date", "开业时间"),
        ("monitor_date", "监控日期"),
        ("approval_status", "审批状态"),
        ("profit_judgment", "盈利性判断"),
        ("delivery_days", "外卖营业天数"),
        ("daily_revenue", "日商_预计达成比例"),
        ("delivery_revenue", "外卖日商_预计达成比例"),
        ("dinein_revenue", "到店日商_预计达成比例"),
        ("group_by", "聚合字段(group_by)"),
    ]
    cols = st.columns(4)
    for idx, (key, label) in enumerate(fields):
        with cols[idx % 4]:
            if key == "group_by":
                cfg["aggregation"]["group_by"] = st.text_input(
                    label,
                    value=cfg["aggregation"].get("group_by", "门店编码"),
                    key="group_by",
                )
            elif detected_columns:
                options = detected_columns[:]
                current = cfg["columns"].get(key, "")
                if current and current not in options:
                    options = [current] + options
                selected = st.selectbox(
                    label,
                    options=options,
                    index=max(options.index(current), 0) if current in options else 0,
                    key=f"{_COL_PREFIX}{key}",
                )
                cfg["columns"][key] = selected
            else:
                cfg["columns"][key] = st.text_input(
                    label, value=cfg["columns"].get(key, ""), key=f"{_COL_PREFIX}{key}"
                )


def _output_field_editor(cfg):
    st.subheader("输出字段")
    labels = {
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
    op1, op2, _ = st.columns([1, 1, 3])
    with op1:
        if st.button("全选", key="select_all_fields", use_container_width=True):
            for key in labels:
                st.session_state[f"{_OUT_PREFIX}{key}"] = True
    with op2:
        if st.button("全不选", key="clear_all_fields", use_container_width=True):
            for key in labels:
                st.session_state[f"{_OUT_PREFIX}{key}"] = False

    cols = st.columns(4)
    for idx, key in enumerate(labels):
        with cols[idx % 4]:
            cfg["output_fields"][key] = st.checkbox(
                labels[key],
                value=cfg["output_fields"].get(key, True),
                key=f"{_OUT_PREFIX}{key}",
            )


def _format_editor(cfg):
    st.subheader("格式与聚合")
    cols = st.columns(4)
    format_items = [
        ("output_mode", "输出模式"),
        ("date_format", "日期格式"),
        ("encoding", "编码"),
        ("decimal_places", "小数位数"),
        ("profit_threshold", "盈利阈值(%)"),
        ("exclude_initial_days", "排除初始天数"),
        ("exclude_based_on", "排除依据"),
        ("trend_window", "趋势窗口(天)"),
    ]
    for idx, (key, label) in enumerate(format_items):
        with cols[idx % 4]:
            if key == "output_mode":
                cfg["output_mode"] = st.selectbox(
                    label,
                    ["detail", "aggregated"],
                    index=0 if cfg.get("output_mode", "detail") == "detail" else 1,
                    format_func=lambda v: "详细模式" if v == "detail" else "聚合模式",
                    key="output_mode",
                )
            elif key == "date_format":
                cfg["format"]["date_format"] = st.text_input(
                    label,
                    value=cfg["format"].get("date_format", "%Y/%m/%d"),
                    key="date_fmt",
                )
            elif key == "encoding":
                cfg["format"]["encoding"] = st.selectbox(
                    label,
                    ["utf-8-sig", "utf-8", "gbk"],
                    index=["utf-8-sig", "utf-8", "gbk"].index(
                        cfg["format"].get("encoding", "utf-8-sig")
                    )
                    if cfg["format"].get("encoding", "utf-8-sig")
                    in ["utf-8-sig", "utf-8", "gbk"]
                    else 0,
                    key="encoding",
                )
            elif key == "decimal_places":
                cfg["format"]["decimal_places"] = st.number_input(
                    label,
                    min_value=0,
                    max_value=8,
                    value=int(cfg["format"].get("decimal_places", 2)),
                    step=1,
                    key="decimal_places",
                )
            elif key == "profit_threshold":
                cfg["aggregation"]["profit_threshold"] = st.number_input(
                    label,
                    min_value=0,
                    max_value=100,
                    value=int(cfg["aggregation"].get("profit_threshold", 50)),
                    step=1,
                    key="threshold",
                )
            elif key == "exclude_initial_days":
                cfg["aggregation"]["exclude_initial_days"] = st.number_input(
                    label,
                    min_value=0,
                    value=int(cfg["aggregation"].get("exclude_initial_days", 60)),
                    step=1,
                    key="exclude_days",
                )
            elif key == "exclude_based_on":
                cfg["aggregation"]["exclude_based_on"] = st.selectbox(
                    label,
                    ["monitor_date", "opening_date"],
                    index=0
                    if cfg["aggregation"].get("exclude_based_on", "monitor_date")
                    == "monitor_date"
                    else 1,
                    format_func=lambda v: "监控日期" if v == "monitor_date" else "开业日期",
                    key="exclude_based_on",
                )
            elif key == "trend_window":
                cfg["aggregation"]["trend_window"] = st.number_input(
                    label,
                    min_value=2,
                    value=int(cfg["aggregation"].get("trend_window", 7)),
                    step=1,
                    key="trend_window",
                )

cfg = copy.deepcopy(st.session_state["cfg"])
left, right = st.columns([0.95, 2.45], gap="small")
uploaded = None
detected_columns = []
with left:
    st.subheader("文件与操作")
    uploaded = st.file_uploader(
        "上传 Excel 文件",
        type=["xlsx", "xls"],
        label_visibility="collapsed",
    )
    if uploaded is not None:
        st.caption(f"已选择: {uploaded.name}")
        suffix = os.path.splitext(uploaded.name)[1] or ".xlsx"
        detected_columns = detect_columns(uploaded.getvalue(), suffix)
        st.caption(f"检测到 {len(detected_columns)} 列")
    run_btn = st.button(
        "开始处理",
        type="primary",
        use_container_width=True,
        disabled=uploaded is None,
    )

    strategy_slot = st.empty()

with right:
    tab1, tab2, tab3 = st.tabs(["列映射", "输出字段", "格式设置"])
    with tab1:
        _compact_field_editor(cfg, detected_columns)
    with tab2:
        _output_field_editor(cfg)
    with tab3:
        _format_editor(cfg)

st.session_state["cfg"] = cfg
strategy_text = (
    f"模式: {'聚合' if cfg.get('output_mode') == 'aggregated' else '详细'} | "
    f"阈值: {cfg['aggregation'].get('profit_threshold', 50)}% | "
    f"趋势窗口: {cfg['aggregation'].get('trend_window', 7)}天 | "
    f"排除: {cfg['aggregation'].get('exclude_initial_days', 0)}天"
)
with strategy_slot.container():
    st.caption("处理策略")
    st.caption(strategy_text)

if uploaded is not None and run_btn:
    suffix = os.path.splitext(uploaded.name)[1] or ".xlsx"
    with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as temp:
        temp.write(uploaded.getbuffer())
        temp_path = temp.name

    try:
        with st.spinner("正在处理，请稍候..."):
            result_df = process_excel_file(temp_path, st.session_state["cfg"])
        st.session_state["result_df"] = result_df
        st.session_state["download_name"] = build_output_filename(st.session_state["cfg"])
    except Exception as exc:
        st.error(f"处理失败: {exc}")
    finally:
        if os.path.exists(temp_path):
            os.remove(temp_path)

if isinstance(st.session_state.get("result_df"), pd.DataFrame):
    result_df = st.session_state["result_df"]
    st.success(f"处理完成: {len(result_df)} 行, {len(result_df.columns)} 列")
    st.dataframe(result_df.head(100), use_container_width=True, height=330)
    csv_bytes = dataframe_to_csv_bytes(
        result_df,
        encoding=st.session_state["cfg"]["format"].get("encoding", "utf-8-sig"),
    )
    st.download_button(
        "下载 CSV",
        data=csv_bytes,
        file_name=st.session_state.get("download_name", "output.csv"),
        mime="text/csv",
        use_container_width=True,
    )
