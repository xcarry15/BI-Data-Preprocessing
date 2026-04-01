# BI数据预处理工具 - 聚合输出功能实现计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 新增聚合输出模式功能，用户开启后可按聚合字段聚合数据，每个聚合仅输出一行汇总数据。

**Architecture:** 在现有处理流程中增加分支逻辑：详细模式保持不变，聚合模式使用 pandas.groupby().agg() 一次性完成所有字段聚合计算，输出去重后的汇总行。

**Tech Stack:** Python 3, pandas, tkinter

---

## 文件变更

- 修改: `process_big_file.py`

---

## 任务分解

### Task 1: 配置变更

**Files:**
- Modify: `process_big_file.py:59-100` (DEFAULT_CONFIG)
- Modify: `config.json` (无需手动，程序启动时自动更新)

- [ ] **Step 1: 在 DEFAULT_CONFIG 的 aggregation 中新增 output_mode 配置**

在 `DEFAULT_CONFIG` 的 `aggregation` 部分添加:

```python
"output_mode": "detail",  # "detail" | "aggregated"
```

- [ ] **Step 2: 确认配置合并逻辑**

程序启动时 `_load_config` 会自动加载 config.json，现有逻辑无需修改。

---

### Task 2: UI 变更 - 添加输出模式选项

**Files:**
- Modify: `process_big_file.py:491-534` (_build_format_tab 方法)

- [ ] **Step 1: 在"聚合设置"区域添加输出模式选项**

在 `_build_format_tab` 方法中，找到 `agg_frame` 的末尾，添加:

```python
frame = ttk.Frame(agg_frame)
frame.pack(fill=tk.X, pady=(8, 2))
ttk.Label(frame, text="输出模式", width=10).pack(side=tk.LEFT)
self.output_mode_var = tk.StringVar(value=self.config["aggregation"].get("output_mode", "detail"))
ttk.Combobox(
    frame,
    textvariable=self.output_mode_var,
    width=18,
    values=["详细模式(保留原始行)", "聚合模式(每组仅输出一行)"],
    state="readonly",
).pack(side=tk.LEFT)
```

- [ ] **Step 2: 在 _collect_config 方法中收集输出模式配置**

找到 `_collect_config` 方法（约581行），在 aggregation 配置收集部分添加:

```python
self.config["aggregation"]["output_mode"] = self.output_mode_var.get()
```

- [ ] **Step 3: 在 _refresh_all_ui 方法中刷新输出模式显示**

找到 `_refresh_all_ui` 方法（约643行），添加:

```python
mode = self.config["aggregation"].get("output_mode", "detail")
self.output_mode_var.set(mode)
```

---

### Task 3: 实现聚合输出模式的数据处理逻辑

**Files:**
- Modify: `process_big_file.py:779-958` (_process_file 方法中的数据处理部分)

- [ ] **Step 1: 在 _process_file 方法开头获取输出模式**

在 `_process_file` 方法中，获取配置后添加:

```python
output_mode = cfg["aggregation"].get("output_mode", "detail")
is_aggregated = output_mode == "aggregated"
```

- [ ] **Step 2: 在详细模式处理逻辑之前添加聚合模式分支**

在现有 `self.log("正在按门店聚合数据...")` 之前添加:

```python
if is_aggregated:
    self._process_aggregated_mode(df, cfg, cols)
else:
    # 现有详细模式处理逻辑保持不变
    self.log("正在按门店聚合数据...")
    # ... 现有处理逻辑 (lines 779-958)
```

- [ ] **Step 3: 实现 _process_aggregated_mode 方法**

在 `_on_closing` 方法之前（约632行）添加:

```python
def _process_aggregated_mode(self, df, cfg, cols):
    """聚合模式处理：每个聚合仅输出一行汇总数据"""
    col_map = cfg["columns"]
    out_fields = cfg["output_fields"]
    threshold = cfg["aggregation"]["profit_threshold"] / 100.0
    exclude_initial_days = cfg["aggregation"].get("exclude_initial_days", 0)
    date_format = cfg["format"]["date_format"]
    
    # 转换日期列
    df["★开业日期"] = df[col_map["opening_date"]].apply(
        lambda x: convert_number_date_to_standard(x, date_format)
    )
    df["★监控日期"] = df[col_map["monitor_date"]].apply(
        lambda x: convert_number_date_to_standard(x, date_format)
    )
    
    # 转换数值列
    df["审批状态"] = pd.to_numeric(df[col_map["approval_status"]], errors="coerce").fillna(0).astype(int)
    df["盈利性判断"] = pd.to_numeric(df[col_map["profit_judgment"]], errors="coerce").fillna(0).astype(int)
    
    # 获取日期列名
    monitor_date_col = "★监控日期"
    opening_date_col = "★开业日期"
    
    # 找出每个聚合中最大监控日期对应的索引
    idx_max_date = df.groupby(col_map["store_code"], sort=False)[monitor_date_col].idxmax()
    
    # 为原始数据行标记是否是该门店最大日期的行
    df["_is_latest"] = False
    df.loc[idx_max_date, "_is_latest"] = True
    
    # 计算盈利相关汇总
    grouped = df.groupby(col_map["store_code"], sort=False)
    
    # 汇总字段
    agg_dict = {}
    
    # 最新值字段（取最大监控日期对应的值）
    agg_dict["审批状态"] = ("审批状态", "last")
    agg_dict["盈利性判断"] = ("盈利性判断", "last")
    agg_dict["★开业日期"] = ("★开业日期", "last")
    agg_dict["★监控日期"] = ("★监控日期", "max")
    
    # 平均值字段
    daily_col = col_map.get("daily_revenue")
    delivery_col = col_map.get("delivery_revenue")
    dinein_col = col_map.get("dinein_revenue")
    
    if daily_col and daily_col in cols:
        agg_dict["日商_预计达成比例"] = (daily_col, "mean")
    if delivery_col and delivery_col in cols:
        agg_dict["外卖日商_预计达成比例"] = (delivery_col, "mean")
    if dinein_col and dinein_col in cols:
        agg_dict["到店日商_预计达成比例"] = (dinein_col, "mean")
    
    # 最大值字段
    delivery_days_col = col_map.get("delivery_days")
    if delivery_days_col and delivery_days_col in cols:
        agg_dict["外卖营业天数"] = (delivery_days_col, "max")
    
    # 执行聚合聚合
    self.log("正在聚合数据...")
    agg_result = grouped.agg(**{name: agg for name, (col, _) in agg_dict.items()})
    
    # 计算盈利相关字段
    profit_stats = grouped.agg(
        profit_sum=("盈利性判断", "sum"),
        profit_count=("盈利性判断", "count")
    )
    agg_result["★合计盈利天数"] = profit_stats["profit_sum"]
    agg_result["★合计监控天数"] = profit_stats["profit_count"]
    
    # 计算盈利占比和50%判断
    if exclude_initial_days > 0:
        # 排除初始天数后重新计算
        agg_result["★盈利天数占比"] = agg_result.apply(
            lambda row: row["★合计盈利天数"] / max(row["★合计监控天数"] - exclude_initial_days, 1)
            if row["★合计监控天数"] > exclude_initial_days
            else row["★合计盈利天数"] / max(row["★合计监控天数"], 1),
            axis=1
        )
    else:
        agg_result["★盈利天数占比"] = agg_result.apply(
            lambda row: row["★合计盈利天数"] / max(row["★合计监控天数"], 1),
            axis=1
        )
    agg_result["★50%盈利判断"] = (agg_result["★盈利天数占比"] >= threshold).astype(int)
    
    # 总门店数量（每个聚合的门店计数）
    agg_result["总门店数量"] = 1  # 每个聚合只有一行，每行代表一个聚合
    
    # ★最新审批状态（=审批状态）
    agg_result["★最新审批状态"] = agg_result["审批状态"]
    
    # ★最新盈利判断（=盈利性判断）
    agg_result["★最新盈利判断"] = agg_result["盈利性判断"]
    
    # 重置索引，门店编码作为普通列
    agg_result = agg_result.reset_index()
    
    # 选择输出的列
    output_columns = []
    
    # 聚合字段
    output_columns.append(col_map["store_code"])
    
    # 基础字段
    output_columns.extend(["★开业日期", "★监控日期", "★最新审批状态", "★最新盈利判断"])
    
    # 汇总字段
    if out_fields.get("total_profit_days"):
        output_columns.append("★合计盈利天数")
    if out_fields.get("total_monitor_days"):
        output_columns.append("★合计监控天数")
    if out_fields.get("total_delivery_days") and "外卖营业天数" in agg_result.columns:
        output_columns.append("外卖营业天数")
    if out_fields.get("latest_approval"):
        output_columns.append("审批状态")
    if out_fields.get("latest_profit"):
        output_columns.append("盈利性判断")
    if out_fields.get("profit_ratio"):
        output_columns.append("★盈利天数占比")
    if out_fields.get("profit_50"):
        output_columns.append("★50%盈利判断")
    if out_fields.get("dinein_ratio") and "日商_预计达成比例" in agg_result.columns:
        output_columns.append("日商_预计达成比例")
    if out_fields.get("delivery_ratio") and "外卖日商_预计达成比例" in agg_result.columns:
        output_columns.append("外卖日商_预计达成比例")
    if out_fields.get("dinein_ratio") and "到店日商_预计达成比例" in agg_result.columns:
        output_columns.append("到店日商_预计达成比例")
    
    # 总门店数量（聚合模式下输出）
    if "总门店数量" in agg_result.columns:
        output_columns.append("总门店数量")
    
    # 过滤存在的列
    output_columns = [c for c in output_columns if c in agg_result.columns]
    
    # 选择输出列
    agg_result = agg_result[output_columns]
    
    # 保存到实例变量，供后续导出使用
    self._aggregated_result = agg_result
```

- [ ] **Step 4: 修改导出逻辑支持聚合模式**

找到导出部分（约959-1010行），在导出CSV之前添加聚合模式处理:

```python
# 在 self.update_progress(90, "正在导出CSV...") 之前添加
if is_aggregated:
    output_df = self._aggregated_result
else:
    # 现有详细模式处理：按字段过滤等操作
    # ... 现有 cols_to_drop 等逻辑
    pass
```

然后修改导出部分使用 `output_df` 代替 `df`:

```python
encoding = cfg["format"]["encoding"]
output_df.to_csv(output_path, index=False, encoding=encoding)
```

**注意：** 需要将 `output_df` 定义在条件判断之前，在详细模式分支中仍然使用 `df`。

---

### Task 4: 测试验证

- [ ] **Step 1: 运行程序测试 UI 显示**

启动程序，检查"格式设置"标签页中是否显示"输出模式"选项。

- [ ] **Step 2: 测试详细模式**

选择"详细模式(保留原始行)"，处理测试文件，确认输出包含所有原始行。

- [ ] **Step 3: 测试聚合模式**

选择"聚合模式(每组仅输出一行)"，处理测试文件，确认每个聚合仅输出一行汇总数据。

---

### Task 5: 提交变更

- [ ] **Step 1: 提交代码**

```bash
git add process_big_file.py
git commit -m "feat: 添加聚合输出模式功能

- 新增输出模式配置项，支持详细模式和聚合模式
- 聚合模式下每个聚合仅输出一行汇总数据
- 支持的聚合方式：最新值、平均值、最大值、计数
- 趋势类字段在聚合模式下不再输出"
```
