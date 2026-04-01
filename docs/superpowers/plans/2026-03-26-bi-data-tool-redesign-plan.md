# BI数据预处理工具 v2.0 实施计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 重构 BI 数据预处理工具，增加灵活的可配置选项（列映射、输出字段选择、日期格式等），同时保持对现有数据的默认兼容性。

**Architecture:** 采用 Tkinter 左右分栏布局，配置与处理逻辑分离，配置持久化为 JSON 文件。

**Tech Stack:** Python 3.x, Tkinter, pandas, JSON

---

## 文件结构

```
process_big_file.py    # 重构后的主程序（单文件）
```

---

## 实施步骤

### Task 1: 创建配置数据结构和核心函数

**Files:**
- Modify: `process_big_file.py:1-20`

- [ ] **Step 1: 添加注释说明和导入**

```python
# BI数据预处理工具 Python版 v2.0 (2026-03-26)
# 支持灵活配置的BI数据预处理工具
# 使用方法: python process_big_file.py

import sys
import os
import tkinter as tk
from tkinter import ttk, filedialog, messagebox
from datetime import datetime
import threading
import json
import copy
import warnings
warnings.filterwarnings('ignore')

try:
    import pandas as pd
    HAS_PANDAS = True
except ImportError:
    HAS_PANDAS = False
```

- [ ] **Step 2: 定义 calculate_consistency 函数**

```python
def calculate_consistency(values):
    """计算一致性"""
    values = [v for v in values if v in [0, 1]]
    if not values:
        return '混合'
    if all(v == 1 for v in values):
        return '全1'
    if all(v == 0 for v in values):
        return '全0'
    return '混合'
```

- [ ] **Step 3: 定义默认配置常量**

```python
DEFAULT_CONFIG = {
    'columns': {
        'store_code': '门店编码',
        'opening_date': '开业时间',
        'monitor_date': '监控日期',
        'approval_status': '审批状态',
        'profit_judgment': '盈利性判断',
        'delivery_days': '外卖营业天数',
        'daily_revenue': '日商_预计达成比例',
        'delivery_revenue': '外卖日商_预计达成比例',
        'dinein_revenue': '到店日商_预计达成比例',
    },
    'output_fields': {
        'latest_approval': True,
        'latest_profit': True,
        'profit_50': True,
        'total_profit_days': True,
        'total_unprofit_days': True,
        'profit_ratio': True,
        'total_monitor_days': True,
        'total_delivery_days': True,
        'dinein_ratio': True,
        'delivery_ratio': True,
        'approval_consistency': True,
        'profit_consistency': True,
    },
    'format': {
        'date_format': '%Y/%m/%d',
        'encoding': 'utf-8-sig',
    },
    'aggregation': {
        'profit_threshold': 50,
        'group_by': '门店编码',
    }
}
```

- [ ] **Step 4: 提交**

---

### Task 2: 实现 App 类基础结构

**Files:**
- Modify: `process_big_file.py:52-65`

- [ ] **Step 1: 创建 App 类基础结构**

```python
class App:
    def __init__(self, root):
        self.root = root
        self.root.title("BI数据预处理工具 v2.0")
        self.root.geometry("900x650")
        self.root.minsize(800, 600)
        
        self.input_file = None
        self.processing = False
        self.detected_columns = []  # 自动检测到的列名
        self.config = copy.deepcopy(DEFAULT_CONFIG)  # 当前配置
        
        self._build_ui()
```

- [ ] **Step 2: 创建 _build_ui 方法框架**

```python
    def _build_ui(self):
        main_frame = ttk.Frame(self.root, padding="10")
        main_frame.pack(fill=tk.BOTH, expand=True)
        
        # 左右分栏
        self.paned = ttk.PanedWindow(main_frame, orient=tk.HORIZONTAL)
        self.paned.pack(fill=tk.BOTH, expand=True)
        
        # 左侧面板 - 文件选择和主操作
        left_frame = ttk.Frame(self.paned, width=300)
        self.paned.add(left_frame, weight=0)
        
        # 右侧面板 - 配置项
        right_frame = ttk.Frame(self.paned)
        self.paned.add(right_frame, weight=1)
        
        self._build_left_panel(left_frame)
        self._build_right_panel(right_frame)
        self._build_log_panel(main_frame)
```

- [ ] **Step 3: 提交**

---

### Task 3: 实现左侧面板（文件选择和主操作）

**Files:**
- Modify: `process_big_file.py`

- [ ] **Step 1: 实现 _build_left_panel 方法**

```python
    def _build_left_panel(self, parent):
        # 文件选择区
        file_frame = ttk.LabelFrame(parent, text="文件选择", padding="8")
        file_frame.pack(fill=tk.X, pady=(0, 10))
        
        self.file_var = tk.StringVar(value="未选择文件")
        ttk.Entry(file_frame, textvariable=self.file_var).pack(fill=tk.X, pady=(0, 5))
        ttk.Button(file_frame, text="浏览...", command=self.select_file).pack(fill=tk.X)
        
        # 主操作区
        action_frame = ttk.LabelFrame(parent, text="操作", padding="8")
        action_frame.pack(fill=tk.X, pady=(0, 10))
        
        self.start_btn = ttk.Button(action_frame, text="开始处理", command=self.start_process)
        self.start_btn.pack(fill=tk.X, pady=(0, 5))
        ttk.Button(action_frame, text="清空日志", command=self.clear_log).pack(fill=tk.X)
        
        # 进度区
        progress_frame = ttk.LabelFrame(parent, text="进度", padding="8")
        progress_frame.pack(fill=tk.X, pady=(0, 10))
        
        self.progress = ttk.Progressbar(progress_frame, mode='determinate')
        self.progress.pack(fill=tk.X, pady=(0, 5))
        self.status_var = tk.StringVar(value="就绪")
        ttk.Label(progress_frame, textvariable=self.status_var, font=('Microsoft YaHei', 8)).pack()
        
        # 配置管理
        config_frame = ttk.LabelFrame(parent, text="配置管理", padding="8")
        config_frame.pack(fill=tk.X)
        
        ttk.Button(config_frame, text="保存配置", command=self.save_config).pack(fill=tk.X, pady=(0, 3))
        ttk.Button(config_frame, text="加载配置", command=self.load_config).pack(fill=tk.X)
```

- [ ] **Step 2: 实现 select_file 方法（带自动列检测）**

```python
    def select_file(self):
        path = filedialog.askopenfilename(
            title="选择Excel文件",
            filetypes=[("Excel文件", "*.xlsx *.xls"), ("所有文件", "*.*")]
        )
        if path:
            self.input_file = path
            self.file_var.set(path)
            self.log(f"已选择: {path}")
            self._detect_columns(path)
```

- [ ] **Step 3: 实现 _detect_columns 方法**

```python
    def _detect_columns(self, path):
        try:
            if HAS_PANDAS:
                df = pd.read_excel(path, sheet_name=0, header=0, nrows=0)
                self.detected_columns = list(df.columns)
                self.log(f"检测到 {len(self.detected_columns)} 列")
                self._auto_fill_config()
            else:
                self.detected_columns = []
        except Exception as e:
            self.log(f"检测列失败: {e}")
            self.detected_columns = []
```

- [ ] **Step 4: 实现 _auto_fill_config 方法（智能匹配）**

```python
    def _auto_fill_config(self):
        if not self.detected_columns:
            return
        col_map = self.config['columns']
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
        combo = self.group_by_var.master.winfo_children()[1]
        combo['values'] = self.detected_columns if self.detected_columns else ['门店编码']
```

- [ ] **Step 5: 提交**

---

### Task 4: 实现右侧面板（配置项）

**Files:**
- Modify: `process_big_file.py`

- [ ] **Step 1: 实现 _build_right_panel 方法**

```python
    def _build_right_panel(self, parent):
        notebook = ttk.Notebook(parent)
        notebook.pack(fill=tk.BOTH, expand=True)
        
        # Tab 1: 列映射
        col_frame = ttk.Frame(notebook, padding="10")
        notebook.add(col_frame, text="列映射")
        self._build_column_mapping_tab(col_frame)
        
        # Tab 2: 输出字段
        field_frame = ttk.Frame(notebook, padding="10")
        notebook.add(field_frame, text="输出字段")
        self._build_output_fields_tab(field_frame)
        
        # Tab 3: 格式设置
        format_frame = ttk.Frame(notebook, padding="10")
        notebook.add(format_frame, text="格式设置")
        self._build_format_tab(format_frame)
```

- [ ] **Step 2: 实现 _build_column_mapping_tab 方法**

```python
    def _build_column_mapping_tab(self, parent):
        required_frame = ttk.LabelFrame(parent, text="必需字段（留空则使用默认值）", padding="8")
        required_frame.pack(fill=tk.X, pady=(0, 10))
        
        self.col_vars = {}
        required_fields = [
            ('store_code', '门店编码'),
            ('opening_date', '开业时间'),
            ('monitor_date', '监控日期'),
            ('approval_status', '审批状态'),
            ('profit_judgment', '盈利性判断'),
        ]
        
        for key, label in required_fields:
            frame = ttk.Frame(required_frame)
            frame.pack(fill=tk.X, pady=2)
            ttk.Label(frame, text=label, width=12).pack(side=tk.LEFT)
            var = tk.StringVar(value=self.config['columns'].get(key, ''))
            self.col_vars[key] = var
            combo = ttk.Combobox(frame, textvariable=var, width=25, state='readonly')
            combo['values'] = self.detected_columns if self.detected_columns else ['']
            combo.pack(side=tk.LEFT, fill=tk.X, expand=True)
        
        optional_frame = ttk.LabelFrame(parent, text="日商字段（可选）", padding="8")
        optional_frame.pack(fill=tk.X)
        
        optional_fields = [
            ('delivery_days', '外卖营业天数'),
            ('daily_revenue', '日商预计'),
            ('delivery_revenue', '外卖日商'),
            ('dinein_revenue', '到店日商'),
        ]
        
        for key, label in optional_fields:
            frame = ttk.Frame(optional_frame)
            frame.pack(fill=tk.X, pady=2)
            ttk.Label(frame, text=label, width=12).pack(side=tk.LEFT)
            var = tk.StringVar(value=self.config['columns'].get(key, ''))
            self.col_vars[key] = var
            combo = ttk.Combobox(frame, textvariable=var, width=25, state='readonly')
            combo['values'] = self.detected_columns if self.detected_columns else ['']
            combo.pack(side=tk.LEFT, fill=tk.X, expand=True)
```

- [ ] **Step 3: 实现 _build_output_fields_tab 方法**

```python
    def _build_output_fields_tab(self, parent):
        btn_frame = ttk.Frame(parent)
        btn_frame.pack(fill=tk.X, pady=(0, 5))
        ttk.Button(btn_frame, text="全选", command=lambda: self._set_all_fields(True)).pack(side=tk.LEFT, padx=(0, 5))
        ttk.Button(btn_frame, text="全不选", command=lambda: self._set_all_fields(False)).pack(side=tk.LEFT)
        
        self.field_vars = {}
        fields_frame = ttk.Frame(parent)
        fields_frame.pack(fill=tk.BOTH, expand=True)
        
        output_fields = [
            ('latest_approval', '★最新审批状态'),
            ('latest_profit', '★最新盈利判断'),
            ('profit_50', '★50%盈利判断'),
            ('total_profit_days', '★合计盈利天数'),
            ('total_unprofit_days', '★合计不盈利天数'),
            ('profit_ratio', '★盈利天数占比'),
            ('total_monitor_days', '★合计监控天数'),
            ('total_delivery_days', '★合计外卖天数'),
            ('dinein_ratio', '★堂食预计占比'),
            ('delivery_ratio', '★到店预计占比'),
            ('approval_consistency', '检查-审批一致性'),
            ('profit_consistency', '检查-盈利一致性'),
        ]
        
        for i, (key, label) in enumerate(output_fields):
            var = tk.BooleanVar(value=self.config['output_fields'].get(key, True))
            self.field_vars[key] = var
            cb = ttk.Checkbutton(fields_frame, text=label, variable=var)
            row = i // 2
            col = i % 2
            cb.grid(row=row, column=col, sticky='w', padx=5, pady=2)
```

- [ ] **Step 4: 实现 _build_format_tab 方法**

```python
    def _build_format_tab(self, parent):
        format_frame = ttk.LabelFrame(parent, text="日期和编码", padding="8")
        format_frame.pack(fill=tk.X, pady=(0, 10))
        
        frame = ttk.Frame(format_frame)
        frame.pack(fill=tk.X, pady=2)
        ttk.Label(frame, text="日期格式", width=10).pack(side=tk.LEFT)
        self.date_format_var = tk.StringVar(value=self.config['format']['date_format'])
        ttk.Combobox(frame, textvariable=self.date_format_var, width=15, 
                     values=['%Y/%m/%d', '%Y-%m-%d', '%m/%d/%Y', '%Y%m%d'],
                     state='readonly').pack(side=tk.LEFT)
        
        frame = ttk.Frame(format_frame)
        frame.pack(fill=tk.X, pady=2)
        ttk.Label(frame, text="编码方式", width=10).pack(side=tk.LEFT)
        self.encoding_var = tk.StringVar(value=self.config['format']['encoding'])
        ttk.Combobox(frame, textvariable=self.encoding_var, width=15,
                     values=['utf-8-sig', 'utf-8', 'gbk'],
                     state='readonly').pack(side=tk.LEFT)
        
        agg_frame = ttk.LabelFrame(parent, text="聚合设置", padding="8")
        agg_frame.pack(fill=tk.X)
        
        frame = ttk.Frame(agg_frame)
        frame.pack(fill=tk.X, pady=2)
        ttk.Label(frame, text="盈利阈值", width=10).pack(side=tk.LEFT)
        self.threshold_var = tk.IntVar(value=self.config['aggregation']['profit_threshold'])
        ttk.Spinbox(frame, from_=0, to=100, textvariable=self.threshold_var, width=8).pack(side=tk.LEFT)
        ttk.Label(frame, text="%").pack(side=tk.LEFT)
```

- [ ] **Step 5: 实现辅助方法**

```python
    def _refresh_column_mappings(self):
        for key, var in self.col_vars.items():
            val = self.config['columns'].get(key, '')
            var.set(val)
            combo = var.master.winfo_children()[1]
            combo['values'] = self.detected_columns if self.detected_columns else ['']
    
    def _set_all_fields(self, value):
        for var in self.field_vars.values():
            var.set(value)
    
    def _collect_config(self):
        for key, var in self.col_vars.items():
            if var.get():
                self.config['columns'][key] = var.get()
        for key, var in self.field_vars.items():
            self.config['output_fields'][key] = var.get()
        self.config['format']['date_format'] = self.date_format_var.get()
        self.config['format']['encoding'] = self.encoding_var.get()
        self.config['aggregation']['profit_threshold'] = self.threshold_var.get()
        self.config['aggregation']['group_by'] = self.group_by_var.get()
```

- [ ] **Step 6: 在 _build_format_tab 中添加聚合字段下拉框**

在盈利阈值同一框架内添加：
```python
        frame = ttk.Frame(agg_frame)
        frame.pack(fill=tk.X, pady=2)
        ttk.Label(frame, text="聚合字段", width=10).pack(side=tk.LEFT)
        self.group_by_var = tk.StringVar(value=self.config['aggregation']['group_by'])
        ttk.Combobox(frame, textvariable=self.group_by_var, width=15,
                     values=self.detected_columns if self.detected_columns else ['门店编码'],
                     state='readonly').pack(side=tk.LEFT)
```

- [ ] **Step 7: 提交**

---

### Task 5: 实现日志面板和配置管理

**Files:**
- Modify: `process_big_file.py`

- [ ] **Step 1: 实现 _build_log_panel 方法**

```python
    def _build_log_panel(self, parent):
        log_frame = ttk.LabelFrame(parent, text="处理日志", padding="5")
        log_frame.pack(fill=tk.BOTH, expand=True, pady=(5, 0))
        
        self.log_text = tk.Text(log_frame, height=6, font=('Consolas', 8))
        self.log_text.pack(fill=tk.BOTH, expand=True, side=tk.LEFT)
        scrollbar = ttk.Scrollbar(log_frame, command=self.log_text.yview)
        scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        self.log_text.config(yscrollcommand=scrollbar.set)
```

- [ ] **Step 2: 实现 log, clear_log, update_progress 方法**

```python
    def log(self, message):
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        self.log_text.insert(tk.END, f"[{timestamp}] {message}\n")
        self.log_text.see(tk.END)
        self.root.update_idletasks()
    
    def clear_log(self):
        self.log_text.delete(1.0, tk.END)
    
    def update_progress(self, value, status=""):
        self.progress['value'] = value
        if status:
            self.status_var.set(status)
        self.root.update_idletasks()
```

- [ ] **Step 3: 实现 save_config 和 load_config 方法**

```python
    def save_config(self):
        self._collect_config()
        path = filedialog.asksaveasfilename(
            title="保存配置",
            defaultextension=".json",
            filetypes=[("JSON文件", "*.json")]
        )
        if path:
            try:
                with open(path, 'w', encoding='utf-8') as f:
                    json.dump(self.config, f, ensure_ascii=False, indent=2)
                self.log(f"配置已保存: {path}")
            except Exception as e:
                messagebox.showerror("错误", f"保存失败: {e}")
    
    def load_config(self):
        path = filedialog.askopenfilename(
            title="加载配置",
            filetypes=[("JSON文件", "*.json"), ("所有文件", "*.*")]
        )
        if path:
            try:
                with open(path, 'r', encoding='utf-8') as f:
                    loaded = json.load(f)
                self.config = loaded
                self._refresh_all_ui()
                self.log(f"配置已加载: {path}")
            except Exception as e:
                messagebox.showerror("错误", f"加载失败: {e}")
    
    def _refresh_all_ui(self):
        self._refresh_column_mappings()
        for key, var in self.field_vars.items():
            var.set(self.config['output_fields'].get(key, True))
        self.date_format_var.set(self.config['format']['date_format'])
        self.encoding_var.set(self.config['format']['encoding'])
        self.threshold_var.set(self.config['aggregation']['profit_threshold'])
        self.group_by_var.set(self.config['aggregation']['group_by'])
        self._refresh_group_by_combo()
```

- [ ] **Step 4: 实现拖放功能**

```python
    def _enable_drag_drop(self):
        def handle_drop(event):
            files = event.data.split()
            if files:
                path = files[0]
                if os.path.isfile(path) and (path.endswith('.xlsx') or path.endswith('.xls')):
                    self.input_file = path
                    self.file_var.set(path)
                    self.log(f"已选择: {path}")
                    self._detect_columns(path)
        
        self.root.drop_target_register(tk.DND.Files)
        self.root.dnd_bind('<<Drop>>', handle_drop)
```

- [ ] **Step 5: 提交**

---

### Task 6: 重构数据处理函数（保持核心逻辑）

**Files:**
- Modify: `process_big_file.py`

- [ ] **Step 1: 重构 convert_number_date_to_standard 函数支持自定义格式**

```python
def convert_number_date_to_standard(value, date_format='%Y/%m/%d'):
    if pd.isna(value) or value == '' or value is None:
        return value
    if isinstance(value, (int, float)) and value > 25569:
        try:
            excel_date = datetime(1899, 12, 30) + pd.Timedelta(days=int(value))
            return excel_date.strftime(date_format)
        except:
            pass
    date_str = str(value)
    if len(date_str) == 8 and date_str.isdigit():
        try:
            year, month, day = date_str[:4], date_str[4:6], date_str[6:8]
            date = datetime(int(year), int(month), int(day))
            return date.strftime(date_format)
        except:
            pass
    return value
```

- [ ] **Step 2: 实现 start_process 方法（带线程）**

```python
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
```

- [ ] **Step 3: 重构 _process_file 方法使用配置**

```python
def _process_file(self):
    try:
        self._collect_config()
        input_path = self.input_file
        cfg = self.config
        
        self.log(f"开始处理...")
        self.log(f"输入: {input_path}")
        self.log(f"文件大小: {os.path.getsize(input_path) / 1024 / 1024:.2f} MB")
        
        self.update_progress(5, "正在解析Excel数据...")
        df = pd.read_excel(input_path, sheet_name=0, header=0, dtype=str, keep_default_na=False)
        self.log(f"读取完成，行数: {len(df)}, 列数: {len(df.columns)}")
        
        cols = {col.strip(): idx for idx, col in enumerate(df.columns)}
        
        col_map = cfg['columns']
        required = [col_map['store_code'], col_map['opening_date'], 
                    col_map['monitor_date'], col_map['approval_status'], 
                    col_map['profit_judgment']]
        missing = [c for c in required if c not in cols]
        if missing:
            raise Exception(f"缺少必需列: {','.join(missing)}")
        
        date_format = cfg['format']['date_format']
        self.update_progress(20, "正在转换日期列...")
        df['★开业日期'] = df[col_map['opening_date']].apply(
            lambda x: convert_number_date_to_standard(x, date_format))
        df['★监控日期'] = df[col_map['monitor_date']].apply(
            lambda x: convert_number_date_to_standard(x, date_format))
        
        df['审批状态'] = pd.to_numeric(df[col_map['approval_status']], errors='coerce').fillna(0).astype(int)
        df['盈利性判断'] = pd.to_numeric(df[col_map['profit_judgment']], errors='coerce').fillna(0).astype(int)
```

- [ ] **Step 3: 继续重构聚合逻辑**

```python
        out_fields = cfg['output_fields']
        threshold = cfg['aggregation']['profit_threshold'] / 100.0
        
        if out_fields['latest_approval']:
            df['★最新审批状态'] = 0
        if out_fields['latest_profit']:
            df['★最新盈利判断'] = 0
        if out_fields['profit_50']:
            df['★50%盈利判断'] = 0
        if out_fields['total_profit_days']:
            df['★合计盈利天数'] = 0
        if out_fields['total_unprofit_days']:
            df['★合计不盈利天数'] = 0
        if out_fields['profit_ratio']:
            df['★盈利天数占比'] = 0.0
        if out_fields['total_monitor_days']:
            df['★合计监控天数'] = 0
        if out_fields['total_delivery_days']:
            df['★合计外卖天数'] = 0
        if out_fields['dinein_ratio']:
            df['★堂食预计占比'] = ''
        if out_fields['delivery_ratio']:
            df['★到店预计占比'] = ''
        if out_fields['approval_consistency']:
            df['检查-审批一致性'] = '混合'
        if out_fields['profit_consistency']:
            df['检查-盈利一致性'] = '混合'
```

- [ ] **Step 4: 重构聚合循环**

```python
        grouped = df.groupby(col_map['store_code'], sort=False)
        total_stores = len(grouped)
        self.log(f"共 {total_stores} 个门店")
        
        store_count = 0
        for store_code, group in grouped:
            store_count += 1
            if store_count % 1000 == 0:
                progress = 25 + int((store_count / total_stores) * 65)
                self.update_progress(progress, f"处理中... {store_count}/{total_stores}")
                self.log(f"  处理进度: {store_count}/{total_stores} 门店")
            
            group_sorted = group.sort_values('★监控日期', ascending=False)
            latest = group_sorted.iloc[0]
            latest_approval = latest['审批状态']
            latest_profit = latest['盈利性判断']
            
            profit_values = group['盈利性判断'].tolist()
            approval_values = group['审批状态'].tolist()
            
            profitable_count = sum(1 for v in profit_values if v == 1)
            unprofitable_count = sum(1 for v in profit_values if v == 0)
            total_days = len(profit_values)
            profit_ratio = profitable_count / total_days if total_days > 0 else 0
            profit_50 = 1 if profit_ratio >= threshold else 0
            
            approval_consistency = calculate_consistency(approval_values)
            profit_consistency = calculate_consistency(profit_values)
            
            mask = df[col_map['store_code']] == store_code
            
            if out_fields['latest_approval']:
                df.loc[mask, '★最新审批状态'] = latest_approval
            if out_fields['latest_profit']:
                df.loc[mask, '★最新盈利判断'] = latest_profit
            if out_fields['profit_50']:
                df.loc[mask, '★50%盈利判断'] = profit_50
            if out_fields['total_profit_days']:
                df.loc[mask, '★合计盈利天数'] = profitable_count
            if out_fields['total_unprofit_days']:
                df.loc[mask, '★合计不盈利天数'] = unprofitable_count
            if out_fields['profit_ratio']:
                df.loc[mask, '★盈利天数占比'] = round(profit_ratio, 4)
            if out_fields['total_monitor_days']:
                df.loc[mask, '★合计监控天数'] = total_days
            if out_fields['approval_consistency']:
                df.loc[mask, '检查-审批一致性'] = approval_consistency
            if out_fields['profit_consistency']:
                df.loc[mask, '检查-盈利一致性'] = profit_consistency
```

- [ ] **Step 5: 重构外卖天数和占比计算**

```python
            if col_map.get('delivery_days') and col_map['delivery_days'] in cols:
                if out_fields['total_delivery_days']:
                    dd = pd.to_numeric(group[col_map['delivery_days']], errors='coerce')
                    delivery_days = int(dd.max()) if dd.notna().any() else 0
                    df.loc[mask, '★合计外卖天数'] = delivery_days
            
            if all(col_map.get(k) and cols.get(col_map[k]) for k in ['daily_revenue', 'delivery_revenue', 'dinein_revenue']):
                if out_fields['dinein_ratio'] or out_fields['delivery_ratio']:
                    daily = pd.to_numeric(latest[col_map['daily_revenue']], errors='coerce')
                    delivery = pd.to_numeric(latest[col_map['delivery_revenue']], errors='coerce')
                    dine_in = pd.to_numeric(latest[col_map['dinein_revenue']], errors='coerce')
                    if pd.notna(daily) and pd.notna(delivery) and pd.notna(dine_in):
                        denom = dine_in - delivery
                        if abs(denom) > 0.0001:
                            x = (daily - delivery) / denom
                            x = max(0, min(1, x))
                            if out_fields['dinein_ratio']:
                                df.loc[mask, '★堂食预计占比'] = round(float(x), 4)
                            if out_fields['delivery_ratio']:
                                df.loc[mask, '★到店预计占比'] = round(float(1 - x), 4)
```

- [ ] **Step 6: 重构导出逻辑**

```python
        self.update_progress(90, "正在导出CSV...")
        self.log("正在导出CSV...")
        
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        base_name = os.path.splitext(os.path.basename(input_path))[0]
        output_path = os.path.join(os.path.dirname(input_path), f"{base_name}_processed_{timestamp}.csv")
        
        counter = 1
        while os.path.exists(output_path):
            output_path = os.path.join(os.path.dirname(input_path), f"{base_name}_processed_{timestamp}_{counter}.csv")
            counter += 1
        
        # 根据 output_fields 配置决定删除未勾选的输出列
        cols_to_drop = []
        out_fields = cfg['output_fields']
        field_to_col = {
            'latest_approval': '★最新审批状态',
            'latest_profit': '★最新盈利判断',
            'profit_50': '★50%盈利判断',
            'total_profit_days': '★合计盈利天数',
            'total_unprofit_days': '★合计不盈利天数',
            'profit_ratio': '★盈利天数占比',
            'total_monitor_days': '★合计监控天数',
            'total_delivery_days': '★合计外卖天数',
            'dinein_ratio': '★堂食预计占比',
            'delivery_ratio': '★到店预计占比',
            'approval_consistency': '检查-审批一致性',
            'profit_consistency': '检查-盈利一致性',
        }
        for key, col_name in field_to_col.items():
            if col_name in df.columns and not out_fields.get(key, True):
                cols_to_drop.append(col_name)
        if cols_to_drop:
            df = df.drop(columns=cols_to_drop)
        
        df = df.loc[:, (df != '').any(axis=0)]
        
        encoding = cfg['format']['encoding']
        df.to_csv(output_path, index=False, encoding=encoding)
        self.update_progress(100, "处理完成!")
        self.log(f"导出完成: {output_path}")
        self.log(f"输出文件大小: {os.path.getsize(output_path) / 1024 / 1024:.2f} MB")
        self.log("处理完成!")
        
        messagebox.showinfo("完成", f"处理成功!\n\n输出文件:\n{output_path}")
```

- [ ] **Step 7: 提交**

---

### Task 7: 最终整合和测试

**Files:**
- Modify: `process_big_file.py`

- [ ] **Step 1: 确保 main 入口正确**

```python
if __name__ == '__main__':
    if not HAS_PANDAS:
        root = tk.Tk()
        root.withdraw()
        messagebox.showerror("错误", "缺少 pandas 库，请运行: pip install pandas")
        sys.exit(1)
    
    root = tk.Tk()
    app = App(root)
    root.mainloop()
```

- [ ] **Step 2: 测试运行**

```bash
cd "D:/0_系统文件夹/桌面/web/T-vda.tstwg.cn-盈利分析可视化/【BI-Data-Tool】/0-BI-Data"
python process_big_file.py
```

- [ ] **Step 3: 验证所有功能**

- [ ] **Step 4: 提交**

---

## 验证清单

- [ ] 文件选择后自动检测列名
- [ ] 下拉框自动填充检测到的列名
- [ ] 所有配置项可保存/加载为 JSON
- [ ] 输出字段全选/全不选功能正常
- [ ] 日期格式配置生效
- [ ] 盈利阈值配置生效
- [ ] 处理日志正常显示（带时间戳）
- [ ] 进度条正确更新
- [ ] 原有数据默认配置处理结果一致
- [ ] 拖放文件功能正常
- [ ] 聚合字段下拉框正常填充

---

**Plan saved to:** `docs/superpowers/plans/2026-03-26-bi-data-tool-redesign-plan.md`
