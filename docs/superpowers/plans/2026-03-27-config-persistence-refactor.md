# 配置持久化重构实现计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 重构配置持久化功能，使用更优雅、稳定的方式实现

**Architecture:** 
- 创建独立的 `ConfigManager` 类统一管理配置的加载、保存
- 程序启动时加载配置，程序关闭时保存配置
- 使用 tkinter 变量追踪实现配置变更监听
- 配置变更只在关闭时保存一次，避免频繁IO

**Tech Stack:** Python, tkinter, json

---

## 1. 创建 ConfigManager 类

**Files:**
- Modify: `process_big_file.py` (在 DEFAULT_CONFIG 之后，App 类之前添加 ConfigManager 类)

- [ ] **Step 1: 添加 ConfigManager 类骨架**

```python
class ConfigManager:
    """配置管理器 - 统一处理配置的加载和保存"""
    
    CONFIG_FILE = "config.json"
    
    def __init__(self, default_config):
        self.default_config = default_config
        self.config = copy.deepcopy(default_config)
        self._load()
    
    def _get_config_path(self):
        return os.path.join(os.path.dirname(os.path.abspath(__file__)), self.CONFIG_FILE)
    
    def _load(self):
        """加载配置文件"""
        config_path = self._get_config_path()
        if os.path.exists(config_path):
            try:
                with open(config_path, "r", encoding="utf-8") as f:
                    loaded = json.load(f)
                self._merge_config(loaded)
            except Exception:
                pass  # 使用默认配置
    
    def _merge_config(self, loaded):
        """合并加载的配置到默认配置"""
        def merge_dict(base, override):
            for key, value in override.items():
                if key in base:
                    if isinstance(value, dict) and isinstance(base[key], dict):
                        merge_dict(base[key], value)
                    else:
                        base[key] = value
        merge_dict(self.config, loaded)
    
    def save(self):
        """保存配置到文件"""
        try:
            with open(self._get_config_path(), "w", encoding="utf-8") as f:
                json.dump(self.config, f, ensure_ascii=False, indent=2)
        except Exception:
            pass
```

- [ ] **Step 2: 验证语法**

Run: `python -m py_compile process_big_file.py`
Expected: 无错误

- [ ] **Step 3: 提交**

```bash
git add process_big_file.py
git commit -m "feat: add ConfigManager class skeleton"
```

---

## 2. 重构 App 类使用 ConfigManager

**Files:**
- Modify: `process_big_file.py`

- [ ] **Step 1: 修改 App.__init__ 使用 ConfigManager**

```python
class App:
    def __init__(self, root):
        self.root = root
        self.style = ttk.Style()
        self.root.title("BI数据预处理工具 v2.0")
        self.root.geometry("900x650")
        self.root.minsize(800, 600)

        self.input_file = None
        self.processing = False
        self.detected_columns = []
        self.config_manager = ConfigManager(DEFAULT_CONFIG)
        self.config = self.config_manager.config

        self._build_ui()
        self.root.protocol("WM_DELETE_WINDOW", self._on_closing)
```

- [ ] **Step 2: 修改 _on_closing 使用 config_manager.save()**

```python
def _on_closing(self):
    self.config_manager.save()
    self.root.destroy()
```

- [ ] **Step 3: 删除旧的 _load_config 和 _auto_save_config 方法**

删除以下方法（如果存在）:
- `_load_config`
- `_auto_save_config`

- [ ] **Step 4: 验证语法**

Run: `python -m py_compile process_big_file.py`
Expected: 无错误

- [ ] **Step 5: 提交**

```bash
git add process_big_file.py
git commit -m "refactor: use ConfigManager in App class"
```

---

## 3. 简化配置变更监听

**Files:**
- Modify: `process_big_file.py`

- [ ] **Step 1: 删除 _on_config_change 方法中的深拷贝比较逻辑**

```python
def _on_config_change(self, *args):
    """配置变更时收集配置（不保存，关闭时统一保存）"""
    self._collect_config()
```

- [ ] **Step 2: 删除所有 trace_add 和事件绑定中的 _on_config_change**

对于 Combobox，删除 `bind("<<ComboboxSelected>>", self._on_config_change)`
对于 Spinbox，删除 `bind("<FocusOut>", self._on_config_change)`

- [ ] **Step 3: 简化配置控件初始化**

控件初始化时直接使用 config 中的值，不需要 trace：

```python
self.date_format_var = tk.StringVar(value=self.config["format"]["date_format"])
# 不需要 trace_add
```

- [ ] **Step 4: 验证语法**

Run: `python -m py_compile process_big_file.py`
Expected: 无错误

- [ ] **Step 5: 提交**

```bash
git add process_big_file.py
git commit -m "refactor: simplify config change tracking"
```

---

## 4. 验证功能

**Files:**
- Modify: `process_big_file.py`

- [ ] **Step 1: 测试程序启动和关闭**

1. 运行程序 `python process_big_file.py`
2. 修改几个配置参数（日期格式、盈利阈值等）
3. 关闭程序
4. 检查 config.json 是否正确保存

- [ ] **Step 2: 测试配置加载**

1. 再次运行程序
2. 验证配置参数是否正确恢复

- [ ] **Step 3: 最终提交**

```bash
git add process_big_file.py
git commit -m "feat: config persistence complete - load on start, save on close"
```

---

## 架构说明

**重构前的问题：**
1. 配置持久化代码分散在多个方法中
2. 使用 trace_add 和事件绑定混搭，容易出错
3. 每次保存都深拷贝比较，效率低
4. 存在重复的 `_auto_save_config` 方法

**重构后的优势：**
1. 配置管理逻辑集中在一个类中
2. 保存时机明确（程序关闭时）
3. 代码简洁易维护
4. 避免频繁IO操作
5. 配置合并机制确保不会丢失默认配置中的字段
