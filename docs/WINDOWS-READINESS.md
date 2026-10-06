# Pulsar Windows 部署就绪审查（DR1）

审查范围：六仓（`pulsar-contracts / core / data / exec / app / ui`，main 头
contracts@902c926、core@524dcc0、data@702e4f8、exec@92ffb48、app 含本文档、
ui@30131ad）源码的 Windows 可移植性静态核查 + 依赖 wheel 可用性核查。
审查方法：全量源码扫描（AST + grep）覆盖文件锁、原子写、路径处理、编码、
子进程调用与平台分支；依赖按 PyPI wheel 平台覆盖逐一核对。

## 结论

**源码层面无需修改即可在 Windows 上部署**：未发现任何 POSIX-only API
（fcntl/msvcrt/flock/fork/signal/resource 均未使用），文本 I/O 全部显式
`encoding="utf-8"`（AST 扫描唯一命中是 `tomllib` 要求的二进制 `open(..., "rb")`，
无编码问题），路径构建全部走 `pathlib`，原子写全部使用跨平台的
`os.replace`（Windows 语义：原子覆盖已存在目标）。

部署侧有两个**环境级**前置项（`tzdata` 必装、git 建议在 PATH），加若干
行为差异注记，见下文风险清单与部署步骤。本次审查**未做任何代码修改**
（可移植性小修额度未动用——因为没有需要修的代码缺陷）。

## 核查明细

| 核查项 | 方法 | 结果 |
|-|-|---|
| 文件锁 | grep `fcntl\|msvcrt\|flock\|os.lockf` | 无命中。数据湖写入串行化用进程内 `threading.Lock`（`DataLake._lock_for`），跨平台 |
| 原子写 | 检查全部落盘点 | `DataLake` 分区写与 watermark 写：`.tmp-<uuid>` + `os.replace`；`pulsar-app` RunManifest：`.manifest.json.tmp` + `os.replace`；metrics/report 均 `write_text(encoding="utf-8")`。均为 Windows 兼容写法 |
| 路径处理 | grep 字符串拼路径 + 逐点复核 | 全部 `pathlib.Path`；`f"bars/{symbol}.raw.csv"` 类片段只作为 `/` 连接的单段相对键交给 `Path /`，Windows 合法 |
| 编码 | AST 扫描全部 `open/read_text/write_text` | 全部显式 `encoding="utf-8"`；唯一无 encoding 的 `open` 是二进制读（`tomllib.load` 要求），无风险 |
| 子进程 | grep `subprocess` | 仅两处：`git -C <repo> rev-parse HEAD`（RunManifest 代码版本盖章、实验 config_commit），`OSError/SubprocessError` 均有回退（退化为包版本号/`"unknown"`），git 缺失不致崩 |
| 平台分支 | grep `sys.platform / platform.` | 无平台分支代码——单一代码路径，三平台同构 |
| 服务边界 | 复核 `pulsar_ui.server` | `uvicorn.run(..., host="127.0.0.1")`，host 硬编码不可配；uvicorn 默认 asyncio loop（uvloop 非依赖，Windows 无影响）；纯 GET API |
| 长路径 | 复核湖/工件路径深度 | 湖分区最深 `bars_1d/symbol=XXXXXX/year=YYYY/part.parquet`，工件 `runs/<run_id>/events.parquet`，自身很短；仅当工作区本身嵌套极深时可能触及 MAX_PATH=260（见风险 R5） |

## 依赖 wheel 可用性（Windows / win_amd64）

锁定栈（`pulsar-contracts/tools/release/requirements-lock.txt`）逐项：

| 依赖 | Windows wheel | 备注 |
|-|-|---|---|
| numpy / pandas / pyarrow / duckdb | 有 | cp311–cp313 win_amd64 全覆盖（duckdb 亦提供 win_arm64） |
| pydantic / pydantic_core | 有 | rust 二进制 wheel，win_amd64 全覆盖 |
| fastapi / starlette / uvicorn / anyio / h11 | 有 | 纯 Python，无平台差异 |
| requests / urllib3 / certifi | 有 | 纯 Python |
| pytz / python-dateutil / six | 有 | 纯 Python |
| akshare 及其依赖 | 有 | akshare 纯 Python；lxml、mini-racer、curl_cffi、cffi 均有 win_amd64 wheel |
| baostock（备源，可选） | 有 | 纯 Python（socket 协议），无平台问题 |
| **tzdata** | **有（必装）** | 见风险 R1——不是任何包的声明依赖，需部署时显式安装 |

## 风险清单

| # | 级别 | 风险 | 说明与缓解 |
|-|-|-|-|
| R1 | **高（部署阻塞，一条命令解决）** | `ZoneInfo("Asia/Shanghai")` 在 Windows 上找不到系统时区库 | `pulsar_contracts.common` 在**模块导入时**构造 `SHANGHAI_TZ`；Windows 无 POSIX 形态系统 tz 数据库，未装 `tzdata` 包时首个 import 即抛 `ZoneInfoNotFoundError`（六仓全灭）。缓解：部署时 `pip install tzdata`（纯 Python，~120KB）。已列入下方部署步骤与 drill 脚本说明。长期可考虑把 `tzdata` 收进 contracts 的依赖（涉及跨仓锚点联动，留待版本发布任务） |
| R2 | 中 | git 不在 PATH 时 RunManifest 代码版本退化为包版本号 | 优雅降级、不崩；但"哪个 commit 上了线"的可追溯性弱化。缓解：安装 Git for Windows（或设 `PULSAR_CODE_VERSION` 环境变量显式盖章） |
| R3 | 中 | `os.replace` 在目标被其它进程持有打开句柄时抛 `PermissionError` | 单用户本地使用（设计边界）不触发；但若回填与 UI / DuckDB 只读连接真并发读写同一 parquet 分区，Windows 的强制文件锁语义比 POSIX 更容易撞上。缓解：避免同一湖目录上并发跑回填与查询（现有工具均为短连接、顺序使用） |
| R4 | 低 | `events.parquet` 工件为单文件直写（非 tmp+replace） | 与平台无关的既定行为：run 结束一次性落盘，中断留下不完整文件的概率极低；重跑即重建。仅记录 |
| R5 | 低 | MAX_PATH=260 限制 | 湖/工件自身路径很短；把工作区放在浅路径（如 `C:\pulsar\`）或开启 Windows 长路径（组策略/注册表 `LongPathsEnabled`）即可彻底规避 |
| R6 | 低 | Windows 控制台缺省代码页下 CLI 中文输出的显示问题 | Python 3.6+ 控制台 I/O 走 UTF-8（PEP 528），`chcp 65001` 或 Windows Terminal 可彻底消除乱码；不影响落盘文件（全部 UTF-8） |
| R7 | 低 | `tools/drill_local.sh` 为 bash 脚本 | 在 Git Bash / WSL 下原样可跑；纯 PowerShell 用户按 QUICKSTART 的分步命令执行等价操作（每步都是单条命令） |

## Windows 部署步骤要点

1. **Python**：安装 Python 3.11+（python.org 安装器，勾选 "Add python.exe to
   PATH"），`py -3.12 -m venv .venv` 建虚拟环境。
2. **（Windows 必做）时区数据**：`.venv\Scripts\pip install tzdata` —— 不装
   则任何 `import pulsar_contracts` 直接失败（R1）。
3. **一条命令安装锁定栈**：
   `.venv\Scripts\pip install -r pulsar-contracts\tools\release\requirements-lock.txt`
   （全部为公网 HTTPS git 锚点 + PyPI wheel，无需凭据）。
4. **免费实时源扩展（需要真实入湖时）**：
   `.venv\Scripts\pip install "pulsar-data[akshare] @ git+https://github.com/vanzeph/pulsar-data.git@702e4f86bf63c661eaa879311607ff689e4eb437"`。
5. **Git for Windows**（建议，R2）：让 RunManifest 记录实验配置的 git commit。
6. 之后按 `docs/QUICKSTART.md` 的入湖 → 实验 → 报告/UI 步骤操作（命令均跨平台，
   路径分隔符由 `pathlib`/CLI 自行处理）。
7. 冒烟验证：`pulsar --version`、`pulsar-data --help`、`python -m pulsar_ui.report
   <run_dir>`、`pulsar-ui --port 7800 --runs-dir runs --lake-dir data\lake`。

## GPU 说明

当前模型器（`equal_weight` / `linear_score` / `ic_weighted`，见
`pulsar-core/src/pulsar_core/modelers.py`）为纯统计实现，**纯 CPU 运行**，
不引入任何 GPU/深度学习依赖（栈内无 torch/tensorflow/onnxruntime），
Windows 部署对 GPU 无任何要求。GPU 能力属于未来 ML 模型器任务（GBDT/神经
网络族）的预留项：届时按该任务的设计引入对应 wheel（torch 等）与本地
训练/推理约定，模型文件按核心引擎设计作为策略资产纳入 RunManifest 版本
管理，本次审查不预置任何 GPU 相关内容。

## 审查方法学附注

- 扫描对象：六仓 `src/` 全量 `.py`（非测试代码；测试同法扫描亦无命中）。
- AST 扫描规则：凡 `open()`（文本模式）/ `.read_text()` / `.write_text()`
  调用缺少 `encoding=` 关键字即报；本次唯一报告项经复核为二进制模式误报。
- grep 规则：`fcntl|msvcrt|flock|os.lockf`、`subprocess|signal|os.fork`、
  `resource|pwd|grp|termios`、`sys.platform|platform.`、字符串路径拼接；
  命中项逐一人工复核（本报告"核查明细"表即全部命中及其处置）。
- 本审查为静态核查（macOS 主机完成）；上真机 Windows 前建议按上述部署
  步骤在目标机复跑 `tools/drill_local.sh`（Git Bash）作为动态确认。
