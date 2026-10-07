# Pulsar 快速开始（本地装载 → 数据入湖 → 训练回测 → 报告看板）

本文是"新机器从零到第一次回测出报告"的最短路径，全部步骤在同一台本地
设备完成（macOS / Linux 原样可跑；Windows 额外先装 `tzdata`，见
`docs/WINDOWS-READINESS.md`）。文末的 `tools/drill_local.sh` 把同一流程
串成一键脚本，本文即该脚本的分步版。

## 0. 本地布局约定

Pulsar 是六仓多包拓扑，建议把"代码"与"数据/配置/产物"放进同一个工作区、
不同子目录（数据湖与 runs 产物**不进任何仓库**）。统一存储引擎（STORE1）
落地后，工作区是**四层分离**布局：代码库（git）/ 存储根（store）/ 数据湖
（lake）/ 运行产物（runs），四层各管各的版本化：

```text
pulsar-workspace/                # 演练工作区（任意名字）
├── repos/                       # 第一层：六仓克隆（代码区，git 管版本）
│   ├── pulsar-contracts/        # 端口契约 + 发布锁定清单（tools/release/）
│   ├── pulsar-core/             # 核心引擎（因子/实验/回测）
│   ├── pulsar-data/             # 数据源适配 + 数据湖 + 回填 CLI
│   ├── pulsar-exec/             # 执行端口实现（回测撮合/模拟）
│   ├── pulsar-app/              # 运行时装配 + CLI（本文档所在仓）
│   └── pulsar-ui/               # 只读看板 + 静态报告
├── store/                       # 第二层：统一存储根（STORE1）
│   ├── catalog.db               #   SQLite catalog（名字/版本/声明注册名/路径引用）
│   └── objects/                 #   内容寻址对象（sha256 扇出目录）
├── data/
│   ├── lake/                    # 第三层：本地数据湖（Parquet 分区）
│   └── backfill-report.json     # 入湖完整性报告
├── runs/                        # 第四层：每次 run 的三工件 + report.html
└── .venv/                       # 虚拟环境
```

要点：`store/` 里放**实验 TOML 与自定义代码**（内容寻址、带历史与回滚），
`lake/` 与 `runs/` 目录**不迁移**——catalog 只记录它们的绝对路径引用。
`experiments/` 散文件目录的老用法仍然可用（§3、§7），但推荐新工作走 store。

克隆六仓：

```bash
mkdir -p pulsar-workspace/repos && cd pulsar-workspace/repos
for r in contracts core data exec app ui; do
    git clone https://github.com/vanzeph/pulsar-$r.git
done
```

## 1. 一条命令安装锁定栈

`pulsar-contracts/tools/release/requirements-lock.txt` 是六仓的完整锁定
环境（六包 git 锚点 + 全部传递依赖钉版）：

```bash
python3 -m venv .venv
.venv/bin/pip install --upgrade pip
.venv/bin/pip install -r repos/pulsar-contracts/tools/release/requirements-lock.txt
```

实测（Apple Silicon、Python 3.13、暖缓存）：约 9 秒装齐六仓 31 个钉版包。
无需任何凭据——全部引用都是公网 HTTPS git 锚点。

需要**真实联网入湖**（akshare 免费源）时加装实时源扩展：

```bash
.venv/bin/pip install "pulsar-data[akshare] @ git+https://github.com/vanzeph/pulsar-data.git@702e4f86bf63c661eaa879311607ff689e4eb437"
```

（锚点取锁定清单里 `pulsar-data` 当行的完整 hash；Windows 用户先
`pip install tzdata`，详见 `docs/WINDOWS-READINESS.md`。）

冒烟：`.venv/bin/pulsar --version`、`.venv/bin/pulsar-data --help`。

## 2. 免费数据入湖（约 20 只 × 近 1 年，真实联网）

```bash
.venv/bin/pulsar-data backfill --source akshare --lake data/lake \
    --start 2025-10-01 --end 2026-09-30 \
    --symbols "SH600519,SH600036,SH601318,SH600276,SH601899,SH603288,SH601127,SH600900,SH601088,SH601012,SZ000001,SZ000858,SZ002415,SZ002594,SZ000651,SZ000333,SZ300750,SZ300059,SH688012,SH688111" \
    --report data/backfill-report.json
```

- 适配器自带限速（默认 0.6s/请求）与质量门：原始价 + 复权因子 + 停牌/
  公司行为一并入湖；某标的源数据自相矛盾时**拒绝猜因子、整标的失败**
  （fail-loud，演练中 SH688981 即如此，换 SH688012 即可）。
- 完整性报告结论看两行：`unexplained_gaps=0` 且 `failed_symbols` 为空，
  即"相对交易日历无未解释缺 bar"。
- 复核已有湖：`.venv/bin/pulsar-data verify --lake data/lake --start
  2025-10-01 --end 2026-09-30 --symbols "<同上清单>"`（带 `--symbols`：
  当前版本不带标的清单时不会应用湖内停牌解释，停牌日会被误报为 gap——
  已知小缺陷，见演练报告）。

实测：20 标的 × 241 个交易日，575 秒（含限速与公司行为抓取），
`unexplained_gaps=0`。

## 3. 配置实验并跑训练 + 回测（research 模式）

实验 = 一份 TOML（因子子集、模型器、组合构建、回测窗口全部配出来，
新实验零代码）。把 `repos/pulsar-core/experiments/momentum_value.toml`
复制到 `experiments/`，改三处：universe 换成你入湖的标的、窗口落在数据
覆盖内（给模型器留足 warmup，`ic_weighted` 需 20 + lookback + horizon + 1
根 bar）、保留 C6 要求的 `status` 字段（research 模式用 `candidate`）：

```toml
[experiment]
id = "drill_momentum_2026"
status = "candidate"                # candidate | active | retired（C6 状态机）

[universe]
symbols = ["SH600519", "SH600036", "..."]   # 你入湖的 20 只

[factors]
names = ["momentum_20", "volatility_20", "reversal_5"]
preprocess = ["winsorize", "zscore"]

[model]
type = "ic_weighted"
params = { lookback = 60, horizon = 5, min_points = 10 }

[portfolio]
method = "top_n"
top_n = 5
rebalance = "monthly"

[backtest]
start = 2026-03-01
end = 2026-09-30
costs = "a_share_default"
seed = 7
```

再写一份 app 级 run 配置（装配用，`experiments/drill_run.toml`）：

```toml
[run]
mode = "research"

[data]
sources = ["akshare"]
lake_dir = "data/lake"

[exec]
venue = "backtest"
```

用本仓自带演练入口跑（一个命令两段：先以 `pulsar research` 装配并归档
app 级 RunManifest，再做实验训练 + 回测并落三工件）：

```bash
.venv/bin/python repos/pulsar-app/tools/drill_runner.py \
    --lake data/lake \
    --experiment experiments/drill_momentum.toml \
    --run-config experiments/drill_run.toml \
    --runs-dir runs
```

产物落在 `runs/<run_id>/`：`run_manifest.json` + `events.parquet` +
`metrics_report.json`。实测 147 个交易日、33 笔成交，约 1 秒（不含入湖）。
说明：`pulsar-app` 的包体按架构基线是装配骨架、不内置插件，所以由
`tools/drill_runner.py` 在进程内注册湖读侧端口与回测撮合器两个插件后再
调用真正的 `pulsar research` CLI（与 `tests/e2e/harness.py` 同一装配缝）。

## 4. 看报告与看板

```bash
# 自包含静态 HTML 报告（双击可看、可离线分享）
.venv/bin/python -m pulsar_ui.report runs/<run_id>

# 本地只读看板（回测对比/因子/交易/数据湖四视图）
.venv/bin/pulsar-ui --port 7800 --runs-dir runs --lake-dir data/lake
# 浏览器打开 http://127.0.0.1:7800/ ；用完 Ctrl-C 关闭（仅绑定 127.0.0.1）
```

看板消费的就是上面三工件与数据湖目录，别无依赖；API 见
`GET /api/runs`、`/api/runs/{id}/equity|trades|manifest`、`/api/lake/coverage`。

## 5. 统一存储与 Agent 写入接口（pulsar store）

`store/` 是工作区的**统一存储根**：实验配置（`experiments`）与自定义代码
（`code`）作为内容寻址对象存入（sha256，同名多版本、可回滚、读取时校验
hash 防篡改）；数据湖与 runs 目录用 `attach` 纳入 catalog 索引（只记路径，
不迁移目录）。写入一律**先校验后落盘**：代码做语法编译 + import 白名单扫描
（`os`/`subprocess`/网络等危险面在硬拒绝清单上，白名单 = numpy/pandas/
math/typing/pulsar_core 公共 API 等，配置处 `PULSAR_STORE_IMPORT_WHITELIST`
或 `pulsar_app.store.validation.DEFAULT_IMPORT_WHITELIST`）+ 注册名冲突检查
（执行一次预览、比对核心注册表与存储内其它代码对象的声明名，随后回滚不留
残留）；实验 TOML 校验语法、凭据基线与 C6 的 `status` 形状。

```bash
# 写入一个自定义因子（examples/store/custom_factor.py 是完整示例）
.venv/bin/pulsar store put repos/pulsar-app/examples/store/custom_factor.py \
    --namespace code --name custom_factor
#   -> code/custom_factor seq=1 hash=9feb8e99855f created
#      declares close_over_ma10 (factor)

# 写入实验 TOML（examples/store/experiment.toml，引用了上面的自定义因子）
.venv/bin/pulsar store put repos/pulsar-app/examples/store/experiment.toml \
    --namespace experiments --name momentum_store_demo

# 湖与 runs 目录纳入 catalog（只记路径引用）
.venv/bin/pulsar store attach data/lake --namespace lake --name default
.venv/bin/pulsar store attach runs --namespace runs --name default

.venv/bin/pulsar store list                      # 四个命名空间一览
.venv/bin/pulsar store history --namespace experiments --name momentum_store_demo
.venv/bin/pulsar store rollback --namespace experiments --name momentum_store_demo --to 1
.venv/bin/pulsar store get --namespace experiments --name momentum_store_demo \
    --hash <完整sha256> --out recovered.toml      # 按 hash 取回同一版本
```

存储根默认 `./store`（或环境变量 `PULSAR_STORE_ROOT`），CLI 加 `--store` 可
显式指定。Python API 与 CLI 完全同面：

```python
from pulsar_app.store import Store
from pulsar_app.store.loader import materialize_code, load_experiment_object

store = Store("store")
version, created = store.put("code", "custom_factor", source_text)
store.attach("lake", "default", "data/lake")

materialize_code(store, "custom_factor")     # 校验后注册进 pulsar-core 注册表
experiment = load_experiment_object(store, "momentum_store_demo")
```

### 5.1 从 store 装配运行（RunManifest 引用对象 hash）

app 级 run 配置加一段 `[store]`（示例 `examples/store/run_store.toml`）：

```toml
[store]
root = "store"
code = ["custom_factor"]                 # 先物化注册，再装配
experiments = ["momentum_store_demo"]    # 内容 hash 钉进 RunManifest
```

`pulsar research` 装配时先把 `code` 列表里的对象按 hash 校验、物化并注册进
核心注册表（因子/模型器/预处理/组合/universe 五张表），再把 code 与
experiments 对象的 `(namespace, name, sha256, seq)` 写进 RunManifest 的
`store_objects` 段。**复现**就是按 manifest 里的 hash 从 store 取回同一份
字节——head 后来怎么变、回滚过几次都不影响。

### 5.2 老路子仍然可用：本地克隆里写代码 + 注册

因子是"代码 + 注册"层（不是配置层）。在**本地克隆**里写（不发布也能用，
只要装成 editable；见下）：

```python
# repos/pulsar-core/src/pulsar_core/my_factors.py（或你自己的包，只要先 import）
from pulsar_contracts import Bar
from pulsar_core.factors import FactorDefinition, register_factor

def compute(bars) -> float | None:
    if len(bars) < 10:
        return None                      # 历史不足 -> 报缺失，不猜
    closes = [b.close for b in bars]
    return closes[-1] / (sum(closes[-10:]) / 10) - 1.0

register_factor(FactorDefinition(
    name="close_over_ma10",
    label="close / MA10 - 1",
    compute=compute,
    direction=1,                         # 越大越好；反向因子填 -1
    min_bars=10,
))
```

在你的运行入口 `import my_factors`（触发注册）后，实验 TOML 里
`factors.names` 加 `"close_over_ma10"` 即用（与 §5 的 store 路子殊途同归：
store 只是让"代码 + 配置"多了版本化、校验与按 hash 复现）。改了核心代码想
跑全套测试：`cd repos/pulsar-core && pip install -e ".[dev]" && pytest`。模型器
（`modelers.py` 的 `MODEL_REGISTRY`，或 `pulsar_core.register_model`）与
universe（`register_universe`）的扩展同一模式。

## 6. ML 模型器与 GPU 训练（可选 extra：pulsar-core[ml]）

ML 能力是**可选 extra**：默认安装零 torch，`import pulsar_core` 与全部非
ML 路径不受影响；只有用到 `mlp_torch` / `lstm_torch` 模型器时才需要装。

### 6.1 安装 torch

macOS / Linux（CPU 版，约 200MB）：

```bash
.venv/bin/pip install "pulsar-core[ml] @ git+https://github.com/vanzeph/pulsar-core.git@<锚点>"
# 或本地克隆开发形态（与 §5 一致）：
.venv/bin/pip install -e "repos/pulsar-core[ml]"
```

Windows + NVIDIA GPU（官方 CUDA 轮子，先装 torch 再装包，extra 即已满足）：

```powershell
.venv\Scripts\pip install torch --index-url https://download.pytorch.org/whl/cu124
.venv\Scripts\pip install -e "repos\pulsar-core[ml]"
```

（cu124 为 CUDA 12.4 轮子线；驱动 ≥ 525 即可，无需单独装 CUDA Toolkit——
轮子自带 CUDA 运行时。装错线时 `torch.cuda.is_available()` 会是 False，
详见 `docs/WINDOWS-READINESS.md` 的 GPU 部署节。）

验证：

```bash
.venv/bin/python -c "import torch; print(torch.__version__, torch.cuda.is_available())"
```

### 6.2 实验配置与设备选择

`repos/pulsar-core/experiments/mlp_torch_example.toml` 是完整示例；模型器
直接进 `[model] type`，参数全在 `params`：

```toml
[model]
type = "mlp_torch"
params = { device = "auto", epochs = 50, lr = 0.01, hidden = [16],
           lookback = 120, horizon = 5, batch_size = 256, seed = 0 }
```

- `device`：`auto`（默认，有 CUDA 用 CUDA、否则 CPU）/ `cuda` / `cpu`；
  强制 `cuda` 但不可用时**自动回退 CPU** 并写进训练环境记录，不中断运行。
- `lstm_torch` 同一入口，多一个 `window`（滚动窗口长度）：序列样本 =
  每标的过去 `window` 天的因子行序列。
- 训练消费 FactorEngine 的预处理因子面板（标签为 `horizon` 日前瞻收益的
  截面 z 分数），打分进既有组合管线——RiskGate 出口不可绕过。

### 6.3 训练工件与"重跑不重训"

给装配/演练入口传 `runs_root`（即本文的 `runs/` 目录）后：

- 训练产物落 `runs/<run_id>/model_artifact/`：`weights.pt`（state_dict）+
  `training_config.json`（参数/因子/标准化/环境）+ `artifact.json`
  （逐文件 sha256）；RunManifest 增加 `model_artifact` 段
  （路径 + 两个 hash + 训练环境）。
- **同一 run_id 重跑不重训**：runner 先查 `runs/<run_id>/model_artifact/`，
  命中即校验 sha256 后从钉版工件加载推理（origin 记为 `pinned`）；
  要强制重训在 `params` 里写 `retrain = true`。
- 确定性契约：CPU 训练/推理严格逐位一致（种子 + 确定性算法模式 +
  环境记录入工件）；GPU 为尽力保证。篡改权重文件会在加载时被
  sha256 校验拒绝，绝不带病推理。

## 7. C6 上下线状态怎么改

`experiment.status` 是装配层强校验的状态机：`candidate` 只能跑 research；
`active` 才允许 paper / live；`retired` 只读复盘（拒绝重跑）。上下线是
人工驱动、就地改那**一行** `status`（experiments 目录本身即注册表，git
历史就是审计轨迹）。推荐用核心引擎的安全入口改（要显式确认 + 记原因）：

```python
from pulsar_core import activate_experiment, retire_experiment

activate_experiment(            # 上线 candidate -> active
    "experiments/drill_momentum.toml",
    reason="passed research acceptance 2026-10-05", operator="you",
    confirmed=True)
retire_experiment(              # 下线 active -> retired（运行中会话立即停止产生新意图）
    "experiments/drill_momentum.toml",
    reason="signal decayed", operator="you")
```

每次装配会把所用配置的 git commit 写进 RunManifest（"哪个版本上了线"
可追溯）；实验文件不在 git 仓库里时该项记 `unknown`——这也是建议把
`experiments/` 单独 git 化的原因。

## 8. 一键复跑

```bash
bash repos/pulsar-app/tools/drill_local.sh /path/to/pulsar-workspace
```

脚本从零执行：建 venv → 锁定清单安装（含 Windows tzdata 探测）→ akshare
扩展 → 20 标的×近 1 年真实入湖 → 写实验/run 配置 → 训练回测 → 三工件
schema 核验 → report.html → UI 冒烟（起、探、关）。全程只写工作区目录。
