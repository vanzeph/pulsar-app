# Pulsar 快速开始（本地装载 → 数据入湖 → 训练回测 → 报告看板）

本文是"新机器从零到第一次回测出报告"的最短路径，全部步骤在同一台本地
设备完成（macOS / Linux 原样可跑；Windows 额外先装 `tzdata`，见
`docs/WINDOWS-READINESS.md`）。文末的 `tools/drill_local.sh` 把同一流程
串成一键脚本，本文即该脚本的分步版。

## 0. 本地布局约定

Pulsar 是六仓多包拓扑，建议把"代码"与"数据/配置/产物"放进同一个工作区、
不同子目录（数据湖与 runs 产物**不进任何仓库**）：

```text
pulsar-workspace/                # 演练工作区（任意名字）
├── repos/                       # 六仓克隆（代码区）
│   ├── pulsar-contracts/        # 端口契约 + 发布锁定清单（tools/release/）
│   ├── pulsar-core/             # 核心引擎（因子/实验/回测）
│   ├── pulsar-data/             # 数据源适配 + 数据湖 + 回填 CLI
│   ├── pulsar-exec/             # 执行端口实现（回测撮合/模拟）
│   ├── pulsar-app/              # 运行时装配 + CLI（本文档所在仓）
│   └── pulsar-ui/               # 只读看板 + 静态报告
├── experiments/                 # 你的实验 TOML（模型注册表，建议 git 管理）
├── data/
│   ├── lake/                    # 本地数据湖（Parquet 分区）
│   └── backfill-report.json     # 入湖完整性报告
├── runs/                        # 每次 run 的三工件 + report.html
└── .venv/                       # 虚拟环境
```

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

## 5. 添加自定义因子（本地克隆里写代码 + 注册）

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
`factors.names` 加 `"close_over_ma10"` 即用。改了核心代码想跑全套测试：
`cd repos/pulsar-core && pip install -e ".[dev]" && pytest`。模型器
（`modelers.py` 的 `MODEL_REGISTRY`）与 universe（`register_universe`）
的扩展同一模式。

## 6. C6 上下线状态怎么改

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

## 7. 一键复跑

```bash
bash repos/pulsar-app/tools/drill_local.sh /path/to/pulsar-workspace
```

脚本从零执行：建 venv → 锁定清单安装（含 Windows tzdata 探测）→ akshare
扩展 → 20 标的×近 1 年真实入湖 → 写实验/run 配置 → 训练回测 → 三工件
schema 核验 → report.html → UI 冒烟（起、探、关）。全程只写工作区目录。
