研究有效性与数据治理审查结论：**当前基线不通过最终排名资格与个人依据退役验收。** 本次独立发现 3 个已执行合成重现的问题。现有离线 OOD 的 9 个场景、26 项检查本次全部通过，但未覆盖这 3 个反例。通过既有夹具不能消除这些发现。

本报告仅针对下面的已检工作树。父代理随后实施的修复不属于本次原始结论，须另做独立复测。

**审查身份与范围**

我是同一产品内、未继承父会话上下文的内部独立上下文代理，负责研究有效性、数据治理及复杂系统风险；不是外部机构，不提供外部背书。审查过程中未获得另一审查者的发现，未再委派、修改文件、访问网络、启动采集或打开生产数据库。未读取个人记录值、私人会话、凭据或个人轨迹。所有执行检查只使用内存 SQLite 和合成数据。

已检 HEAD 为 `575ef0c97966542c8292525a08601ee9765451b9`，检查末尾再次读取未变。该树存在与预算有关的脏改动：`configs/orcid-2026-production.json`、`configs/streaming.json`、`src/gapi/task.py`、`tests/test_task_resources.py`。因此 HEAD 本身不能完整表示所审内容；以下实际文件 SHA-256 是本报告的工作树基线。未把旧 CI 或既有完整回归报告当作新工作树的执行证明。

源码根为 `C:/Users/MzgyM/Documents/Codex/2026-10-21/oo`，以下源码行号相对此根。

| 已检关键文件 | 实际字节 SHA-256 |
|---|---|
| `src/gapi/global_analysis.py` | `34acd7e008f04e884bdb14ae45c4bbacd29486a8a415f5610a7add663d5df86d` |
| `src/gapi/career_store.py` | `4f03fdf9ef078a9000ad737f210e863f7dfdbb17967f5772cae46aba2b281214` |
| `src/gapi/career_rules.py` | `24e5b2c8ccc738c82cf8183f3eb95cddfee8cfd8a020152fb625033501da9da8` |
| `src/gapi/career_normalization.py` | `71f8ec2586a19130adeee140fc9099fe81928cd7500d9606fade2e93f68733fc` |
| `src/gapi/corroboration.py` | `b6dd655f5a8be190a66c83161f3d39c7e57fc7226277447648000db7a763ae80` |
| `src/gapi/official_profiles.py` | `21be9874b5d43dd0c7789314992ea0da46b430d85f1e1377c41330afd033f988` |
| `src/gapi/collection.py` | `b6badadd93c0a0c8b5b40282372aef54630c18c221395386896a3c9be0edc3ce` |
| `src/gapi/task.py` | `ab8cad38c15c37009d0f6a87ae15ead78a966e0671856ded275ee78f689e3dc5` |
| `src/gapi/ood.py` | `de1d7a3be085fd6908b1994dfcefbc8bc0e1908c754138d93eed84c9a9e66b38` |
| `src/gapi/frozen_input.py` | `fa609ab5696abb5db393c020ab428b113d46c1d4380abd190f19949e26740e5e` |
| `src/gapi/publication.py` | `7e909b0bb6d32f6c31d61373ab4dd8258692251b0ffea55dead75ad1fd7883fe` |
| `configs/streaming.json` | `1f922e9b5b273ec98612b2caa2b7dccb3db314b2abafed793e3209969a3ee723` |
| `configs/orcid-2026-production.json` | `1b81a9f45751b1e2a3034113c70668be75b8d3aa07f9df6cda916581b3b27864` |
| `docs/METHODOLOGY.md` | `ffeef4af7ad4a0ab031da5aa2e99d912113a8285ee62fe10fb559f2be916a982` |
| `docs/STREAMING.md` | `edd87ecfc85ad5f3d4662fd90efadbb88902c1b0d2689d5dfa5aefe70eaba1ca` |
| `docs/OOD.md` | `bed0fd8d16ad27f6600623ec6f6a8cfcd16af8962fd0d082cc446444d4ed992e` |

交付包在审查期间由父代理加入“独立上下文多智能体红队”的最新验收口径，这是已观察到的文档变化，不是本代理修改。最后读取的交付包 SHA-256 为 `42befdb3b71d205b14f2354a2877183d673a1e50309a09be843806c5e4e48856`；配套进展 JSON 为 `a165b3f4d9e2fba134d5704891f1056b79f2c8aeb68e640d7fa89139c9fba652`。这些文件仍可能由任务 owner 更新。

**R1 — P2：年度评分门控没有绑定当前来源身份，弱于完整性摘要的判断**

位置：

- `src/gapi/career_store.py:29–40`，`verified_archive_sql`
- `src/gapi/career_store.py:513–516`，`source_integrity` 的身份匹配判断
- `src/gapi/global_analysis.py:175–181`、`316–318`，关系和分母使用该评分门控

`source_integrity()` 只有在 `runs.identity` 等于当前来源规格与规则的摘要、且状态为 `verified` 时，才报告完整性通过。然而实际评分 SQL 只要求同名来源有 `runs.status='verified'`，不比较身份。两个权威判断可以对同一个来源给出相反结论。

已执行的最小内存重现：

```python
import sqlite3
from types import SimpleNamespace
from gapi.career_store import verified_archive_sql

db = sqlite3.connect(":memory:")
db.executescript("""
CREATE TABLE runs(source TEXT, identity TEXT, status TEXT);
CREATE TABLE events(source TEXT);
""")
db.execute("INSERT INTO runs VALUES(?,?,?)",
           ("annual", "old-source-identity", "verified"))
db.execute("INSERT INTO events VALUES(?)", ("annual",))

task = SimpleNamespace(config={
    "rules_version": "new-rules",
    "sources": {
        "annual": {
            "kind": "orcid-summary-tar-gzip",
            "version": "new-version",
        }
    },
})
predicate, params = verified_archive_sql(task)
admitted = db.execute(
    "SELECT count(*) FROM events e WHERE " + predicate, params
).fetchone()[0]
assert admitted == 1       # 本次实际结果；预期应拒绝身份不匹配
```

影响：若评分入口绑定的 task 与被读数据库或冻结输入的来源身份不一致，旧来源的 verified 状态可能足以让年度事件进入分数和强度分母。`CareerStore.start()` 有续采身份检查，但它不是所有只读评分与冻结输入读取路径的证明；不能用写入入口的保护代替评分边界的保护。本次没有证明生产库已发生这种漂移，发现是可重现的评分边界缺陷。

最小修复：在现有来源身份 owner 下，让评分门控与完整性摘要共享“当前来源身份匹配且已验证”的条件；不要再各自维护一套含义。复测应覆盖当前身份合法通过、旧 version、旧 rules、缺 run、partial/failed，以及普通非档案来源的合法读取。

**R2 — P1：被排除的未完成学历仍能进入强度分母，改变已发表的强度分数**

位置：

- `src/gapi/global_analysis.py:196–201`：关系输入对 `orcid-*` 重新执行 completed-only 分类
- `src/gapi/global_analysis.py:303–325`：强度分母直接按原始/已有解释的 kind/status 计人数，没有执行相同的完成资格判断
- `src/gapi/career_normalization.py:56–99`：常规词汇解释主要作用于 `orcid-public-api`，年度来源不会自动获得同样的排除解释
- `src/gapi/orcid_stream.py:112–123`：原始 ingress 用旧分类保存学历与 observed 状态

年度来源中 `role="PhD not completed"` 的原始事件可以具有 `kind="education"`、`stage="doctorate"`、`status="observed"`。关系构造会把其 stage 排除；分母却仍计入这个人。结果是未完成学历没有新增任何有效关系，仍能压低某机构的强度分数并抬高其他机构的相对分数。

以下是本次实际执行的 fixture 构造。它使用项目已有 `_Fixture` 的纯内存实现，明确绑定年度源身份；不打开任务目录或生产库：

```python
from gapi.ood import _Budget, _Fixture
from gapi.global_analysis import rank_global
from gapi.task import digest

f = _Fixture(_Budget())
f.task.config.update(
    rules_version="career-rules/1",
    sources={
        "orcid-2026-summary": {
            "kind": "orcid-summary-tar-gzip",
            "version": "synthetic-fixed",
        }
    },
)

for person, origin in [
    ("synthetic-one", "A"),
    ("synthetic-two", "C"),
]:
    f.event(
        person, origin, "education", "doctorate",
        source="orcid-2026-summary",
        start=2000, end=2005, role="PhD", status="observed",
    )
    f.event(
        person, "B", "employment", "academic",
        source="orcid-2026-summary",
        start=2010, role="Professor", status="observed",
    )

identity = digest({
    "source": f.task.config["sources"]["orcid-2026-summary"],
    "rules": "career-rules/1",
})
f.db.execute("UPDATE runs SET identity=?", (identity,))
before = rank_global(f)

f.event(
    "synthetic-unfinished", "A", "education", "doctorate",
    source="orcid-2026-summary",
    start=2000, end=2005,
    role="PhD not completed", status="observed",
)
after = rank_global(f)
assert before["edges"] == after["edges"]
```

实际结果：

| 项目 | 加入未完成学历前 | 加入后 |
|---|---:|---:|
| A 的观测培养人数分母 | 1 | 2 |
| A 的强度分数 | 0.37012987012983717 | 0.33333333333333326 |
| C 的强度分数 | 0.37012987012983717 | 0.40692640692634113 |
| 有效边集合 | 两条关系 | 完全不变 |

这不是就业率分母不足的泛化争论，而是当前声明为同一解释人口的分子、分母实际采用不同完成资格。当前参考 `network_score` 的边没有被这条负例改变；直接受影响的是 `network_intensity_score` 及其解释。

最小修复：在现有资格 owner 中定义供关系与强度分母共同使用的训练事件资格，并保留人口定义中有意允许的差异。至少覆盖年度与 API、已/未 normalize、未完成与未知标签、合法完成学历，以及没有就业事件但合法进入观测培养人口的情况。固定来源身份、有效边不变时，注入被明确排除的学历不得改变分母或强度分数。

**R3 — P2：单条履历内部起止年逆序仍形成有效关系**

位置：

- `src/gapi/global_analysis.py:137–149`
- `src/gapi/orcid_stream.py:115–123`
- `src/gapi/career_rules.py` 的 `year()` 只验证单个年份格式与范围

当前关系资格核验了“入职年不得早于培养结束年”，却没有核验学历和任职各自的 `year_start <= year_end`。这允许内部自相矛盾的履历进入矩阵。

已执行重现：

```python
from gapi.global_analysis import observed_relations

training = {
    "source": "synthetic",
    "locator": "fixture",
    "status": "observed",
    "institution": "A",
    "kind": "education",
    "stage": "doctorate",
    "year_start": 2010,
    "year_end": 2005,
}
job = dict(
    training,
    institution="B",
    kind="employment",
    stage="academic",
    year_start=2006,
    year_end=2000,
)

bad_training = list(observed_relations([
    training,
    dict(job, year_end=2020),
]))
bad_job = list(observed_relations([
    dict(training, year_start=2000),
    job,
]))

assert len(bad_training) == 1   # 本次实际结果
assert len(bad_job) == 1        # 本次实际结果
```

影响：有数据错误、来源矛盾或畸形输入时，矩阵会把不可能的区间当成有效关系；错误任职区间还可能改变重叠任职判断。当前 OOD 的“日期逆序”仅覆盖入职早于毕业，未覆盖事件内部逆序。

最小修复：在统一事件资格边界拒绝已知起止年倒置的事件，保留原始证据并给出明确排除理由；缺结束年仍按既有“持续任职/未知”语义处理。复测应包含两种内部倒置、同年合法、缺 start/end，以及两个事件之间的先后合法性。

**已执行及静态确认的保护**

本次新执行 `python -B -m gapi.ood`，退出码 0；9 个场景、26 项检查通过。它验证了这些有限合成条件：

- 雇主指向培养机构的传播方向，正反方向确实产生不同分数。
- 重复关系不放大权重，替代来源删除后仍能重建关系。
- 同名不同稳定 ID 不发生夹具内误联接。
- 明确 unresolved 与未提交成员不改变基准网络。
- 密集博士后记录被去重，博士后没有变成博士学位。
- 缺关键年份或入职早于毕业不被补造。
- 删源解除重叠任职时，固定网络与扩展网络分别报告。
- 行整体放大不改变行归一化结果；非法权重被拒绝，概率质量守恒。

OOD 输出的部分代码哈希与本报告实际文件字节哈希不同，原因可由 `src/gapi/ood.py:161` 直接解释：它先把 CRLF 替换成 LF 再哈希。此差异不能作为代码在审查中漂移的证据。

静态确认：

- 全球路径 `src/gapi/global_analysis.py:330–340` 始终保留 `formal_rank=None`，不因多来源名称就自动生成正式排名；奖项和综合分仍缺失。
- `src/gapi/corroboration.py` 要求同一有效 ORCID、官方页面主体绑定、字段及必要日期一致、成员和证据摘要存在，并区分官方引用复制与 self assertion；两个数据库名称本身不是独立来源。
- 跨源关系按稳定 person key 分组，再按机构对与 stage 去重；缺日期、同机构关系和已发现重叠任职被排除。
- 年度 partial、failed、budget-stopped 等状态不满足当前评分门控；R1 是 verified 状态与来源身份没有共同绑定的问题。
- 全局输出明确标注全球枚举与普查不完整，参数场景不覆盖未知全球缺失。
- 交付包准确说明公开图复算不能证明个人事实，删掉个人依据之后也不能从公开图哈希恢复原事实。

执行记录中有一次合成检查命令的 Python 括号错误，退出码 1；随后修正同一检查并取得 R3 的两个实际结果。另一次文本搜索因 Windows glob 写法失败；后来通过明确源码文件完成相关阅读。这些失败未触及任何数据。

**研究与治理仍未通过的门槛**

以下不是声称已发现真实个人事实错误，而是本次审查权限与当前证据下不能完成的验收：

1. **事实资格与独立性：UNKNOWN。** 未读取个人记录，因此没有对真实履历抽样核实学位完成、日期、联合任职、机构消歧或来源复制链。官方域名上的个人页面也可能来自本人维护或导入；当前 origin 标签证明的是所实现的两种发布渠道与字段一致，不能自动证明生成过程相互独立。对“独立事实已确认”的更强声明，需要有界的来源溯源核验。

2. **年度与候选完成：未达到最终验收。** 交付包明确保留年度 EOF/CRC/长度/MD5 未完成、拒绝成员与冻结候选未全部结算的状态。本次未打开生产库，故这些具体数量是交付材料中的带时点声明，未由我实时重算；不得把我的代码审查转换成它们已完成。

3. **真实损失漏斗与公平性：UNKNOWN。** 已检输出有来源、国家、status 聚合与参数场景，但未见足以验收分语言、学科、地区、证据等级及各资格排除环节的真实损失漏斗。本次夹具不能估计真实误合并率、漏合并率、不同地区公平性或未知缺失。不能把无公开履历、接口失败、未分类和低影响力合成同一个零。

4. **许可：仅检查了本地机制与声明。** `publication.py` 有版本化权限、机构聚合范围、来源许可依据摘要和官方 URL 结构检查；这些是项目采用的发布政策，不是法律结论。本代理未访问当前上游条款，不确认其现行内容、真实使用方式或所有衍生关系都获许可。

5. **最终个人依据退役：尚无可验收的实现。** 交付包第 23–27 行要求全门槛满足后，在既有排他机制下精确识别数据库、旁文件、冻结恢复链和临时副本，留非个人删除清单、失败续结算及幂等回读；第 25 行同时承认最终命令尚未实现或执行。源码搜索没有发现对应最终退役路径。既有 `frozen_input.py` 的压缩、临时文件清理和前驱退休不是全任务个人依据退役。该功能在真实实现和独立测试前不能判通过。

**复杂系统审查**

| 维度 | 已检判断与证据边界 |
|---|---|
| 观察与因果 | PageRank 的“有影响力雇主向培养机构传递质量”是测量选择。代码和说明正确否认就业概率与培养因果效应；本次没有识别训练质量、招生选择、学科规模、地理与既有声望的因果贡献。 |
| 激励与权力 | 高可见度机构、更完整自填履历和受支持官方模板更容易形成可计量边。发布排名可能鼓励完善资料或策略性标注而非真实质量提升。需要损失漏斗与证据等级展示，不能仅用高分强化自身有效性的解释。 |
| 反馈与延迟 | 自报、官方页面、年度档案和招聘变动的更新周期不同；缺结束年被当持续任职会长期制造重叠并排除真实后来任职。当前保守处理减少误加边，同时可能产生差异化漏计；尚未量化。 |
| 适应与拓扑 | 删源可能解除冲突并新增端点。实现已正确区分固定与扩展网络；参数范围仍不能代表真实未来招聘网络适应或未知缺失。 |
| 级联 | 来源故障、词汇规则修正、机构 crosswalk 和资格判断可同时改变很多边与排名。R1–R3 是具体可触发路径；版本与输入冻结需共同绑定，旧分数不能静默继承新规则解释。 |
| 存活性 | `collection.py` 使用有界阶段、持久 backoff、来源隔离和遇争用退出，减少单源故障拖死整个运行。没有在本次证明生产规模长期推进、完整来源必定终结或存储余量充分。 |
| 公平与饥饿 | `career_store.py:273–314` 为到期重试保留配额，并对优先/普通候选交错；`collection.py` 依据本次运行的来源服务时长调度。这支持有限执行内的公平，不证明反复短运行、无限新增候选或持久慢源下有总体饥饿上界。实际尾部年龄和来源等待时间仍未检验。 |
| 死锁 | 既有 OS writer lock 和 SQLite 有界等待、争用退出减少无界互等；本次未运行活体并发或故障注入，不作全局无死锁保证。最终退役还必须证明不会与活动 reader/writer 争用或重建已删除对象。 |

**结算建议**

当前应保留“机构级观测研究、覆盖和审查限制公开”的证据上限；**正式排名、全源完成、真实事实已独立确认及最终本地个人依据退役均不通过**。先修复 R1–R3，并由新的独立上下文复测相同反例和合法负控制；此后仍需分别完成数据完整性、事实核验、真实损失漏斗和退役执行路径的门槛。我的结果不授权扩展访问、发送个人依据、启动采集或执行删除。

本代理未自行落盘，报告由父代理写入现有交付包；没有创建第二状态库或待办队列。
