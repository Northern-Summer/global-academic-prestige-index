# GAPI 七项问题独立复测

2026-11-03，本次受检实际工作树中，**R1–R3、S1–S4 的原始缺陷均在下述有限反例及合法控制范围内判为已修复；未发现七项原缺陷残留。** 这不等于全源采集完成、真实事实准确、全球排名有效、全规模可用或个人依据退役通过。

## 身份、权限与版本边界

审查者是同一产品内、未继承父会话对话上下文的内部独立上下文代理；不是外部机构，也不是第三方认证。接收的任务材料包含两份原问题报告及复测范围。执行接近结束时，父代理补充作者完整回归、OOD、真实库读锁限时的自述；本报告不把这些自述计为独立执行证据。独立构造夹具与断言，但复用了项目内存 schema/_Fixture 及 stdlib HTTP 缓冲合成器，因此不宣称统计独立或完全工具链独立。

只读取项目源码、配置、适用规则及指定原报告。未打开生产数据库、私人会话、凭据或个人记录，未启动采集、连接网络、改源码或委派。所有 Python 执行均使用 `python -B -` 标准输入脚本；测试只创建 `SQLite :memory:`、BytesIO 和内存对象。主要测试进程将 `socket.socket.connect` 替换为立即拒绝。唯一持久写入是本报告；没有测试脚本、缓存或第二状态库落盘。

已读取适用的 Documents/Codex 上级 AGENTS.md；项目根没有 AGENTS.md，src/tests 搜索未发现更窄规则。读取 software-engineering-workflow、humanizer-zh 与其 i-have-adhd 表达入口。未查询历史记忆。HEAD 为 `575ef0c97966542c8292525a08601ee9765451b9`；工作树有既存脏改动，HEAD 不能代替本节文件哈希。以下实际字节 SHA-256 在测试前后重算一致。

| 文件（源码相对项目 oo 根） | SHA-256 |
|---|---|
| src/gapi/career_store.py | c064582144c6051b1909eb8926611bd07b8545e0bfc31f9af1fa6c3bcd6cd391 |
| src/gapi/career_rules.py | 62b30942f98a244de94746be143587a935d4be303c874cabb31af600eac7418e |
| src/gapi/global_analysis.py | 97e9569f198bba42e00654eb98c999e3c9dc731343e2f2980f48a28745ef6b86 |
| src/gapi/orcid_stream.py | 4d135638aafa4e36232052b5e0374b0c74275fed274bc3a88f96e972f1827c7c |
| src/gapi/source_http.py | 25908b154bf925fd02721f76b214f07eabcd94de80afbc212c98ac694880e1df |
| src/gapi/task.py | a0da2deba10965b059148618c35e151e191a4ba67d6dbb2f3acc725d9eb035b0 |
| src/gapi/career_normalization.py | 71f8ec2586a19130adeee140fc9099fe81928cd7500d9606fade2e93f68733fc |
| src/gapi/ood.py | de1d7a3be085fd6908b1994dfcefbc8bc0e1908c754138d93eed84c9a9e66b38 |
| tests/test_http_trickle.py | fbd424b80786d893875c54f297d4393d1f16393c436d74a825fc8e30cb4991fb |
| configs/streaming.json | 1f922e9b5b273ec98612b2caa2b7dccb3db314b2abafed793e3209969a3ee723 |
| configs/orcid-2026-production.json | 1b81a9f45751b1e2a3034113c70668be75b8d3aa07f9df6cda916581b3b27864 |
| 原报告 GAPI-redteam-research.md | 192cdc1b83cd7eee316692b900f993a233dc080bf902e5b5054c22791f1ebe7a |
| 原报告 GAPI-redteam-software.md | b18250a474100ae2a5e5099e62d53018b4f7a30b8a37f4321b9220cf19832a18 |

## 逐项复测

### R1：来源身份与评分资格——已修复（受测范围）

`career_store.py:29` 的 `verified_archive_sql` 现在绑定 source、verified 状态及 `source_identity`；`:46` 的 source_identity 也是获取/完整性路径共用的来源规格与规则摘要。

执行独立的 runs/events 内存最小表，配置 annual 与 api 两源，直接执行该生产 SQL 谓词。当前摘要+verified 返回 annual、api；旧身份、旧 rules、provisional、failed、缺 run 均仅返回 api。随后在实际 `rank_global` 路径，用一对合法年度培养/任职输入：当前 v2 摘要产生 1 条边；将 run.identity 改为由真实 v1 来源配置计算的摘要后，返回 0 条边，全部培养分母为 null。普通非档案源在最小表中始终合法保留。

未测试真实冻结恢复链或生产来源漂移发生率；该结论只证明当前身份检查及受测评分入口。

### R2：未完成学历污染分母——已修复（受测范围）

`career_rules.py:29` 的 `training_observation_eligible` 共用于 `observed_relations` 和 `global_analysis.py:306–328` 的 SQL UDF。明确未完成/荣誉资格以及逆序区间不会进入培养人口；无就业与未知标签可按既有“观测培养人口”语义进入，未冒充已完成学位。

实际执行 annual/API × 有/无 interpretation 表四种组合。各由 A→B、C→B 两人的合法记录构成基线，再向 A 注入四个独立人员：`PhD not completed`、`博士未完成`、`Doutorado interrompido`、`honorary PhD`。四种组合中，边集合、每个节点分母和强度分数都与基线完全相同。再加入无就业的合法 PhD 与 unknown degree，A 的分母从 1 合法增加为 3。夹具仅在缺表组合关闭夹具自己的 interpretation 插入功能；实际 rank_global、资格 owner 和 SQL 未替换。

这些有限词例不证明所有语言词汇准确，也不证明真实数据公平性或事实完成资格。

### R3：事件内部起止年倒置——已修复（受测范围）

`career_rules.py:22` 的 valid_interval 被培训和任职资格共同调用。使用独立字典直接调用生产 observed_relations，实际关系数量如下：

| 输入变化 | 关系数 |
|---|---:|
| 培养 start=2006、end=2005 | 0 |
| 任职 start=2010、end=2009 | 0 |
| 培养与任职均在 2005 同年开始/结束 | 1 |
| 培养缺 start、end=2005 | 1 |
| 培养缺 end | 0 |
| 任职缺 start | 0 |
| 任职缺 end（持续任职） | 1 |
| 任职 start=2004 早于培养 end=2005 | 0 |

拒绝已知倒置没有改变受测未知端点、同年和跨事件先后语义。

### S1：公开统计 Python 全人口集合——已修复；全规模可用性未验证

`global_analysis.py:360` 使用 SQLite `count(DISTINCT person),count(*)`，国家和状态也通过 SQL 聚合。实际执行 rank_global(public=True)，只将 public_sources 授权列表替换为合成源；没有替换关系函数或数据库执行器。内存库写入 100,000 条事件、50,000 个不同 64 字符 SHA-256 人员键，各人两条记录，国家 AA/ZZ，observed/unresolved 各半，institution=null（合法没有可排名关系）。

实测输出：people=50,000，events=100,000，countries=[AA,ZZ]，event_status={observed:50,000, unresolved:50,000}。SQLite trace 确认执行 population 聚合 SQL，没有旧的 SELECT person,country,status 人口逐行查询。tracemalloc 当前保留 3,733,773 B、峰值 4,186,173 B，包含本次调用的首次模块加载等 Python 开销；这不是系统 RSS，也没有计入 SQLite 原生内存，不可与旧测量直接估算全局速度/内存收益。

受测连接显式沿用生产 `temp_store=MEMORY`、cache_size=-8192、hard_heap_limit=134217728。生产 CareerStore 构造器相同设置已静态核对。SQLite 精确 DISTINCT 仍需维护内部去重结构，其资源随基数增长，受 SQLite 堆限约束；不是常数内存算法。

额外在另一内存夹具写入 20,000 条事件后，将 SQLite hard_heap_limit 故意降至 4,194,304 B，再调用实际公开 rank_global，捕获 MemoryError，未返回排名结果。最后 trace 为 interpretation 表存在性查询，**失败发生在聚合前**，故不能称已针对 DISTINCT 耗尽点注入，也不证明所有出口都安全发布。未调用发布器或观察任何真实公开指针；原报告要求的“预算失败后正式公开指针不变”在本次权限内仍未验证。128 MiB 原生堆限制不等于全进程 512 MiB 硬保证。本次未测试真实规模耗时、峰值或生产统计可用性。

### S2：tar 尾部零填充预算——已修复（受测范围）

`orcid_stream.py:264–277` 在每块尾部读取前检查内存和 max_seconds，并用包含终止块在内的 1 MiB 填充上限阻止无限零膨胀；HashReader 提前拒绝超过冻结 compressed length。

实际调用生产 stream_archive，使用 BytesIO/gzip、合成 Store 协议及内存 SQLite，不连接 CareerStore 文件数据库。结果：

| 独立输入/注入 | 实际结果 | 内存检查次数 |
|---|---|---:|
| 正确长度/校验和、10,240 B 零 tar | verified | 2 |
| 正确长度/校验和、2 MiB 零膨胀 | failed：tar zero padding exceeds bounded expansion limit | 16 |
| 两终止块后非零 x | failed：nonzero data after tar end | 1 |
| 首次压缩读取后合成时钟从 0 变 100，max_seconds=1 | budget_stopped | 1 |
| 错误 SHA-256 | failed：upstream checksum mismatch | 2 |
| gzip 尾部截短 3 字节 | failed：Compressed file ended before the end-of-stream marker was reached | 1 |
| 声明压缩长度比真实少 1 B | failed：compressed source exceeds frozen length | 0 |

另构造一个 invalid-synthetic-path 成员，以 commit_members=2 暂存后在 padding 检查处超时：budget_stopped、已提交成员=0；同字节重放后 verified、已提交成员=1。这里的成员是应拒绝的合成成员，Store.member 为内存 nullcontext；结果证明生产批处理控制流在此例可丢弃待提交并重放，不是文件数据库事务、强杀或 journal 恢复测试。

1 MiB 上限在常见 tar padding 控制中通过；尚未证明所有官方归档均兼容。单个同步 read 的耗时不能被调用前检查抢占，本次没有声称严格实时硬截止，也未完整 fuzz tar/gzip。

### S3：robots body 阶段截止——已修复（受测范围）

`source_http.py:279` 现在通过 SourceResponse(..., self.remaining) 分段读取 robots。复用真实 stdlib HTTPResponse 缓冲合成器，每个底层 body 读取推进合成时钟 1 秒；普通与 ManagedResponse 两条路线分别在 within(2) 内读取 `User-agent: * / Allow` 文本。两者实际均：elapsed=2、raw reads=2、抛 StageDeadline、响应关闭、robots cache 为空。合法 Allow 正常通过，Disallow 正常抛 PermissionError，两者关闭响应并缓存完整规则。

未向远端发请求；没有重测全部 404/HTML/429 或真实 socket 阻塞超时。受测慢读缺口关闭，不等于网络层严格实时保证。

### S4：256 MiB 预留底线——已修复（受测范围）

`task.py:16` 定义 MIN_RESERVE_BYTES，`:48–50` 以 type(x) is int 和下限约束构造。调用真实 Task.__init__，只 mock Path.read_text 提供合法配置变体、Path.mkdir 防止落盘，以及 Task.check 隔离磁盘检查。实际结果：1、268435455、True、268435456.0 全拒绝；268435456 与 314572800 整数通过。

另调用真实 Task.check，合成 used=budget-reserve：anticipated=0 通过，anticipated=1 抛 BudgetStop。资源 owner 和 disk_usage 由内存控制值隔离，故这是算术/拒绝路径证据，不是实际磁盘容量或并发写入证明。

## 执行失败与限制

独立测试不是作者新增测试的转述。成功批次的 shell 退出码均为 0，覆盖本报告上述断言。另保留两次测试构造失败：第一次 R2 夹具删掉 interpretation 表后仍调用夹具自动插入，产生 `sqlite3.OperationalError: no such table: event_interpretations`；随后仅修正夹具插入控制，实际生产函数不变，完整重跑通过。第一次 S2 时钟在下一次外层检查就到期，实际得到 provisional、成员=1，断言原本期望 padding 内 budget_stopped 失败；这符合既有“完整成员可结算但归档未 verified”外层边界。将注入移到 padding 检查后重跑，才得到报告所述待提交重放结果。没有删掉或忽略这些工具失败，也没有据此修改源码。

未运行作者完整回归；未调用发布、删除、采集或生产 monitoring。有限局部测试不证明真实全量库完整性、归档 EOF/CRC/上游 checksum 已完成、拒绝成员已结算、来源事实独立、真实损失漏斗/公平性、长期采集存活性或全球无死锁。方法与来源许可现行内容也未联网复核。

## 结论

七项已报告缺陷在当前哈希所标识工作树及受测条件内已修复。S1 的正式公开指针失败回读、真实规模可用性，以及 S2 的官方全量兼容和文件级恢复仍未验证。**正式排名、整体采集完成和最终本地个人依据/数据库删除均未因本复测通过；本报告不批准任何删除，也不授予新的访问或外部效果权限。** 保留原报告及现有工作树，没有覆盖前驱证据或无关并发内容。
