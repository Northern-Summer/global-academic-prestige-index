当前候选存在 **1 项高优先级、3 项中优先级缺口**，不能据此声称已完整满足资源安全验收。成员幂等、事务异常恢复、HTTP/XML 解码和若干归档输入防护在下述合成范围内通过。此报告是**独立上下文的内部代理审查**，不代表外部机构审查。

本轮只读检查实际工作树；未打开生产数据库、未建立文件数据库、未访问网络、未启动采集、未修改文件。复现使用 `python -B`、内存输入、`SQLite :memory:` 和已明确选择的无文件测试。最终阶段收到作者将修改相关文件的通知，**下列哈希、发现与结论均绑定通知前实际受检版本，后续修复尚未复审**。

受检基线为 `HEAD 575ef0c97966542c8292525a08601ee9765451b9`。开始时跟踪文件已有脏改动：`configs/orcid-2026-production.json`、`configs/streaming.json`、`src/gapi/task.py`、`tests/test_task_resources.py`，因此 HEAD 不能单独标识受检内容。

### 发现 1 — P1：公开覆盖率统计保留全人口键，绕开 512 MiB 采样约束

- **位置：** `src/gapi/global_analysis.py:348–354`，尤其 349–351。
- **实际行为：** `rank_global(public=True)` 查询逐条人员记录，将所有不同人员键加入 `people=set()`。这个集合按人口规模增长；循环内没有 `check_memory()`、人数界限或分区计数。
- **已执行复现：** 调用实际 `rank_global`，仅将前序关系输入和非相关附加信息替换为空合成数据，数据库适配器向该实际查询返回 100,000 个不同的合成 64 字符人员键。使用 `tracemalloc`，从人口统计循环开始到结束：
  ```json
  {
    "public_people": 100000,
    "memory_check_calls_during_population": 0,
    "retained_bytes_growth_during_population": 14694304
  }
  ```
  没有制造 512 MiB 的实际超限；上述执行证明了实际函数的线性人口级保留及这段路径没有预算采样。当前生产人口和实际峰值未测量。
- **影响：** 总人口增长时，即使图的边数已经受控，这一统计仍可突破进程预算或触发进程终止。之后的内存检查不能撤销已经发生的资源超限。该问题影响公开导出和全量规模扩展。
- **最小修复：** 由 SQLite 执行 `count(DISTINCT person)`、状态分组和国家去重，返回小型统计；保留连接现有 SQLite 堆限制。若仍在 Python 计数，应采用明确的有界算法并保留精确计数语义，不能采样代替。
- **复测：** 在合成高基数输入上比较统计精确一致性，验证 Python 端不保留全部人员键；增加预算失败场景，确认未发布或改写最终公开指针。须测真实 `rank_global` 路径，而非只测新计数辅助函数。

### 发现 2 — P2：tar 结束后的零填充解压不检查时间预算、内存或总解压量

- **位置：** `src/gapi/orcid_stream.py:249–268`，直接缺口为 255–257。
- **实际行为：** 正常成员扫描在外层和成员体内检查时间；遇到两个 tar 零块后，进入单独 `while compressed.read(CHUNK)`，仅检查解压内容是否全零。该循环没有 `max_seconds` 检查、`check_memory()` 或总解压字节上限。CRC、压缩长度和 checksum 都在 drain 完成后才核验。
- **已执行复现：** `gzip.compress(b"\0" * (2 * 1024 * 1024), mtime=0)`；固定配置中的实际压缩长度及正确 SHA-256；内存 Store 不连接数据库；raw 首次读取后将被测时钟从 0 提升为 100，调用实际：
  ```python
  stream_archive(store, "x", raw, max_seconds=1, max_members=1)
  ```
  输出为：
  ```json
  {
    "status": "verified",
    "elapsed_clock": 100.0,
    "limit_seconds": 1,
    "decoded_bytes": 2097152,
    "memory_checks": 0
  }
  ```
- **影响：** 高膨胀零尾数据可让一个本应有界的批次持续解压。checksum 只能在消耗完资源后拒绝变更输入，不能保护这一过程。该复现**不表示未通过 checksum 的数据会晋级**，也不表示一次 2 MiB 复现已突破内存预算。
- **最小修复：** 对 drain 每个解压块调用与成员扫描相同的预算检查；规定并执行 tar 尾部填充/全流解压的有界规则，保持官方正常归档兼容；在预算到达时留下可重放且未 verified 的状态。总输入字节上限也应尽早执行，而非只在 EOF 比较声明大小。
- **复测：** 正常 tar padding、超时零尾、高膨胀零尾、非零尾、截断 CRC、正确/错误 checksum，以及批量暂存成员在终止时的持久化状态都须覆盖。作者告知正在修复；本报告不把该告知当作通过证据。

### 发现 3 — P2：robots.txt 读取绕过阶段总时间检查

- **位置：** `src/gapi/source_http.py:279–285`。
- **实际行为：** 该路径直接使用 `_open()` 返回的响应，随后调用 `r.read()`；没有使用 `SourceResponse(..., self.remaining)`，循环中也没有 `remaining()`。普通文档/API body 已有的 deadline 防护未覆盖 robots body。
- **已执行复现：** 复用项目 `test_http_trickle.py` 的真实 `http.client.HTTPResponse` 缓冲合成器：每次底层读取推进时钟 1 秒，body 是 23 字节合法 robots 文本。对普通响应和 `ManagedResponse` 各运行一次：
  ```python
  with http.within(2):
      http.allowed("https://www.tamucc.edu/example")
  ```
  两条路线均返回 allowed，输出均为：
  ```json
  {
    "elapsed": 23.0,
    "stage_budget": 2,
    "returned_allowed": true,
    "raw_reads": 23
  }
  ```
  本轮没有网络请求。
- **影响：** 慢速 robots 响应可占用远超 collection 阶段额度的时间，阻塞单流采集的其他来源。262,144 字节上限约束字节数，但不约束慢速读取耗时。没有证据表明它绕过 robots 拒绝或 HTTP 403。
- **最小修复：** 将 robots response 统一包装为带 `remaining()` 的 `SourceResponse`，并使用其分段读取；超时后关闭响应，不缓存不完整 robots 规则。仍需承认单次阻塞 socket timeout 的固有限制。
- **复测：** 用同一普通/ManagedResponse 合成慢速输入验证 2 秒边界触发 `StageDeadline`、响应关闭、robots cache 不被填充；同时保留正常 Allow、Disallow、404 和 HTML 响应测试。

### 发现 4 — P2：配置验证允许把 256 MiB 预留下调至 1 字节

- **位置：** `src/gapi/task.py:47–48`。
- **实际行为：** 仅验证 `0 < reserve_bytes < budget_bytes`；与总存储上限和内存上限不同，没有守住题设要求的 256 MiB 预留底线，也未拒绝非整数的 reserve。
- **已执行复现：** 读取当前配置，仅把 `reserve_bytes` 改为 1。通过 mock 禁止 `Path.mkdir`，并隔离不属于构造器字段验证的 `Task.check`，调用实际 `Task.__init__`；构造器接受并保留：
  ```json
  {"accepted_reserve_bytes": 1}
  ```
  此复现证明的是构造器验证缺口，不是实际磁盘溢出。
- **当前事实：** 受检两份配置均为 `268435456`；没有发现当前生产配置已经下调预留。
- **影响：** 新任务/独立冻结配置可以合法通过当前 owner 的验证，却降低 rollback journal 和导出安全余量。已有嵌套任务的 owner freeze 一致性检查不能保护新 owner 创建时的这一缺口。
- **最小修复：** 若 256 MiB 是固定授权底线，要求整数并验证 `256*1024**2 <= reserve_bytes < budget_bytes`。将其作为 Task 的明确常量/契约测试，保持较大预留可用。
- **复测：** `1`、`256MiB-1`、布尔、浮点拒绝；`256MiB` 和更大合法值通过；总预算临界点仍拒绝超额写入。

### 已执行的正向证据及证据上限

**34 个纯内存测试通过，失败 0，错误 0，用时 0.148 秒。** 执行时将 `socket.socket.connect` 替换为立即拒绝，未连接任何实际服务。选择范围：

- `test_source_http.HTTPTests` 全类：gzip JSON、展开限制、截断/损坏/尾随、range 身份与位置、403 无重试、429/Retry-After、robots、URL、阶段和 body deadline。
- `test_xml_transport.XMLTransportTests` 全类及 `test_http_trickle.HTTPTrickleTests` 全类：实际 stdlib body 缓冲、gzip XML、CRC、截断、多成员/尾随、展开上限、partial chunked body 关闭。
- PAX 两个纯函数测试：path/size/link/sparse/hdrcharset/畸形 mtime 以及长度/截断/padding/超限。
- 四个事件 codec 测试：精确 Unicode roundtrip、有限计数、字节上限、坏摘要/帧/UTF-8、截断/尾随/压缩膨胀。
- 两个纯 redirect handler 测试：ORCID 身份跳转关闭响应、不请求目标；不受信任目标被拒绝。

对上述带文件 fixture 的少数类，只选纯函数/handler 方法并显式关闭其 setUp/tearDown；没有调用创建临时文件的 fixture。

另以实际 `CareerStore` 方法、实际 SQLite DDL、事件存储和 `stream_archive`，仅构造 `:memory:` 数据库，执行了：

| 合成情况 | 中断后 | 重放后 |
|---|---|---|
| 正常归档读两次 | 第一次 1 事件/1 成员 | 第二次仍 1/1，verified |
| 成员提交前注入 KeyboardInterrupt | 0 事件/0 成员，interrupted | 1/1，verified，quick_check=ok |
| 实际 commit 后注入 KeyboardInterrupt | 1/1，interrupted | 仍 1/1，verified，quick_check=ok |
| 已输出事件后的不完整 XML | 0 事件/1 rejected 成员 | 归档完整性 verified，但坏成员未作为 observed |

最后一项体现了**归档字节完整与成员语义有效是不同状态**。这些内存异常注入不等于操作系统进程强杀、断电或磁盘 journal 恢复验证；文件级崩溃测试只读过代码，未执行。

静态检查另确认：

- `CareerStore.member` 对 `BaseException` 回滚；ledger 与事件同事务提交；批处理缓存只有完整成员进入 flush。
- 链接、特殊 tar 类型、语义 PAX、非法成员路径不能成为正常 summary；XML DTD/entity 明确拒绝。未做完整 tar 解析器模糊测试。
- `candidate_selection` 记录 config digest、rowid 上界、来源计数和 provenance hash；校验重新计算 prefix，当前策略对配置变化**拒绝续用**，并不自动提供跨配置迁移。相关现有文件级候选/崩溃测试未执行。
- `exclusive_writer` 使用 resource owner 的 OS 文件锁，嵌套资源契约核对 owner freeze；只静态检查，未为验证锁而触碰活动 writer 的文件。
- 冻结 SQLite/row 输入持有私有数据，不直接公开；公开 aggregate 的字段为机构级契约。此轮没有读取实际人员记录或导出实际个人轨迹，也没有对整个发布目录执行数据泄漏扫描。
- aggregate 单文件通过独占 staging、fsync、replace 发布，并比较同字节避免重写；browser bundle 最后更新 manifest。没有进行实际公开发布或远端重复发布试验。

### 未检范围及退役结论

没有打开生产 SQLite；未验证其当前 integrity、实际空间、水位、512 MiB 峰值、活动锁持有者、完整 ORCID 全量 checksum 或真实 403/429 服务行为。32,000,000,000 字节上限的构造器与共享预算实现已读，未进行大容量磁盘压测；不能把有限静态检查当作总量硬保证。

**最后一份本地数据库的退役未验。** 在 `src/gapi` 和 CLI 注册范围未发现执行“删除最后本地数据库”的命令与完整前置校验；已有 `frozen_input` 的 retire/unlink 是压缩后继验证后的派生快照替换，不能据此认定最后数据库可安全删除。退役前至少还需真实验证：writer 停止并结算、无未提交/未恢复工作、保留完整采集备份而非仅 ranking projection、冻结候选行号/配置和依赖保全、历史版本实际可恢复/复算、公开发布回读和独立保留位置。此报告没有批准任何删除。

### 关键受检文件 SHA-256

以下为**修复前实际内容**的原始文件字节 SHA-256，可用于保存本轮边界；后续文件变化必须重新验证相关发现。

```text
src/gapi/source_http.py
58c8ea1055dff49410e895abfdf9eec4916efeb24461415191036d0e18f25f1f
src/gapi/orcid_stream.py
f1207e7d6c3e0d91344f90e6390c0e6c8427ddc7d71b2e217653eda4bd800f66
src/gapi/career_store.py
4f03fdf9ef078a9000ad737f210e863f7dfdbb17967f5772cae46aba2b281214
src/gapi/task.py
ab8cad38c15c37009d0f6a87ae15ead78a966e0671856ded275ee78f689e3dc5
src/gapi/candidate_selection.py
7c2976c5042f3181c53f2966ea759868e192b29685904ce7c4e1675ac9de6e48
src/gapi/event_storage.py
94d1e9d5aaec74258f0cb463a4a8e164d8d0b9a9a33e0bd93553273fd60b8a94
src/gapi/frozen_input.py
fa609ab5696abb5db393c020ab428b113d46c1d4380abd190f19949e26740e5e
src/gapi/global_analysis.py
34acd7e008f04e884bdb14ae45c4bbacd29486a8a415f5610a7add663d5df86d
src/gapi/derived_export.py
26674d9b6b8a439faa840d2f4a7591616240f2e4eb69634a8e96b2b33b90fab9
src/gapi/row_snapshot.py
c20dbaf3076aff1fb23b17d14b724aee39dae62c228624198575368a3f00de65
src/gapi/stream_cli.py
6a93bad3ffacfff2bbc023d5b413134cfcb3ee472e748db60285353ee43db26c
src/gapi/publication.py
7e909b0bb6d32f6c31d61373ab4dd8258692251b0ffea55dead75ad1fd7883fe
configs/streaming.json
1f922e9b5b273ec98612b2caa2b7dccb3db314b2abafed793e3209969a3ee723
configs/orcid-2026-production.json
1b81a9f45751b1e2a3034113c70668be75b8d3aa07f9df6cda916581b3b27864
tests/test_streaming.py
a20763865245640629d961195b7321961461c9f10a1afe10dbeecab12e71cb16
tests/test_task_resources.py
b31cf8e36635b40119a7c5a627bdd45b3d3d0313901bad4f1f00f3d1aa65105a
tests/test_candidate_selection.py
7822c7f1b51b91f188eee96fd406d19924755d0a4000c93c9816334952670970
tests/test_archive_commit_batches.py
f86f3fff301be1b7aca4954edf06fcf569e1a85a2ddc2cf28c8df29173aacfce
tests/test_source_http.py
c6d1c1dfe0f6e18c3be70584ffccb1965cc2e3566309952a2d9a40725244ff20
tests/test_http_trickle.py
fbd424b80786d893875c54f297d4393d1f16393c436d74a825fc8e30cb4991fb
tests/test_xml_transport.py
8c8ee69d234cf25bbb5d022f88303fa9c278b2bb8f730a3dcb4455bda58e1115
tests/test_event_storage.py
3a473be09b686b5574a9cdd0bb7dbb3f7b7eb7cc1e8b372a94eec739bda2735a
tests/test_frozen_input.py
2c16ada37a998327cc1e84752488c9c31b7416dfc3a1dea6462844cec4484a0d
```
