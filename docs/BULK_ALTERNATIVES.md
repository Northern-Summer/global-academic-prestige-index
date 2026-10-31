# 批量采集替代路径核验

核验日期：2026-10-30。本文记录可用的替代分发渠道，不是已导入数据或完成证明。

## 已核实的官方托管查询路线

ORCID 官方页面说明其与 Digital Science 合作，将 2023、2024、2025 公共数据档放入 Google BigQuery。此路径可在服务端选择教育、任职及必要证据字段，避免把整份压缩包传输和保存到个人电脑。

- ORCID 官方入口：https://info.orcid.org/orcid-public-data-file-on-google-big-query/
- 数据托管方目录：https://docs.dimensions.ai/bigquery/open-datasets.html
- 托管方查询及字段示例：https://bigquery-lab.dimensions.ai/tutorials/09-orcid/

本次均通过公开 HTTPS 页面实读。官方入口提到 2023–2025；托管方当前目录和可执行示例明确列出的表为 `ds-open-datasets.orcid.summaries_2024`，许可 CC0。不得把该表当成已冻结的 ORCID 2026 快照，也不得从页面文字推定其他表名、表结构或版本完整性。

示例明确展示教育字段路径 `activities.educations.groups -> records` 和任职路径 `activities.employments.groups -> records`，以及 `record.role_title`。不能只凭这些示例推定全部证据字段已满足本项目契约；首次接入需要读取表元数据和有限记录核对声明来源、日期、组织标识及可追溯定位。

## 实际执行边界

当前运行时未发现 `bq` 或 `gcloud` 命令。此结果不证明 Google 身份或其他客户端一定不存在。官方教程要求 Google 身份验证及项目，当前尚未执行身份验证、创建项目、开启计费或运行云端查询。

本次对公开表元数据 REST 地址 `https://bigquery.googleapis.com/bigquery/v2/projects/ds-open-datasets/datasets/orcid/tables/summaries_2024` 的一次匿名只读请求返回 HTTP 401；未切换身份或渠道绕过。用户已明确拒绝此路线，理由为账户风险。该路线已排除，不进行登录、创建项目、安装客户端或执行查询；既有无需新增账户的采集继续。

托管方说明可利用每月 1 TB 免费层，但免费额度不是绝不收费的保证。本任务约束为无云费用，因此不能直接以普通计费项目执行。只可在确认未关联计费且满足 Sandbox 限制后运行；先检查元数据、dry run、限定扫描量和输出量。不能因免费额度宣传自行打开计费。

## 接入后仍须保持的契约

1. 单独冻结托管数据表、版本、元数据指纹、SQL 和处理范围；不同年份不覆盖既有快照。
2. 服务端只选择必要字段，结果按有界页流入现有单写入者；不保存原料，不复制完整结果成巨型 JSON。
3. ORCID 托管档与 ORCID API 是同一来源体系，不能作为独立交叉核验。Wikidata 和官方机构来源继续独立调度。
4. 记录总数、包含履历数、通过结构校验数、关系数、正式资格数分开统计；全表扫描完成也不等于全球学者普查完整。
5. 云端完成和本地 2 GiB 准入分别验证。若必要结构化证据超限，停止晋升并保留检查点，不删除证据或静默抽样。

此路线尚未接入 canonical 数据。现有有限 API 队列继续采集，不等待此路线。
