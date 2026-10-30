# ORCID 剩余异常的官方源码核查

2026-10-30 通过已有授权的 GitHub 只读接口读取 ORCID 官方公开仓库默认分支和完整文件树，再读取固定提交中的两个异常映射器；原始文件只在有界内存解析，没有克隆仓库或保存原始源码。

官方版本：`6e030cf482d38ff754b7dec07d2c132f1d4c89d6`。

直接支持错误码解释的文件：[OrcidCoreExceptionMapper.java](https://github.com/ORCID/ORCID-Source/blob/6e030cf482d38ff754b7dec07d2c132f1d4c89d6/orcid-core/src/main/java/org/orcid/core/exception/OrcidCoreExceptionMapper.java)。整文件 SHA-256：`15b18fa78a6edc4301e1e03b18c8f36f5faf58efaea3aa05c8e90c19d72de149`。

- 第105、110行：`9016` 映射未找到结果类异常，HTTP404。
- 第113行：`9018` 映射 `LockedException`，HTTP409。
- 锁定与“已停用”不是同一映射，不能根据409单独认定停用、合并或永久消失。

另检查 [OrcidExceptionMapper.java](https://github.com/ORCID/ORCID-Source/blob/6e030cf482d38ff754b7dec07d2c132f1d4c89d6/orcid-api-common/src/main/java/org/orcid/api/common/jaxb/OrcidExceptionMapper.java)，SHA-256：`71b0053b51e0a6653e31216d92bbb2962ee8e06312c2e924347941c7f4c162bb`。其旧版异常响应把明确的废弃记录和主记录定位分别处理，与账户锁定区分。

这解释已留存错误观察的含义，不证明某个锁定永久存在、不授权绕过锁定，也不为未知人员补造履历。历史身份重定向和XML根身份不一致的记录继续按原候选规则处理，不能合并为同一人或计为已取得证据。六条异常仍公开为覆盖缺口；没有把它们改成成功或为了凑完成率而拒绝。

搜索摘要没有提供错误码证据；上述结论来自实际读取的官方固定源码。没有修改账号、认证、网络、人员实体或排名规则。
