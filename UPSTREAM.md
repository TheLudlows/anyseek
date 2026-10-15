# SeekPower 上游来源与导入基线

导入日期：2026-10-13（Asia/Singapore）。

| 项目 | 值 |
| --- | --- |
| 上游 | https://github.com/SeekStorm/SeekStorm |
| 上游分支 | `main` |
| 固定上游 commit | `edf6f77e554c472b3729a547b8e08fb84a590a76` |
| 上游时间（UTC） | `2026-10-12T18:15:56Z` |
| 上游 Git tree | `df5ce14d7348ffaba9ab7fa3b9b8e196e8545e55` |
| 核心 crate 版本 | `3.4.1` |
| 目标主分支 | `TheLudlows/seekpower:master` |
| 原目标主分支 commit | `3bd806e9d01c8cdbfc8881c4271cfbc76e519e6d` |
| 原样导入 commit | `1986b14215c82a07fac77d7c5b169b90743d76a4` |

## 导入策略

以原 master 为父提交，创建一个树内容与固定上游完全相同的新提交。旧版 seekpower 工作树被整体替换；不是逐文件叠加，也不是改写历史。旧版代码仍能通过原 commit 找回。

导入提交没有重命名 crate，没有删除任何上游功能，保留全部源码、Web UI、二进制资源、LICENSE 和第三方许可。尤其保留 `seekstorm_server/web/`。MCP、内置模型、文件导入等待裁剪项在本次仍原样存在。

在原样导入提交之后，单独增加以下 SeekPower 文档：

- [SEEKPOWER_PLAN.md](SEEKPOWER_PLAN.md)：裁剪、单机可靠性与分布式演进计划。
- [STATUS.md](STATUS.md)：已完成/未完成的执行状态。
- [VALIDATION.md](VALIDATION.md)：导入一致性证据与未执行测试。
- 本文件：来源、固定基线与同步政策。

上游 README、ARCHITECTURE 和 CHANGELOG 保留，不把上游产品描述当作 SeekPower 已完成的演进能力。当前默认主分支保持 `master`，不新建或改名为 `main`。

## 验证与可复现比较

[导入工作流](https://github.com/TheLudlows/seekpower/actions/runs/38363655157) 完成 Git 树身份校验、空差异校验和 Git 对象完整性校验。其作用是导入到临时分支；主分支由后续核验后的快进更新完成，不由工作流自动覆盖。

原样导入树：`df5ce14d7348ffaba9ab7fa3b9b8e196e8545e55`。

后续开发者可在本地复核（非构建测试）：

```bash
git fetch https://github.com/SeekStorm/SeekStorm.git edf6f77e554c472b3729a547b8e08fb84a590a76
git rev-parse '1986b14215c82a07fac77d7c5b169b90743d76a4^{tree}'
git diff --exit-code edf6f77e554c472b3729a547b8e08fb84a590a76 1986b14215c82a07fac77d7c5b169b90743d76a4 --
```

## 后续开发

Seekpower 独立维护和演进，不设上游同步要求。本文件记录历史导入基线，保留上游和第三方许可、必要来源及修改说明。
