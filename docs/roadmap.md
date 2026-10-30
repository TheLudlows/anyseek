# 下一步路线

以 2026-10-30 主干（已合入 #90、#98、#102）为基线：写入已收敛为一层 Record WAL，`/ingest` 返回持久业务结果，全文与向量异步消费；稳定记录身份、独立缓存合并和历史回收已有实现。完成状态与验证证据见 [STATUS](../STATUS.md)，接口语义见[异步写入](async-ingestion.md)。

## 当前优化任务

| 顺序 | Issue | 工作与完成标准 |
| --- | --- | --- |
| 1 | [#103](https://github.com/TheLudlows/anyseek/issues/103) | 有效记录 fingerprint 必需，加载统一检查，删除旧包特殊分支 |
| 2 | [#104](https://github.com/TheLudlows/anyseek/issues/104) | 统一 Hybrid 与 RRF 参数入口，保留默认及加权行为 |
| 3 | [#105](https://github.com/TheLudlows/anyseek/issues/105) | 清除无效配置、伪测试维度和当前文档中的旧架构表述 |
| 4 | [#106](https://github.com/TheLudlows/anyseek/issues/106) | 在在途工作、pins、备份和回执约束释放后，有界回收已删除集合代次 |
| 5 | [#107](https://github.com/TheLudlows/anyseek/issues/107) | 测量 Catalog/Generation 全量处理成本，以收益决定是否局部更新 |
| 6 | [#94](https://github.com/TheLudlows/anyseek/issues/94) | 澄清逻辑向量索引与 tail/Segment 所有权，保留多组件查询和全局 Top-K |

不预设统一内部水位、UUID 替换 Operation ID、统一资源框架、重建任务执行器或合并恢复文件。保留 Stage.members、routing_key 和必要一致性校验；各项实现与验收按对应 issue 推进，不以建单代表完成。

## 容量与外部验收

完整逻辑 State 的驻内存与状态容量限制、全服务缓存总额度、热点/冷恢复/慢 I/O/慢输出下的组合隔离仍需按实际需求评估。真实云 S3、系统崩溃/断电和跨 AES 验证须独立记录，不以本地 fixture 或进程退出代替。

SQLite 仅保存协调元数据，规范原文/原向量归 Payload，Tantivy 与向量/元数据组件保存派生检索数据。幂等历史回收保留期限和持久 epoch 拒绝规则，不能简单删除旧回执或提高容量来解除重试边界。

超大 collection 分片、自动 split/merge、跨主机 HA、直接远端 Directory、预取与自动调度按容量需求后置。模型推理、解析切块、Embedding/Reflect、SQL/JOIN、完整图语言和旧格式迁移不进入本轮。
