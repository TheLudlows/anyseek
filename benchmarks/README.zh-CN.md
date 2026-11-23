# 基准代码与实验证据

**简体中文** · [English](README.md)

各实验目录分别保存工作负载、正确性检查和历史结果。这里提供共用执行代码。

- `shared_query_compare.py` 核验种子副本并执行 ABBA 进程调度。`query-cost/compare.py` 默认保留一组 ABBA，`query-alignment/allocation_compare.py` 默认保留三组，并隔离采集子进程资源。两者都支持 `--rounds` 和 `--[no-]resources`。如结果目录已存在则拒绝执行，保留失败运行的数据库和此前结果。
- `resource_run.py` 为诊断探针提供独立进程的 CPU/RSS 采集。
- `shared_record_http.py` 提供可丢弃 Record 实例的生命周期、传输、有上限的显式 Busy 重试、回执一致性和文件哈希。查询对齐和写入成本实验复用它，同时各自负责工作负载生成、公开游标检查与采样。分配预加载路径仍由各实验管理。查询对齐保留线程本地 HTTP 状态。
- `shared_http_engine.py` 是既有容量和分词引擎抽象，含跨引擎配置。它有自己的 API 与工作负载契约，继续独立于 Record 传输层。

在 Linux 上执行 `python3 benchmarks/test_shared_harness.py` 检查辅助逻辑回归。它覆盖 ABBA 默认值、资源辅助文件、失败运行保留与重试安全，与服务性能测量独立。无分配插桩的计时仍与分配器插桩分别执行。运行器重构不会重新生成或改写历史结果，保留每项实验记录的源码与二进制哈希。
