# AnySeek 项目指导

**简体中文** · [English](AGENTS.en.md)

## 改动后清理

涉及功能变更、缺陷修复、重构、迁移、删除、重命名、文档或测试调整时，使用本仓库的 `$cleanup-after-change`，读取并遵循[技能说明](.agents/skills/cleanup-after-change/SKILL.md)，在决定测试增量时和交付前执行。清理和必要验证属于任务完成条件；范围限于本次授权改动及其直接影响。纯问答与只读分析无需触发。

## 文档与验证

遵循[文档约定](docs/README.md)维护中英文对照，按[开发与验证](docs/development.md)选择与变更相关的检查和验收。保留现有兼容承诺和必要测试门槛；技能不替代这些要求。
