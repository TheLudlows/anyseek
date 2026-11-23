# AnySeek documentation

**English** · [简体中文](README.md)

| What you need | Where to start |
| --- | --- |
| Quick start | [Repository README](../README.en.md#quick-start) |
| Start, configure, and call the API | [Service guide](../anyseek/src/server/README.en.md) covers keys, S3, fields, writes and retries, queries, backups, and quotas. See also [example HTTP requests](../anyseek/src/server/test_api.rest). |
| Use and deploy the console | [Console guide](../console/README.en.md) covers tenant connections, import, embedding services, API proxying, HTTPS, and runtime status. |
| Features and limitations | [Feature overview](features.en.md) |
| Search configuration and ranking | [Text normalization, N-grams, field length, stable ordering, and Weighted RRF](search.en.md) |
| Architecture, failures, and shutdown | [Current architecture](architecture.en.md) covers modules, queryable tails, Segment ownership, fencing after failures, and service draining. |
| Consistency and write proofs | [Record contract](record-model.en.md) covers identity, retries, TTL, authorization, and the commit anchor for ordinary writes. |
| Persistence and recovery | [Storage protocol](storage.en.md) covers local and SQLite/S3 authority, checkpoints, backups, and reclamation. |
| Development and verification | [Development guide](development.en.md) covers local builds, fault tests, S3 acceptance, CI responsibilities, and the full matrix. |
| Future work and performance evidence | [Roadmap and evidence index](roadmap.en.md). Raw data and reproduction commands are in the corresponding benchmarks directories. |
| Corpus and license | [MIRACL corpus](../miracl-corpus-v1.0-zh/README.en.md) and [license](../LICENSE) |

Update the service guide and runnable examples together when an interface changes. Describe design invariants in their protocol documents and keep verification commands in the development guide. Performance reports retain their pinned revisions, raw evidence, and reproduction steps. The roadmap indexes these reports. Consult Git history, PRs, and CI for historical plans, completed work logs, and test organization history.

Each document comes in Simplified Chinese and English. Use the links at the top to switch languages. Update both versions in the same change and keep sections, tables, commands, parameters, numbers, and verification conclusions aligned. Code examples and original evidence use the same content.
