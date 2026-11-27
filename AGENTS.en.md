# AnySeek project guidance

[简体中文](AGENTS.md) · **English**

## Cleanup after change

For feature changes, bug fixes, refactors, migrations, removals, renames, or documentation and test changes, use this repository's `$cleanup-after-change`. Read and follow the [skill instructions](.agents/skills/cleanup-after-change/SKILL.en.md) when deciding test changes and before delivery. Cleanup and necessary validation are part of task completion; limit them to the authorized change and its direct consequences. Questions and read-only analysis do not trigger this workflow.

## Documentation and validation

Maintain corresponding Chinese and English versions under the [documentation conventions](docs/README.en.md), and select relevant checks and acceptance work from [development and validation](docs/development.en.md). Preserve existing compatibility commitments and required test gates; the skill does not replace these requirements.
