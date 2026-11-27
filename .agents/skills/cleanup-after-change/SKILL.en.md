---
name: cleanup-after-change
description: "Clean up superseded code, redundant implementation paths and tests, unused configuration and dependencies, and stale documentation after repository changes. Use when deciding test changes and as a completion step for feature changes, bug fixes, refactors, migrations, removals, and renames, including requests to 精简、清理旧代码、删除旧逻辑、同步文档、精简测试 or avoid redundant code and tests. Apply to the current change and its direct consequences; do not trigger for questions or analysis without requested edits."
---

# Cleanup after change

**English** · [简体中文](SKILL.md)

Make cleanup part of implementation completion. Work within the authorized change and its direct consequences, following applicable project instructions and compatibility commitments. Perform justified cleanup without seeking confirmation for each removal. Do not treat this skill as authorization to clean the entire repository, delete the user's work, or change project rules.

## 1. Identify scope and replacements

- Inspect the current working tree, task requirements, and relevant diff first; distinguish this change from the user's existing modifications.
- Identify the new or changed entry points and the implementations, configuration, terminology, and documentation they replace. If implementation is unfinished, complete it before cleanup.
- Search for and reuse existing capabilities before adding code; prefer updating the existing implementation. New modules, wrappers, abstractions, or execution paths must address a concrete current need.
- Follow only leftovers directly related to this change. Stop when nothing needs removal; do not invent a refactor to reduce line counts.

## 2. Remove and consolidate along usage relationships

- Prefer rg to search old symbols, string names, configuration keys, routes, commands, and documentation references. Determine their purpose using call chains, exports, registries, reflection, plugin discovery, and build configuration.
- Once the new implementation has taken over and the old behavior is no longer needed, remove the old functions, branches, wrappers, entry points, flags, dependencies, scripts, examples, and comments together.
- Prefer one implementation for the same behavior. Do not add duplicate implementations, commented-out old code, backup copies, or abstractions for hypothetical needs.
- Retain compatibility layers, old paths, and fallbacks only for explicit compatibility commitments, actual callers, or unfinished migrations, and record the concrete reason. Preserve fault handling, validation, and boundary handling that still serve a purpose.
- Do not keep redundant code because it might be useful later. Do not infer that public SDK/API entry points, external commands, deployment configuration, persistent formats, or old-data readers can be deleted merely because the repository contains no references.
- Preserve historical database migrations, upgrade paths that are still required, and valid historical records. When unresolved questions affect external users or data compatibility, finish the cleanup that can be determined and then explain the remaining questions.
- Do not remove, overwrite, or revert unrelated user modifications or files. Do not substitute global cleanup or hard resets for item-by-item cleanup.

## 3. Maintain documentation in place

- Update existing authoritative documentation, navigation, links, configuration examples, and commands. Remove obsolete guidance so replaced and current approaches are not both presented as current.
- Preserve valid content in partially outdated documents. Respect explicitly maintained versioned documentation, change history, and archives.
- Update corresponding language versions when the project already maintains them. Do not expand the translation scope because of this skill.
- Unless explicitly requested by the user or required by project workflow, do not add completion reports, cleanup reports, refactoring summaries, duplicate guides, or one-off explanatory files.

## 4. Control test additions

- Inspect existing coverage before adding a test, and identify the specific regression the new case would detect that is not already covered. Prefer reusing, updating, or appropriately parameterizing existing tests. It is acceptable to add no cases when coverage is sufficient; do not add tests to meet a case count or merely raise a coverage number.
- Add necessary tests only for uncovered behavior changes, defect regressions, real boundaries or failure risks, and explicit project acceptance requirements. For reversible, low-impact changes to wording, formatting, comments, and similar content that do not change behavior, run applicable existing checks by default without adding tests.
- Do not mechanically create a test for every function, branch, or file. Avoid tests that only verify private names, source strings, line counts, internal call order, or mocks themselves, unless those are an explicit external contract.
- Assert observable behavior. Derive expected values from requirements, known examples, or independent evidence; do not copy the implementation under test to calculate expectations or add tests whose expected results would reproduce the implementation's mistakes.
- Within this change's scope, merge or remove tests confirmed to be duplicates, obsolete, or without independent assertion value. First establish that the remaining cases can still detect the regressions originally covered, and preserve clear failure diagnosis. Similar inputs, code, or assertions, or a green test run after deletion, do not by themselves prove redundancy.
- Preserve effective coverage of distinct boundaries, permissions, concurrency conditions, integration wiring, and failure modes, especially historical defects and data correctness. Do not remove useful cases to reduce counts or hide regressions by deleting tests or weakening assertions.
- Follow explicit project test requirements. Briefly explain necessary test additions, merges, or removals in the delivery response; do not create a separate test report or invent test work when none occurred.

## 5. Validate and deliver

- Search replaced symbols, configuration, and terminology again. Inspect each remaining match and distinguish valid compatibility and historical references.
- Run the builds, type or static checks, and meaningful behavior tests relevant to the change according to project conventions. Cover affected callers when shared entry points change.
- Check documentation links, entry-point registration, and dependency declarations for consistency. Stop expanding validation once the evidence is sufficient; perform additional checks only for a concrete remaining risk or a required project gate.
- Review the final diff for missed cleanup, unrelated changes, and accidental deletion. Respect the original task's scope for commits, releases, and authorization.
- In the normal delivery response, briefly describe removed or consolidated items, necessary retained items and their reasons, validation results, and actual blockers. Do not create a separate report or fabricate deletions or test results.
