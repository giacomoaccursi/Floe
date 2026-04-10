# Contributing to Floe

First off, thanks for taking the time to contribute! ❤️

All types of contributions are encouraged and valued — whether it's reporting a bug, suggesting a feature, improving documentation, or writing code. Please read the relevant section before making your contribution.

> If you like the project but don't have time to contribute, there are other ways to support it:
> - Star the project on GitHub
> - Mention it in your project's README
> - Share it with colleagues who work with Spark and Iceberg

## Table of Contents

- [I Have a Question](#i-have-a-question)
- [Reporting Bugs](#reporting-bugs)
- [Suggesting Enhancements](#suggesting-enhancements)
- [Your First Code Contribution](#your-first-code-contribution)
- [Improving the Documentation](#improving-the-documentation)
- [Commit Messages](#commit-messages)

## I Have a Question

Before opening an issue, please:

1. Read the [documentation](https://giacomoaccursi.github.io/floe/)
2. Search existing [issues](https://github.com/giacomoaccursi/floe/issues) for your question

If you still need help, [open an issue](https://github.com/giacomoaccursi/floe/issues/new) with as much context as possible (Spark version, Scala version, config YAML, stack trace).

## Reporting Bugs

### Before Submitting

- Make sure you are using the latest version
- Check the [documentation](https://giacomoaccursi.github.io/floe/) to confirm it's a bug and not a configuration issue
- Search existing [issues](https://github.com/giacomoaccursi/floe/issues?q=label%3Abug) to see if it's already reported

### How to Submit

[Open an issue](https://github.com/giacomoaccursi/floe/issues/new) and include:

- Steps to reproduce the problem
- Expected behavior vs actual behavior
- Your YAML config (sanitized — remove credentials and sensitive data)
- Stack trace
- Spark, Scala, Iceberg, and Java versions

## Suggesting Enhancements

[Open an issue](https://github.com/giacomoaccursi/floe/issues/new) and describe:

- What you want to achieve
- Why existing functionality doesn't cover it
- How you envision the solution (YAML config example, API usage, etc.)

Keep in mind that Floe is focused on declarative batch ETL with Iceberg. Features outside this scope (streaming, multi-engine support, orchestration) are unlikely to be accepted.

## Your First Code Contribution

### Setup

```bash
git clone https://github.com/giacomoaccursi/floe.git
cd floe
sbt compile
sbt test
```

Requires Java 17+ and SBT 1.9+.

### Workflow

1. Create a branch from `develop` (`feat/my-feature`, `fix/my-fix`)
2. Write your code following the [code quality rules](https://giacomoaccursi.github.io/floe/contributing/code-quality/)
3. Add tests for new functionality or bug fixes
4. Run `sbt scalafmtAll` before committing
5. Run `sbt test` and make sure all tests pass
6. Open a PR targeting `develop`

### Bug Fix Workflow

Bug fixes follow a strict order:

1. Write a test that reproduces the problem (must fail with current code)
2. Implement the minimal fix
3. Verify the test passes

A bug fix without a covering test will not be accepted.

### Code Style

- Prefer immutability, `Option` over `null`, pattern matching
- Small single-responsibility functions
- Cache DataFrames with `try-finally` to guarantee `unpersist()`
- Minimize Spark actions (`count()`, `collect()`, `save()`)
- Do not add features, refactoring, or improvements beyond what the PR addresses

See the full [Code Quality Rules](https://giacomoaccursi.github.io/floe/contributing/code-quality/) for details.

## Improving the Documentation

Documentation lives in `site-docs/` and is built with [MkDocs Material](https://squidfunk.github.io/mkdocs-material/). To preview locally:

```bash
pip install mkdocs-material pymdown-extensions
mkdocs serve
```

Then open `http://localhost:8000`.

## Commit Messages

We use [Conventional Commits](https://www.conventionalcommits.org/):

```
<type>(<scope>): <description>
```

Types: `feat`, `fix`, `perf`, `refactor`, `test`, `docs`, `chore`.

Examples:
- `feat(validation): add expression rule type`
- `fix(iceberg): handle null partition values in SCD2`
- `docs(guides): update orphan detection examples`

## Legal Notice

When contributing to this project, you agree that you have authored 100% of the content, that you have the necessary rights to the content, and that the content you contribute may be provided under the [Apache License 2.0](LICENSE).
