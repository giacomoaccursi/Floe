# Contributing

Thanks for your interest in contributing to Floe.

## Quick start

```bash
git clone https://github.com/giacomoaccursi/floe.git
cd floe
sbt compile
sbt test
```

Requires Java 17+ and SBT 1.9+.

## Workflow

1. Create a branch from `develop` (`feat/my-feature`, `fix/my-fix`)
2. Make changes, commit with [Conventional Commits](https://www.conventionalcommits.org/) format
3. Open a PR targeting `develop`
4. Pass CI (compile, test, scalafmt)

## Commit format

```
<type>(<scope>): <description>
```

Types: `feat`, `fix`, `perf`, `refactor`, `test`, `docs`, `chore`. Max 72 characters, no body.

## Bug fixes

1. Write a test that reproduces the problem (must fail)
2. Fix the bug
3. Show the test passing

## Code style

- Prefer immutability, `Option` over `null`, pattern matching
- Cache with `try-finally`, minimize Spark actions
- Run `sbt scalafmtAll` before committing

## Full guide

See the [Development Guide](https://giacomoaccursi.github.io/floe/contributing/development/) and [Code Quality Rules](https://giacomoaccursi.github.io/floe/contributing/code-quality/) for details.
