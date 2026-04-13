# Development Workflow

## Conventional Commits

All commits follow the Conventional Commits format:

```
<type>(<scope>): <description>
```

| Type | Usage |
|------|-------|
| `feat` | New feature |
| `fix` | Bug fix |
| `perf` | Performance improvement |
| `refactor` | Code restructuring (no behavior change) |
| `test` | Adding or updating tests |
| `docs` | Documentation changes |
| `chore` | Build, CI, tooling changes |

Rules:

- Scope is optional but recommended (e.g., `fix(validation): handle NULL in range comparisons`)

### Examples

```
feat(iceberg): add Glue catalog provider
fix(scd2): prevent duplicate versions on idempotent runs
test(validation): add property tests for PK uniqueness
docs(site): add cloud deployment guide
refactor(orchestration): extract flow ordering logic
perf(dag): cache intermediate join results
chore(ci): upgrade SBT to 1.10
```

## Branch strategy

The project uses a two-branch model:

- **`develop`** — active development branch. All feature branches merge here.
- **`main`** — stable release branch. Only `develop` merges into `main` during releases.

### Branch naming

```
feat/short-description
fix/short-description
docs/short-description
refactor/short-description
```

### Workflow

1. Create a branch from `develop`
2. Make changes, commit with conventional commits
3. Open a PR targeting `develop`
4. Pass CI checks (compile, test, lint)
5. Get code review approval
6. Squash-merge into `develop`

## Bug fix process

Bug fixes follow a mandatory order:

1. **Write a test that reproduces the problem** — the test must fail with the current code and clearly describe the incorrect behavior.
2. **Fix the bug** — implement the minimal necessary fix.
3. **Show the test passing** — run the test and confirm green output before considering the work complete.

Fixing a bug without a covering test is not acceptable.

## CI/CD pipeline

The CI pipeline runs on every push and PR:

```
1. Scalafmt check    sbt scalafmtCheckAll
2. Compile           sbt compile
3. Test + coverage   sbt coverage test coverageReport
4. Publish results   JUnit report + coverage artifact
```

### Test configuration

- Framework: **ScalaTest** with `AnyFlatSpec` or `AnyWordSpec` style
- Property-based testing: **ScalaCheck** for edge cases and invariants
- JVM: forked with 2 GB heap (`-Xmx2g`)
- Execution: sequential (`parallelExecution := false`) to avoid SparkSession conflicts
- Spark: local mode with `master("local[*]")`, `spark.sql.shuffle.partitions = 1`, and `spark.driver.bindAddress = 127.0.0.1`

### Release process

Releases are automated via [semantic-release](https://semantic-release.gitbook.io/):

1. Merge `develop` into `main`
2. Semantic-release analyzes commit messages and determines the next version (`feat` → minor, `fix` → patch)
3. Creates a git tag, generates the CHANGELOG, and publishes a GitHub Release
4. CI publishes the artifact to GitHub Packages

No manual tagging is needed.

## PR process

### Before opening a PR

- [ ] Code compiles without warnings
- [ ] All tests pass (`sbt test`)
- [ ] New code has test coverage
- [ ] Commit messages follow conventional commits format
- [ ] No unrelated changes included

### PR description

Include:

- What the change does (one sentence)
- Why it's needed
- How to test it
- Any breaking changes

### Review checklist

Reviewers check:

- Correctness and edge cases
- Test coverage
- Scala style and Spark best practices (see [Code Quality](code-quality.md))
- Documentation updates if needed

## Related

- [Development Setup](development.md) — clone, build, test
- [Code Quality](code-quality.md) — style rules and Spark best practices
