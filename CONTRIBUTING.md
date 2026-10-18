# Contributing

<!-- conventions:begin: synced from vexouLuna/conventions; edit it there, then run `conventions sync` -->
## Set up the hooks once per clone

```bash
git config core.hooksPath .conventions/githooks
```

Git does not carry hooks in a clone, so this is the one step nothing can do for you (`conventions sync`
does it as a side effect). Without it, the branch rules below are only enforced in CI, which is a slower
way to hear about a typo. A repo's own extra checks live in `.githooks/local/pre-commit`, which the
shared hook runs after its own.

## Branches

Branch names follow [Conventional Branch](https://conventional-branch.github.io/): `<type>/<description>`,
where the description is lowercase letters, digits and single hyphens.

| Type | For |
|------|-----|
| `feature/` | a new capability, e.g. `feature/part-mapping` |
| `bugfix/` | a fix, e.g. `bugfix/issue-42-restricted-seek` |
| `hotfix/` | a fix that can't wait for the usual path, e.g. `hotfix/broken-deploy` |
| `release/` | preparing a version, e.g. `release/1-2-0` |
| `chore/` | dependencies, tooling, docs, anything with no behaviour change, e.g. `chore/bump-vite` |

A ticket number is just another word in the description.
`.conventions/githooks/check-branch-name.sh <name>` says whether a name passes, and CI runs the same
script (the `conventions / branch-name` check) against the branch a pull request comes from.

### Release flow: trunk

**`main` is merge-only.** The `pre-commit` hook refuses a commit made on `main`, `master`, `dev` or
`develop`. Work on a branch and merge it into `main` through a pull request:

```bash
git switch -c feature/what-you-are-doing
```

Concluding a merge that hit conflicts is a commit on `main`, and the hook lets that one through: the
rule is about where work starts, not where it lands. `git commit --no-verify` skips the hook entirely.
It exists for the day you need it, not for the day you are in a hurry.

## Checks

Every pull request runs:

- **`conventions / branch-name`:** the branch name, and in a `flow = "dev"` repo whether it may merge
  into its base.
- **`conventions / check`:** the synced files match the version pinned in `.conventions.toml`, and the
  repo follows the conventions for its profile. A public repo is also checked for private
  infrastructure (addresses, server paths).
- **`conventions / version`:** every file that carries the version (`pyproject.toml`, `uv.lock`,
  `__version__`, `package.json`, `package-lock.json`) says the same. In a `flow = "dev"` repo a pull
  request into `main` must also raise it, to a version with no tag yet.
- **`ci / …`:** the repo's lint, tests and build, from the reusable workflows in
  [vexouLuna/conventions](https://github.com/vexouLuna/conventions).

They are required checks on `main`. The repo's settings, protection included, are set by
`conventions repo-settings --apply`.

## Shared conventions

This section, `.conventions/`, `.gitattributes` and the other synced files come from
[vexouLuna/conventions](https://github.com/vexouLuna/conventions), at the version pinned in
`.conventions.toml`. Don't edit them here: change them there, release, and bump the pin (Renovate opens
that pull request). After a bump, rewrite the copies and commit them:

```bash
uvx --from "git+https://github.com/vexouLuna/conventions@$(sed -n 's/^version *= *"\(.*\)"/\1/p' .conventions.toml)" conventions sync
```
<!-- conventions:end -->

## Releases

Both applications pin a tag's release tarball
(`vex-platform @ https://github.com/vexouLuna/vex-platform/archive/refs/tags/v0.2.0.tar.gz`; not `git+`,
which needs git in their slim Docker images), so
a change reaches them only when a release is tagged on `main` and their pin moves. Tags follow semver;
anything that changes a public signature, a table or an API shape is a minor bump before 1.0.

The SQL under `src/vex_platform/migrations/sql/` is frozen once released: a schema change is a new
revision, never an edit. See [docs/conventions.md](docs/conventions.md#migrations-and-procrastinate-versions).
