# Shared rules for every vexouLuna repo

Synced from [vexouLuna/conventions](https://github.com/vexouLuna/conventions). Each repo's own `CLAUDE.md`
imports this file on its first line and adds only what is specific to that repo. To change a rule here,
change it in vexouLuna/conventions, release it, and bump the pin. Never edit the copy in `.conventions/`.

## Before you start

- Read `CONTRIBUTING.md`. It covers branches, the checks CI runs and how releases work in this repo.
- `.conventions.toml` says which profile the repo follows and its release flow (`flow = "trunk"` or
  `flow = "dev"`).

## Git

- **Never commit on `main`** (or on `dev` in a `flow = "dev"` repo). Work on a branch and merge through a
  pull request.
- **Branch names follow [Conventional Branch](https://conventional-branch.github.io/):**
  `feature|bugfix|hotfix|release|chore/<lowercase-hyphenated-description>`.
  - CI fails any other name. That includes the `claude/...` names a worktree starts with, so rename
    the branch (`git branch -m chore/what-it-does`) **before the first push**.
- In a `flow = "dev"` repo, pull requests target `dev`. Only `dev`, `release/*` and `hotfix/*` merge into
  `main`.
- Commit messages follow Conventional Commits: `feat:`, `fix:`, `docs:`, `chore:`, `refactor:`, `test:`,
  `ci:`, optionally with a scope, e.g. `feat(backup): …`.
- Commit or push only when asked. Never leave work uncommitted at the end of a task without saying so.
- Never skip hooks (`--no-verify`) unless the user asks for it.

## Nothing undocumented, nothing by hand

- A procedure that matters (a deploy step, a setting, a one-off fix on a server) is either committed as
  a script and documented, or it doesn't happen. Don't leave hand-made state behind.
- **Repo settings** (merge methods, branch deletion, branch protection, required checks) change only
  through `conventions repo-settings --apply`, never by hand in the GitHub UI.
- **Synced files are never edited in place:** everything in `.conventions/`, `.gitattributes`, `.nvmrc`,
  and the blocks between `conventions:begin` / `conventions:end` markers. `conventions check` (a required
  CI job) fails if they differ from the pinned version.

## Public repos and private infrastructure

- **No private infrastructure in a public repo:** no machine hostnames, private IP addresses, server
  paths, proxy or tunnel config, deploy scripts or backup wiring. Those live only in the private
  infrastructure repo. `conventions check --public` greps for them.
- **Permissions or settings that name private infrastructure go in user-level Claude settings**
  (`~/.claude/settings.json`), never in a repo's `.claude/settings.json`.
- **Never write a secret's value** into a file, a commit, a PR, an issue or a reply. Name the secret and
  where it lives instead.

## Code conventions

- **Python:**
  - 3.13, pinned in `.python-version`.
  - uv, with `uv.lock` committed; CI runs `uv sync --locked`.
  - ruff extends `.conventions/ruff.toml`.
  - mypy is `strict`. Per-module relaxations go in `[[tool.mypy.overrides]]`, each with a reason.
- **Node:** 22, pinned in `.nvmrc` and in `engines.node`.
- **Containers:**
  - One multi-stage `Dockerfile` at the repo root. A repo with several services builds them as
    `--target`s.
  - The image runs as a non-root user and carries its own `HEALTHCHECK`.
  - Images are `ghcr.io/vexouluna/<repo>`, plus `-<service>` when a repo has several.
- **Compose files:**
  - `compose.yaml` is production-capable and pulls the published image.
  - `compose.dev.yaml` is for local development.
  - No other compose file names.
- **Health endpoints:** every service serves `/healthz` (alive) and `/readyz` (ready to take traffic,
  dependencies included).
- **Workflows:** `.github/workflows/ci.yml` runs the checks and `publish.yml` publishes. Both call the
  reusable workflows in vexouLuna/conventions, pinned to the same version as `.conventions.toml`.

## Web repos (profiles `site` and `node-lib`)

- **Mobile has the same functions as desktop:** reflow or scroll; never hide controls or columns.
- **Images not yet supplied stay placeholders.** Keep a list of what's still needed.
- **Libraries reach sites only through releases:** changeset → version PR → merge → `vX.Y.Z` tag → bump
  the tag in the site's `package.json`.
- **Backend APIs consumed by a site are read-only from its point of view.** Ask for a new endpoint
  rather than working around a missing one in the browser.
