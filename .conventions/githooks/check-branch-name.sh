#!/bin/sh
# The one place branch rules are written down. The pre-commit hook calls it for the branch you are on,
# and CI calls it (through check-pr-branches.sh) for the branch a pull request comes from, so both say
# the same thing.
#
#     .conventions/githooks/check-branch-name.sh feature/add-quotes
#
# Conventional Branch (https://conventional-branch.github.io/): <type>/<description>, where the
# description is lowercase words joined by single hyphens. A ticket number is just another word:
# `bugfix/issue-42-cooldown-off-by-one`.
#
# Synced from vexouLuna/conventions: change it there, not here.
set -eu

branch=${1:-}
types='feature|bugfix|hotfix|release|chore'

if [ -z "$branch" ]; then
    echo "check-branch-name: no branch given" >&2
    exit 2
fi

# Committing on a long-lived branch is what this is here to stop: work reaches main (and dev, in a repo
# with flow = "dev") through a merge, not directly.
case "$branch" in
    main | master | dev | develop)
        cat >&2 <<EOF
Refusing to commit on '$branch'.

Work happens on a branch and reaches $branch as a merge:

    git switch -c feature/what-you-are-doing

If you have already made changes here, take them with you — the command above keeps them.
EOF
        exit 1
        ;;
esac

if ! printf '%s' "$branch" | grep -Eq "^($types)/[a-z0-9]+(-[a-z0-9]+)*$"; then
    cat >&2 <<EOF
'$branch' is not a Conventional Branch name.

    <type>/<description>    types: $(echo "$types" | tr '|' ' ')

The description is lowercase letters, digits and single hyphens — no spaces, underscores, capitals,
double hyphens or a trailing one. For example:

    feature/publish-packs-globally
    bugfix/issue-42-cooldown-off-by-one
    chore/bump-dependencies

    git branch -m <new-name>    renames the branch you are on
EOF
    exit 1
fi
