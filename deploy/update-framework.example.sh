#!/usr/bin/env bash
set -Eeuo pipefail

# Copy this file to deploy/update-framework.sh and edit it for your deployment.
# ShinBot calls this script from [admin].framework_update_dir when the operator
# runs `update framework` in the console. A zero exit code requests a restart.

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

echo "[shinbot-update] repo: $repo_root"

if [[ ! -d .git ]]; then
  echo "[shinbot-update] not a git checkout: $repo_root" >&2
  exit 2
fi

if [[ -n "$(git status --porcelain)" ]]; then
  echo "[shinbot-update] working tree is dirty; refusing to update" >&2
  git status --short
  exit 3
fi

branch="$(git branch --show-current)"
upstream="$(git rev-parse --abbrev-ref --symbolic-full-name '@{upstream}')"
echo "[shinbot-update] branch: ${branch:-detached}"
echo "[shinbot-update] upstream: $upstream"

git fetch --prune
git merge --ff-only "$upstream"

if command -v uv >/dev/null 2>&1; then
  echo "[shinbot-update] syncing python dependencies"
  uv sync --group dev
fi

echo "[shinbot-update] done"
