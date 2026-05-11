# Pi Feature Sync Workflow

This document describes how to keep the `pi-feature` branch synchronized with upstream.

## Branch Overview

| Branch | Purpose | Push URL |
|--------|---------|----------|
| `main` | Clean mirror of pingdotgg/t3code | read-only |
| `pi-feature` | Pi agent implementation | Changhochien/t3code |

## Updating Main (Always Fast-Forward)

```bash
git checkout main
git pull origin main
```

## Syncing Pi Feature Branch

### Option 1: Rebase (Recommended for clean history)

```bash
git checkout pi-feature
git fetch origin
git rebase origin/main

# If conflicts occur:
git status
# Fix conflicts, then:
git add .
git rebase --continue

# Push updates
git push --force-with-lease origin pi-feature
```

### Option 2: Merge

```bash
git checkout pi-feature
git fetch origin
git merge origin/main

# Push updates
git push origin pi-feature
```

## Using the Sync Script

```bash
./scripts/sync-pi-branch.sh
```

## Notes

- `main` branch is reset to `origin/main` - never commit directly to it
- `pi-feature` contains all pi implementation changes
- Use `git log pi-feature --not main` to see pi-specific commits
