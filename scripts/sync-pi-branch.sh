#!/bin/bash
# Sync pi-feature branch with latest main

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(dirname "$SCRIPT_DIR")"

cd "$REPO_DIR"

echo "=== Syncing t3code-fork ==="
echo ""

# Update main first
echo "1. Updating main branch..."
git checkout main
git fetch origin
git reset --hard origin/main
echo "   ✓ main is now at $(git rev-parse --short origin/main)"
echo ""

# Update pi-feature
echo "2. Rebasing pi-feature on latest main..."
git checkout pi-feature
git rebase origin/main
echo "   ✓ pi-feature rebased successfully"
echo ""

# Push pi-feature
echo "3. Pushing pi-feature..."
git push --force-with-lease origin pi-feature
echo "   ✓ pi-feature pushed"
echo ""

echo "=== Sync Complete ==="
echo ""
echo "Current state:"
git log --oneline -3 pi-feature
