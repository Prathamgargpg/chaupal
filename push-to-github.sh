#!/usr/bin/env bash
# Pushes this folder to https://github.com/Prathamgargpg/chaupal
# Prereq: install Git (git-scm.com). Optional: GitHub CLI (cli.github.com) and run `gh auth login` once.
set -e
REPO=chaupal
USER=Prathamgargpg

git init -q
git add .
git commit -qm "Chaupal video meetings" || true
git branch -M main

if command -v gh >/dev/null 2>&1; then
  gh repo create "$REPO" --public --source=. --remote=origin --push
else
  echo "Create an EMPTY repo named '$REPO' at https://github.com/new (no README), then press Enter."
  read -r _
  git remote remove origin 2>/dev/null || true
  git remote add origin "https://github.com/$USER/$REPO.git"
  git push -u origin main   # sign in with your GitHub username + a personal access token as the password
fi
echo "Done: https://github.com/$USER/$REPO"
