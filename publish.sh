#!/usr/bin/env bash
set -euo pipefail

# --- Run tests ---
echo "Running tests..."
cargo test

# --- Check code formatting ---
echo "Checking code formatting..."
cargo fmt --check

# --- Configuration ---
TOKEN="${CRATES_IO_TOKEN:-}"

# --- Validation checks ---
if [[ -z "$TOKEN" ]]; then
    echo "Error: CRATES_IO_TOKEN environment variable is not set."
    echo "Please set it with: export CRATES_IO_TOKEN=your_token_here"
    exit 1
fi

# Check if working directory is clean
if ! git diff-index --quiet HEAD --; then
    echo "Error: Working directory is not clean. Please commit or stash changes first."
    git status --short
    exit 1
fi

# --- Login to crates.io ---
echo "$TOKEN" | cargo login

# --- Read current version ---
package_name=$(cargo pkgid | cut -d'#' -f1)  # e.g., fx-event-bus
current_version=$(cargo pkgid | cut -d'#' -f2)  # e.g., 0.1.0
IFS='.' read -r major minor patch <<< "$current_version"

echo "Current version: $current_version"
echo "Which part do you want to bump? (major/minor/patch)"
read -r part

case "$part" in
    major)
        major=$((major + 1))
        minor=0
        patch=0
        ;;
    minor)
        minor=$((minor + 1))
        patch=0
        ;;
    patch)
        patch=$((patch + 1))
        ;;
    *)
        echo "Invalid choice. Use major, minor, or patch."
        exit 1
        ;;
esac

new_version="$major.$minor.$patch"
echo "Bumping version: $current_version → $new_version"

# --- Update Cargo.toml ---
sed -i "s/^version = \".*\"/version = \"$new_version\"/" Cargo.toml

# --- Update Cargo.lock to reflect the version change ---
cargo update -p "$package_name"

# --- Commit the version bump ---
git add Cargo.toml Cargo.lock
git commit -m "chore: bumped version from $current_version to $new_version"

# --- Create git tag ---
git tag "v$new_version" -m "Release v$new_version"

# --- Dry-run publishing ---
echo "Performing dry-run to check package..."
cargo publish --dry-run

# --- Confirm publishing ---
read -p "Dry-run succeeded. Do you want to publish for real? [y/N] " confirm
if [[ "$confirm" =~ ^[Yy]$ ]]; then
  echo "Publishing crate..."
  cargo publish
  echo "✅ Crate published successfully!"

  # Push the tag to remote
  echo "Pushing tag v$new_version to remote..."
  git push origin "v$new_version"
  echo "🏷️ Tag pushed successfully!"
else
  echo "Publishing aborted."
fi
