#!/usr/bin/env bash
set -euo pipefail

# --- Run tests ---
echo "Running tests..."
cargo test --lib

# --- Configuration ---
TOKEN="${CRATES_IO_TOKEN:-}"  # safer than hardcoding

# --- Login to crates.io ---
echo "$TOKEN" | cargo login

# --- Read current version ---
current_version=$(cargo pkgid | sed 's/.*#//')  # e.g., 0.1.0
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

# --- Commit the version bump ---
git add Cargo.toml
git commit -m "chore: bumped version from $current_version to $new_version"

# --- Dry-run publishing ---
echo "Performing dry-run to check package..."
cargo publish --dry-run

# --- Confirm publishing ---
read -p "Dry-run succeeded. Do you want to publish for real? [y/N] " confirm
if [[ "$confirm" =~ ^[Yy]$ ]]; then
  echo "Publishing crate..."
  cargo publish
  echo "✅ Crate published successfully!"
else
  echo "Publishing aborted."
fi
