#!/usr/bin/env bash
set -euo pipefail

tag="${BUILDKITE_TAG:-}"

if [[ ! "${tag}" =~ ^chalk-v([0-9]+\.[0-9]+\.[0-9]+)(-rc\.[0-9]+)?$ ]]; then
  echo "Release builds must use tags like chalk-v0.8.0 or chalk-v0.8.0-rc.1; got '${tag}'." >&2
  exit 64
fi

version="${BASH_REMATCH[1]}${BASH_REMATCH[2]:-}"
workspace_version="$(awk -F'"' '/^version = / { print $2; exit }' Cargo.toml)"

if [[ "${workspace_version}" != "${version}" ]]; then
  echo "Tag ${tag} resolves to version ${version}, but Cargo.toml workspace version is ${workspace_version}." >&2
  exit 1
fi

bash .buildkite/ci.sh fmt
bash .buildkite/ci.sh clippy
bash .buildkite/ci.sh chalk-consumer
cargo test --no-fail-fast --all-targets --all-features --workspace

buildkite-agent annotate --style "success" --context "release" \
  "Validated Iceberg Rust fork release ${tag} at ${BUILDKITE_COMMIT}."
