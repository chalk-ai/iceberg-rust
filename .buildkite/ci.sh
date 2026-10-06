#!/usr/bin/env bash
set -euo pipefail

task="${1:-}"

retry_install() {
  cargo install "$@" || cargo install "$@"
}

ensure_cargo_bin() {
  local bin="$1"
  shift

  if ! command -v "${bin}" >/dev/null 2>&1; then
    retry_install "$@"
  fi
}

ensure_cargo_bin_version() {
  local bin="$1"
  local version="$2"
  shift 2

  if ! command -v "${bin}" >/dev/null 2>&1 || ! "${bin}" --version | grep -Fq "${version}"; then
    retry_install --force "$@"
  fi
}

case "${task}" in
  fmt)
    cargo fmt --all -- --check
    ;;
  toml)
    ensure_cargo_bin taplo taplo-cli@0.9.3 --locked
    taplo check
    ;;
  clippy)
    cargo clippy --all-targets --all-features --workspace -- -D warnings
    ;;
  build)
    cargo build --all-targets --all-features --workspace
    ;;
  no-default-features)
    cargo build -p iceberg --no-default-features
    ;;
  test)
    cargo test --no-fail-fast --all-targets --all-features --workspace --exclude iceberg-integration-tests
    ;;
  doc-test)
    cargo test --no-fail-fast --doc --all-features --workspace
    ;;
  chalk-consumer)
    cargo check -p iceberg --features storage-gcs
    cargo check -p iceberg-catalog-glue
    cargo check -p iceberg-catalog-rest
    ;;
  docker-integration-test)
    cargo test --no-fail-fast -p iceberg-integration-tests --all-features
    ;;
  cargo-machete)
    ensure_cargo_bin cargo-machete cargo-machete@0.7.0 --locked
    cargo machete
    ;;
  audit)
    ensure_cargo_bin_version cargo-audit 0.22.2 cargo-audit@0.22.2 --locked
    cargo audit
    ;;
  typos)
    ensure_cargo_bin typos typos-cli@1.42.3 --locked
    typos
    ;;
  *)
    echo "usage: $0 {fmt|toml|clippy|build|no-default-features|test|doc-test|chalk-consumer|docker-integration-test|cargo-machete|audit|typos}" >&2
    exit 64
    ;;
esac
