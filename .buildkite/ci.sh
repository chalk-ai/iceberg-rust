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
    python3 .buildkite/test-check-required-tests.py
    mkdir -p target/chalk-consumer
    evidence_dir="$(mktemp -d target/chalk-consumer/run.XXXXXX)"
    for package in iceberg iceberg-catalog-glue iceberg-catalog-rest; do
      features="iceberg/storage-gcs"
      if [[ "${package}" == iceberg ]]; then
        features="storage-gcs"
      fi
      test_command=(cargo test --locked --lib -p "${package}" --features "${features}" --color never -- --format pretty --color never)
      printf '%s\n' "${test_command[@]}" > "${evidence_dir}/${package}.command"
      test_status=0
      "${test_command[@]}" 2>&1 | tee "${evidence_dir}/${package}.log" || test_status=$?
      printf '%s\n' "${test_status}" > "${evidence_dir}/${package}.status"
    done
    python3 .buildkite/check-required-tests.py --logs "${evidence_dir}"
    ;;
  docker-integration-test)
    cargo test --no-fail-fast -p iceberg-integration-tests --all-features
    ;;
  cargo-machete)
    ensure_cargo_bin cargo-machete cargo-machete@0.7.0 --locked
    cargo machete
    ;;
  audit)
    ensure_cargo_bin cargo-audit cargo-audit@0.21.2 --locked
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
