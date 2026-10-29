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
    test_status=0
    cargo test \
      --no-fail-fast \
      --all-targets \
      --all-features \
      --workspace \
      --exclude iceberg-catalog-glue \
      --exclude iceberg-catalog-hms \
      --exclude iceberg-catalog-rest \
      --exclude iceberg-integration-tests \
      --exclude iceberg-storage-opendal || test_status=$?
    for package in iceberg-catalog-glue iceberg-catalog-hms iceberg-catalog-rest iceberg-storage-opendal; do
      cargo test --no-fail-fast --lib --all-features -p "${package}" || test_status=$?
    done
    exit "${test_status}"
    ;;
  doc-test)
    cargo test --no-fail-fast --doc --all-features --workspace
    ;;
  chalk-consumer)
    test_status=0
    for package in iceberg iceberg-catalog-glue iceberg-catalog-rest; do
      cargo test --locked --lib -p "${package}" || test_status=$?
    done
    cargo test --locked --lib -p iceberg-storage-opendal --features opendal-gcs || test_status=$?
    exit "${test_status}"
    ;;
  docker-integration-test)
    test_status=0
    trap 'make docker-down' EXIT
    make docker-up
    cargo test --no-fail-fast -p iceberg-integration-tests --all-features || test_status=$?
    cargo test --no-fail-fast -p iceberg-catalog-glue --test glue_catalog_test || test_status=$?
    for hms_endpoint in localhost:9083 localhost:9084; do
      ICEBERG_TEST_HMS_ENDPOINT="${hms_endpoint}" \
        cargo test --no-fail-fast -p iceberg-catalog-hms --test hms_catalog_test || test_status=$?
    done
    cargo test --no-fail-fast -p iceberg-catalog-rest --test rest_catalog_test || test_status=$?
    cargo test --no-fail-fast -p iceberg-storage-opendal --all-features --test file_io_gcs_test --test file_io_s3_test || test_status=$?
    if [[ "$(uname -s)" == "Linux" ]]; then
      docker compose -f dev/docker-compose.yaml --profile hdfs up -d --build --wait hdfs-namenode hdfs-datanode || test_status=$?
      cargo test --no-fail-fast -p iceberg-storage-opendal --all-features --test file_io_hdfs_test -- --ignored || test_status=$?
    fi
    exit "${test_status}"
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
