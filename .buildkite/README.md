<!--
  ~ Licensed to the Apache Software Foundation (ASF) under one
  ~ or more contributor license agreements.  See the NOTICE file
  ~ distributed with this work for additional information
  ~ regarding copyright ownership.  The ASF licenses this file
  ~ to you under the Apache License, Version 2.0 (the
  ~ "License"); you may not use this file except in compliance
  ~ with the License.  You may obtain a copy of the License at
  ~
  ~   http://www.apache.org/licenses/LICENSE-2.0
  ~
  ~ Unless required by applicable law or agreed to in writing,
  ~ software distributed under the License is distributed on an
  ~ "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  ~ KIND, either express or implied.  See the License for the
  ~ specific language governing permissions and limitations
  ~ under the License.
-->

# Chalk consumer tests

`bash .buildkite/ci.sh chalk-consumer` runs the core, Glue and REST library tests
with default features and `iceberg/storage-gcs`, matching Chalk's storage profile.
The separate workspace test lane retains its broader all-features coverage.

All three package commands run, and a nonzero Cargo exit status fails the lane.
Buildkite's job output contains their results; no separate test-name inventory or
log parser determines success.

Requiring the pipeline's GitHub status for merges is a separate repository
protection setting. Fork results must match the revision Chalk consumes; Chalk's
adapter, Velox, SQL and persistent compatibility tests remain in the monorepo.

## Docker fixtures

The workspace and integration test lanes build MinIO server/client fixture images
from `crates/test_utils/testdata/minio/Dockerfile`. They retain server release
`RELEASE.2025-05-24T17-08-30Z` and client release
`RELEASE.2025-05-21T01-59-54Z`; official source commits and archive checksums, plus
build/runtime image digests, are pinned. The images include CA certificates and
Go's bundled timezone database. Docker's build cache reuses them across suites;
no image publication or registry credentials are required.

Run `docker compose build minio mc` from a catalog fixture directory to check the
images independently. The normal test commands still start the real fixtures and
fail if image construction or service startup fails.
