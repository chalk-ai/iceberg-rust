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

# Chalk consumer test gate

`bash .buildkite/ci.sh chalk-consumer` runs the core, Glue and REST library tests
with default features and `iceberg/storage-gcs`, matching Chalk's storage profile.
The separate workspace test lane retains its broader all-features coverage.

`required-tests.json` preserves the named cases required by Chalk's vendored-crate
CI. Required cases must run and pass: filtering, removal, `#[ignore]`, failure and
missing results fail the gate. Change the inventory only when intentionally
renaming or replacing coverage; do not regenerate it from a successful test run.

Each invocation uses a fresh directory under `target/chalk-consumer/`. Buildkite
uploads package logs, exact commands and exit statuses, plus `evidence.json` with
the commit, Cargo lock hash, inventory hash and named outcomes. A test process
failure still fails the gate even if every required assertion passed before it.

The lane is a hard failure in this pipeline. Requiring the pipeline's GitHub
status for merges is a separate repository protection setting. Fork results must
match the revision Chalk consumes; Chalk's adapter, Velox, SQL and persistent
compatibility tests remain in the monorepo.
