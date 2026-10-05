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

# Chalk fork upgrade compatibility

This branch combines the 0.8 fork's Glue/schema fixes with the native patches
from Chalk's Iceberg coverage stack. The comparison uses Chalk commit
`af07255dc554ca4b581098b026ca9e57d04f8a49` and fork PRs #12 and #13.

| Contract | Crate implementation and regression |
| --- | --- |
| Dropped equality-delete keys and historical pruning | `scan/{task,context,mod}.rs`; retained-schema field IDs remain available internally, independently of visible projection. |
| Overwrite and snapshot totals | `transaction/{overwrite,snapshot}.rs`, `spec/snapshot_summary.rs`; preserve per-manifest schemas/specs, count actual removals, retain unknown totals and check arithmetic. |
| Table replacement during retry | `transaction/mod.rs`; refuse to replay writes onto a different table UUID. |
| External Parquet without field IDs | `arrow/reader.rs`; recursive name mapping supplies field IDs consistently to projection, filters and equality deletes. |
| Catalog errors | Glue distinguishes missing tables, absent metadata, failed reads and retryable update errors. REST create conflicts retain `TableAlreadyExists`. HTTP fixtures exercise the catalog methods. |
| Type compatibility | Duration schemas map to long; temporal values, large string/binary constants and Glue timestamp variants retain their regression coverage. Duration schema conversion does not rescale values. |
| UUID manifest writes | `avro/schema.rs`; UUIDs retain Iceberg’s fixed 16-byte physical encoding and UUID logical annotation. |
| Existing transaction behavior | Retry jitter and deletion-only manifest retention remain present. |
| Partition constants | Table columns retain their declared Arrow types. Encoded virtual fields are materialized at the DataFusion output boundary. |

Older custom APIs for directory listing, partition-spec replacement, cached
pruning, delete metadata and retry telemetry remain available. Chalk's active
fanout writer uses the retained bounded `close_collecting_durations` method;
the upstream trait's separate close implementation is unchanged.

`.buildkite/required-tests.json` names the required native regressions.
`bash .buildkite/ci.sh chalk-consumer` executes them and records the exact commit
and lockfile. These tests do not replace Chalk's adapter, Velox, SQL or persistent
table suites, and do not qualify format-v3 tables or deletion vectors. File-catalog
publication and shared SQL MERGE behavior are outside this crate patch set.

A separate existing limitation remains: apache-avro 0.20 and 0.21 both misread
fixed-width UUID partition values in manifests as length-prefixed bytes. Restoring
the writer format does not repair that reader bug or qualify UUID-partitioned
tables. Nested dropped equality-delete keys also remain unsupported.
