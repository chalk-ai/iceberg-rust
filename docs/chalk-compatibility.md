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
`167b99033113232e7be9222643e4011f2746d0a6` (including the strict MERGE transaction patch)
and fork PRs #12 and #14.

| Contract | Crate implementation and regression |
| --- | --- |
| Dropped equality-delete keys and historical pruning | `scan/{task,context,mod}.rs`; retained-schema field IDs remain available internally, independently of visible projection. |
| Overwrite and snapshot totals | `transaction/{overwrite,snapshot}.rs`, `spec/snapshot_summary.rs`; preserve per-manifest schemas/specs, count actual removals, retain unknown totals and check arithmetic. |
| Transaction read state | `transaction/mod.rs`; refuse replacement UUIDs and preserve `RebasePolicy::Forbid` through refresh, validation-only commits, transient retries and commit construction without refresh. Default `Allow` transactions retain their rebase behavior. |
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

`bash .buildkite/ci.sh chalk-consumer` runs the core/Glue/REST library tests
with Chalk's storage features and the committed Cargo lockfile. Cargo failures
fail the Buildkite job. These tests do not replace Chalk's adapter, Velox, SQL
or persistent table suites, and do not qualify format-v3 tables or deletion vectors. File-catalog
publication, SQL planning and duplicate-match MERGE semantics are outside this crate
patch set.

A separate existing limitation remains: apache-avro 0.20 and 0.21 both misread
fixed-width UUID partition values in manifests as length-prefixed bytes. Restoring
the writer format does not repair that reader bug or qualify UUID-partitioned
tables. Nested dropped equality-delete keys also remain unsupported.

`RebasePolicy::Forbid` compares the original metadata and location on each catalog
refresh. Publication requirements protect UUID, main snapshot, schema/spec/sort
IDs and assigned-ID counters; they cannot assert arbitrary property-only or exact
metadata-path changes after the refresh. `into_table_commit_no_refresh` carries
those requirements but performs no catalog refresh of its own.

Chalk must pass MERGE's original validated table and select `Forbid`; porting the
crate API alone does not update the adapter or qualify SQL MERGE on the upgraded
build. The Chalk dependency pin and integration tests must use the intended fork
revision.
