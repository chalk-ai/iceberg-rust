// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

// Source: https://github.com/apache/iceberg-rust/pull/2185 (open, not yet merged).

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use async_trait::async_trait;
use uuid::Uuid;

use crate::error::Result;
use crate::spec::{
    DataFile, FormatVersion, ManifestContentType, ManifestEntry, ManifestFile,
    ManifestWriterBuilder, Operation,
};
use crate::table::Table;
use crate::transaction::snapshot::{
    DefaultManifestProcess, SnapshotProduceOperation, SnapshotProducer,
};
use crate::transaction::{ActionCommit, TransactionAction};

/// OverwriteAction is a transaction action for overwriting data files in the table.
///
/// Creates a snapshot with `Operation::Overwrite` semantics — adds new data files and
/// optionally removes existing data files by rewriting affected manifests with those
/// entries marked as `ManifestStatus::Deleted`.
pub struct OverwriteAction {
    check_duplicate: bool,
    commit_uuid: Option<Uuid>,
    key_metadata: Option<Vec<u8>>,
    snapshot_properties: HashMap<String, String>,
    added_data_files: Vec<DataFile>,
    deleted_data_files: Vec<DataFile>,
}

impl OverwriteAction {
    pub(crate) fn new() -> Self {
        Self {
            check_duplicate: true,
            commit_uuid: None,
            key_metadata: None,
            snapshot_properties: HashMap::default(),
            added_data_files: vec![],
            deleted_data_files: vec![],
        }
    }

    /// Set whether to check duplicate files.
    pub fn with_check_duplicate(mut self, v: bool) -> Self {
        self.check_duplicate = v;
        self
    }

    /// Add data files to the snapshot.
    pub fn add_data_files(mut self, data_files: impl IntoIterator<Item = DataFile>) -> Self {
        self.added_data_files.extend(data_files);
        self
    }

    /// Specify data files to be removed from the table in this overwrite.
    pub fn delete_data_files(mut self, data_files: impl IntoIterator<Item = DataFile>) -> Self {
        self.deleted_data_files.extend(data_files);
        self
    }

    /// Set commit UUID for the snapshot.
    pub fn set_commit_uuid(mut self, commit_uuid: Uuid) -> Self {
        self.commit_uuid = Some(commit_uuid);
        self
    }

    /// Set key metadata for manifest files.
    pub fn set_key_metadata(mut self, key_metadata: Vec<u8>) -> Self {
        self.key_metadata = Some(key_metadata);
        self
    }

    /// Set snapshot summary properties.
    pub fn set_snapshot_properties(mut self, snapshot_properties: HashMap<String, String>) -> Self {
        self.snapshot_properties = snapshot_properties;
        self
    }
}

#[async_trait]
impl TransactionAction for OverwriteAction {
    async fn commit(self: Arc<Self>, table: &Table) -> Result<ActionCommit> {
        let snapshot_producer = SnapshotProducer::new(
            table,
            self.commit_uuid.unwrap_or_else(Uuid::now_v7),
            self.key_metadata.clone(),
            self.snapshot_properties.clone(),
            self.added_data_files.clone(),
        );

        snapshot_producer.validate_added_data_files()?;

        if self.check_duplicate {
            snapshot_producer.validate_duplicate_files().await?;
        }

        let deleted_file_paths: HashSet<String> = self
            .deleted_data_files
            .iter()
            .map(|f| f.file_path.clone())
            .collect();

        let snapshot_id = snapshot_producer.snapshot_id();
        let removed_data_files = self.deleted_data_files.clone();
        snapshot_producer
            .commit(
                OverwriteOperation {
                    deleted_file_paths,
                    snapshot_id,
                    removed_data_files,
                },
                DefaultManifestProcess,
            )
            .await
    }
}

struct OverwriteOperation {
    deleted_file_paths: HashSet<String>,
    snapshot_id: i64,
    removed_data_files: Vec<DataFile>,
}

impl SnapshotProduceOperation for OverwriteOperation {
    fn operation(&self) -> Operation {
        Operation::Overwrite
    }

    fn removed_data_files(&self) -> &[DataFile] {
        &self.removed_data_files
    }

    async fn delete_entries(
        &self,
        _snapshot_produce: &SnapshotProducer<'_>,
    ) -> Result<Vec<ManifestEntry>> {
        Ok(vec![])
    }

    async fn existing_manifest(
        &self,
        snapshot_produce: &mut SnapshotProducer<'_>,
    ) -> Result<Vec<ManifestFile>> {
        let Some(snapshot) = snapshot_produce.table.metadata().current_snapshot() else {
            return Ok(vec![]);
        };

        let manifest_list = snapshot
            .load_manifest_list(
                snapshot_produce.table.file_io(),
                &snapshot_produce.table.metadata_ref(),
            )
            .await?;

        if self.deleted_file_paths.is_empty() {
            return Ok(manifest_list
                .entries()
                .iter()
                .filter(|entry| entry.has_added_files() || entry.has_existing_files())
                .cloned()
                .collect());
        }

        let mut result = Vec::new();

        for manifest_file in manifest_list.entries() {
            if !manifest_file.has_added_files() && !manifest_file.has_existing_files() {
                continue;
            }

            let manifest = manifest_file
                .load_manifest(snapshot_produce.table.file_io())
                .await?;

            let has_deletes = manifest.entries().iter().any(|entry| {
                entry.is_alive() && self.deleted_file_paths.contains(entry.file_path())
            });

            if has_deletes {
                let rewritten = self
                    .rewrite_manifest(snapshot_produce, manifest_file, &manifest)
                    .await?;
                result.push(rewritten);
            } else {
                result.push(manifest_file.clone());
            }
        }

        Ok(result)
    }
}

impl OverwriteOperation {
    /// Rewrite a manifest, marking entries whose file paths are in `deleted_file_paths`
    /// as `ManifestStatus::Deleted`.
    async fn rewrite_manifest(
        &self,
        snapshot_produce: &SnapshotProducer<'_>,
        manifest_file: &ManifestFile,
        manifest: &crate::spec::Manifest,
    ) -> Result<ManifestFile> {
        let table = snapshot_produce.table;

        let new_manifest_path = format!(
            "{}/metadata/{}-m-overwrite.avro",
            table.metadata().location(),
            Uuid::now_v7(),
        );
        let output_file = table.file_io().new_output(&new_manifest_path)?;
        // These entries retain their original partition tuples and field IDs.
        // Only their status changes; the manifest's schema and spec must not.
        let builder = ManifestWriterBuilder::new(
            output_file,
            Some(self.snapshot_id),
            manifest_file.key_metadata.clone(),
            manifest.metadata().schema().clone(),
            manifest.metadata().partition_spec().clone(),
        );

        let mut writer = match table.metadata().format_version() {
            FormatVersion::V1 => builder.build_v1(),
            FormatVersion::V2 => match manifest_file.content {
                ManifestContentType::Data => builder.build_v2_data(),
                ManifestContentType::Deletes => builder.build_v2_deletes(),
            },
            FormatVersion::V3 => match manifest_file.content {
                ManifestContentType::Data => builder.build_v3_data(),
                ManifestContentType::Deletes => builder.build_v3_deletes(),
            },
        };

        // Deleted entries belong to prior snapshots and must not become Existing.
        for entry in manifest.entries().iter().filter(|entry| entry.is_alive()) {
            if self.deleted_file_paths.contains(entry.file_path()) {
                let mut deleted: ManifestEntry = (**entry).clone();
                deleted.snapshot_id = Some(self.snapshot_id);
                writer.add_delete_entry(deleted)?;
            } else {
                let cloned: ManifestEntry = (**entry).clone();
                writer.add_existing_entry(cloned)?;
            }
        }

        writer.write_manifest_file().await
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use crate::spec::{
        DataContentType, DataFileBuilder, DataFileFormat, Literal, MAIN_BRANCH, Operation, Struct,
    };
    use crate::transaction::tests::make_v2_minimal_table;
    use crate::transaction::{Transaction, TransactionAction};
    use crate::{TableRequirement, TableUpdate};

    fn test_data_file(path: &str, partition_spec_id: i32) -> crate::spec::DataFile {
        DataFileBuilder::default()
            .content(DataContentType::Data)
            .file_path(path.to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(100)
            .record_count(1)
            .partition_spec_id(partition_spec_id)
            .partition(Struct::from_iter([Some(Literal::long(300))]))
            .build()
            .unwrap()
    }

    #[tokio::test]
    async fn test_empty_data_overwrite_action() {
        let table = make_v2_minimal_table();
        let tx = Transaction::new(&table);
        let action = tx.overwrite().add_data_files(vec![]);
        assert!(Arc::new(action).commit(&table).await.is_err());
    }

    #[tokio::test]
    async fn test_overwrite_snapshot_properties() {
        let table = make_v2_minimal_table();
        let tx = Transaction::new(&table);

        let mut snapshot_properties = HashMap::new();
        snapshot_properties.insert("key".to_string(), "val".to_string());

        let data_file = test_data_file(
            "test/1.parquet",
            table.metadata().default_partition_spec_id(),
        );

        let action = tx
            .overwrite()
            .set_snapshot_properties(snapshot_properties)
            .add_data_files(vec![data_file]);
        let mut action_commit = Arc::new(action).commit(&table).await.unwrap();
        let updates = action_commit.take_updates();

        let new_snapshot = if let TableUpdate::AddSnapshot { snapshot } = &updates[0] {
            snapshot
        } else {
            unreachable!()
        };
        assert_eq!(
            new_snapshot
                .summary()
                .additional_properties
                .get("key")
                .unwrap(),
            "val"
        );
    }

    #[tokio::test]
    async fn test_overwrite_incompatible_partition_value() {
        let table = make_v2_minimal_table();
        let tx = Transaction::new(&table);

        let data_file = DataFileBuilder::default()
            .content(DataContentType::Data)
            .file_path("test/3.parquet".to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(100)
            .record_count(1)
            .partition_spec_id(table.metadata().default_partition_spec_id())
            .partition(Struct::from_iter([Some(Literal::string("test"))]))
            .build()
            .unwrap();

        let action = tx.overwrite().add_data_files(vec![data_file]);
        assert!(Arc::new(action).commit(&table).await.is_err());
    }

    #[tokio::test]
    async fn test_overwrite_basic() {
        let table = make_v2_minimal_table();
        let tx = Transaction::new(&table);

        let data_file = test_data_file(
            "test/3.parquet",
            table.metadata().default_partition_spec_id(),
        );

        let action = tx.overwrite().add_data_files(vec![data_file.clone()]);
        let mut action_commit = Arc::new(action).commit(&table).await.unwrap();
        let updates = action_commit.take_updates();
        let requirements = action_commit.take_requirements();

        assert!(
            matches!((&updates[0],&updates[1]), (TableUpdate::AddSnapshot { snapshot },TableUpdate::SetSnapshotRef { reference,ref_name }) if snapshot.snapshot_id() == reference.snapshot_id && ref_name == MAIN_BRANCH)
        );

        assert_eq!(
            vec![
                TableRequirement::UuidMatch {
                    uuid: table.metadata().uuid()
                },
                TableRequirement::RefSnapshotIdMatch {
                    r#ref: MAIN_BRANCH.to_string(),
                    snapshot_id: table.metadata().current_snapshot_id
                }
            ],
            requirements
        );

        let new_snapshot = if let TableUpdate::AddSnapshot { snapshot } = &updates[0] {
            snapshot
        } else {
            unreachable!()
        };
        assert_eq!(new_snapshot.summary().operation, Operation::Overwrite);

        let manifest_list = new_snapshot
            .load_manifest_list(table.file_io(), table.metadata())
            .await
            .unwrap();
        assert_eq!(1, manifest_list.entries().len());
        assert_eq!(
            manifest_list.entries()[0].sequence_number,
            new_snapshot.sequence_number()
        );

        let manifest = manifest_list.entries()[0]
            .load_manifest(table.file_io())
            .await
            .unwrap();
        assert_eq!(1, manifest.entries().len());
        assert_eq!(
            new_snapshot.sequence_number(),
            manifest.entries()[0]
                .sequence_number()
                .expect("Inherit sequence number by load manifest")
        );
        assert_eq!(
            new_snapshot.snapshot_id(),
            manifest.entries()[0].snapshot_id().unwrap()
        );
        assert_eq!(data_file, *manifest.entries()[0].data_file());
    }

    async fn live_snapshot_files(
        table: &crate::table::Table,
        snapshot_id: i64,
    ) -> crate::Result<Vec<crate::spec::DataFile>> {
        let snapshot = table.metadata().snapshot_by_id(snapshot_id).unwrap();
        let manifests = snapshot
            .load_manifest_list(table.file_io(), table.metadata())
            .await?;
        let mut files = Vec::new();
        for manifest_file in manifests.entries() {
            let manifest = manifest_file.load_manifest(table.file_io()).await?;
            assert_eq!(
                manifest.metadata().partition_spec().spec_id(),
                manifest_file.partition_spec_id
            );
            for entry in manifest.entries().iter().filter(|entry| entry.is_alive()) {
                assert_eq!(
                    entry.data_file().partition_spec_id(),
                    manifest_file.partition_spec_id
                );
                files.push(entry.data_file().clone());
            }
        }
        files.sort_by(|left, right| left.file_path().cmp(right.file_path()));
        Ok(files)
    }

    #[tokio::test]
    async fn test_overwrite_rejects_invalid_removed_partition() {
        let table = make_v2_minimal_table();
        for unknown_spec in [false, true] {
            let file = DataFileBuilder::default()
                .content(DataContentType::Data)
                .file_path("test/removed.parquet".to_string())
                .file_format(DataFileFormat::Parquet)
                .file_size_in_bytes(100)
                .record_count(1)
                .partition_spec_id(
                    table.metadata().default_partition_spec_id() + i32::from(unknown_spec),
                )
                .partition(Struct::from_iter([Some(Literal::string("invalid"))]))
                .build()
                .unwrap();
            let tx = Transaction::new(&table);
            let result = Arc::new(tx.overwrite().delete_data_files(vec![file]))
                .commit(&table)
                .await;
            let error = result.err().unwrap();
            let source = std::error::Error::source(&error)
                .unwrap()
                .downcast_ref::<crate::Error>()
                .unwrap();
            assert_eq!(source.kind(), crate::ErrorKind::DataInvalid);
            assert!(source.message().contains(if unknown_spec {
                "unknown partition spec"
            } else {
                "incompatible with retained table schemas"
            }));
        }
    }

    #[tokio::test]
    async fn test_overwrite_does_not_restore_previously_deleted_files()
    -> std::result::Result<(), Box<dyn std::error::Error + Send + Sync>> {
        use crate::memory::{MEMORY_CATALOG_WAREHOUSE, MemoryCatalogBuilder};
        use crate::spec::{NestedField, PrimitiveType, Schema, Type};
        use crate::transaction::ApplyTransactionAction;
        use crate::{Catalog, CatalogBuilder, NamespaceIdent, TableCreation};

        let directory = tempfile::tempdir()?;
        let warehouse = format!("file://{}", directory.path().display());
        let catalog = MemoryCatalogBuilder::default()
            .load(
                "test",
                HashMap::from([(MEMORY_CATALOG_WAREHOUSE.to_string(), warehouse.clone())]),
            )
            .await?;
        let namespace = NamespaceIdent::new("test".to_string());
        catalog.create_namespace(&namespace, HashMap::new()).await?;
        let schema = Schema::builder()
            .with_fields([
                NestedField::optional(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
            ])
            .build()?;
        let mut table = catalog
            .create_table(
                &namespace,
                TableCreation::builder()
                    .name("rows".to_string())
                    .schema(schema)
                    .build(),
            )
            .await?;
        let make_file = |name: &str| {
            DataFileBuilder::default()
                .content(DataContentType::Data)
                .file_path(format!("{warehouse}/{name}.parquet"))
                .file_format(DataFileFormat::Parquet)
                .file_size_in_bytes(3_000_000_000)
                .record_count(1)
                .partition_spec_id(0)
                .partition(Struct::from_iter([]))
                .build()
        };
        let first = make_file("a")?;
        let second = make_file("b")?;
        let third = make_file("c")?;
        let original_files = vec![first.clone(), second.clone()];
        // Both files share a manifest so deleting one leaves a mixed-status manifest.
        let tx = Transaction::new(&table);
        table = tx
            .fast_append()
            .add_data_files(original_files.clone())
            .apply(tx)?
            .commit(&catalog)
            .await?;
        let original_snapshot = table.metadata().current_snapshot_id().unwrap();
        let assert_totals = |table: &crate::table::Table, expected_files: u64| {
            let properties = &table
                .metadata()
                .current_snapshot()
                .unwrap()
                .summary()
                .additional_properties;
            assert_eq!(properties["total-data-files"], expected_files.to_string());
            assert_eq!(properties["total-records"], expected_files.to_string());
            assert_eq!(
                properties["total-files-size"],
                (expected_files * 3_000_000_000).to_string()
            );
        };
        assert_totals(&table, 2);

        let tx = Transaction::new(&table);
        table = tx
            .overwrite()
            .delete_data_files([first])
            .add_data_files([third.clone()])
            .apply(tx)?
            .commit(&catalog)
            .await?;
        let partial_snapshot = table.metadata().current_snapshot_id().unwrap();
        assert_totals(&table, 2);
        assert_eq!(
            table
                .metadata()
                .current_snapshot()
                .unwrap()
                .summary()
                .additional_properties["deleted-data-files"],
            "1"
        );
        let partial_files = vec![second, third];
        assert_eq!(
            live_snapshot_files(&table, partial_snapshot).await?,
            partial_files
        );

        let tx = Transaction::new(&table);
        table = tx
            .overwrite()
            .delete_data_files(partial_files.clone())
            .apply(tx)?
            .commit(&catalog)
            .await?;
        let overwrite_snapshot = table.metadata().current_snapshot_id().unwrap();
        assert_totals(&table, 0);
        assert!(
            live_snapshot_files(&table, overwrite_snapshot)
                .await?
                .is_empty()
        );
        assert_eq!(
            live_snapshot_files(&table, partial_snapshot).await?,
            partial_files
        );
        assert_eq!(
            live_snapshot_files(&table, original_snapshot).await?,
            original_files
        );
        let replacement = make_file("d")?;
        let tx = Transaction::new(&table);
        table = tx
            .overwrite()
            .add_data_files([replacement.clone()])
            .apply(tx)?
            .commit(&catalog)
            .await?;
        assert_totals(&table, 1);
        assert_eq!(
            live_snapshot_files(&table, table.metadata().current_snapshot_id().unwrap()).await?,
            vec![replacement]
        );
        assert!(
            live_snapshot_files(&table, overwrite_snapshot)
                .await?
                .is_empty()
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_overwrite_preserves_mixed_partition_layouts()
    -> std::result::Result<(), Box<dyn std::error::Error + Send + Sync>> {
        use crate::memory::{MEMORY_CATALOG_WAREHOUSE, MemoryCatalogBuilder};
        use crate::spec::{NestedField, PartitionSpec, PrimitiveType, Schema, Transform, Type};
        use crate::transaction::ApplyTransactionAction;
        use crate::{Catalog, CatalogBuilder, NamespaceIdent, TableCommit, TableCreation};

        for extra_partition_field in [false, true] {
            for empty in [false, true] {
                let directory = tempfile::tempdir()?;
                let warehouse = format!("file://{}", directory.path().display());
                let catalog = MemoryCatalogBuilder::default()
                    .load(
                        "test",
                        HashMap::from([(MEMORY_CATALOG_WAREHOUSE.to_string(), warehouse.clone())]),
                    )
                    .await?;
                let namespace = NamespaceIdent::new("test".to_string());
                catalog.create_namespace(&namespace, HashMap::new()).await?;
                let schema = Schema::builder()
                    .with_fields([
                        NestedField::optional(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
                        NestedField::optional(2, "region", Type::Primitive(PrimitiveType::String))
                            .into(),
                    ])
                    .build()?;
                let old_spec = PartitionSpec::builder(schema.clone())
                    .add_partition_field("id", "id", Transform::Identity)?
                    .build()?;
                let mut table = catalog
                    .create_table(
                        &namespace,
                        TableCreation::builder()
                            .name("rows".to_string())
                            .schema(schema)
                            .partition_spec(old_spec.into_unbound())
                            .build(),
                    )
                    .await?;
                let uuid = table.metadata().uuid();
                let make_file = |name: &str, spec_id, partition| {
                    DataFileBuilder::default()
                        .content(DataContentType::Data)
                        .file_path(format!("{warehouse}/{name}.parquet"))
                        .file_format(DataFileFormat::Parquet)
                        .file_size_in_bytes(100)
                        .record_count(1)
                        .partition_spec_id(spec_id)
                        .partition(partition)
                        .build()
                };
                let old_files = vec![
                    make_file("old-a", 0, Struct::from_iter([Some(Literal::int(10))]))?,
                    make_file("old-b", 0, Struct::from_iter([Some(Literal::int(11))]))?,
                ];
                let tx = Transaction::new(&table);
                table = tx
                    .fast_append()
                    .add_data_files(old_files.clone())
                    .apply(tx)?
                    .commit(&catalog)
                    .await?;
                let old_snapshot = table.metadata().current_snapshot_id().unwrap();

                // The old identity tuple remains int even after promoting its source
                // column; the new spec starts with a string and can also be wider.
                let schema = Schema::builder()
                    .with_schema_id(1)
                    .with_fields([
                        NestedField::optional(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
                        NestedField::optional(2, "region", Type::Primitive(PrimitiveType::String))
                            .into(),
                    ])
                    .build()?;
                let mut spec = PartitionSpec::builder(schema.clone())
                    .with_spec_id(1)
                    .with_last_assigned_field_id(table.metadata().last_partition_id())
                    .add_partition_field("region", "region", Transform::Identity)?;
                let mut partition = vec![Some(Literal::string("east"))];
                if extra_partition_field {
                    spec = spec.add_partition_field("id", "id", Transform::Identity)?;
                    partition.push(Some(Literal::long(20)));
                }
                table = catalog
                    .update_table(
                        TableCommit::builder()
                            .ident(table.identifier().clone())
                            .requirements(vec![TableRequirement::UuidMatch { uuid }])
                            .updates(vec![
                                TableUpdate::AddSchema { schema },
                                TableUpdate::SetCurrentSchema { schema_id: -1 },
                                TableUpdate::AddSpec {
                                    spec: spec.build()?.into_unbound(),
                                },
                                TableUpdate::SetDefaultSpec { spec_id: -1 },
                            ])
                            .build(),
                    )
                    .await?;
                let partition = Struct::from_iter(partition);
                let new_file = make_file(
                    "new",
                    table.metadata().default_partition_spec_id(),
                    partition.clone(),
                )?;
                let tx = Transaction::new(&table);
                table = tx
                    .fast_append()
                    .add_data_files(vec![new_file.clone()])
                    .apply(tx)?
                    .commit(&catalog)
                    .await?;
                let mixed_snapshot = table.metadata().current_snapshot_id().unwrap();
                let mixed_files = live_snapshot_files(&table, mixed_snapshot).await?;
                assert_eq!(mixed_files.len(), 3);
                assert_eq!(live_snapshot_files(&table, old_snapshot).await?, old_files);
                let specs: HashMap<_, _> = table
                    .metadata()
                    .partition_specs_iter()
                    .map(|spec| (spec.spec_id(), spec.clone()))
                    .collect();

                let replacement = make_file(
                    "replacement",
                    table.metadata().default_partition_spec_id(),
                    partition.clone(),
                )?;
                let added_files = if empty {
                    vec![]
                } else {
                    vec![replacement.clone()]
                };
                let tx = Transaction::new(&table);
                table = tx
                    .overwrite()
                    .delete_data_files(mixed_files.clone())
                    .add_data_files(added_files)
                    .apply(tx)?
                    .commit(&catalog)
                    .await?;
                let overwrite_snapshot = table.metadata().current_snapshot_id().unwrap();
                let expected_files = if empty {
                    vec![]
                } else {
                    vec![replacement.clone()]
                };
                assert_eq!(
                    live_snapshot_files(&table, overwrite_snapshot).await?,
                    expected_files
                );
                assert_eq!(live_snapshot_files(&table, old_snapshot).await?, old_files);
                assert_eq!(
                    live_snapshot_files(&table, mixed_snapshot).await?,
                    mixed_files
                );
                assert_eq!(table.metadata().uuid(), uuid);
                assert_eq!(
                    table
                        .metadata()
                        .partition_specs_iter()
                        .map(|spec| (spec.spec_id(), spec.clone()))
                        .collect::<HashMap<_, _>>(),
                    specs
                );
                assert_eq!(table.metadata().current_schema_id(), 1);
                assert_eq!(table.metadata().default_partition_spec_id(), 1);
                let summary = &table
                    .metadata()
                    .current_snapshot()
                    .unwrap()
                    .summary()
                    .additional_properties;
                assert_eq!(summary["deleted-records"], mixed_files.len().to_string());
                assert_eq!(summary["total-records"], expected_files.len().to_string());

                let appended = make_file("appended", 1, partition)?;
                let tx = Transaction::new(&table);
                table = tx
                    .fast_append()
                    .add_data_files(vec![appended.clone()])
                    .apply(tx)?
                    .commit(&catalog)
                    .await?;
                let mut expected_appended = expected_files.clone();
                expected_appended.push(appended);
                expected_appended.sort_by(|left, right| left.file_path().cmp(right.file_path()));
                assert_eq!(
                    live_snapshot_files(&table, table.metadata().current_snapshot_id().unwrap())
                        .await?,
                    expected_appended
                );
                assert_eq!(
                    live_snapshot_files(&table, overwrite_snapshot).await?,
                    expected_files
                );
                assert_eq!(
                    live_snapshot_files(&table, mixed_snapshot).await?,
                    mixed_files
                );
            }
        }
        Ok(())
    }

    // This upstream test depends on `crate::memory::tests::new_memory_catalog` and
    // `crate::transaction::tests::make_v3_minimal_table_in_catalog`, neither of which
    // exists in this fork, so it has never compiled here. Disabled until a fork sync
    // brings in those helpers.
    #[cfg(any())]
    #[tokio::test]
    async fn test_overwrite_with_deleted_files() {
        use crate::memory::tests::new_memory_catalog;
        use crate::transaction::ApplyTransactionAction;
        use crate::transaction::tests::make_v3_minimal_table_in_catalog;

        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;
        let spec_id = table.metadata().default_partition_spec_id();

        let original_file = test_data_file("test/original.parquet", spec_id);
        let tx = Transaction::new(&table);
        let action = tx.fast_append().add_data_files(vec![original_file.clone()]);
        let tx = action.apply(tx).unwrap();
        let table = tx.commit(&catalog).await.unwrap();

        let snapshot = table.metadata().current_snapshot().unwrap();
        let manifest_list = snapshot
            .load_manifest_list(table.file_io(), table.metadata())
            .await
            .unwrap();
        assert_eq!(1, manifest_list.entries().len());

        let replacement_file = test_data_file("test/replacement.parquet", spec_id);
        let tx = Transaction::new(&table);
        let action = tx
            .overwrite()
            .add_data_files(vec![replacement_file.clone()])
            .delete_data_files(vec![original_file.clone()]);
        let tx = action.apply(tx).unwrap();
        let table = tx.commit(&catalog).await.unwrap();

        let snapshot = table.metadata().current_snapshot().unwrap();
        assert_eq!(snapshot.summary().operation, Operation::Overwrite);

        let manifest_list = snapshot
            .load_manifest_list(table.file_io(), table.metadata())
            .await
            .unwrap();

        assert_eq!(2, manifest_list.entries().len());

        let mut all_entries = vec![];
        for manifest_file in manifest_list.entries() {
            let manifest = manifest_file.load_manifest(table.file_io()).await.unwrap();
            for entry in manifest.entries() {
                all_entries.push((entry.status(), entry.file_path().to_string()));
            }
        }

        assert!(
            all_entries
                .iter()
                .any(|(status, path)| *status == ManifestStatus::Deleted
                    && path == "test/original.parquet"),
            "Original file should be marked as Deleted, entries: {all_entries:?}",
        );

        assert!(
            all_entries
                .iter()
                .any(|(status, path)| *status == ManifestStatus::Added
                    && path == "test/replacement.parquet"),
            "Replacement file should be marked as Added, entries: {all_entries:?}",
        );

        // Verify snapshot summary reports the deleted file.
        assert_eq!(
            snapshot
                .summary()
                .additional_properties
                .get("deleted-data-files")
                .map(|s| s.as_str()),
            Some("1")
        );
        assert_eq!(
            snapshot
                .summary()
                .additional_properties
                .get("deleted-records")
                .map(|s| s.as_str()),
            Some("1")
        );

        // Step 3: Fast append after overwrite — delete-only manifest must survive.
        let appended_file = test_data_file("test/appended.parquet", spec_id);
        let tx = Transaction::new(&table);
        let action = tx.fast_append().add_data_files(vec![appended_file.clone()]);
        let tx = action.apply(tx).unwrap();
        let table = tx.commit(&catalog).await.unwrap();

        let snapshot = table.metadata().current_snapshot().unwrap();
        let manifest_list = snapshot
            .load_manifest_list(table.file_io(), table.metadata())
            .await
            .unwrap();

        // 3 manifests: rewritten (deleted entry), overwrite added, fast_append added.
        assert_eq!(3, manifest_list.entries().len());

        let mut all_entries = vec![];
        for manifest_file in manifest_list.entries() {
            let manifest = manifest_file.load_manifest(table.file_io()).await.unwrap();
            for entry in manifest.entries() {
                all_entries.push((entry.status(), entry.file_path().to_string()));
            }
        }

        // The deleted entry must still be present after fast_append.
        assert!(
            all_entries
                .iter()
                .any(|(status, path)| *status == ManifestStatus::Deleted
                    && path == "test/original.parquet"),
            "Deleted entry should survive fast_append, entries: {all_entries:?}",
        );
    }
}
