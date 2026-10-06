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

//! This module contains transaction api.
//!
//! The transaction API enables changes to be made to an existing table.
//!
//! Note that this may also have side effects, such as producing new manifest
//! files.
//!
//! Below is a basic example using the "fast-append" action:
//!
//! ```ignore
//! use iceberg::transaction::{ApplyTransactionAction, Transaction};
//! use iceberg::Catalog;
//!
//! // Create a transaction.
//! let tx = Transaction::new(my_table);
//!
//! // Create a `FastAppendAction` which will not rewrite or append
//! // to existing metadata. This will create a new manifest.
//! let action = tx.fast_append().add_data_files(my_data_files);
//!
//! // Apply the fast-append action to the given transaction, returning
//! // the newly updated `Transaction`.
//! let tx = action.apply(tx).unwrap();
//!
//!
//! // End the transaction by committing to an `iceberg::Catalog`
//! // implementation. This will cause a table update to occur.
//! let table = tx
//!     .commit(&some_catalog_impl)
//!     .await
//!     .unwrap();
//! ```

/// The `ApplyTransactionAction` trait provides an `apply` method
/// that allows users to apply a transaction action to a `Transaction`.
mod action;

pub use action::*;
mod append;
mod overwrite;
mod partition_spec;
mod row_delta;
mod snapshot;
mod sort_order;
mod update_location;
mod update_properties;
mod update_statistics;
mod upgrade_format_version;

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use backon::{BackoffBuilder, ExponentialBackoff, ExponentialBuilder, RetryableWithContext};

use crate::error::Result;
use crate::spec::{MAIN_BRANCH, TableProperties, UnboundPartitionSpec};
use crate::table::Table;
use crate::transaction::action::BoxedTransactionAction;
use crate::transaction::append::FastAppendAction;
use crate::transaction::overwrite::OverwriteAction;
use crate::transaction::partition_spec::ReplacePartitionSpecAction;
use crate::transaction::row_delta::RowDeltaAction;
use crate::transaction::sort_order::ReplaceSortOrderAction;
use crate::transaction::update_location::UpdateLocationAction;
use crate::transaction::update_properties::UpdatePropertiesAction;
use crate::transaction::update_statistics::UpdateStatisticsAction;
use crate::transaction::upgrade_format_version::UpgradeFormatVersionAction;
use crate::{Catalog, Error, ErrorKind, TableCommit, TableRequirement, TableUpdate};

/// Whether transaction actions may be reapplied to a newer table state.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum RebasePolicy {
    /// Refresh the table and retry eligible conflicts.
    #[default]
    Allow,
    /// Preserve the original read state and fail conflicts without retrying.
    ///
    /// Catalog requirements protect the UUID, main snapshot, schema, partition spec,
    /// sort order, and assigned IDs at publication. They do not assert an exact
    /// metadata location or arbitrary properties changed after a refresh.
    Forbid,
}

/// Table transaction.
#[derive(Clone)]
pub struct Transaction {
    table: Table,
    actions: Vec<BoxedTransactionAction>,
    rebase_policy: RebasePolicy,
}

impl Transaction {
    /// Creates a new transaction.
    pub fn new(table: &Table) -> Self {
        Self {
            table: table.clone(),
            actions: vec![],
            rebase_policy: RebasePolicy::Allow,
        }
    }

    /// Sets whether actions can be reapplied to a newer table state.
    pub fn with_rebase_policy(mut self, policy: RebasePolicy) -> Self {
        self.rebase_policy = policy;
        self
    }

    fn update_table_metadata(table: Table, updates: &[TableUpdate]) -> Result<Table> {
        let mut metadata_builder = table.metadata().clone().into_builder(None);
        for update in updates {
            metadata_builder = update.clone().apply(metadata_builder)?;
        }

        Ok(table.with_metadata(Arc::new(metadata_builder.build()?.metadata)))
    }

    /// Applies an [`ActionCommit`] to the given [`Table`], returning a new [`Table`] with updated metadata.
    /// Also appends any derived [`TableUpdate`]s and [`TableRequirement`]s to the provided vectors.
    fn apply(
        table: Table,
        mut action_commit: ActionCommit,
        existing_updates: &mut Vec<TableUpdate>,
        existing_requirements: &mut Vec<TableRequirement>,
    ) -> Result<Table> {
        let updates = action_commit.take_updates();
        let requirements = action_commit.take_requirements();

        for requirement in &requirements {
            requirement.check(Some(table.metadata()))?;
        }

        let updated_table = Self::update_table_metadata(table, &updates)?;

        existing_updates.extend(updates);
        existing_requirements.extend(requirements);

        Ok(updated_table)
    }

    /// Sets table to a new version.
    pub fn upgrade_table_version(&self) -> UpgradeFormatVersionAction {
        UpgradeFormatVersionAction::new()
    }

    /// Update table's property.
    pub fn update_table_properties(&self) -> UpdatePropertiesAction {
        UpdatePropertiesAction::new()
    }

    /// Creates a fast append action.
    pub fn fast_append(&self) -> FastAppendAction {
        FastAppendAction::new()
    }

    /// Creates a row delta action for row-level modifications.
    pub fn row_delta(&self) -> RowDeltaAction {
        RowDeltaAction::new()
    }

    /// Creates an overwrite action for replacing data files.
    pub fn overwrite(&self) -> OverwriteAction {
        OverwriteAction::new()
    }

    /// Creates replace sort order action.
    pub fn replace_sort_order(&self) -> ReplaceSortOrderAction {
        ReplaceSortOrderAction::new()
    }

    /// Creates replace partition spec action.
    pub fn replace_partition_spec(
        &self,
        partition_spec: UnboundPartitionSpec,
    ) -> ReplacePartitionSpecAction {
        ReplacePartitionSpecAction::new(partition_spec)
    }

    /// Set the location of table
    pub fn update_location(&self) -> UpdateLocationAction {
        UpdateLocationAction::new()
    }

    /// Update the statistics of table
    pub fn update_statistics(&self) -> UpdateStatisticsAction {
        UpdateStatisticsAction::new()
    }

    /// Commit transaction once, with no internal retry.
    pub async fn commit_once(self, catalog: &dyn Catalog) -> Result<Table> {
        if self.actions.is_empty() && self.rebase_policy == RebasePolicy::Allow {
            return Ok(self.table);
        }
        let mut tx = self;
        tx.do_commit(catalog).await
    }

    /// Commit transaction.
    pub async fn commit(self, catalog: &dyn Catalog) -> Result<Table> {
        if self.actions.is_empty() && self.rebase_policy == RebasePolicy::Allow {
            // nothing to commit
            return Ok(self.table);
        }

        let table_props =
            TableProperties::try_from(self.table.metadata().properties()).map_err(|e| {
                Error::new(ErrorKind::DataInvalid, "Invalid table properties").with_source(e)
            })?;

        let backoff = Self::build_backoff(table_props)?;
        let tx = self;

        let retry_count = Arc::new(AtomicUsize::new(0));
        let error_kinds: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));

        let result = (|mut tx: Transaction| async {
            let result = tx.do_commit(catalog).await;
            if let Err(ref e) = result
                && e.retryable()
            {
                retry_count.fetch_add(1, Ordering::SeqCst);
                if let Ok(mut kinds) = error_kinds.lock() {
                    kinds.push(format!("{:?}", e.kind()));
                }
            }
            (tx, result)
        })
        .retry(backoff)
        .sleep(tokio::time::sleep)
        .context(tx)
        .when(|e| e.retryable())
        .await
        .1;

        let attempts = retry_count.load(Ordering::SeqCst);
        let error_kinds_list = error_kinds.lock().unwrap().join(",");

        match result {
            Ok(table) => {
                let mut table_with_retries = table.with_retry_attempts(attempts);
                if attempts > 0 && !error_kinds_list.is_empty() {
                    table_with_retries =
                        table_with_retries.with_retry_error_kinds(error_kinds_list);
                }
                Ok(table_with_retries)
            }
            Err(e) => {
                let mut err = e.with_context("retry_attempts", attempts.to_string());
                if !error_kinds_list.is_empty() {
                    err = err.with_context("retry_error_kinds", error_kinds_list);
                }
                Err(err)
            }
        }
    }

    fn build_backoff(props: TableProperties) -> Result<ExponentialBackoff> {
        Ok(ExponentialBuilder::new()
            .with_jitter()
            .with_min_delay(Duration::from_millis(props.commit_min_retry_wait_ms))
            .with_max_delay(Duration::from_millis(props.commit_max_retry_wait_ms))
            .with_total_delay(Some(Duration::from_millis(
                props.commit_total_retry_timeout_ms,
            )))
            .with_max_times(props.commit_num_retries)
            .with_factor(2.0)
            .build())
    }

    async fn build_table_commit_from_current_base(&self) -> Result<TableCommit> {
        let mut current_table = self.table.clone();
        let mut existing_updates: Vec<TableUpdate> = vec![];
        // Actions may change schema or partition state locally. The catalog must
        // validate against the original read base, before any action is applied.
        let mut existing_requirements = if self.rebase_policy == RebasePolicy::Forbid {
            let metadata = self.table.metadata();
            vec![
                TableRequirement::UuidMatch {
                    uuid: metadata.uuid(),
                },
                TableRequirement::RefSnapshotIdMatch {
                    r#ref: MAIN_BRANCH.to_string(),
                    snapshot_id: metadata.current_snapshot_id(),
                },
                TableRequirement::CurrentSchemaIdMatch {
                    current_schema_id: metadata.current_schema_id(),
                },
                TableRequirement::LastAssignedFieldIdMatch {
                    last_assigned_field_id: metadata.last_column_id(),
                },
                TableRequirement::DefaultSpecIdMatch {
                    default_spec_id: metadata.default_partition_spec_id(),
                },
                TableRequirement::LastAssignedPartitionIdMatch {
                    last_assigned_partition_id: metadata.last_partition_id(),
                },
                TableRequirement::DefaultSortOrderIdMatch {
                    default_sort_order_id: metadata.default_sort_order_id(),
                },
            ]
        } else {
            vec![]
        };

        for action in &self.actions {
            let action_commit = Arc::clone(action).commit(&current_table).await?;
            // apply action commit to current_table
            current_table = Self::apply(
                current_table,
                action_commit,
                &mut existing_updates,
                &mut existing_requirements,
            )?;
        }

        Ok(TableCommit::builder()
            .ident(self.table.identifier().to_owned())
            .updates(existing_updates)
            .requirements(existing_requirements)
            .build())
    }

    async fn do_commit(&mut self, catalog: &dyn Catalog) -> Result<Table> {
        let refreshed = catalog.load_table(self.table.identifier()).await?;

        // Replaying actions against refreshed metadata is safe only for the same
        // table identity; a reused catalog name must not inherit pending writes.
        if refreshed.metadata().uuid() != self.table.metadata().uuid() {
            return Err(Error::new(
                ErrorKind::CatalogCommitConflicts,
                format!(
                    "Iceberg table UUID changed while committing {}",
                    self.table.identifier()
                ),
            )
            .with_retryable(false)
            .with_context("expected_uuid", self.table.metadata().uuid().to_string())
            .with_context("found_uuid", refreshed.metadata().uuid().to_string()));
        }

        if self.table.metadata() != refreshed.metadata()
            || self.table.metadata_location() != refreshed.metadata_location()
        {
            if self.rebase_policy == RebasePolicy::Forbid {
                return Err(Error::new(
                    ErrorKind::CatalogCommitConflicts,
                    format!(
                        "Iceberg table metadata changed while committing {}; recompute the transaction from the current table",
                        self.table.identifier()
                    ),
                )
                .with_retryable(false));
            }
            // Current base is stale, so re-apply the transaction actions against the refreshed
            // table before constructing the commit.
            self.table = refreshed.clone();
        }

        let table_commit = self.build_table_commit_from_current_base().await?;
        catalog.update_table(table_commit).await.map_err(|err| {
            if self.rebase_policy == RebasePolicy::Forbid
                && err.kind() == ErrorKind::CatalogCommitConflicts
            {
                err.with_retryable(false)
            } else {
                err
            }
        })
    }

    /// Build a [`TableCommit`] from the transaction's current base table without refreshing it
    /// from the catalog first.
    /// With [`RebasePolicy::Forbid`], the commit carries original-base requirements;
    /// the caller owns publication, conflict handling and any full-metadata comparison.
    pub async fn into_table_commit_no_refresh(self) -> Result<TableCommit> {
        self.build_table_commit_from_current_base().await
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::fs::File;
    use std::io::BufReader;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU32, Ordering};

    use crate::catalog::MockCatalog;
    use crate::io::FileIOBuilder;
    use crate::spec::{
        MAIN_BRANCH, NestedField, Operation, PrimitiveType, Schema, Snapshot, SnapshotReference,
        SnapshotRetention, SortOrder, Summary, TableMetadata, Transform, Type,
        UnboundPartitionSpec,
    };
    use crate::table::Table;
    use crate::transaction::{ApplyTransactionAction, RebasePolicy, Transaction};
    use crate::{Catalog, Error, ErrorKind, Result, TableCreation, TableIdent, TableUpdate};

    pub fn make_v1_table() -> Table {
        let file = File::open(format!(
            "{}/testdata/table_metadata/{}",
            env!("CARGO_MANIFEST_DIR"),
            "TableMetadataV1Valid.json"
        ))
        .unwrap();
        let reader = BufReader::new(file);
        let resp = serde_json::from_reader::<_, TableMetadata>(reader).unwrap();

        Table::builder()
            .metadata(resp)
            .metadata_location("s3://bucket/test/location/metadata/v1.json".to_string())
            .identifier(TableIdent::from_strs(["ns1", "test1"]).unwrap())
            .file_io(FileIOBuilder::new("memory").build().unwrap())
            .build()
            .unwrap()
    }

    pub fn make_v2_table() -> Table {
        let file = File::open(format!(
            "{}/testdata/table_metadata/{}",
            env!("CARGO_MANIFEST_DIR"),
            "TableMetadataV2Valid.json"
        ))
        .unwrap();
        let reader = BufReader::new(file);
        let resp = serde_json::from_reader::<_, TableMetadata>(reader).unwrap();

        Table::builder()
            .metadata(resp)
            .metadata_location("s3://bucket/test/location/metadata/v1.json".to_string())
            .identifier(TableIdent::from_strs(["ns1", "test1"]).unwrap())
            .file_io(FileIOBuilder::new("memory").build().unwrap())
            .build()
            .unwrap()
    }

    pub fn make_v2_minimal_table() -> Table {
        let file = File::open(format!(
            "{}/testdata/table_metadata/{}",
            env!("CARGO_MANIFEST_DIR"),
            "TableMetadataV2ValidMinimal.json"
        ))
        .unwrap();
        let reader = BufReader::new(file);
        let resp = serde_json::from_reader::<_, TableMetadata>(reader).unwrap();

        Table::builder()
            .metadata(resp)
            .metadata_location("s3://bucket/test/location/metadata/v1.json".to_string())
            .identifier(TableIdent::from_strs(["ns1", "test1"]).unwrap())
            .file_io(FileIOBuilder::new("memory").build().unwrap())
            .build()
            .unwrap()
    }

    pub(crate) async fn make_v3_minimal_table_in_catalog(catalog: &impl Catalog) -> Table {
        let table_ident =
            TableIdent::from_strs([format!("ns1-{}", uuid::Uuid::new_v4()), "test1".to_string()])
                .unwrap();

        catalog
            .create_namespace(table_ident.namespace(), HashMap::new())
            .await
            .unwrap();

        let file = File::open(format!(
            "{}/testdata/table_metadata/{}",
            env!("CARGO_MANIFEST_DIR"),
            "TableMetadataV3ValidMinimal.json"
        ))
        .unwrap();
        let reader = BufReader::new(file);
        let base_metadata = serde_json::from_reader::<_, TableMetadata>(reader).unwrap();

        let table_creation = TableCreation::builder()
            .schema((**base_metadata.current_schema()).clone())
            .partition_spec((**base_metadata.default_partition_spec()).clone())
            .sort_order((**base_metadata.default_sort_order()).clone())
            .name(table_ident.name().to_string())
            .format_version(crate::spec::FormatVersion::V3)
            .build();

        catalog
            .create_table(table_ident.namespace(), table_creation)
            .await
            .unwrap()
    }

    /// Helper function to create a test table with retry properties
    pub(super) fn setup_test_table(num_retries: &str) -> Table {
        let table = make_v2_table();

        // Set retry properties
        let mut props = HashMap::new();
        props.insert("commit.retry.min-wait-ms".to_string(), "10".to_string());
        props.insert("commit.retry.max-wait-ms".to_string(), "100".to_string());
        props.insert(
            "commit.retry.total-timeout-ms".to_string(),
            "1000".to_string(),
        );
        props.insert(
            "commit.retry.num-retries".to_string(),
            num_retries.to_string(),
        );

        // Update table properties
        let metadata = table
            .metadata()
            .clone()
            .into_builder(None)
            .set_properties(props)
            .unwrap()
            .build()
            .unwrap()
            .metadata;

        table.with_metadata(Arc::new(metadata))
    }

    /// Helper function to create a transaction with a simple update action
    fn create_test_transaction(table: &Table) -> Transaction {
        let tx = Transaction::new(table);
        tx.update_table_properties()
            .set("test.key".to_string(), "test.value".to_string())
            .apply(tx)
            .unwrap()
    }

    /// Helper function to set up a mock catalog with retryable errors
    fn setup_mock_catalog_with_retryable_errors(
        success_after_attempts: Option<u32>,
        expected_calls: usize,
    ) -> MockCatalog {
        let mut mock_catalog = MockCatalog::new();

        mock_catalog
            .expect_load_table()
            .returning_st(|_| Box::pin(async move { Ok(make_v2_table()) }));

        let attempts = AtomicU32::new(0);
        mock_catalog
            .expect_update_table()
            .times(expected_calls)
            .returning_st(move |_| {
                if let Some(success_after_attempts) = success_after_attempts {
                    attempts.fetch_add(1, Ordering::SeqCst);
                    if attempts.load(Ordering::SeqCst) <= success_after_attempts {
                        Box::pin(async move {
                            Err(
                                Error::new(ErrorKind::CatalogCommitConflicts, "Commit conflict")
                                    .with_retryable(true),
                            )
                        })
                    } else {
                        Box::pin(async move { Ok(make_v2_table()) })
                    }
                } else {
                    // Always fail with retryable error
                    Box::pin(async move {
                        Err(
                            Error::new(ErrorKind::CatalogCommitConflicts, "Commit conflict")
                                .with_retryable(true),
                        )
                    })
                }
            });

        mock_catalog
    }

    /// Helper function to set up a mock catalog with non-retryable error
    fn setup_mock_catalog_with_non_retryable_error() -> MockCatalog {
        let mut mock_catalog = MockCatalog::new();

        mock_catalog
            .expect_load_table()
            .returning_st(|_| Box::pin(async move { Ok(make_v2_table()) }));

        mock_catalog
            .expect_update_table()
            .times(1) // Should only be called once since error is not retryable
            .returning_st(move |_| {
                Box::pin(async move {
                    Err(Error::new(ErrorKind::Unexpected, "Non-retryable error")
                        .with_retryable(false))
                })
            });

        mock_catalog
    }

    #[tokio::test]
    async fn test_commit_rejects_table_replacement_before_and_after_conflict() {
        for conflict_first in [false, true] {
            let original = setup_test_table("3");
            let replacement = original.clone().with_metadata(Arc::new(
                original
                    .metadata()
                    .clone()
                    .into_builder(None)
                    .assign_uuid(uuid::Uuid::new_v4())
                    .build()
                    .unwrap()
                    .metadata,
            ));
            let mut catalog = MockCatalog::new();
            let loads = AtomicU32::new(0);
            let original_copy = original.clone();
            catalog
                .expect_load_table()
                .times(if conflict_first { 2 } else { 1 })
                .returning_st(move |_| {
                    let table = if conflict_first && loads.fetch_add(1, Ordering::SeqCst) == 0 {
                        original_copy.clone()
                    } else {
                        replacement.clone()
                    };
                    Box::pin(async move { Ok(table) })
                });
            catalog
                .expect_update_table()
                .times(usize::from(conflict_first))
                .returning_st(|_| {
                    Box::pin(async {
                        Err(
                            Error::new(ErrorKind::CatalogCommitConflicts, "Commit conflict")
                                .with_retryable(true),
                        )
                    })
                });
            let err = create_test_transaction(&original)
                .commit(&catalog)
                .await
                .unwrap_err();
            assert_eq!(err.kind(), ErrorKind::CatalogCommitConflicts);
            assert!(!err.retryable());
            assert!(err.to_string().contains("UUID changed"));
        }
    }

    fn concurrent_changes(table: &Table) -> Result<Vec<(&'static str, Table)>> {
        let metadata = table.metadata();
        let snapshot = Snapshot::builder()
            .with_snapshot_id(123456789)
            .with_parent_snapshot_id(metadata.current_snapshot_id())
            .with_sequence_number(metadata.last_sequence_number() + 1)
            .with_timestamp_ms(metadata.last_updated_ms() + 1)
            .with_manifest_list("s3://bucket/test/location/manifest-list.avro")
            .with_schema_id(metadata.current_schema_id())
            .with_summary(Summary {
                operation: Operation::Append,
                additional_properties: HashMap::new(),
            })
            .build();
        let renamed_schema = Schema::builder()
            .with_fields(
                metadata
                    .current_schema()
                    .as_struct()
                    .fields()
                    .iter()
                    .map(|field| {
                        let mut field = field.as_ref().clone();
                        field.name = format!("{}_renamed", field.name);
                        Arc::new(field)
                    }),
            )
            .build()?;
        let extended_schema = metadata
            .current_schema()
            .as_ref()
            .clone()
            .into_builder()
            .with_fields([Arc::new(NestedField::optional(
                metadata.last_column_id() + 1,
                "new_column",
                Type::Primitive(PrimitiveType::Long),
            ))])
            .build()?;
        let new_spec = UnboundPartitionSpec::builder()
            .add_partition_field(2, "y", Transform::Identity)?
            .build();
        let changes = [
            ("uuid", vec![TableUpdate::AssignUuid {
                uuid: uuid::Uuid::new_v4(),
            }]),
            ("snapshot", vec![
                TableUpdate::AddSnapshot { snapshot },
                TableUpdate::SetSnapshotRef {
                    ref_name: MAIN_BRANCH.to_string(),
                    reference: SnapshotReference::new(
                        123456789,
                        SnapshotRetention::branch(None, None, None),
                    ),
                },
            ]),
            ("schema", vec![
                TableUpdate::AddSchema {
                    schema: renamed_schema,
                },
                TableUpdate::SetCurrentSchema { schema_id: -1 },
            ]),
            ("assigned field ID", vec![TableUpdate::AddSchema {
                schema: extended_schema,
            }]),
            ("partition spec", vec![
                TableUpdate::AddSpec {
                    spec: UnboundPartitionSpec::builder().build(),
                },
                TableUpdate::SetDefaultSpec { spec_id: -1 },
            ]),
            ("assigned partition ID", vec![TableUpdate::AddSpec {
                spec: new_spec,
            }]),
            ("sort order", vec![
                TableUpdate::AddSortOrder {
                    sort_order: SortOrder::unsorted_order(),
                },
                TableUpdate::SetDefaultSortOrder { sort_order_id: -1 },
            ]),
        ];
        changes
            .into_iter()
            .map(|(name, updates)| {
                Ok((
                    name,
                    Transaction::update_table_metadata(table.clone(), &updates)?,
                ))
            })
            .collect()
    }

    fn catalog_with_publication_state(loaded: Table, at_publication: Table) -> MockCatalog {
        let mut catalog = MockCatalog::new();
        catalog.expect_load_table().times(1).returning_st(move |_| {
            let loaded = loaded.clone();
            Box::pin(async move { Ok(loaded) })
        });
        catalog
            .expect_update_table()
            .times(1)
            .returning_st(move |commit| {
                let at_publication = at_publication.clone();
                Box::pin(async move { commit.apply(at_publication) })
            });
        catalog
    }

    #[tokio::test]
    async fn test_forbid_rebase_rejects_changes_before_refresh_and_publication() -> Result<()> {
        for original in [make_v2_table(), make_v2_minimal_table()] {
            for (change, changed) in concurrent_changes(&original)? {
                for at_publication in [false, true] {
                    let mut catalog = if at_publication {
                        // The initial load is unchanged; the catalog validates against
                        // another writer's state when publication is attempted.
                        catalog_with_publication_state(original.clone(), changed.clone())
                    } else {
                        let mut catalog = MockCatalog::new();
                        let changed = changed.clone();
                        catalog.expect_load_table().times(1).returning_st(move |_| {
                            let changed = changed.clone();
                            Box::pin(async move { Ok(changed) })
                        });
                        catalog.expect_update_table().times(0);
                        catalog
                    };
                    // Empty MERGE output still needs validation of the absent snapshot.
                    let tx = if original.metadata().current_snapshot_id().is_none() {
                        Transaction::new(&original)
                    } else {
                        create_test_transaction(&original)
                    }
                    .with_rebase_policy(RebasePolicy::Forbid);
                    let err = tx.commit(&catalog).await.unwrap_err();
                    assert_eq!(err.kind(), ErrorKind::CatalogCommitConflicts, "{change}");
                    assert!(
                        !err.retryable(),
                        "{change}, at publication: {at_publication}"
                    );
                    catalog.checkpoint();
                }
            }
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_forbid_rebase_rejects_location_and_property_changes_at_refresh() -> Result<()> {
        let original = make_v2_table();
        let property_change =
            Transaction::update_table_metadata(original.clone(), &[TableUpdate::SetProperties {
                updates: HashMap::from([("concurrent".to_string(), "value".to_string())]),
            }])?;
        for changed in [
            original
                .clone()
                .with_metadata_location("s3://bucket/other.metadata.json".to_string()),
            property_change,
        ] {
            let mut catalog = MockCatalog::new();
            catalog.expect_load_table().times(1).returning_st(move |_| {
                let changed = changed.clone();
                Box::pin(async move { Ok(changed) })
            });
            catalog.expect_update_table().times(0);
            let err = create_test_transaction(&original)
                .with_rebase_policy(RebasePolicy::Forbid)
                .commit_once(&catalog)
                .await
                .unwrap_err();
            assert_eq!(err.kind(), ErrorKind::CatalogCommitConflicts);
            assert!(!err.retryable());
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_forbid_rebase_validates_original_state_before_actions() -> Result<()> {
        let original = make_v2_minimal_table().with_metadata_location(format!(
            "s3://bucket/test/location/metadata/00000-{}.metadata.json",
            uuid::Uuid::new_v4(),
        ));
        let tx = create_test_transaction(&original).with_rebase_policy(RebasePolicy::Forbid);
        let tx = tx
            .replace_partition_spec(UnboundPartitionSpec::builder().build())
            .apply(tx)?;
        let catalog = catalog_with_publication_state(original.clone(), original.clone());
        let committed = tx.commit(&catalog).await?;
        assert_ne!(
            committed.metadata().default_partition_spec_id(),
            original.metadata().default_partition_spec_id()
        );
        assert_eq!(
            committed
                .metadata()
                .properties()
                .get("test.key")
                .map(String::as_str),
            Some("test.value")
        );

        let catalog = catalog_with_publication_state(original.clone(), original.clone());
        let validated = Transaction::new(&original)
            .with_rebase_policy(RebasePolicy::Forbid)
            .commit_once(&catalog)
            .await?;
        // A catalog may publish a new metadata file for a validation-only commit.
        assert_eq!(validated.metadata().uuid(), original.metadata().uuid());
        assert_eq!(validated.metadata().current_snapshot_id(), None);
        assert_eq!(
            validated.metadata().current_schema(),
            original.metadata().current_schema()
        );
        assert_eq!(
            validated.metadata().default_partition_spec(),
            original.metadata().default_partition_spec()
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_forbid_rebase_does_not_retry_catalog_conflicts() {
        let original = setup_test_table("3");
        let mut catalog = MockCatalog::new();
        let loaded = original.clone();
        catalog.expect_load_table().times(1).returning_st(move |_| {
            let loaded = loaded.clone();
            Box::pin(async move { Ok(loaded) })
        });
        catalog.expect_update_table().times(1).returning_st(|_| {
            Box::pin(async {
                Err(
                    Error::new(ErrorKind::CatalogCommitConflicts, "Concurrent commit")
                        .with_retryable(true),
                )
            })
        });
        let err = create_test_transaction(&original)
            .with_rebase_policy(RebasePolicy::Forbid)
            .commit(&catalog)
            .await
            .unwrap_err();
        assert_eq!(err.kind(), ErrorKind::CatalogCommitConflicts);
        assert!(!err.retryable());
    }

    #[tokio::test]
    async fn test_forbid_rebase_retries_transient_errors_only_while_read_base_matches() -> Result<()>
    {
        for metadata_changed in [false, true] {
            let original = setup_test_table("3").with_metadata_location(format!(
                "s3://bucket/test/location/metadata/00000-{}.metadata.json",
                uuid::Uuid::new_v4(),
            ));
            let refreshed = if metadata_changed {
                Transaction::update_table_metadata(original.clone(), &[
                    TableUpdate::SetProperties {
                        updates: HashMap::from([("concurrent".to_string(), "value".to_string())]),
                    },
                ])?
            } else {
                original.clone()
            };
            assert_eq!(
                refreshed.metadata().current_snapshot_id(),
                original.metadata().current_snapshot_id()
            );
            let mut catalog = MockCatalog::new();
            let loads = AtomicU32::new(0);
            let first_load = original.clone();
            catalog.expect_load_table().times(2).returning_st(move |_| {
                let table = if loads.fetch_add(1, Ordering::SeqCst) == 0 {
                    first_load.clone()
                } else {
                    refreshed.clone()
                };
                Box::pin(async move { Ok(table) })
            });
            let attempts = AtomicU32::new(0);
            let at_publication = original.clone();
            catalog
                .expect_update_table()
                .times(if metadata_changed { 1 } else { 2 })
                .returning_st(move |commit| {
                    let first_attempt = attempts.fetch_add(1, Ordering::SeqCst) == 0;
                    let table = at_publication.clone();
                    Box::pin(async move {
                        if first_attempt {
                            // The failure occurs before publication; retrying is safe
                            // only if the next refresh still matches the original read.
                            Err(
                                Error::new(ErrorKind::Unexpected, "Transient transport failure")
                                    .with_retryable(true),
                            )
                        } else {
                            commit.apply(table)
                        }
                    })
                });
            let result = create_test_transaction(&original)
                .with_rebase_policy(RebasePolicy::Forbid)
                .commit(&catalog)
                .await;
            if metadata_changed {
                let err = result.unwrap_err();
                assert_eq!(err.kind(), ErrorKind::CatalogCommitConflicts);
                assert!(!err.retryable());
                assert!(
                    err.context()
                        .iter()
                        .any(|(key, value)| *key == "retry_attempts" && value == "1")
                );
            } else {
                let committed = result?;
                assert_eq!(committed.retry_attempts(), Some(1));
                assert_eq!(
                    committed
                        .metadata()
                        .properties()
                        .get("test.key")
                        .map(String::as_str),
                    Some("test.value")
                );
            }
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_allow_rebase_preserves_concurrent_metadata_changes() -> Result<()> {
        let original = make_v2_table();
        let changed = Transaction::update_table_metadata(original.clone(), &[
            TableUpdate::SetProperties {
                updates: HashMap::from([("concurrent".to_string(), "value".to_string())]),
            },
            TableUpdate::AddSpec {
                spec: UnboundPartitionSpec::builder().build(),
            },
            TableUpdate::SetDefaultSpec { spec_id: -1 },
        ])?
        .with_metadata_location(format!(
            "s3://bucket/test/location/metadata/00001-{}.metadata.json",
            uuid::Uuid::new_v4(),
        ));
        let catalog = catalog_with_publication_state(changed.clone(), changed);
        let tx = create_test_transaction(&original);
        let tx = tx
            .replace_partition_spec(
                original
                    .metadata()
                    .default_partition_spec()
                    .as_ref()
                    .clone()
                    .into_unbound(),
            )
            .apply(tx)?;
        let committed = tx.commit(&catalog).await?;
        assert_eq!(
            committed
                .metadata()
                .properties()
                .get("concurrent")
                .map(String::as_str),
            Some("value")
        );
        assert_eq!(
            committed
                .metadata()
                .properties()
                .get("test.key")
                .map(String::as_str),
            Some("test.value")
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_forbid_rebase_no_refresh_commit_preserves_original_requirements() -> Result<()> {
        for original in [make_v2_table(), make_v2_minimal_table()] {
            let original = original.with_metadata_location(format!(
                "s3://bucket/test/location/metadata/00000-{}.metadata.json",
                uuid::Uuid::new_v4(),
            ));
            for with_action in [false, true] {
                let tx = if with_action {
                    create_test_transaction(&original)
                } else {
                    Transaction::new(&original)
                }
                .with_rebase_policy(RebasePolicy::Forbid);
                // Commit construction has no catalog to refresh. Its requirements must
                // still reject a changed base when the caller submits it for publication.
                for (change, changed) in concurrent_changes(&original)? {
                    let err = tx
                        .clone()
                        .into_table_commit_no_refresh()
                        .await?
                        .apply(changed)
                        .unwrap_err();
                    assert_eq!(err.kind(), ErrorKind::CatalogCommitConflicts, "{change}");
                }
                let committed = tx
                    .into_table_commit_no_refresh()
                    .await?
                    .apply(original.clone())?;
                assert_eq!(committed.metadata().uuid(), original.metadata().uuid());
                assert_eq!(
                    committed.metadata().current_snapshot_id(),
                    original.metadata().current_snapshot_id()
                );
                assert_eq!(
                    committed
                        .metadata()
                        .properties()
                        .get("test.key")
                        .map(String::as_str),
                    with_action.then_some("test.value")
                );
            }
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_commit_retryable_error() {
        // Create a test table with retry properties
        let table = setup_test_table("3");

        // Create a transaction with a simple update action
        let tx = create_test_transaction(&table);

        // Create a mock catalog that fails twice then succeeds
        let mock_catalog = setup_mock_catalog_with_retryable_errors(Some(2), 3);

        // Commit the transaction
        let result = tx.commit(&mock_catalog).await;

        // Verify the result
        assert!(result.is_ok(), "Transaction should eventually succeed");
    }

    #[tokio::test]
    async fn test_commit_non_retryable_error() {
        // Create a test table with retry properties
        let table = setup_test_table("3");

        // Create a transaction with a simple update action
        let tx = create_test_transaction(&table);

        // Create a mock catalog that fails with non-retryable error
        let mock_catalog = setup_mock_catalog_with_non_retryable_error();

        // Commit the transaction
        let result = tx.commit(&mock_catalog).await;

        // Verify the result
        assert!(result.is_err(), "Transaction should fail immediately");
        if let Err(err) = result {
            assert_eq!(err.kind(), ErrorKind::Unexpected);
            assert_eq!(err.message(), "Non-retryable error");
            assert!(!err.retryable(), "Error should not be retryable");
        }
    }

    #[tokio::test]
    async fn test_commit_max_retries_exceeded() {
        // Create a test table with retry properties (only allow 2 retries)
        let table = setup_test_table("2");

        // Create a transaction with a simple update action
        let tx = create_test_transaction(&table);

        // Create a mock catalog that always fails with retryable error
        let mock_catalog = setup_mock_catalog_with_retryable_errors(None, 3); // Initial attempt + 2 retries = 3 total attempts

        // Commit the transaction
        let result = tx.commit(&mock_catalog).await;

        // Verify the result
        assert!(result.is_err(), "Transaction should fail after max retries");
        if let Err(err) = result {
            assert_eq!(err.kind(), ErrorKind::CatalogCommitConflicts);
            assert_eq!(err.message(), "Commit conflict");
            assert!(err.retryable(), "Error should be retryable");
        }
    }
}

#[cfg(test)]
mod test_row_lineage {
    use crate::memory::tests::new_memory_catalog;
    use crate::spec::{
        DataContentType, DataFile, DataFileBuilder, DataFileFormat, Literal, Struct,
    };
    use crate::transaction::tests::make_v3_minimal_table_in_catalog;
    use crate::transaction::{ApplyTransactionAction, Transaction};

    #[tokio::test]
    async fn test_fast_append_with_row_lineage() {
        // Helper function to create a data file with specified number of rows
        fn file_with_rows(record_count: u64) -> DataFile {
            DataFileBuilder::default()
                .content(DataContentType::Data)
                .file_path(format!("test/{record_count}.parquet"))
                .file_format(DataFileFormat::Parquet)
                .file_size_in_bytes(100)
                .record_count(record_count)
                .partition(Struct::from_iter([Some(Literal::long(0))]))
                .partition_spec_id(0)
                .build()
                .unwrap()
        }
        let catalog = new_memory_catalog().await;

        let table = make_v3_minimal_table_in_catalog(&catalog).await;

        // Check initial state - next_row_id should be 0
        assert_eq!(table.metadata().next_row_id(), 0);

        // First fast append with 30 rows
        let tx = Transaction::new(&table);
        let data_file_30 = file_with_rows(30);
        let action = tx.fast_append().add_data_files(vec![data_file_30]);
        let tx = action.apply(tx).unwrap();
        let table = tx.commit(&catalog).await.unwrap();

        // Check snapshot and table state after first append
        let snapshot = table.metadata().current_snapshot().unwrap();
        assert_eq!(snapshot.first_row_id(), Some(0));
        assert_eq!(table.metadata().next_row_id(), 30);

        // Check written manifest for first_row_id
        let manifest_list = table
            .metadata()
            .current_snapshot()
            .unwrap()
            .load_manifest_list(table.file_io(), table.metadata())
            .await
            .unwrap();

        assert_eq!(manifest_list.entries().len(), 1);
        let manifest_file = &manifest_list.entries()[0];
        assert_eq!(manifest_file.first_row_id, Some(0));

        // Second fast append with 17 and 11 rows
        let tx = Transaction::new(&table);
        let data_file_17 = file_with_rows(17);
        let data_file_11 = file_with_rows(11);
        let action = tx
            .fast_append()
            .add_data_files(vec![data_file_17, data_file_11]);
        let tx = action.apply(tx).unwrap();
        let table = tx.commit(&catalog).await.unwrap();

        // Check snapshot and table state after second append
        let snapshot = table.metadata().current_snapshot().unwrap();
        assert_eq!(snapshot.first_row_id(), Some(30));
        assert_eq!(table.metadata().next_row_id(), 30 + 17 + 11);

        // Check written manifest for first_row_id
        let manifest_list = table
            .metadata()
            .current_snapshot()
            .unwrap()
            .load_manifest_list(table.file_io(), table.metadata())
            .await
            .unwrap();
        assert_eq!(manifest_list.entries().len(), 2);
        let manifest_file = &manifest_list.entries()[1];
        assert_eq!(manifest_file.first_row_id, Some(30));
    }
}
