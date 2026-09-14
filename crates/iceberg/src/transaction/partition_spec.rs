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

use std::sync::Arc;

use async_trait::async_trait;

use crate::error::Result;
use crate::spec::UnboundPartitionSpec;
use crate::table::Table;
use crate::transaction::{ActionCommit, TransactionAction};
use crate::{TableRequirement, TableUpdate};

// Chalk fork note: upstream has the retryable transaction-action framework
// (apache/iceberg-rust#1420, commit b10d48e14fad), the ReplaceSortOrderAction
// analogue (apache/iceberg-rust#1441, commit f0c5d3d641eb), and the
// last_partition_id accessor used for optimistic concurrency
// (apache/iceberg-rust#1438, commit 1725a3b3b510). It does not yet expose
// replace_partition_spec as a first-class transaction action, so this mirrors
// ReplaceSortOrderAction for partition evolution.
/// Transaction action for replacing the default partition spec.
pub struct ReplacePartitionSpecAction {
    partition_spec: UnboundPartitionSpec,
}

impl ReplacePartitionSpecAction {
    pub fn new(partition_spec: UnboundPartitionSpec) -> Self {
        ReplacePartitionSpecAction { partition_spec }
    }
}

#[async_trait]
impl TransactionAction for ReplacePartitionSpecAction {
    async fn commit(self: Arc<Self>, table: &Table) -> Result<ActionCommit> {
        let current_schema = table.metadata().current_schema();

        let updates = vec![
            TableUpdate::AddSpec {
                spec: self.partition_spec.clone(),
            },
            TableUpdate::SetDefaultSpec { spec_id: -1 },
        ];

        let requirements = vec![
            TableRequirement::CurrentSchemaIdMatch {
                current_schema_id: current_schema.schema_id(),
            },
            TableRequirement::DefaultSpecIdMatch {
                default_spec_id: table.metadata().default_partition_spec_id(),
            },
            TableRequirement::LastAssignedPartitionIdMatch {
                last_assigned_partition_id: table.metadata().last_partition_id(),
            },
        ];

        Ok(ActionCommit::new(updates, requirements))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use as_any::Downcast;

    use crate::spec::{PartitionSpec, Transform};
    use crate::transaction::partition_spec::ReplacePartitionSpecAction;
    use crate::transaction::tests::make_v2_table;
    use crate::transaction::{ApplyTransactionAction, Transaction, TransactionAction};

    fn partition_spec_for_table(table: &crate::table::Table) -> PartitionSpec {
        PartitionSpec::builder(table.metadata().current_schema().clone())
            .add_partition_field("y", "y", Transform::Identity)
            .unwrap()
            .build()
            .unwrap()
    }

    #[test]
    fn test_replace_partition_spec() {
        let table = make_v2_table();
        let tx = Transaction::new(&table);
        let partition_spec = partition_spec_for_table(&table).into_unbound();

        let tx = tx
            .replace_partition_spec(partition_spec.clone())
            .apply(tx)
            .unwrap();

        let replace_partition_spec = (*tx.actions[0])
            .downcast_ref::<ReplacePartitionSpecAction>()
            .unwrap();

        assert_eq!(replace_partition_spec.partition_spec, partition_spec);
    }

    #[tokio::test]
    async fn test_replace_partition_spec_commit_updates_default_spec() {
        let table = make_v2_table();
        let partition_spec = partition_spec_for_table(&table);
        let old_default_spec_id = table.metadata().default_partition_spec_id();
        let old_spec_count = table.metadata().partition_specs_iter().len();
        let action = Arc::new(ReplacePartitionSpecAction::new(
            partition_spec.clone().into_unbound(),
        ));

        let mut commit = action.commit(&table).await.unwrap();
        let updates = commit.take_updates();
        let requirements = commit.take_requirements();

        assert_eq!(updates.len(), 2);
        assert_eq!(requirements.len(), 3);
        for requirement in &requirements {
            requirement.check(Some(table.metadata())).unwrap();
        }

        let updated_table = Transaction::update_table_metadata(table, &updates).unwrap();
        let updated_metadata = updated_table.metadata();

        assert!(
            updated_metadata
                .default_partition_spec()
                .is_compatible_with(&partition_spec)
        );
        assert_ne!(
            updated_metadata.default_partition_spec_id(),
            old_default_spec_id
        );
        assert!(
            updated_metadata
                .partition_spec_by_id(old_default_spec_id)
                .is_some()
        );
        assert_eq!(
            updated_metadata.partition_specs_iter().len(),
            old_spec_count + 1
        );
    }
}
