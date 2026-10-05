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

use std::collections::BTreeSet;
use std::sync::Arc;

use futures::stream::BoxStream;
use serde::{Deserialize, Serialize, Serializer};

use crate::expr::BoundPredicate;
use crate::spec::{
    DataContentType, DataFile, DataFileFormat, ManifestEntryRef, NameMapping, PartitionSpec,
    PrimitiveType, Schema, SchemaRef, Struct, TableMetadata, Type,
};
use crate::{Error, ErrorKind, Result};

/// Extend a scan schema with dropped top-level fields needed by applicable equality deletes.
///
/// Field IDs, not names, identify delete keys. Recovered fields receive private names so a
/// reused user column name cannot bind a delete to the replacement column. Callers must keep
/// their visible schema and output projection unchanged; this schema is only for reading.
pub fn schema_with_equality_delete_fields(
    schema: SchemaRef,
    metadata: &TableMetadata,
    equality_ids: impl IntoIterator<Item = i32>,
) -> Result<SchemaRef> {
    let missing_ids = equality_ids
        .into_iter()
        .filter(|id| schema.field_by_id(*id).is_none())
        .collect::<BTreeSet<_>>();
    if missing_ids.is_empty() {
        return Ok(schema);
    }

    let mut history = metadata.schemas_iter().collect::<Vec<_>>();
    history.sort_by_key(|schema| schema.schema_id());
    let mut recovered = Vec::with_capacity(missing_ids.len());
    let mut names = schema
        .field_id_to_name_map()
        .values()
        .map(|name| name.to_lowercase())
        .collect::<BTreeSet<_>>();
    for id in missing_ids {
        let mut definitions = history.iter().filter_map(|schema| schema.field_by_id(id));
        let mut field = definitions
            .next()
            .ok_or_else(|| {
                Error::new(
                    ErrorKind::DataInvalid,
                    format!("Equality delete field {id} is absent from retained table schemas"),
                )
            })?
            .as_ref()
            .clone();
        if history.iter().any(|schema| {
            schema.field_by_id(id).is_some() && schema.as_struct().field_by_id(id).is_none()
        }) || !matches!(field.field_type.as_ref(), Type::Primitive(_))
        {
            return Err(Error::new(
                ErrorKind::FeatureUnsupported,
                format!("Recovering nested equality delete field {id} is not supported"),
            ));
        }
        // Schema IDs need not encode chronology. Reconcile legal widening promotions instead
        // of choosing whichever retained schema happens to be visited first.
        for definition in definitions {
            field.field_type = Box::new(equality_delete_field_type(
                id,
                &field.field_type,
                &definition.field_type,
            )?);
        }
        field.name = format!("__iceberg_equality_delete_{id}");
        while !names.insert(field.name.to_lowercase()) {
            field.name.push('_');
        }
        // Files written before the key was added represent its missing value as null.
        field.required = false;
        recovered.push(Arc::new(field));
    }

    Ok(Arc::new(
        schema
            .as_ref()
            .clone()
            .into_builder()
            .with_fields(recovered)
            .build()?,
    ))
}

fn equality_delete_field_type(id: i32, left: &Type, right: &Type) -> Result<Type> {
    use PrimitiveType::{Decimal, Double, Float, Int, Long};
    use Type::Primitive;

    let primitive = match (left, right) {
        _ if left == right => return Ok(left.clone()),
        (Primitive(Int), Primitive(Long)) | (Primitive(Long), Primitive(Int)) => Long,
        (Primitive(Float), Primitive(Double)) | (Primitive(Double), Primitive(Float)) => Double,
        (
            Primitive(Decimal {
                precision: left_precision,
                scale: left_scale,
            }),
            Primitive(Decimal {
                precision: right_precision,
                scale: right_scale,
            }),
        ) if left_scale == right_scale => Decimal {
            precision: (*left_precision).max(*right_precision),
            scale: *left_scale,
        },
        _ => {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                format!(
                    "Incompatible retained types for equality delete field {id}: {left} and {right}"
                ),
            ));
        }
    };
    Ok(Primitive(primitive))
}

/// A stream of [`FileScanTask`].
pub type FileScanTaskStream = BoxStream<'static, Result<FileScanTask>>;

/// Serialization helper that always returns NotImplementedError.
/// Used for fields that should not be serialized but we want to be explicit about it.
fn serialize_not_implemented<S, T>(_: &T, _: S) -> std::result::Result<S::Ok, S::Error>
where S: Serializer {
    Err(serde::ser::Error::custom(
        "Serialization not implemented for this field",
    ))
}

/// Deserialization helper that always returns NotImplementedError.
/// Used for fields that should not be deserialized but we want to be explicit about it.
fn deserialize_not_implemented<'de, D, T>(_: D) -> std::result::Result<T, D::Error>
where D: serde::Deserializer<'de> {
    Err(serde::de::Error::custom(
        "Deserialization not implemented for this field",
    ))
}

/// A task to scan part of file.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct FileScanTask {
    /// The start offset of the file to scan.
    pub start: u64,
    /// The length of the file to scan.
    pub length: u64,
    /// The number of records in the file to scan.
    ///
    /// This is an optional field, and only available if we are
    /// reading the entire data file.
    pub record_count: Option<u64>,

    /// The data file path corresponding to the task.
    pub data_file_path: String,

    /// The format of the file to scan.
    pub data_file_format: DataFileFormat,

    /// The read schema, including any dropped fields required by equality deletes.
    /// `project_field_ids` controls the output independently of these internal fields.
    pub schema: SchemaRef,
    /// The field ids to project.
    pub project_field_ids: Vec<i32>,
    /// The predicate to filter.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub predicate: Option<BoundPredicate>,

    /// The list of delete files that may need to be applied to this data file
    pub deletes: Vec<FileScanTaskDeleteFile>,

    /// Partition data from the manifest entry, used to identify which columns can use
    /// constant values from partition metadata vs. reading from the data file.
    /// Per the Iceberg spec, only identity-transformed partition fields should use constants.
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(serialize_with = "serialize_not_implemented")]
    #[serde(deserialize_with = "deserialize_not_implemented")]
    pub partition: Option<Struct>,

    /// The partition spec for this file, used to distinguish identity transforms
    /// (which use partition metadata constants) from non-identity transforms like
    /// bucket/truncate (which must read source columns from the data file).
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(serialize_with = "serialize_not_implemented")]
    #[serde(deserialize_with = "deserialize_not_implemented")]
    pub partition_spec: Option<Arc<PartitionSpec>>,

    /// Name mapping from table metadata (property: schema.name-mapping.default),
    /// used to resolve field IDs from column names when Parquet files lack field IDs
    /// or have field ID conflicts.
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(serialize_with = "serialize_not_implemented")]
    #[serde(deserialize_with = "deserialize_not_implemented")]
    pub name_mapping: Option<Arc<NameMapping>>,

    /// Whether this scan task should treat column names as case-sensitive when binding predicates.
    pub case_sensitive: bool,

    /// Full data file metadata from the manifest.
    ///
    /// This is kept for in-memory consumers that need to reapply predicate pruning or
    /// build overwrite commits from planned tasks without rereading manifests.
    #[serde(skip)]
    pub data_file: Option<Box<DataFile>>,

    /// The manifest-level sequence number of this data file entry.
    #[serde(default)]
    pub data_sequence_number: Option<i64>,
}

impl FileScanTask {
    /// Returns the data file path of this file scan task.
    pub fn data_file_path(&self) -> &str {
        &self.data_file_path
    }

    /// Returns the project field id of this file scan task.
    pub fn project_field_ids(&self) -> &[i32] {
        &self.project_field_ids
    }

    /// Returns the predicate of this file scan task.
    pub fn predicate(&self) -> Option<&BoundPredicate> {
        self.predicate.as_ref()
    }

    /// Returns the schema of this file scan task as a reference
    pub fn schema(&self) -> &Schema {
        &self.schema
    }

    /// Returns the schema of this file scan task as a SchemaRef
    pub fn schema_ref(&self) -> SchemaRef {
        self.schema.clone()
    }
}

#[derive(Debug)]
pub(crate) struct DeleteFileContext {
    pub(crate) manifest_entry: ManifestEntryRef,
    pub(crate) partition_spec_id: i32,
}

impl From<&DeleteFileContext> for FileScanTaskDeleteFile {
    fn from(ctx: &DeleteFileContext) -> Self {
        FileScanTaskDeleteFile {
            file_path: ctx.manifest_entry.file_path().to_string(),
            file_type: ctx.manifest_entry.content_type(),
            partition_spec_id: ctx.partition_spec_id,
            equality_ids: ctx.manifest_entry.data_file.equality_ids.clone(),
            record_count: ctx.manifest_entry.record_count(),
            file_format: ctx.manifest_entry.data_file.file_format(),
            file_size_in_bytes: ctx.manifest_entry.data_file.file_size_in_bytes(),
            sequence_number: ctx.manifest_entry.sequence_number(),
            content_offset: ctx.manifest_entry.data_file.content_offset(),
            content_size_in_bytes: ctx.manifest_entry.data_file.content_size_in_bytes(),
            referenced_data_file: ctx.manifest_entry.data_file.referenced_data_file(),
        }
    }
}

/// A task to scan part of file.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct FileScanTaskDeleteFile {
    /// The delete file path
    pub file_path: String,

    /// delete file type
    pub file_type: DataContentType,

    /// partition id
    pub partition_spec_id: i32,

    /// equality ids for equality deletes (null for anything other than equality-deletes)
    pub equality_ids: Option<Vec<i32>>,

    /// Number of records in the delete file.
    pub record_count: u64,

    /// File format of the delete file.
    pub file_format: DataFileFormat,

    /// Size of the delete file in bytes.
    pub file_size_in_bytes: u64,

    /// Manifest-level sequence number of this delete file entry.
    #[serde(default)]
    pub sequence_number: Option<i64>,

    /// Byte offset of the deletion-vector blob inside the Puffin file.
    #[serde(default)]
    pub content_offset: Option<i64>,

    /// Byte length of the deletion-vector blob.
    #[serde(default)]
    pub content_size_in_bytes: Option<i64>,

    /// Path of the data file this deletion vector applies to.
    #[serde(default)]
    pub referenced_data_file: Option<String>,
}

impl Default for FileScanTaskDeleteFile {
    fn default() -> Self {
        Self {
            file_path: String::new(),
            file_type: DataContentType::PositionDeletes,
            partition_spec_id: 0,
            equality_ids: None,
            record_count: 0,
            file_format: DataFileFormat::Parquet,
            file_size_in_bytes: 0,
            sequence_number: None,
            content_offset: None,
            content_size_in_bytes: None,
            referenced_data_file: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;
    use crate::spec::{FormatVersion, NestedField, PartitionSpec, SortOrder, TableMetadataBuilder};

    fn metadata_with_history(types: &[PrimitiveType]) -> TableMetadata {
        let schema = |field_type: &PrimitiveType| {
            Schema::builder()
                .with_fields([Arc::new(NestedField::optional(
                    1,
                    "id",
                    Type::Primitive(field_type.clone()),
                ))])
                .build()
                .unwrap()
        };
        let mut builder = TableMetadataBuilder::new(
            schema(&types[0]),
            PartitionSpec::unpartition_spec(),
            SortOrder::unsorted_order(),
            "memory://table".to_string(),
            FormatVersion::V2,
            HashMap::new(),
        )
        .unwrap();
        for field_type in &types[1..] {
            builder = builder.add_schema(schema(field_type)).unwrap();
        }
        builder.build().unwrap().metadata
    }

    #[test]
    fn equality_delete_fields_preserve_visible_names_and_identity() {
        let metadata = metadata_with_history(&[PrimitiveType::Long]);
        let schema = Arc::new(
            Schema::builder()
                .with_fields([
                    Arc::new(NestedField::optional(
                        2,
                        "id",
                        Type::Primitive(PrimitiveType::String),
                    )),
                    Arc::new(NestedField::optional(
                        3,
                        "__ICEBERG_EQUALITY_DELETE_1",
                        Type::Primitive(PrimitiveType::String),
                    )),
                ])
                .build()
                .unwrap(),
        );
        let result = schema_with_equality_delete_fields(schema.clone(), &metadata, [1, 1]).unwrap();
        assert_eq!(result.as_struct().fields().len(), 3);
        assert_eq!(result.field_by_name("id").unwrap().id, 2);
        assert_eq!(
            result.field_by_id(1).unwrap().name,
            "__iceberg_equality_delete_1_"
        );
        assert_eq!(
            result.field_by_id(1).unwrap().field_type.as_ref(),
            &Type::Primitive(PrimitiveType::Long)
        );
        assert!(schema.field_by_id(1).is_none());
        assert!(Arc::ptr_eq(
            &schema,
            &schema_with_equality_delete_fields(schema.clone(), &metadata, [2]).unwrap()
        ));
    }

    #[test]
    fn equality_delete_fields_reconcile_promotions_independently_of_history_order() {
        for types in [vec![PrimitiveType::Int, PrimitiveType::Long], vec![
            PrimitiveType::Long,
            PrimitiveType::Int,
        ]] {
            let metadata = metadata_with_history(&types);
            let result = schema_with_equality_delete_fields(
                Arc::new(Schema::builder().build().unwrap()),
                &metadata,
                [1],
            )
            .unwrap();
            assert_eq!(
                result.field_by_id(1).unwrap().field_type.as_ref(),
                &Type::Primitive(PrimitiveType::Long)
            );
        }
        let metadata = metadata_with_history(&[
            PrimitiveType::Decimal {
                precision: 20,
                scale: 2,
            },
            PrimitiveType::Decimal {
                precision: 10,
                scale: 2,
            },
        ]);
        let result = schema_with_equality_delete_fields(
            Arc::new(Schema::builder().build().unwrap()),
            &metadata,
            [1],
        )
        .unwrap();
        assert_eq!(
            result.field_by_id(1).unwrap().field_type.as_ref(),
            &Type::Primitive(PrimitiveType::Decimal {
                precision: 20,
                scale: 2
            })
        );
    }

    #[test]
    fn equality_delete_fields_reject_missing_or_incompatible_definitions() {
        let metadata = metadata_with_history(&[PrimitiveType::Long, PrimitiveType::String]);
        let schema = Arc::new(Schema::builder().build().unwrap());
        let error = schema_with_equality_delete_fields(schema.clone(), &metadata, [1]).unwrap_err();
        assert!(error.to_string().contains("Incompatible retained types"));
        let error = schema_with_equality_delete_fields(schema, &metadata, [999]).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("absent from retained table schemas")
        );
    }
}
