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

use std::collections::HashMap;
use std::fmt::{Debug, Formatter};
use std::net::ToSocketAddrs;
use std::sync::Arc;
use std::time::Duration;

use anyhow::anyhow;
use async_trait::async_trait;
use hive_metastore::{
    GetTableRequest, LockComponent, LockLevel, LockRequest, LockState, LockType,
    ThriftHiveMetastoreClient, ThriftHiveMetastoreClientBuilder,
    ThriftHiveMetastoreGetDatabaseException, ThriftHiveMetastoreGetTableReqException,
    UnlockRequest,
};
use iceberg::io::{FileIO, FileIOBuilder, StorageFactory};
use iceberg::spec::{TableMetadata, TableMetadataBuilder};
use iceberg::table::Table;
use iceberg::{
    Catalog, CatalogBuilder, Error, ErrorKind, MetadataLocation, Namespace, NamespaceIdent, Result,
    TableCommit, TableCreation, TableIdent,
};
use volo_thrift::MaybeException;

use super::utils::*;
use crate::error::{from_io_error, from_thrift_error, from_thrift_exception};

/// HMS catalog address
pub const HMS_CATALOG_PROP_URI: &str = "uri";

/// HMS Catalog thrift transport
pub const HMS_CATALOG_PROP_THRIFT_TRANSPORT: &str = "thrift_transport";
/// HMS Catalog framed thrift transport
pub const THRIFT_TRANSPORT_FRAMED: &str = "framed";
/// HMS Catalog buffered thrift transport
pub const THRIFT_TRANSPORT_BUFFERED: &str = "buffered";

/// HMS Catalog warehouse location
pub const HMS_CATALOG_PROP_WAREHOUSE: &str = "warehouse";

/// Builder for [`HmsCatalog`].
#[derive(Debug)]
pub struct HmsCatalogBuilder {
    config: HmsCatalogConfig,
    storage_factory: Option<Arc<dyn StorageFactory>>,
}

impl Default for HmsCatalogBuilder {
    fn default() -> Self {
        Self {
            config: HmsCatalogConfig {
                name: None,
                address: "".to_string(),
                thrift_transport: HmsThriftTransport::default(),
                warehouse: "".to_string(),
                props: HashMap::new(),
            },
            storage_factory: None,
        }
    }
}

impl CatalogBuilder for HmsCatalogBuilder {
    type C = HmsCatalog;

    fn with_storage_factory(mut self, storage_factory: Arc<dyn StorageFactory>) -> Self {
        self.storage_factory = Some(storage_factory);
        self
    }

    fn load(
        mut self,
        name: impl Into<String>,
        props: HashMap<String, String>,
    ) -> impl Future<Output = Result<Self::C>> + Send {
        self.config.name = Some(name.into());

        if props.contains_key(HMS_CATALOG_PROP_URI) {
            self.config.address = props.get(HMS_CATALOG_PROP_URI).cloned().unwrap_or_default();
        }

        if let Some(tt) = props.get(HMS_CATALOG_PROP_THRIFT_TRANSPORT) {
            self.config.thrift_transport = match tt.to_lowercase().as_str() {
                THRIFT_TRANSPORT_FRAMED => HmsThriftTransport::Framed,
                THRIFT_TRANSPORT_BUFFERED => HmsThriftTransport::Buffered,
                _ => HmsThriftTransport::default(),
            };
        }

        if props.contains_key(HMS_CATALOG_PROP_WAREHOUSE) {
            self.config.warehouse = props
                .get(HMS_CATALOG_PROP_WAREHOUSE)
                .cloned()
                .unwrap_or_default();
        }

        self.config.props = props
            .into_iter()
            .filter(|(k, _)| {
                k != HMS_CATALOG_PROP_URI
                    && k != HMS_CATALOG_PROP_THRIFT_TRANSPORT
                    && k != HMS_CATALOG_PROP_WAREHOUSE
            })
            .collect();

        let result = {
            if self.config.name.is_none() {
                Err(Error::new(
                    ErrorKind::DataInvalid,
                    "Catalog name is required",
                ))
            } else if self.config.address.is_empty() {
                Err(Error::new(
                    ErrorKind::DataInvalid,
                    "Catalog address is required",
                ))
            } else if self.config.warehouse.is_empty() {
                Err(Error::new(
                    ErrorKind::DataInvalid,
                    "Catalog warehouse is required",
                ))
            } else {
                HmsCatalog::new(self.config, self.storage_factory)
            }
        };

        std::future::ready(result)
    }
}

/// Which variant of the thrift transport to communicate with HMS
/// See: <https://github.com/apache/thrift/blob/master/doc/specs/thrift-rpc.md#framed-vs-unframed-transport>
#[derive(Debug, Default)]
pub enum HmsThriftTransport {
    /// Use the framed transport
    Framed,
    /// Use the buffered transport (default)
    #[default]
    Buffered,
}

/// Hive metastore Catalog configuration.
#[derive(Debug)]
pub(crate) struct HmsCatalogConfig {
    name: Option<String>,
    address: String,
    thrift_transport: HmsThriftTransport,
    warehouse: String,
    props: HashMap<String, String>,
}

struct HmsClient(Arc<ThriftHiveMetastoreClient>);

struct TableLockGuard {
    client: Arc<ThriftHiveMetastoreClient>,
    lock_id: Option<i64>,
}

impl TableLockGuard {
    async fn release(&mut self) -> Result<()> {
        let Some(lock_id) = self.lock_id else {
            return Ok(());
        };

        self.client
            .unlock(UnlockRequest { lockid: lock_id })
            .await
            .map(from_thrift_exception)
            .map_err(from_thrift_error)??;
        self.lock_id = None;
        Ok(())
    }
}

impl Drop for TableLockGuard {
    fn drop(&mut self) {
        let Some(lock_id) = self.lock_id.take() else {
            return;
        };
        let client = Arc::clone(&self.client);
        let Ok(handle) = tokio::runtime::Handle::try_current() else {
            return;
        };
        handle.spawn(async move {
            let result = client
                .unlock(UnlockRequest { lockid: lock_id })
                .await
                .map(from_thrift_exception)
                .map_err(from_thrift_error);
            if let Err(error) = result {
                tracing::warn!(lock_id, error = ?error, "failed to release abandoned Hive Metastore table lock");
            }
        });
    }
}

async fn release_lock_on_error(mut lock: TableLockGuard, error: Error) -> Error {
    if let Err(unlock_error) = lock.release().await {
        tracing::warn!(error = ?unlock_error, "failed to release Hive Metastore table lock after an error");
    }
    error
}

/// Hive metastore Catalog.
pub struct HmsCatalog {
    config: HmsCatalogConfig,
    client: HmsClient,
    file_io: FileIO,
}

impl Debug for HmsCatalog {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HmsCatalog")
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

impl HmsCatalog {
    /// Create a new hms catalog.
    fn new(
        config: HmsCatalogConfig,
        storage_factory: Option<Arc<dyn StorageFactory>>,
    ) -> Result<Self> {
        let address = config
            .address
            .as_str()
            .to_socket_addrs()
            .map_err(from_io_error)?
            .next()
            .ok_or_else(|| {
                Error::new(
                    ErrorKind::Unexpected,
                    format!("invalid address: {}", config.address),
                )
            })?;

        let builder = ThriftHiveMetastoreClientBuilder::new("hms").address(address);

        let client = match &config.thrift_transport {
            HmsThriftTransport::Framed => builder
                .make_codec(volo_thrift::codec::default::DefaultMakeCodec::framed())
                .build(),
            HmsThriftTransport::Buffered => builder
                .make_codec(volo_thrift::codec::default::DefaultMakeCodec::buffered())
                .build(),
        };

        let factory = storage_factory.ok_or_else(|| {
            Error::new(
                ErrorKind::Unexpected,
                "StorageFactory must be provided for HmsCatalog. Use `with_storage_factory` to configure it.",
            )
        })?;
        let file_io = FileIOBuilder::new(factory)
            .with_props(&config.props)
            .build();

        Ok(Self {
            config,
            client: HmsClient(Arc::new(client)),
            file_io,
        })
    }
    /// Get the catalogs `FileIO`
    pub fn file_io(&self) -> FileIO {
        self.file_io.clone()
    }

    async fn load_hive_table(
        &self,
        db_name: &str,
        table_name: &str,
    ) -> Result<hive_metastore::Table> {
        Ok(self
            .client
            .0
            .get_table_req(GetTableRequest {
                db_name: db_name.to_string().into(),
                tbl_name: table_name.to_string().into(),
                ..Default::default()
            })
            .await
            .map(from_thrift_exception)
            .map_err(from_thrift_error)??
            .table)
    }

    async fn acquire_table_lock(&self, db_name: &str, table_name: &str) -> Result<TableLockGuard> {
        let mut response = self
            .client
            .0
            .lock(LockRequest {
                component: vec![LockComponent {
                    r#type: LockType::EXCLUSIVE,
                    level: LockLevel::TABLE,
                    dbname: db_name.to_string().into(),
                    tablename: Some(table_name.to_string().into()),
                    ..Default::default()
                }],
                user: "iceberg-rust".into(),
                hostname: "iceberg-rust".into(),
                ..Default::default()
            })
            .await
            .map(from_thrift_exception)
            .map_err(from_thrift_error)??;

        let lock_id = response.lockid;
        let lock = TableLockGuard {
            client: Arc::clone(&self.client.0),
            lock_id: Some(lock_id),
        };
        for _ in 0..600 {
            match response.state {
                state if state == LockState::ACQUIRED => return Ok(lock),
                state if state == LockState::WAITING => {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    let next_response = self
                        .client
                        .0
                        .check_lock(hive_metastore::CheckLockRequest {
                            lockid: lock_id,
                            ..Default::default()
                        })
                        .await
                        .map(from_thrift_exception)
                        .map_err(from_thrift_error);
                    response = match next_response {
                        Ok(Ok(response)) => response,
                        Ok(Err(error)) | Err(error) => {
                            return Err(release_lock_on_error(lock, error).await);
                        }
                    };
                }
                state if state == LockState::NOT_ACQUIRED || state == LockState::ABORT => {
                    let error = Error::new(
                        ErrorKind::CatalogCommitConflicts,
                        format!("Hive Metastore returned lock state {state:?}"),
                    );
                    return Err(release_lock_on_error(lock, error).await);
                }
                state => {
                    let error = Error::new(
                        ErrorKind::CatalogCommitConflicts,
                        format!("Hive Metastore returned lock state {state:?}"),
                    );
                    return Err(release_lock_on_error(lock, error).await);
                }
            }
        }

        let error = Error::new(
            ErrorKind::CatalogCommitConflicts,
            "Timed out waiting for the Hive Metastore table lock",
        );
        Err(release_lock_on_error(lock, error).await)
    }

    async fn update_table_locked(
        &self,
        commit: TableCommit,
        db_name: &str,
        table_ident: &TableIdent,
    ) -> Result<Table> {
        let hive_table = self.load_hive_table(db_name, table_ident.name()).await?;
        let metadata_location = get_metadata_location(&hive_table.parameters)?;
        let metadata = TableMetadata::read_from(&self.file_io, &metadata_location).await?;
        let current_table = Table::builder()
            .file_io(self.file_io())
            .metadata_location(metadata_location)
            .metadata(metadata)
            .identifier(table_ident.clone())
            .build()?;

        let staged_table = commit.apply(current_table)?;
        let staged_metadata_location = staged_table.metadata_location_result()?.to_string();
        staged_table
            .metadata()
            .write_to(staged_table.file_io(), &staged_metadata_location)
            .await?;

        let updated_hive_table = update_hive_table(
            hive_table,
            staged_table.metadata().current_schema(),
            staged_table.metadata().location().to_string(),
            staged_metadata_location,
            staged_table.metadata().properties(),
        )?;

        self.client
            .0
            .alter_table(
                db_name.to_string().into(),
                table_ident.name().to_string().into(),
                updated_hive_table,
            )
            .await
            .map(from_thrift_exception)
            .map_err(from_thrift_error)??;

        Ok(staged_table)
    }
}

#[async_trait]
impl Catalog for HmsCatalog {
    /// HMS doesn't support nested namespaces.
    ///
    /// We will return empty list if parent is some.
    ///
    /// Align with java implementation: <https://github.com/apache/iceberg/blob/9bd62f79f8cd973c39d14e89163cb1c707470ed2/hive-metastore/src/main/java/org/apache/iceberg/hive/HiveCatalog.java#L305C26-L330>
    async fn list_namespaces(
        &self,
        parent: Option<&NamespaceIdent>,
    ) -> Result<Vec<NamespaceIdent>> {
        let dbs = if parent.is_some() {
            return Ok(vec![]);
        } else {
            self.client
                .0
                .get_all_databases()
                .await
                .map(from_thrift_exception)
                .map_err(from_thrift_error)??
        };

        Ok(dbs
            .into_iter()
            .map(|v| NamespaceIdent::new(v.into()))
            .collect())
    }

    /// Creates a new namespace with the given identifier and properties.
    ///
    /// Attempts to create a namespace defined by the `namespace`
    /// parameter and configured with the specified `properties`.
    ///
    /// This function can return an error in the following situations:
    ///
    /// - If `hive.metastore.database.owner-type` is specified without
    /// `hive.metastore.database.owner`,
    /// - Errors from `validate_namespace` if the namespace identifier does not
    /// meet validation criteria.
    /// - Errors from `convert_to_database` if the properties cannot be
    /// successfully converted into a database configuration.
    /// - Errors from the underlying database creation process, converted using
    /// `from_thrift_error`.
    async fn create_namespace(
        &self,
        namespace: &NamespaceIdent,
        properties: HashMap<String, String>,
    ) -> Result<Namespace> {
        let database = convert_to_database(namespace, &properties)?;

        self.client
            .0
            .create_database(database)
            .await
            .map(from_thrift_exception)
            .map_err(from_thrift_error)??;

        Ok(Namespace::with_properties(namespace.clone(), properties))
    }

    /// Retrieves a namespace by its identifier.
    ///
    /// Validates the given namespace identifier and then queries the
    /// underlying database client to fetch the corresponding namespace data.
    /// Constructs a `Namespace` object with the retrieved data and returns it.
    ///
    /// This function can return an error in any of the following situations:
    /// - If the provided namespace identifier fails validation checks
    /// - If there is an error querying the database, returned by
    /// `from_thrift_error`.
    async fn get_namespace(&self, namespace: &NamespaceIdent) -> Result<Namespace> {
        let name = validate_namespace(namespace)?;

        let db = self
            .client
            .0
            .get_database(name.into())
            .await
            .map(from_thrift_exception)
            .map_err(from_thrift_error)??;

        let ns = convert_to_namespace(&db)?;

        Ok(ns)
    }

    /// Checks if a namespace exists within the Hive Metastore.
    ///
    /// Validates the namespace identifier by querying the Hive Metastore
    /// to determine if the specified namespace (database) exists.
    ///
    /// # Returns
    /// A `Result<bool>` indicating the outcome of the check:
    /// - `Ok(true)` if the namespace exists.
    /// - `Ok(false)` if the namespace does not exist, identified by a specific
    /// `UserException` variant.
    /// - `Err(...)` if an error occurs during validation or the Hive Metastore
    /// query, with the error encapsulating the issue.
    async fn namespace_exists(&self, namespace: &NamespaceIdent) -> Result<bool> {
        let name = validate_namespace(namespace)?;

        let resp = self.client.0.get_database(name.into()).await;

        match resp {
            Ok(MaybeException::Ok(_)) => Ok(true),
            Ok(MaybeException::Exception(ThriftHiveMetastoreGetDatabaseException::O1(_))) => {
                Ok(false)
            }
            Ok(MaybeException::Exception(exception)) => Err(Error::new(
                ErrorKind::Unexpected,
                "Operation failed for hitting thrift error".to_string(),
            )
            .with_source(anyhow!("thrift error: {exception:?}"))),
            Err(err) => Err(from_thrift_error(err)),
        }
    }

    /// Asynchronously updates properties of an existing namespace.
    ///
    /// Converts the given namespace identifier and properties into a database
    /// representation and then attempts to update the corresponding namespace
    /// in the Hive Metastore.
    ///
    /// # Returns
    /// Returns `Ok(())` if the namespace update is successful. If the
    /// namespace cannot be updated due to missing information or an error
    /// during the update process, an `Err(...)` is returned.
    async fn update_namespace(
        &self,
        namespace: &NamespaceIdent,
        properties: HashMap<String, String>,
    ) -> Result<()> {
        let db = convert_to_database(namespace, &properties)?;

        let name = match &db.name {
            Some(name) => name,
            None => {
                return Err(Error::new(
                    ErrorKind::DataInvalid,
                    "Database name must be specified",
                ));
            }
        };

        self.client
            .0
            .alter_database(name.clone(), db)
            .await
            .map(from_thrift_exception)
            .map_err(from_thrift_error)??;

        Ok(())
    }

    /// Asynchronously drops a namespace from the Hive Metastore.
    ///
    /// # Returns
    /// A `Result<()>` indicating the outcome:
    /// - `Ok(())` signifies successful namespace deletion.
    /// - `Err(...)` signifies failure to drop the namespace due to validation
    /// errors, connectivity issues, or Hive Metastore constraints.
    async fn drop_namespace(&self, namespace: &NamespaceIdent) -> Result<()> {
        let name = validate_namespace(namespace)?;

        self.client
            .0
            .drop_database(name.into(), false, false)
            .await
            .map(from_thrift_exception)
            .map_err(from_thrift_error)??;

        Ok(())
    }

    /// Asynchronously lists all tables within a specified namespace.
    ///
    /// # Returns
    ///
    /// A `Result<Vec<TableIdent>>`, which is:
    /// - `Ok(vec![...])` containing a vector of `TableIdent` instances, each
    /// representing a table within the specified namespace.
    /// - `Err(...)` if an error occurs during namespace validation or while
    /// querying the database.
    async fn list_tables(&self, namespace: &NamespaceIdent) -> Result<Vec<TableIdent>> {
        let name = validate_namespace(namespace)?;

        let tables = self
            .client
            .0
            .get_all_tables(name.into())
            .await
            .map(from_thrift_exception)
            .map_err(from_thrift_error)??;

        let tables = tables
            .iter()
            .map(|table| TableIdent::new(namespace.clone(), table.to_string()))
            .collect();

        Ok(tables)
    }

    /// Creates a new table within a specified namespace using the provided
    /// table creation settings.
    ///
    /// # Returns
    /// A `Result` wrapping a `Table` object representing the newly created
    /// table.
    ///
    /// # Errors
    /// This function may return an error in several cases, including invalid
    /// namespace identifiers, failure to determine a default storage location,
    /// issues generating or writing table metadata, and errors communicating
    /// with the Hive Metastore.
    async fn create_table(
        &self,
        namespace: &NamespaceIdent,
        mut creation: TableCreation,
    ) -> Result<Table> {
        let db_name = validate_namespace(namespace)?;
        let table_name = creation.name.clone();

        let location = match &creation.location {
            Some(location) => location.clone(),
            None => {
                let ns = self.get_namespace(namespace).await?;
                let location = get_default_table_location(&ns, &table_name, &self.config.warehouse);
                creation.location = Some(location.clone());
                location
            }
        };
        let metadata = TableMetadataBuilder::from_table_creation(creation)?
            .build()?
            .metadata;

        let metadata_location =
            MetadataLocation::new_with_table_location(location.clone()).to_string();

        metadata.write_to(&self.file_io, &metadata_location).await?;

        let hive_table = convert_to_hive_table(
            db_name.clone(),
            metadata.current_schema(),
            table_name.clone(),
            location,
            metadata_location.clone(),
            metadata.properties(),
        )?;

        self.client
            .0
            .create_table(hive_table)
            .await
            .map(from_thrift_exception)
            .map_err(from_thrift_error)??;

        Table::builder()
            .file_io(self.file_io())
            .metadata_location(metadata_location)
            .metadata(metadata)
            .identifier(TableIdent::new(NamespaceIdent::new(db_name), table_name))
            .build()
    }

    /// Loads a table from the Hive Metastore and constructs a `Table` object
    /// based on its metadata.
    ///
    /// # Returns
    /// A `Result` wrapping a `Table` object that represents the loaded table.
    ///
    /// # Errors
    /// This function may return an error in several scenarios, including:
    /// - Failure to validate the namespace.
    /// - Failure to retrieve the table from the Hive Metastore.
    /// - Absence of metadata location information in the table's properties.
    /// - Issues reading or deserializing the table's metadata file.
    async fn load_table(&self, table: &TableIdent) -> Result<Table> {
        let db_name = validate_namespace(table.namespace())?;

        let hive_table = self.load_hive_table(&db_name, table.name()).await?;

        let metadata_location = get_metadata_location(&hive_table.parameters)?;

        let metadata = TableMetadata::read_from(&self.file_io, &metadata_location).await?;

        Table::builder()
            .file_io(self.file_io())
            .metadata_location(metadata_location)
            .metadata(metadata)
            .identifier(TableIdent::new(
                NamespaceIdent::new(db_name),
                table.name.clone(),
            ))
            .build()
    }

    /// Asynchronously drops a table from the database.
    ///
    /// # Errors
    /// Returns an error if:
    /// - The namespace provided in `table` cannot be validated
    /// or does not exist.
    /// - The underlying database client encounters an error while
    /// attempting to drop the table. This includes scenarios where
    /// the table does not exist.
    /// - Any network or communication error occurs with the database backend.
    async fn drop_table(&self, table: &TableIdent) -> Result<()> {
        let db_name = validate_namespace(table.namespace())?;

        self.client
            .0
            .drop_table(db_name.into(), table.name.clone().into(), false)
            .await
            .map(from_thrift_exception)
            .map_err(from_thrift_error)??;

        Ok(())
    }

    /// Asynchronously checks the existence of a specified table
    /// in the database.
    ///
    /// # Returns
    /// - `Ok(true)` if the table exists in the database.
    /// - `Ok(false)` if the table does not exist in the database.
    /// - `Err(...)` if an error occurs during the process
    async fn table_exists(&self, table: &TableIdent) -> Result<bool> {
        let db_name = validate_namespace(table.namespace())?;
        let table_name = table.name.clone();

        let resp = self
            .client
            .0
            .get_table_req(GetTableRequest {
                db_name: db_name.into(),
                tbl_name: table_name.into(),
                ..Default::default()
            })
            .await;

        match resp {
            Ok(MaybeException::Ok(_)) => Ok(true),
            Ok(MaybeException::Exception(ThriftHiveMetastoreGetTableReqException::O2(_))) => {
                Ok(false)
            }
            Ok(MaybeException::Exception(exception)) => Err(Error::new(
                ErrorKind::Unexpected,
                "Operation failed for hitting thrift error".to_string(),
            )
            .with_source(anyhow!("thrift error: {exception:?}"))),
            Err(err) => Err(from_thrift_error(err)),
        }
    }

    /// Asynchronously renames a table within the database
    /// or moves it between namespaces (databases).
    ///
    /// # Returns
    /// - `Ok(())` on successful rename or move of the table.
    /// - `Err(...)` if an error occurs during the process.
    async fn rename_table(&self, src: &TableIdent, dest: &TableIdent) -> Result<()> {
        let src_dbname = validate_namespace(src.namespace())?;
        let dest_dbname = validate_namespace(dest.namespace())?;

        let src_tbl_name = src.name.clone();
        let dest_tbl_name = dest.name.clone();

        let mut tbl = self
            .client
            .0
            .get_table_req(GetTableRequest {
                db_name: src_dbname.clone().into(),
                tbl_name: src_tbl_name.clone().into(),
                ..Default::default()
            })
            .await
            .map(from_thrift_exception)
            .map_err(from_thrift_error)??
            .table;

        tbl.db_name = Some(dest_dbname.into());
        tbl.table_name = Some(dest_tbl_name.into());

        self.client
            .0
            .alter_table(src_dbname.into(), src_tbl_name.into(), tbl)
            .await
            .map(from_thrift_exception)
            .map_err(from_thrift_error)??;

        Ok(())
    }

    async fn register_table(
        &self,
        table_ident: &TableIdent,
        metadata_location: String,
    ) -> Result<Table> {
        let db_name = validate_namespace(table_ident.namespace())?;
        let metadata = TableMetadata::read_from(&self.file_io, &metadata_location).await?;
        let hive_table = convert_to_hive_table(
            db_name,
            metadata.current_schema(),
            table_ident.name().to_string(),
            metadata.location().to_string(),
            metadata_location.clone(),
            metadata.properties(),
        )?;

        self.client
            .0
            .create_table(hive_table)
            .await
            .map(from_thrift_exception)
            .map_err(from_thrift_error)??;

        Table::builder()
            .file_io(self.file_io())
            .metadata_location(metadata_location)
            .metadata(metadata)
            .identifier(table_ident.clone())
            .build()
    }

    async fn update_table(&self, commit: TableCommit) -> Result<Table> {
        let table_ident = commit.identifier().clone();
        let db_name = validate_namespace(table_ident.namespace())?;
        let mut lock = self
            .acquire_table_lock(&db_name, table_ident.name())
            .await?;

        let result = self
            .update_table_locked(commit, &db_name, &table_ident)
            .await;
        let unlock_result = lock.release().await;

        return match (result, unlock_result) {
            (Err(error), _) => Err(error),
            (Ok(_), Err(error)) => Err(error),
            (Ok(table), Ok(())) => Ok(table),
        };
    }
}
