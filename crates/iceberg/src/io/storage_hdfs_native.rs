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

//! HDFS storage over the pure-Rust `hdfs-native` client (no JVM).
//!
//! Paths look like `hdfs://<authority>/<path>` or `viewfs://<authority>/<path>`, where the
//! authority is a namenode `host:port`, an HA nameservice, or a viewfs mount table. Cluster
//! configuration for nameservices and mount tables is read from `HADOOP_CONF_DIR`.

use std::collections::HashMap;
use std::sync::Mutex;

use opendal::Operator;
use opendal::services::HdfsNativeConfig;

use crate::{Error, ErrorKind, Result};

/// One operator per cluster root (`scheme://authority`). Each operator owns an HDFS client
/// and its namenode connections, so building one per file would reconnect on every read.
#[derive(Debug, Default)]
pub(crate) struct HdfsNativeOperators {
    by_cluster_root: Mutex<HashMap<String, Operator>>,
}

/// Splits `scheme://authority/path` into the cluster root and the path below it.
fn split_cluster_root(path: &str) -> Result<(&str, &str)> {
    let invalid = || {
        Error::new(
            ErrorKind::DataInvalid,
            format!(
                "Invalid HDFS url: {path}, expected hdfs://<authority>/<path> or viewfs://<authority>/<path>"
            ),
        )
    };
    let scheme_end = path.find("://").ok_or_else(invalid)?;
    let authority_start = scheme_end + "://".len();
    let authority_end = path[authority_start..]
        .find('/')
        .map(|i| authority_start + i)
        .ok_or_else(invalid)?;
    if authority_end == authority_start {
        return Err(invalid());
    }
    Ok((&path[..authority_end], &path[authority_end + 1..]))
}

impl HdfsNativeOperators {
    /// Returns the operator for `path`'s cluster and the path relative to its root (`/`).
    pub(crate) fn create_operator<'a>(&self, path: &'a str) -> Result<(Operator, &'a str)> {
        let (cluster_root, relative) = split_cluster_root(path)?;
        let mut operators = self
            .by_cluster_root
            .lock()
            .map_err(|_| Error::new(ErrorKind::Unexpected, "HDFS operator cache lock poisoned"))?;
        if let Some(op) = operators.get(cluster_root) {
            return Ok((op.clone(), relative));
        }
        let mut cfg = HdfsNativeConfig::default();
        cfg.name_node = Some(cluster_root.to_string());
        cfg.root = Some("/".to_string());
        let op = Operator::from_config(cfg)?.finish();
        operators.insert(cluster_root.to_string(), op.clone());
        Ok((op, relative))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn splits_namenode_nameservice_and_viewfs_roots() {
        assert_eq!(
            split_cluster_root("hdfs://nn:8020/warehouse/t/metadata/v1.json").unwrap(),
            ("hdfs://nn:8020", "warehouse/t/metadata/v1.json")
        );
        assert_eq!(
            split_cluster_root("hdfs://ns1/a").unwrap(),
            ("hdfs://ns1", "a")
        );
        assert_eq!(
            split_cluster_root("viewfs://cluster/x/y").unwrap(),
            ("viewfs://cluster", "x/y")
        );
    }

    #[test]
    fn rejects_paths_without_authority_or_path() {
        assert!(split_cluster_root("hdfs:///a").is_err());
        assert!(split_cluster_root("hdfs://nn:8020").is_err());
        assert!(split_cluster_root("/no/scheme").is_err());
    }
}
