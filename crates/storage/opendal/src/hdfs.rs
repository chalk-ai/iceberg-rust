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

//! HDFS and ViewFS storage backend via OpenDAL's native Rust implementation.

use std::collections::HashMap;
use std::sync::RwLock;

use iceberg::io::HDFS_NAME_NODE;
use iceberg::{Error, ErrorKind, Result};
use opendal::Operator;
use opendal::services::HdfsNativeConfig;
use url::Url;

use crate::utils::from_opendal_error;

pub(crate) fn hdfs_config_parse(
    mut properties: HashMap<String, String>,
) -> Result<HdfsNativeConfig> {
    let mut config = HdfsNativeConfig::default();

    if let Some(name_node) = properties.remove(HDFS_NAME_NODE) {
        config.name_node = Some(name_node);
    }

    // OpenDAL 0.55 exposes only the native client's root, NameNode, and
    // append settings. Hadoop-prefixed properties are reserved for the
    // richer configuration surface in newer OpenDAL releases.
    drop(properties);

    Ok(config)
}

pub(crate) fn parse_hdfs_path(path: &str) -> Result<(Option<String>, &str)> {
    let url = Url::parse(path).map_err(|error| {
        Error::new(
            ErrorKind::DataInvalid,
            format!("Invalid hdfs path: {path}: {error}"),
        )
    })?;
    if !matches!(url.scheme(), "hdfs" | "viewfs") {
        return Err(Error::new(
            ErrorKind::DataInvalid,
            format!("Invalid HDFS path: {path}, expected scheme `hdfs://` or `viewfs://`"),
        ));
    }

    if !url.username().is_empty() || url.password().is_some() {
        return Err(Error::new(
            ErrorKind::DataInvalid,
            format!("Invalid HDFS path: {path}, userinfo is not supported"),
        ));
    }

    if url.port() == Some(0) {
        return Err(Error::new(
            ErrorKind::DataInvalid,
            format!("Invalid HDFS path: {path}, port must be greater than zero"),
        ));
    }

    let name_node = url.host_str().filter(|host| !host.is_empty()).map(|host| {
        let host = match url.host() {
            Some(url::Host::Ipv6(_)) if !host.starts_with('[') => format!("[{host}]"),
            _ => host.to_string(),
        };
        let authority = url
            .port()
            .map(|port| format!("{host}:{port}"))
            .unwrap_or(host);
        format!("{}://{authority}", url.scheme())
    });

    let after_scheme = &path[url.scheme().len() + "://".len()..];
    let relative_path = match after_scheme.find('/') {
        Some(index) => after_scheme[index..].trim_start_matches('/'),
        None => "",
    };

    Ok((name_node, relative_path))
}

pub(crate) fn hdfs_create_operator<'a>(
    path: &'a str,
    config: &HdfsNativeConfig,
    operators: &RwLock<HashMap<String, Operator>>,
) -> Result<(Operator, &'a str)> {
    let (authority_name_node, relative_path) = parse_hdfs_path(path)?;
    let name_node = match config.name_node.clone().or(authority_name_node) {
        Some(name_node) => name_node,
        None => {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                format!(
                    "Invalid HDFS path: {path}, authority-less paths require the `{HDFS_NAME_NODE}` property"
                ),
            ));
        }
    };

    {
        let cache = operators
            .read()
            .map_err(|_| Error::new(ErrorKind::Unexpected, "HDFS operator cache lock poisoned"))?;
        if let Some(operator) = cache.get(&name_node) {
            return Ok((operator.clone(), relative_path));
        }
    }

    let mut cache = operators
        .write()
        .map_err(|_| Error::new(ErrorKind::Unexpected, "HDFS operator cache lock poisoned"))?;
    let operator = match cache.get(&name_node) {
        Some(operator) => operator.clone(),
        None => {
            let mut config = config.clone();
            config.name_node = Some(name_node.clone());
            let operator = Operator::from_config(config)
                .map_err(from_opendal_error)?
                .finish();
            cache.insert(name_node, operator.clone());
            operator
        }
    };

    Ok((operator, relative_path))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_name_node_property() {
        let config = hdfs_config_parse(HashMap::from([
            (
                HDFS_NAME_NODE.to_string(),
                "hdfs://nn1:8020,hdfs://nn2:8020".to_string(),
            ),
            (
                "hadoop.dfs.client.failover.random.order".to_string(),
                "true".to_string(),
            ),
            ("unrelated.key".to_string(), "ignored".to_string()),
        ]))
        .unwrap();

        assert_eq!(
            config.name_node.as_deref(),
            Some("hdfs://nn1:8020,hdfs://nn2:8020")
        );
    }

    #[test]
    fn parses_authority_and_relative_path() {
        assert_eq!(
            parse_hdfs_path("hdfs://nameservice/a/b").unwrap(),
            (Some("hdfs://nameservice".to_string()), "a/b")
        );
        assert_eq!(
            parse_hdfs_path("hdfs://nn:8020/foo").unwrap(),
            (Some("hdfs://nn:8020".to_string()), "foo")
        );
        assert_eq!(parse_hdfs_path("hdfs:///a/b").unwrap(), (None, "a/b"));
        assert_eq!(
            parse_hdfs_path("viewfs://warehouse/a/b").unwrap(),
            (Some("viewfs://warehouse".to_string()), "a/b")
        );
    }

    #[test]
    fn preserves_ipv6_authorities() {
        assert_eq!(
            parse_hdfs_path("hdfs://[::1]:8020/a").unwrap(),
            (Some("hdfs://[::1]:8020".to_string()), "a")
        );
    }

    #[test]
    fn rejects_non_hdfs_paths() {
        let error = parse_hdfs_path("file:///tmp/x").unwrap_err();
        assert!(
            error
                .to_string()
                .contains("expected scheme `hdfs://` or `viewfs://`")
        );
    }

    #[test]
    fn rejects_invalid_authority_details() {
        assert!(parse_hdfs_path("hdfs://user@nn:8020/a").is_err());
        assert!(parse_hdfs_path("hdfs://nn:0/a").is_err());
        assert!(parse_hdfs_path("hdfs:///a").is_ok());
    }

    #[test]
    fn configured_name_node_takes_precedence() {
        let config = hdfs_config_parse(HashMap::from([(
            HDFS_NAME_NODE.to_string(),
            "hdfs://configured:8020".to_string(),
        )]))
        .unwrap();
        let operators = RwLock::new(HashMap::new());

        let (_, relative_path) =
            hdfs_create_operator("hdfs://from-path:9000/a/b", &config, &operators).unwrap();

        assert_eq!(relative_path, "a/b");
        assert!(
            operators
                .read()
                .unwrap()
                .contains_key("hdfs://configured:8020")
        );
    }

    #[test]
    fn caches_operators_per_name_node() {
        let config = HdfsNativeConfig::default();
        let operators = RwLock::new(HashMap::new());

        hdfs_create_operator("hdfs://nn1:8020/a", &config, &operators).unwrap();
        hdfs_create_operator("hdfs://nn1:8020/b", &config, &operators).unwrap();
        hdfs_create_operator("hdfs://nn2:8020/c", &config, &operators).unwrap();

        assert_eq!(operators.read().unwrap().len(), 2);
    }
}
