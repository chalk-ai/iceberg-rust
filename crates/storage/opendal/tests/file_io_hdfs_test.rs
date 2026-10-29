// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0

//! Integration tests for the native HDFS OpenDAL backend.
//!
//! These tests require the HDFS services in `dev/docker-compose.yaml` and use
//! host networking, so they are ignored by default and intended for Linux CI.

#![cfg(feature = "opendal-hdfs-native")]

use std::sync::Arc;

use bytes::Bytes;
use iceberg::io::{FileIO, FileIOBuilder, HDFS_NAME_NODE};
use iceberg_storage_opendal::OpenDalStorageFactory;
use iceberg_test_utils::{get_hdfs_endpoint, normalize_test_name_with_parts, set_up};

fn file_io() -> FileIO {
    set_up();
    FileIOBuilder::new(Arc::new(OpenDalStorageFactory::Hdfs)).build()
}

fn test_path(suffix: &str) -> String {
    format!(
        "{}/{}",
        get_hdfs_endpoint(),
        normalize_test_name_with_parts!(suffix)
    )
}

#[tokio::test]
#[ignore = "Linux-only: HDFS docker fixture uses host networking"]
async fn test_file_io_hdfs_write_read_and_metadata() {
    let file_io = file_io();
    let path = test_path("test_file_io_hdfs_write_read_and_metadata");
    let content = Bytes::from_static(b"hello hdfs");
    let _ = file_io.delete(&path).await;

    file_io
        .new_output(&path)
        .unwrap()
        .write(content.clone())
        .await
        .unwrap();

    assert!(file_io.exists(&path).await.unwrap());
    assert_eq!(
        file_io.new_input(&path).unwrap().read().await.unwrap(),
        content
    );
    assert_eq!(
        file_io
            .new_input(&path)
            .unwrap()
            .metadata()
            .await
            .unwrap()
            .size,
        10
    );
}

#[tokio::test]
#[ignore = "Linux-only: HDFS docker fixture uses host networking"]
async fn test_file_io_hdfs_reader_range() {
    let file_io = file_io();
    let path = test_path("test_file_io_hdfs_reader_range");
    let _ = file_io.delete(&path).await;
    file_io
        .new_output(&path)
        .unwrap()
        .write(Bytes::from_static(b"abcdefghij"))
        .await
        .unwrap();

    let reader = file_io.new_input(&path).unwrap().reader().await.unwrap();
    assert_eq!(
        reader.read(0..5).await.unwrap(),
        Bytes::from_static(b"abcde")
    );
    assert_eq!(
        reader.read(5..10).await.unwrap(),
        Bytes::from_static(b"fghij")
    );
}

#[tokio::test]
#[ignore = "Linux-only: HDFS docker fixture uses host networking"]
async fn test_file_io_hdfs_delete_prefix() {
    let file_io = file_io();
    let directory = test_path("test_file_io_hdfs_delete_prefix");
    let _ = file_io.delete_prefix(&directory).await;

    for index in 0..3 {
        file_io
            .new_output(format!("{directory}/file-{index}"))
            .unwrap()
            .write(Bytes::from_static(b"payload"))
            .await
            .unwrap();
    }
    file_io.delete_prefix(&directory).await.unwrap();

    for index in 0..3 {
        assert!(
            !file_io
                .exists(&format!("{directory}/file-{index}"))
                .await
                .unwrap()
        );
    }
}

#[tokio::test]
#[ignore = "Linux-only: HDFS docker fixture uses host networking"]
async fn test_file_io_hdfs_configured_name_node() {
    set_up();
    let file_io = FileIOBuilder::new(Arc::new(OpenDalStorageFactory::Hdfs))
        .with_prop(HDFS_NAME_NODE, get_hdfs_endpoint())
        .build();
    let path = format!(
        "hdfs://logical-nameservice/{}",
        normalize_test_name_with_parts!("test_file_io_hdfs_configured_name_node")
    );
    let _ = file_io.delete(&path).await;

    file_io
        .new_output(&path)
        .unwrap()
        .write(Bytes::from_static(b"configured name node"))
        .await
        .unwrap();
    assert_eq!(
        file_io.new_input(&path).unwrap().read().await.unwrap(),
        Bytes::from_static(b"configured name node")
    );
}

#[tokio::test]
#[ignore = "Linux-only: HDFS docker fixture uses host networking"]
async fn test_file_io_viewfs_path_uses_configured_name_node() {
    set_up();
    let file_io = FileIOBuilder::new(Arc::new(OpenDalStorageFactory::Hdfs))
        .with_prop(HDFS_NAME_NODE, get_hdfs_endpoint())
        .build();
    let path = format!(
        "viewfs://warehouse/{}",
        normalize_test_name_with_parts!("test_file_io_viewfs_path")
    );
    let _ = file_io.delete(&path).await;

    file_io
        .new_output(&path)
        .unwrap()
        .write(Bytes::from_static(b"viewfs path"))
        .await
        .unwrap();
    assert_eq!(
        file_io.new_input(&path).unwrap().read().await.unwrap(),
        Bytes::from_static(b"viewfs path")
    );
}
