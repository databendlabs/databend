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

use super::blob::Blob;
use crate::io::{FileIOBuilder, InputFile};
use crate::puffin::compression::CompressionCodec;
use crate::puffin::metadata::{BlobMetadata, CREATED_BY_PROPERTY, FileMetadata};

const JAVA_TESTDATA: &str = "testdata/puffin/java-generated";
const EMPTY_UNCOMPRESSED: &str = "empty-puffin-uncompressed.bin";
const METRIC_UNCOMPRESSED: &str = "sample-metric-data-uncompressed.bin";
const METRIC_ZSTD_COMPRESSED: &str = "sample-metric-data-compressed-zstd.bin";

fn input_file_for_test_data(path: &str) -> InputFile {
    FileIOBuilder::new_fs_io()
        .build()
        .unwrap()
        .new_input(env!("CARGO_MANIFEST_DIR").to_owned() + "/" + path)
        .unwrap()
}

pub(crate) fn java_empty_uncompressed_input_file() -> InputFile {
    input_file_for_test_data(&[JAVA_TESTDATA, EMPTY_UNCOMPRESSED].join("/"))
}

pub(crate) fn java_uncompressed_metric_input_file() -> InputFile {
    input_file_for_test_data(&[JAVA_TESTDATA, METRIC_UNCOMPRESSED].join("/"))
}

pub(crate) fn java_zstd_compressed_metric_input_file() -> InputFile {
    input_file_for_test_data(&[JAVA_TESTDATA, METRIC_ZSTD_COMPRESSED].join("/"))
}

pub(crate) fn empty_footer_payload() -> FileMetadata {
    FileMetadata {
        blobs: Vec::new(),
        properties: HashMap::new(),
    }
}

pub(crate) fn empty_footer_payload_bytes() -> Vec<u8> {
    serde_json::to_string::<FileMetadata>(&empty_footer_payload())
        .unwrap()
        .as_bytes()
        .to_vec()
}

pub(crate) fn empty_footer_payload_bytes_length_bytes() -> [u8; 4] {
    u32::to_le_bytes(empty_footer_payload_bytes().len() as u32)
}

pub(crate) const METRIC_BLOB_0_TYPE: &str = "some-blob";
pub(crate) const METRIC_BLOB_0_INPUT_FIELDS: [i32; 1] = [1];
pub(crate) const METRIC_BLOB_0_SNAPSHOT_ID: i64 = 2;
pub(crate) const METRIC_BLOB_0_SEQUENCE_NUMBER: i64 = 1;
pub(crate) const METRIC_BLOB_0_DATA: &str = "abcdefghi";

pub(crate) fn zstd_compressed_metric_blob_0_metadata() -> BlobMetadata {
    BlobMetadata {
        r#type: METRIC_BLOB_0_TYPE.to_string(),
        fields: METRIC_BLOB_0_INPUT_FIELDS.to_vec(),
        snapshot_id: METRIC_BLOB_0_SNAPSHOT_ID,
        sequence_number: METRIC_BLOB_0_SEQUENCE_NUMBER,
        offset: 4,
        length: 22,
        compression_codec: CompressionCodec::Zstd,
        properties: HashMap::new(),
    }
}

pub(crate) fn uncompressed_metric_blob_0_metadata() -> BlobMetadata {
    BlobMetadata {
        r#type: METRIC_BLOB_0_TYPE.to_string(),
        fields: METRIC_BLOB_0_INPUT_FIELDS.to_vec(),
        snapshot_id: METRIC_BLOB_0_SNAPSHOT_ID,
        sequence_number: METRIC_BLOB_0_SEQUENCE_NUMBER,
        offset: 4,
        length: 9,
        compression_codec: CompressionCodec::None,
        properties: HashMap::new(),
    }
}

pub(crate) fn blob_0() -> Blob {
    Blob::builder()
        .r#type(METRIC_BLOB_0_TYPE.to_string())
        .fields(METRIC_BLOB_0_INPUT_FIELDS.to_vec())
        .snapshot_id(METRIC_BLOB_0_SNAPSHOT_ID)
        .sequence_number(METRIC_BLOB_0_SEQUENCE_NUMBER)
        .data(METRIC_BLOB_0_DATA.as_bytes().to_vec())
        .properties(HashMap::new())
        .build()
}

pub(crate) const METRIC_BLOB_1_TYPE: &str = "some-other-blob";
pub(crate) const METRIC_BLOB_1_INPUT_FIELDS: [i32; 1] = [2];
pub(crate) const METRIC_BLOB_1_SNAPSHOT_ID: i64 = 2;
pub(crate) const METRIC_BLOB_1_SEQUENCE_NUMBER: i64 = 1;
pub(crate) const METRIC_BLOB_1_DATA: &str =
    "some blob \u{0000} binary data 🤯 that is not very very very very very very long, is it?";

pub(crate) fn uncompressed_metric_blob_1_metadata() -> BlobMetadata {
    BlobMetadata {
        r#type: METRIC_BLOB_1_TYPE.to_string(),
        fields: METRIC_BLOB_1_INPUT_FIELDS.to_vec(),
        snapshot_id: METRIC_BLOB_1_SNAPSHOT_ID,
        sequence_number: METRIC_BLOB_1_SEQUENCE_NUMBER,
        offset: 13,
        length: 83,
        compression_codec: CompressionCodec::None,
        properties: HashMap::new(),
    }
}

pub(crate) fn zstd_compressed_metric_blob_1_metadata() -> BlobMetadata {
    BlobMetadata {
        r#type: METRIC_BLOB_1_TYPE.to_string(),
        fields: METRIC_BLOB_1_INPUT_FIELDS.to_vec(),
        snapshot_id: METRIC_BLOB_1_SNAPSHOT_ID,
        sequence_number: METRIC_BLOB_1_SEQUENCE_NUMBER,
        offset: 26,
        length: 77,
        compression_codec: CompressionCodec::Zstd,
        properties: HashMap::new(),
    }
}

pub(crate) fn blob_1() -> Blob {
    Blob::builder()
        .r#type(METRIC_BLOB_1_TYPE.to_string())
        .fields(METRIC_BLOB_1_INPUT_FIELDS.to_vec())
        .snapshot_id(METRIC_BLOB_1_SNAPSHOT_ID)
        .sequence_number(METRIC_BLOB_1_SEQUENCE_NUMBER)
        .data(METRIC_BLOB_1_DATA.as_bytes().to_vec())
        .properties(HashMap::new())
        .build()
}

pub(crate) const CREATED_BY_PROPERTY_VALUE: &str = "Test 1234";

pub(crate) fn file_properties() -> HashMap<String, String> {
    let mut properties = HashMap::new();
    properties.insert(
        CREATED_BY_PROPERTY.to_string(),
        CREATED_BY_PROPERTY_VALUE.to_string(),
    );
    properties
}

pub(crate) fn uncompressed_metric_file_metadata() -> FileMetadata {
    FileMetadata {
        blobs: vec![
            uncompressed_metric_blob_0_metadata(),
            uncompressed_metric_blob_1_metadata(),
        ],
        properties: file_properties(),
    }
}

pub(crate) fn zstd_compressed_metric_file_metadata() -> FileMetadata {
    FileMetadata {
        blobs: vec![
            zstd_compressed_metric_blob_0_metadata(),
            zstd_compressed_metric_blob_1_metadata(),
        ],
        properties: file_properties(),
    }
}
