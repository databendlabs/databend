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

use std::collections::{HashMap, HashSet};

use bytes::Bytes;
use serde::{Deserialize, Serialize};

use crate::io::{FileRead, InputFile};
use crate::puffin::compression::CompressionCodec;
use crate::{Error, ErrorKind, Result};

/// Human-readable identification of the application writing the file, along with its version.
/// Example: "Trino version 381"
pub const CREATED_BY_PROPERTY: &str = "created-by";

/// Metadata about a blob.
/// For more information, see: https://iceberg.apache.org/puffin-spec/#blobmetadata
#[derive(Debug, PartialEq, Eq, Serialize, Deserialize, Clone)]
#[serde(rename_all = "kebab-case")]
pub struct BlobMetadata {
    pub(crate) r#type: String,
    pub(crate) fields: Vec<i32>,
    pub(crate) snapshot_id: i64,
    pub(crate) sequence_number: i64,
    pub(crate) offset: u64,
    pub(crate) length: u64,
    #[serde(skip_serializing_if = "CompressionCodec::is_none")]
    #[serde(default)]
    pub(crate) compression_codec: CompressionCodec,
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    #[serde(default)]
    pub(crate) properties: HashMap<String, String>,
}

impl BlobMetadata {
    #[inline]
    /// See blob types: https://iceberg.apache.org/puffin-spec/#blob-types
    pub fn blob_type(&self) -> &str {
        &self.r#type
    }

    #[inline]
    /// List of field IDs the blob was computed for; the order of items is used to compute sketches stored in the blob.
    pub fn fields(&self) -> &[i32] {
        &self.fields
    }

    #[inline]
    /// ID of the Iceberg table's snapshot the blob was computed from
    pub fn snapshot_id(&self) -> i64 {
        self.snapshot_id
    }

    #[inline]
    /// Sequence number of the Iceberg table's snapshot the blob was computed from
    pub fn sequence_number(&self) -> i64 {
        self.sequence_number
    }

    #[inline]
    /// The offset in the file where the blob contents start
    pub fn offset(&self) -> u64 {
        self.offset
    }

    #[inline]
    /// The length of the blob stored in the file (after compression, if compressed)
    pub fn length(&self) -> u64 {
        self.length
    }

    #[inline]
    /// The compression codec used to compress the data
    pub fn compression_codec(&self) -> CompressionCodec {
        self.compression_codec
    }

    #[inline]
    /// Arbitrary meta-information about the blob
    pub fn properties(&self) -> &HashMap<String, String> {
        &self.properties
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub(crate) enum Flag {
    FooterPayloadCompressed = 0,
}

impl Flag {
    pub(crate) fn byte_idx(self) -> u8 {
        (self as u8) / 8
    }

    pub(crate) fn bit_idx(self) -> u8 {
        (self as u8) % 8
    }

    fn matches(self, byte_idx: u8, bit_idx: u8) -> bool {
        self.byte_idx() == byte_idx && self.bit_idx() == bit_idx
    }

    fn from(byte_idx: u8, bit_idx: u8) -> Result<Flag> {
        if Flag::FooterPayloadCompressed.matches(byte_idx, bit_idx) {
            Ok(Flag::FooterPayloadCompressed)
        } else {
            Err(Error::new(
                ErrorKind::DataInvalid,
                format!("Unknown flag byte {byte_idx} and bit {bit_idx} combination"),
            ))
        }
    }
}

/// Metadata about a puffin file.
///
/// For more information, see: https://iceberg.apache.org/puffin-spec/#filemetadata
#[derive(Debug, PartialEq, Eq, Serialize, Deserialize, Clone)]
pub struct FileMetadata {
    pub(crate) blobs: Vec<BlobMetadata>,
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    #[serde(default)]
    pub(crate) properties: HashMap<String, String>,
}

impl FileMetadata {
    pub(crate) const MAGIC_LENGTH: u8 = 4;
    pub(crate) const MAGIC: [u8; FileMetadata::MAGIC_LENGTH as usize] = [0x50, 0x46, 0x41, 0x31];

    /// We use the term FOOTER_STRUCT to refer to the fixed-length portion of the Footer.
    /// The structure of the Footer specification is illustrated below:
    ///
    /// ```text                                             
    ///        Footer
    ///        ┌────────────────────┐                 
    ///        │  Magic (4 bytes)   │                 
    ///        │                    │                 
    ///        ├────────────────────┤                 
    ///        │   FooterPayload    │                 
    ///        │  (PAYLOAD_LENGTH)  │                 
    ///        ├────────────────────┤ ◀─┐             
    ///        │ FooterPayloadSize  │   │             
    ///        │     (4 bytes)      │   │             
    ///        ├────────────────────┤                 
    ///        │  Flags (4 bytes)   │  FOOTER_STRUCT  
    ///        │                    │                 
    ///        ├────────────────────┤   │             
    ///        │  Magic (4 bytes)   │   │             
    ///        │                    │   │             
    ///        └────────────────────┘ ◀─┘  
    /// ```                      
    const FOOTER_STRUCT_PAYLOAD_LENGTH_OFFSET: u8 = 0;
    const FOOTER_STRUCT_PAYLOAD_LENGTH_LENGTH: u8 = 4;
    const FOOTER_STRUCT_FLAGS_OFFSET: u8 = FileMetadata::FOOTER_STRUCT_PAYLOAD_LENGTH_OFFSET
        + FileMetadata::FOOTER_STRUCT_PAYLOAD_LENGTH_LENGTH;
    pub(crate) const FOOTER_STRUCT_FLAGS_LENGTH: u8 = 4;
    const FOOTER_STRUCT_MAGIC_OFFSET: u8 =
        FileMetadata::FOOTER_STRUCT_FLAGS_OFFSET + FileMetadata::FOOTER_STRUCT_FLAGS_LENGTH;
    pub(crate) const FOOTER_STRUCT_LENGTH: u8 =
        FileMetadata::FOOTER_STRUCT_MAGIC_OFFSET + FileMetadata::MAGIC_LENGTH;

    /// Constructs new puffin `FileMetadata`
    pub fn new(blobs: Vec<BlobMetadata>, properties: HashMap<String, String>) -> Self {
        Self { blobs, properties }
    }

    fn check_magic(bytes: &[u8]) -> Result<()> {
        if bytes == FileMetadata::MAGIC {
            Ok(())
        } else {
            Err(Error::new(
                ErrorKind::DataInvalid,
                format!(
                    "Bad magic value: {:?} should be {:?}",
                    bytes,
                    FileMetadata::MAGIC
                ),
            ))
        }
    }

    async fn read_footer_payload_length(
        file_read: &dyn FileRead,
        input_file_length: u64,
    ) -> Result<u32> {
        let start = input_file_length - FileMetadata::FOOTER_STRUCT_LENGTH as u64;
        let end = start + FileMetadata::FOOTER_STRUCT_PAYLOAD_LENGTH_LENGTH as u64;
        let footer_payload_length_bytes = file_read.read(start..end).await?;
        let mut buf = [0; 4];
        buf.copy_from_slice(&footer_payload_length_bytes);
        let footer_payload_length = u32::from_le_bytes(buf);
        Ok(footer_payload_length)
    }

    async fn read_footer_bytes(
        file_read: &dyn FileRead,
        input_file_length: u64,
        footer_payload_length: u32,
    ) -> Result<Bytes> {
        let footer_length = footer_payload_length as u64
            + FileMetadata::FOOTER_STRUCT_LENGTH as u64
            + FileMetadata::MAGIC_LENGTH as u64;
        let start = input_file_length - footer_length;
        let end = input_file_length;
        file_read.read(start..end).await
    }

    fn decode_flags(footer_bytes: &[u8]) -> Result<HashSet<Flag>> {
        let mut flags = HashSet::new();

        for byte_idx in 0..FileMetadata::FOOTER_STRUCT_FLAGS_LENGTH {
            let byte_offset = footer_bytes.len()
                - FileMetadata::MAGIC_LENGTH as usize
                - FileMetadata::FOOTER_STRUCT_FLAGS_LENGTH as usize
                + byte_idx as usize;

            let flag_byte = *footer_bytes.get(byte_offset).ok_or_else(|| {
                Error::new(ErrorKind::DataInvalid, "Index range is out of bounds.")
            })?;

            for bit_idx in 0..8 {
                if ((flag_byte >> bit_idx) & 1) != 0 {
                    let flag = Flag::from(byte_idx, bit_idx)?;
                    flags.insert(flag);
                }
            }
        }

        Ok(flags)
    }

    fn extract_footer_payload_as_str(
        footer_bytes: &[u8],
        footer_payload_length: u32,
    ) -> Result<String> {
        let flags = FileMetadata::decode_flags(footer_bytes)?;
        let footer_compression_codec = if flags.contains(&Flag::FooterPayloadCompressed) {
            CompressionCodec::Lz4
        } else {
            CompressionCodec::None
        };

        let start_offset = FileMetadata::MAGIC_LENGTH as usize;
        let end_offset =
            FileMetadata::MAGIC_LENGTH as usize + usize::try_from(footer_payload_length)?;
        let footer_payload_bytes = footer_bytes
            .get(start_offset..end_offset)
            .ok_or_else(|| Error::new(ErrorKind::DataInvalid, "Index range is out of bounds."))?;
        let decompressed_footer_payload_bytes =
            footer_compression_codec.decompress(footer_payload_bytes.into())?;

        String::from_utf8(decompressed_footer_payload_bytes).map_err(|src| {
            Error::new(ErrorKind::DataInvalid, "Footer is not a valid UTF-8 string")
                .with_source(src)
        })
    }

    fn from_json_str(string: &str) -> Result<FileMetadata> {
        serde_json::from_str::<FileMetadata>(string).map_err(|src| {
            Error::new(ErrorKind::DataInvalid, "Given string is not valid JSON").with_source(src)
        })
    }

    /// Returns the file metadata about a Puffin file
    pub(crate) async fn read(input_file: &InputFile) -> Result<FileMetadata> {
        let file_read = input_file.reader().await?;

        let first_four_bytes = file_read.read(0..FileMetadata::MAGIC_LENGTH.into()).await?;
        FileMetadata::check_magic(&first_four_bytes)?;

        let input_file_length = input_file.metadata().await?.size;
        let footer_payload_length =
            FileMetadata::read_footer_payload_length(&file_read, input_file_length).await?;
        let footer_bytes =
            FileMetadata::read_footer_bytes(&file_read, input_file_length, footer_payload_length)
                .await?;

        let magic_length = FileMetadata::MAGIC_LENGTH as usize;
        // check first four bytes of footer
        FileMetadata::check_magic(&footer_bytes[..magic_length])?;
        // check last four bytes of footer
        FileMetadata::check_magic(&footer_bytes[footer_bytes.len() - magic_length..])?;

        let footer_payload_str =
            FileMetadata::extract_footer_payload_as_str(&footer_bytes, footer_payload_length)?;

        FileMetadata::from_json_str(&footer_payload_str)
    }

    /// Reads file_metadata in puffin file with a prefetch hint
    ///
    /// `prefetch_hint` is used to try to fetch the entire footer in one read. If
    /// the entire footer isn't fetched in one read the function will call the regular
    /// read option.
    #[allow(dead_code)]
    pub(crate) async fn read_with_prefetch(
        input_file: &InputFile,
        prefetch_hint: u8,
    ) -> Result<FileMetadata> {
        if prefetch_hint > 16 {
            let input_file_length = input_file.metadata().await?.size;
            let file_read = input_file.reader().await?;

            // Hint cannot be larger than input file
            if prefetch_hint as u64 > input_file_length {
                return FileMetadata::read(input_file).await;
            }

            // Read footer based on prefetchi hint
            let start = input_file_length - prefetch_hint as u64;
            let end = input_file_length;
            let footer_bytes = file_read.read(start..end).await?;

            let payload_length_start =
                footer_bytes.len() - (FileMetadata::FOOTER_STRUCT_LENGTH as usize);
            let payload_length_end =
                payload_length_start + (FileMetadata::FOOTER_STRUCT_PAYLOAD_LENGTH_LENGTH as usize);
            let payload_length_bytes = &footer_bytes[payload_length_start..payload_length_end];

            let mut buf = [0; 4];
            buf.copy_from_slice(payload_length_bytes);
            let footer_payload_length = u32::from_le_bytes(buf);

            // If the (footer payload length + FOOTER_STRUCT_LENGTH + MAGIC_LENGTH) is greater
            // than the fetched footer then you can have it read regularly from a read with no
            // prefetch while passing in the footer_payload_length.
            let footer_length = (footer_payload_length as usize)
                + FileMetadata::FOOTER_STRUCT_LENGTH as usize
                + FileMetadata::MAGIC_LENGTH as usize;
            if footer_length > prefetch_hint as usize {
                return FileMetadata::read(input_file).await;
            }

            // Read footer bytes
            let footer_start = footer_bytes.len() - footer_length;
            let footer_end = footer_bytes.len();
            let footer_bytes = &footer_bytes[footer_start..footer_end];

            let magic_length = FileMetadata::MAGIC_LENGTH as usize;
            // check first four bytes of footer
            FileMetadata::check_magic(&footer_bytes[..magic_length])?;
            // check last four bytes of footer
            FileMetadata::check_magic(&footer_bytes[footer_bytes.len() - magic_length..])?;

            let footer_payload_str =
                FileMetadata::extract_footer_payload_as_str(footer_bytes, footer_payload_length)?;
            return FileMetadata::from_json_str(&footer_payload_str);
        }

        FileMetadata::read(input_file).await
    }

    #[inline]
    /// Metadata about blobs in file
    pub fn blobs(&self) -> &[BlobMetadata] {
        &self.blobs
    }

    #[inline]
    /// Arbitrary meta-information, like writer identification/version.
    pub fn properties(&self) -> &HashMap<String, String> {
        &self.properties
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use bytes::Bytes;
    use tempfile::TempDir;

    use crate::io::{FileIOBuilder, InputFile};
    use crate::puffin::metadata::{BlobMetadata, CompressionCodec, FileMetadata};
    use crate::puffin::test_utils::{
        empty_footer_payload, empty_footer_payload_bytes, empty_footer_payload_bytes_length_bytes,
        java_empty_uncompressed_input_file, java_uncompressed_metric_input_file,
        java_zstd_compressed_metric_input_file, uncompressed_metric_file_metadata,
        zstd_compressed_metric_file_metadata,
    };

    const INVALID_MAGIC_VALUE: [u8; 4] = [80, 70, 65, 0];

    async fn input_file_with_bytes(temp_dir: &TempDir, slice: &[u8]) -> InputFile {
        let file_io = FileIOBuilder::new_fs_io().build().unwrap();

        let path_buf = temp_dir.path().join("abc.puffin");
        let temp_path = path_buf.to_str().unwrap();
        let output_file = file_io.new_output(temp_path).unwrap();

        output_file
            .write(Bytes::copy_from_slice(slice))
            .await
            .unwrap();

        output_file.to_input_file()
    }

    async fn input_file_with_payload(temp_dir: &TempDir, payload_str: &str) -> InputFile {
        let payload_bytes = payload_str.as_bytes();

        let mut bytes = vec![];
        bytes.extend(FileMetadata::MAGIC.to_vec());
        bytes.extend(FileMetadata::MAGIC.to_vec());
        bytes.extend(payload_bytes);
        bytes.extend(u32::to_le_bytes(payload_bytes.len() as u32));
        bytes.extend(vec![0, 0, 0, 0]);
        bytes.extend(FileMetadata::MAGIC);

        input_file_with_bytes(temp_dir, &bytes).await
    }

    #[tokio::test]
    async fn test_file_starting_with_invalid_magic_returns_error() {
        let temp_dir = TempDir::new().unwrap();

        let mut bytes = vec![];
        bytes.extend(INVALID_MAGIC_VALUE.to_vec());
        bytes.extend(FileMetadata::MAGIC.to_vec());
        bytes.extend(empty_footer_payload_bytes());
        bytes.extend(empty_footer_payload_bytes_length_bytes());
        bytes.extend(vec![0, 0, 0, 0]);
        bytes.extend(FileMetadata::MAGIC);

        let input_file = input_file_with_bytes(&temp_dir, &bytes).await;

        assert_eq!(
            FileMetadata::read(&input_file)
                .await
                .unwrap_err()
                .to_string(),
            "DataInvalid => Bad magic value: [80, 70, 65, 0] should be [80, 70, 65, 49]",
        )
    }

    #[tokio::test]
    async fn test_file_with_invalid_magic_at_start_of_footer_returns_error() {
        let temp_dir = TempDir::new().unwrap();

        let mut bytes = vec![];
        bytes.extend(FileMetadata::MAGIC.to_vec());
        bytes.extend(INVALID_MAGIC_VALUE.to_vec());
        bytes.extend(empty_footer_payload_bytes());
        bytes.extend(empty_footer_payload_bytes_length_bytes());
        bytes.extend(vec![0, 0, 0, 0]);
        bytes.extend(FileMetadata::MAGIC);

        let input_file = input_file_with_bytes(&temp_dir, &bytes).await;

        assert_eq!(
            FileMetadata::read(&input_file)
                .await
                .unwrap_err()
                .to_string(),
            "DataInvalid => Bad magic value: [80, 70, 65, 0] should be [80, 70, 65, 49]",
        )
    }

    #[tokio::test]
    async fn test_file_ending_with_invalid_magic_returns_error() {
        let temp_dir = TempDir::new().unwrap();

        let mut bytes = vec![];
        bytes.extend(FileMetadata::MAGIC.to_vec());
        bytes.extend(FileMetadata::MAGIC.to_vec());
        bytes.extend(empty_footer_payload_bytes());
        bytes.extend(empty_footer_payload_bytes_length_bytes());
        bytes.extend(vec![0, 0, 0, 0]);
        bytes.extend(INVALID_MAGIC_VALUE);

        let input_file = input_file_with_bytes(&temp_dir, &bytes).await;

        assert_eq!(
            FileMetadata::read(&input_file)
                .await
                .unwrap_err()
                .to_string(),
            "DataInvalid => Bad magic value: [80, 70, 65, 0] should be [80, 70, 65, 49]",
        )
    }

    #[tokio::test]
    async fn test_encoded_payload_length_larger_than_actual_payload_length_returns_error() {
        let temp_dir = TempDir::new().unwrap();

        let mut bytes = vec![];
        bytes.extend(FileMetadata::MAGIC.to_vec());
        bytes.extend(FileMetadata::MAGIC.to_vec());
        bytes.extend(empty_footer_payload_bytes());
        bytes.extend(u32::to_le_bytes(
            empty_footer_payload_bytes().len() as u32 + 1,
        ));
        bytes.extend(vec![0, 0, 0, 0]);
        bytes.extend(FileMetadata::MAGIC.to_vec());

        let input_file = input_file_with_bytes(&temp_dir, &bytes).await;

        assert_eq!(
            FileMetadata::read(&input_file)
                .await
                .unwrap_err()
                .to_string(),
            "DataInvalid => Bad magic value: [49, 80, 70, 65] should be [80, 70, 65, 49]",
        )
    }

    #[tokio::test]
    async fn test_encoded_payload_length_smaller_than_actual_payload_length_returns_error() {
        let temp_dir = TempDir::new().unwrap();

        let mut bytes = vec![];
        bytes.extend(FileMetadata::MAGIC.to_vec());
        bytes.extend(FileMetadata::MAGIC.to_vec());
        bytes.extend(empty_footer_payload_bytes());
        bytes.extend(u32::to_le_bytes(
            empty_footer_payload_bytes().len() as u32 - 1,
        ));
        bytes.extend(vec![0, 0, 0, 0]);
        bytes.extend(FileMetadata::MAGIC.to_vec());

        let input_file = input_file_with_bytes(&temp_dir, &bytes).await;

        assert_eq!(
            FileMetadata::read(&input_file)
                .await
                .unwrap_err()
                .to_string(),
            "DataInvalid => Bad magic value: [70, 65, 49, 123] should be [80, 70, 65, 49]",
        )
    }

    #[tokio::test]
    async fn test_lz4_compressed_footer_returns_error() {
        let temp_dir = TempDir::new().unwrap();

        let mut bytes = vec![];
        bytes.extend(FileMetadata::MAGIC.to_vec());
        bytes.extend(FileMetadata::MAGIC.to_vec());
        bytes.extend(empty_footer_payload_bytes());
        bytes.extend(empty_footer_payload_bytes_length_bytes());
        bytes.extend(vec![0b00000001, 0, 0, 0]);
        bytes.extend(FileMetadata::MAGIC.to_vec());

        let input_file = input_file_with_bytes(&temp_dir, &bytes).await;

        assert_eq!(
            FileMetadata::read(&input_file)
                .await
                .unwrap_err()
                .to_string(),
            "FeatureUnsupported => LZ4 decompression is not supported currently",
        )
    }

    #[tokio::test]
    async fn test_unknown_byte_bit_combination_returns_error() {
        let temp_dir = TempDir::new().unwrap();

        let mut bytes = vec![];
        bytes.extend(FileMetadata::MAGIC.to_vec());
        bytes.extend(FileMetadata::MAGIC.to_vec());
        bytes.extend(empty_footer_payload_bytes());
        bytes.extend(empty_footer_payload_bytes_length_bytes());
        bytes.extend(vec![0b00000010, 0, 0, 0]);
        bytes.extend(FileMetadata::MAGIC.to_vec());

        let input_file = input_file_with_bytes(&temp_dir, &bytes).await;

        assert_eq!(
            FileMetadata::read(&input_file)
                .await
                .unwrap_err()
                .to_string(),
            "DataInvalid => Unknown flag byte 0 and bit 1 combination",
        )
    }

    #[tokio::test]
    async fn test_non_utf8_string_payload_returns_error() {
        let temp_dir = TempDir::new().unwrap();

        let payload_bytes: [u8; 4] = [0, 159, 146, 150];
        let payload_bytes_length_bytes: [u8; 4] = u32::to_le_bytes(payload_bytes.len() as u32);

        let mut bytes = vec![];
        bytes.extend(FileMetadata::MAGIC.to_vec());
        bytes.extend(FileMetadata::MAGIC.to_vec());
        bytes.extend(payload_bytes);
        bytes.extend(payload_bytes_length_bytes);
        bytes.extend(vec![0, 0, 0, 0]);
        bytes.extend(FileMetadata::MAGIC.to_vec());

        let input_file = input_file_with_bytes(&temp_dir, &bytes).await;

        assert_eq!(
            FileMetadata::read(&input_file)
                .await
                .unwrap_err()
                .to_string(),
            "DataInvalid => Footer is not a valid UTF-8 string, source: invalid utf-8 sequence of 1 bytes from index 1",
        )
    }

    #[tokio::test]
    async fn test_minimal_valid_file_returns_file_metadata() {
        let temp_dir = TempDir::new().unwrap();

        let mut bytes = vec![];
        bytes.extend(FileMetadata::MAGIC.to_vec());
        bytes.extend(FileMetadata::MAGIC.to_vec());
        bytes.extend(empty_footer_payload_bytes());
        bytes.extend(empty_footer_payload_bytes_length_bytes());
        bytes.extend(vec![0, 0, 0, 0]);
        bytes.extend(FileMetadata::MAGIC);

        let input_file = input_file_with_bytes(&temp_dir, &bytes).await;

        assert_eq!(
            FileMetadata::read(&input_file).await.unwrap(),
            FileMetadata {
                blobs: vec![],
                properties: HashMap::new(),
            }
        )
    }

    #[tokio::test]
    async fn test_returns_file_metadata_property() {
        let temp_dir = TempDir::new().unwrap();

        let input_file = input_file_with_payload(
            &temp_dir,
            r#"{
                "blobs" : [ ],
                "properties" : {
                    "a property" : "a property value"
                }
            }"#,
        )
        .await;

        assert_eq!(
            FileMetadata::read(&input_file).await.unwrap(),
            FileMetadata {
                blobs: vec![],
                properties: {
                    let mut map = HashMap::new();
                    map.insert("a property".to_string(), "a property value".to_string());
                    map
                },
            }
        )
    }

    #[tokio::test]
    async fn test_returns_file_metadata_properties() {
        let temp_dir = TempDir::new().unwrap();

        let input_file = input_file_with_payload(
            &temp_dir,
            r#"{
                "blobs" : [ ],
                "properties" : {
                    "a property" : "a property value",
                    "another one": "also with value"
                }
            }"#,
        )
        .await;

        assert_eq!(
            FileMetadata::read(&input_file).await.unwrap(),
            FileMetadata {
                blobs: vec![],
                properties: {
                    let mut map = HashMap::new();
                    map.insert("a property".to_string(), "a property value".to_string());
                    map.insert("another one".to_string(), "also with value".to_string());
                    map
                },
            }
        )
    }

    #[tokio::test]
    async fn test_returns_error_if_blobs_field_is_missing() {
        let temp_dir = TempDir::new().unwrap();

        let input_file = input_file_with_payload(
            &temp_dir,
            r#"{
                "properties" : {}
            }"#,
        )
        .await;

        assert_eq!(
            FileMetadata::read(&input_file)
                .await
                .unwrap_err()
                .to_string(),
            format!(
                "DataInvalid => Given string is not valid JSON, source: missing field `blobs` at line 3 column 13"
            ),
        )
    }

    #[tokio::test]
    async fn test_returns_error_if_blobs_field_is_bad() {
        let temp_dir = TempDir::new().unwrap();

        let input_file = input_file_with_payload(
            &temp_dir,
            r#"{
                "blobs" : {}
            }"#,
        )
        .await;

        assert_eq!(
            FileMetadata::read(&input_file)
                .await
                .unwrap_err()
                .to_string(),
            format!(
                "DataInvalid => Given string is not valid JSON, source: invalid type: map, expected a sequence at line 2 column 26"
            ),
        )
    }

    #[tokio::test]
    async fn test_returns_blobs_metadatas() {
        let temp_dir = TempDir::new().unwrap();

        let input_file = input_file_with_payload(
            &temp_dir,
            r#"{
                "blobs" : [
                    {
                        "type" : "type-a",
                        "fields" : [ 1 ],
                        "snapshot-id" : 14,
                        "sequence-number" : 3,
                        "offset" : 4,
                        "length" : 16
                    },
                    {
                        "type" : "type-bbb",
                        "fields" : [ 2, 3, 4 ],
                        "snapshot-id" : 77,
                        "sequence-number" : 4,
                        "offset" : 21474836470000,
                        "length" : 79834
                    }
                ]
            }"#,
        )
        .await;

        assert_eq!(
            FileMetadata::read(&input_file).await.unwrap(),
            FileMetadata {
                blobs: vec![
                    BlobMetadata {
                        r#type: "type-a".to_string(),
                        fields: vec![1],
                        snapshot_id: 14,
                        sequence_number: 3,
                        offset: 4,
                        length: 16,
                        compression_codec: CompressionCodec::None,
                        properties: HashMap::new(),
                    },
                    BlobMetadata {
                        r#type: "type-bbb".to_string(),
                        fields: vec![2, 3, 4],
                        snapshot_id: 77,
                        sequence_number: 4,
                        offset: 21474836470000,
                        length: 79834,
                        compression_codec: CompressionCodec::None,
                        properties: HashMap::new(),
                    },
                ],
                properties: HashMap::new(),
            }
        )
    }

    #[tokio::test]
    async fn test_returns_properties_in_blob_metadata() {
        let temp_dir = TempDir::new().unwrap();

        let input_file = input_file_with_payload(
            &temp_dir,
            r#"{
                "blobs" : [
                    {
                        "type" : "type-a",
                        "fields" : [ 1 ],
                        "snapshot-id" : 14,
                        "sequence-number" : 3,
                        "offset" : 4,
                        "length" : 16,
                        "properties" : {
                            "some key" : "some value"
                        }
                    }
                ]
            }"#,
        )
        .await;

        assert_eq!(
            FileMetadata::read(&input_file).await.unwrap(),
            FileMetadata {
                blobs: vec![BlobMetadata {
                    r#type: "type-a".to_string(),
                    fields: vec![1],
                    snapshot_id: 14,
                    sequence_number: 3,
                    offset: 4,
                    length: 16,
                    compression_codec: CompressionCodec::None,
                    properties: {
                        let mut map = HashMap::new();
                        map.insert("some key".to_string(), "some value".to_string());
                        map
                    },
                }],
                properties: HashMap::new(),
            }
        )
    }

    #[tokio::test]
    async fn test_returns_error_if_blobs_fields_value_is_outside_i32_range() {
        let temp_dir = TempDir::new().unwrap();

        let out_of_i32_range_number: i64 = i32::MAX as i64 + 1;

        let input_file = input_file_with_payload(
            &temp_dir,
            &format!(
                r#"{{
                    "blobs" : [
                        {{
                            "type" : "type-a",
                            "fields" : [ {out_of_i32_range_number} ],
                            "snapshot-id" : 14,
                            "sequence-number" : 3,
                            "offset" : 4,
                            "length" : 16
                        }}
                    ]
                }}"#
            ),
        )
        .await;

        assert_eq!(
            FileMetadata::read(&input_file)
                .await
                .unwrap_err()
                .to_string(),
            format!(
                "DataInvalid => Given string is not valid JSON, source: invalid value: integer `{out_of_i32_range_number}`, expected i32 at line 5 column 51"
            ),
        )
    }

    #[tokio::test]
    async fn test_returns_errors_if_footer_payload_is_not_encoded_in_json_format() {
        let temp_dir = TempDir::new().unwrap();

        let input_file = input_file_with_payload(&temp_dir, r#""blobs" = []"#).await;

        assert_eq!(
            FileMetadata::read(&input_file)
                .await
                .unwrap_err()
                .to_string(),
            "DataInvalid => Given string is not valid JSON, source: invalid type: string \"blobs\", expected struct FileMetadata at line 1 column 7",
        )
    }

    #[tokio::test]
    async fn test_read_file_metadata_of_uncompressed_empty_file() {
        let input_file = java_empty_uncompressed_input_file();

        let file_metadata = FileMetadata::read(&input_file).await.unwrap();
        assert_eq!(file_metadata, empty_footer_payload())
    }

    #[tokio::test]
    async fn test_read_file_metadata_of_uncompressed_metric_data() {
        let input_file = java_uncompressed_metric_input_file();

        let file_metadata = FileMetadata::read(&input_file).await.unwrap();
        assert_eq!(file_metadata, uncompressed_metric_file_metadata())
    }

    #[tokio::test]
    async fn test_read_file_metadata_of_zstd_compressed_metric_data() {
        let input_file = java_zstd_compressed_metric_input_file();

        let file_metadata = FileMetadata::read_with_prefetch(&input_file, 64)
            .await
            .unwrap();
        assert_eq!(file_metadata, zstd_compressed_metric_file_metadata())
    }

    #[tokio::test]
    async fn test_read_file_metadata_of_empty_file_with_prefetching() {
        let input_file = java_empty_uncompressed_input_file();
        let file_metadata = FileMetadata::read_with_prefetch(&input_file, 64)
            .await
            .unwrap();

        assert_eq!(file_metadata, empty_footer_payload());
    }

    #[tokio::test]
    async fn test_read_file_metadata_of_uncompressed_metric_data_with_prefetching() {
        let input_file = java_uncompressed_metric_input_file();
        let file_metadata = FileMetadata::read_with_prefetch(&input_file, 64)
            .await
            .unwrap();

        assert_eq!(file_metadata, uncompressed_metric_file_metadata());
    }

    #[tokio::test]
    async fn test_read_file_metadata_of_zstd_compressed_metric_data_with_prefetching() {
        let input_file = java_zstd_compressed_metric_input_file();
        let file_metadata = FileMetadata::read_with_prefetch(&input_file, 64)
            .await
            .unwrap();

        assert_eq!(file_metadata, zstd_compressed_metric_file_metadata());
    }
}
