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

use serde::{Deserialize, Serialize};

use crate::{Error, ErrorKind, Result};

/// Data compression formats
#[derive(Debug, PartialEq, Eq, Clone, Copy, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum CompressionCodec {
    #[default]
    /// No compression
    None,
    /// LZ4 single compression frame with content size present
    Lz4,
    /// Zstandard single compression frame with content size present
    Zstd,
}

impl CompressionCodec {
    pub(crate) fn decompress(&self, bytes: Vec<u8>) -> Result<Vec<u8>> {
        match self {
            CompressionCodec::None => Ok(bytes),
            CompressionCodec::Lz4 => Err(Error::new(
                ErrorKind::FeatureUnsupported,
                "LZ4 decompression is not supported currently",
            )),
            CompressionCodec::Zstd => {
                let decompressed = zstd::stream::decode_all(&bytes[..])?;
                Ok(decompressed)
            }
        }
    }

    pub(crate) fn compress(&self, bytes: Vec<u8>) -> Result<Vec<u8>> {
        match self {
            CompressionCodec::None => Ok(bytes),
            CompressionCodec::Lz4 => Err(Error::new(
                ErrorKind::FeatureUnsupported,
                "LZ4 compression is not supported currently",
            )),
            CompressionCodec::Zstd => {
                let writer = Vec::<u8>::new();
                let mut encoder = zstd::stream::Encoder::new(writer, 3)?;
                encoder.include_checksum(true)?;
                encoder.set_pledged_src_size(Some(bytes.len().try_into()?))?;
                std::io::copy(&mut &bytes[..], &mut encoder)?;
                let compressed = encoder.finish()?;
                Ok(compressed)
            }
        }
    }

    pub(crate) fn is_none(&self) -> bool {
        matches!(self, CompressionCodec::None)
    }
}

#[cfg(test)]
mod tests {
    use crate::puffin::compression::CompressionCodec;

    #[tokio::test]
    async fn test_compression_codec_none() {
        let compression_codec = CompressionCodec::None;
        let bytes_vec = [0_u8; 100].to_vec();

        let compressed = compression_codec.compress(bytes_vec.clone()).unwrap();
        assert_eq!(bytes_vec, compressed);

        let decompressed = compression_codec.decompress(compressed.clone()).unwrap();
        assert_eq!(compressed, decompressed)
    }

    #[tokio::test]
    async fn test_compression_codec_lz4() {
        let compression_codec = CompressionCodec::Lz4;
        let bytes_vec = [0_u8; 100].to_vec();

        assert_eq!(
            compression_codec
                .compress(bytes_vec.clone())
                .unwrap_err()
                .to_string(),
            "FeatureUnsupported => LZ4 compression is not supported currently",
        );

        assert_eq!(
            compression_codec
                .decompress(bytes_vec.clone())
                .unwrap_err()
                .to_string(),
            "FeatureUnsupported => LZ4 decompression is not supported currently",
        )
    }

    #[tokio::test]
    async fn test_compression_codec_zstd() {
        let compression_codec = CompressionCodec::Zstd;
        let bytes_vec = [0_u8; 100].to_vec();

        let compressed = compression_codec.compress(bytes_vec.clone()).unwrap();
        assert!(compressed.len() < bytes_vec.len());

        let decompressed = compression_codec.decompress(compressed.clone()).unwrap();
        assert_eq!(decompressed, bytes_vec)
    }
}
