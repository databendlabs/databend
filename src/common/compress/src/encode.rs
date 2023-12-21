// Copyright 2021 Datafuse Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::io::Result;

use async_compression::codec::BrotliEncoder;
use async_compression::codec::BzEncoder;
use async_compression::codec::DeflateEncoder;
use async_compression::codec::Encode;
use async_compression::codec::GzipEncoder;
use async_compression::codec::LzmaEncoder;
use async_compression::codec::XzEncoder;
use async_compression::codec::ZlibEncoder;
use async_compression::codec::ZstdEncoder;
use async_compression::util::PartialBuffer;
use async_compression::Level;
use brotli::enc::backward_references::BrotliEncoderParams;
use databend_common_exception::ErrorCode;

use crate::CompressAlgorithm;

#[derive(Debug)]
pub enum CompressCodec {
    /// Encoder for [`CompressAlgorithm::Brotli`]
    ///
    /// BrotliEncoder is too large that is 2592 bytes
    /// Wrap into box to reduce the total size of the enum
    Brotli(Box<BrotliEncoder>),
    /// Encoder for [`CompressAlgorithm::Bz2`]
    Bz2(BzEncoder),
    /// Encoder for [`CompressAlgorithm::Deflate`]
    Deflate(DeflateEncoder),
    /// Encoder for [`CompressAlgorithm::Gzip`]
    Gzip(GzipEncoder),
    /// Encoder for [`CompressAlgorithm::Lzma`]
    Lzma(LzmaEncoder),
    /// Encoder for [`CompressAlgorithm::Xz`]
    Xz(XzEncoder),
    /// Encoder for [`CompressAlgorithm::Zlib`]
    Zlib(ZlibEncoder),
    /// Encoder for [`CompressAlgorithm::Zstd`]
    Zstd(ZstdEncoder),
}

impl From<CompressAlgorithm> for CompressCodec {
    fn from(v: CompressAlgorithm) -> Self {
        match v {
            CompressAlgorithm::Brotli => {
                CompressCodec::Brotli(Box::new(BrotliEncoder::new(BrotliEncoderParams::default())))
            }
            CompressAlgorithm::Bz2 => {
                CompressCodec::Bz2(BzEncoder::new(Level::Default.into_bzip2(), 0))
            }
            CompressAlgorithm::Deflate => {
                CompressCodec::Deflate(DeflateEncoder::new(Level::Default.into_flate2()))
            }
            CompressAlgorithm::Gzip => {
                CompressCodec::Gzip(GzipEncoder::new(Level::Default.into_flate2()))
            }
            CompressAlgorithm::Lzma => {
                CompressCodec::Lzma(LzmaEncoder::new(Level::Default.into_xz2()))
            }
            CompressAlgorithm::Xz => CompressCodec::Xz(XzEncoder::new(Level::Default.into_xz2())),
            CompressAlgorithm::Zlib => {
                CompressCodec::Zlib(ZlibEncoder::new(Level::Default.into_flate2()))
            }
            CompressAlgorithm::Zstd => {
                CompressCodec::Zstd(ZstdEncoder::new(Level::Default.into_zstd()))
            }
        }
    }
}

impl Encode for CompressCodec {
    fn encode(
        &mut self,
        input: &mut PartialBuffer<impl AsRef<[u8]>>,
        output: &mut PartialBuffer<impl AsRef<[u8]> + AsMut<[u8]>>,
    ) -> Result<()> {
        match self {
            CompressCodec::Brotli(v) => v.encode(input, output),
            CompressCodec::Bz2(v) => v.encode(input, output),
            CompressCodec::Deflate(v) => v.encode(input, output),
            CompressCodec::Gzip(v) => v.encode(input, output),
            CompressCodec::Lzma(v) => v.encode(input, output),
            CompressCodec::Xz(v) => v.encode(input, output),
            CompressCodec::Zlib(v) => v.encode(input, output),
            CompressCodec::Zstd(v) => v.encode(input, output),
        }
    }

    fn flush(
        &mut self,
        output: &mut PartialBuffer<impl AsRef<[u8]> + AsMut<[u8]>>,
    ) -> Result<bool> {
        match self {
            CompressCodec::Brotli(v) => v.flush(output),
            CompressCodec::Bz2(v) => v.flush(output),
            CompressCodec::Deflate(v) => v.flush(output),
            CompressCodec::Gzip(v) => v.flush(output),
            CompressCodec::Lzma(v) => v.flush(output),
            CompressCodec::Xz(v) => v.flush(output),
            CompressCodec::Zlib(v) => v.flush(output),
            CompressCodec::Zstd(v) => v.flush(output),
        }
    }

    fn finish(
        &mut self,
        output: &mut PartialBuffer<impl AsRef<[u8]> + AsMut<[u8]>>,
    ) -> Result<bool> {
        match self {
            CompressCodec::Brotli(v) => v.finish(output),
            CompressCodec::Bz2(v) => v.finish(output),
            CompressCodec::Deflate(v) => v.finish(output),
            CompressCodec::Gzip(v) => v.finish(output),
            CompressCodec::Lzma(v) => v.finish(output),
            CompressCodec::Xz(v) => v.finish(output),
            CompressCodec::Zlib(v) => v.finish(output),
            CompressCodec::Zstd(v) => v.finish(output),
        }
    }
}

impl CompressCodec {
    pub fn compress_all(
        &mut self,
        to_compress: &[u8],
    ) -> databend_common_exception::Result<Vec<u8>> {
        let mut compress_bufs = vec![];
        let mut input = PartialBuffer::new(to_compress);
        let buf_size = to_compress.len().min(4096);

        loop {
            let mut output = PartialBuffer::new(vec![0u8; buf_size]);
            self.encode(&mut input, &mut output).map_err(|e| {
                ErrorCode::InvalidCompressionData(format!("compression data invalid: {e}"))
            })?;
            let written = output.written().len();
            if written > 0 {
                let mut output = output.into_inner();
                output.truncate(written);
                compress_bufs.push(output);
            }
            if input.unwritten().is_empty() {
                break;
            }
        }

        loop {
            let mut output = PartialBuffer::new(vec![0u8; buf_size]);
            let finished = self.finish(&mut output).map_err(|e| {
                ErrorCode::InvalidCompressionData(format!("compression data invalid: {e}"))
            })?;
            let written = output.written().len();
            if written > 0 {
                let mut output = output.into_inner();
                output.truncate(written);
                compress_bufs.push(output);
            }
            if finished {
                break;
            }
        }
        Ok(compress_bufs.concat())
    }
}

#[cfg(test)]
mod tests {
    use rand::prelude::*;

    use super::*;
    use crate::DecompressDecoder;

    #[tokio::test]
    async fn test_decompress_bytes_zlib() -> databend_common_exception::Result<()> {
        let _ = env_logger::try_init();

        let mut rng = ThreadRng::default();
        let size = rng.gen_range(1..16 * 1024);
        let mut content = vec![0; size];
        rng.fill_bytes(&mut content);
        for algo in [
            CompressAlgorithm::Zlib,
            CompressAlgorithm::Gzip,
            CompressAlgorithm::Bz2,
            CompressAlgorithm::Zstd,
            CompressAlgorithm::Deflate,
            CompressAlgorithm::Xz,
            CompressAlgorithm::Lzma,
        ] {
            let mut encoder = CompressCodec::from(algo);
            let compressed = encoder.compress_all(&content)?;
            let mut decoder = DecompressDecoder::new(algo);
            let decompressed = decoder.decompress_all(&compressed)?;
            assert_eq!(
                decompressed,
                content,
                "fail to compress {algo:?}, {} {} {}",
                size,
                compressed.len(),
                decompressed.len()
            );
        }

        Ok(())
    }
}
