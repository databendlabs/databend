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
use std::path::PathBuf;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;

use async_compression::codec::BrotliDecoder;
use async_compression::codec::BzDecoder;
use async_compression::codec::Decode;
use async_compression::codec::DeflateDecoder;
use async_compression::codec::GzipDecoder;
use async_compression::codec::LzmaDecoder;
use async_compression::codec::XzDecoder;
use async_compression::codec::ZlibDecoder;
use async_compression::codec::ZstdDecoder;
use async_compression::util::PartialBuffer;
use bytes::Buf;
use bytes::BytesMut;
use futures::io::BufReader;
use futures::ready;
use futures::AsyncBufRead;
use futures::AsyncRead;
use log::trace;
use pin_project::pin_project;
use serde::Deserialize;
use serde::Serialize;

/// CompressAlgorithm represents all compress algorithm that OpenDAL supports.
#[derive(Serialize, Deserialize, Copy, Clone, Eq, PartialEq, Debug)]
pub enum CompressAlgorithm {
    /// [Brotli](https://github.com/google/brotli) compression format.
    Brotli,
    /// [bzip2](http://sourceware.org/bzip2/) compression format.
    Bz2,
    /// [Deflate](https://datatracker.ietf.org/doc/html/rfc1951) Compressed Data Format.
    ///
    /// Similar to [`CompressAlgorithm::Gzip`] and [`CompressAlgorithm::Zlib`]
    Deflate,
    /// [Gzip](https://datatracker.ietf.org/doc/html/rfc1952) compress format.
    ///
    /// Similar to [`CompressAlgorithm::Deflate`] and [`CompressAlgorithm::Zlib`]
    Gzip,
    /// [LZMA](https://www.7-zip.org/sdk.html) compress format.
    Lzma,
    /// [Xz](https://tukaani.org/xz/) compress format, the successor of [`CompressAlgorithm::Lzma`].
    Xz,
    /// [Zlib](https://datatracker.ietf.org/doc/html/rfc1950) compress format.
    ///
    /// Similar to [`CompressAlgorithm::Deflate`] and [`CompressAlgorithm::Gzip`]
    Zlib,
    /// [Zstd](https://github.com/facebook/zstd) compression algorithm
    Zstd,
}

impl CompressAlgorithm {
    /// Get the file extension of this compress algorithm.
    pub fn extension(&self) -> &str {
        match self {
            CompressAlgorithm::Brotli => "br",
            CompressAlgorithm::Bz2 => "bz2",
            CompressAlgorithm::Deflate => "deflate",
            CompressAlgorithm::Gzip => "gz",
            CompressAlgorithm::Lzma => "lzma",
            CompressAlgorithm::Xz => "xz",
            CompressAlgorithm::Zlib => "zl",
            CompressAlgorithm::Zstd => "zstd",
        }
    }

    /// Create CompressAlgorithm from file extension.
    ///
    /// If the file extension is not supported, `None` will be return instead.
    pub fn from_extension(ext: &str) -> Option<CompressAlgorithm> {
        match ext {
            "br" => Some(CompressAlgorithm::Brotli),
            "bz2" => Some(CompressAlgorithm::Bz2),
            "deflate" => Some(CompressAlgorithm::Deflate),
            "gz" => Some(CompressAlgorithm::Gzip),
            "lzma" => Some(CompressAlgorithm::Lzma),
            "xz" => Some(CompressAlgorithm::Xz),
            "zl" => Some(CompressAlgorithm::Zlib),
            "zstd" | "zst" => Some(CompressAlgorithm::Zstd),
            _ => None,
        }
    }

    /// Create CompressAlgorithm from file path.
    ///
    /// If the extension in file path is not supported, `None` will be return instead.
    pub fn from_path(path: &str) -> Option<CompressAlgorithm> {
        let ext = PathBuf::from(path)
            .extension()
            .map(|s| s.to_string_lossy())?
            .to_string();

        CompressAlgorithm::from_extension(&ext)
    }
}

impl From<CompressAlgorithm> for DecompressCodec {
    fn from(v: CompressAlgorithm) -> Self {
        match v {
            CompressAlgorithm::Brotli => DecompressCodec::Brotli(Box::new(BrotliDecoder::new())),
            CompressAlgorithm::Bz2 => DecompressCodec::Bz2(BzDecoder::new()),
            CompressAlgorithm::Deflate => DecompressCodec::Deflate(DeflateDecoder::new()),
            CompressAlgorithm::Gzip => DecompressCodec::Gzip(GzipDecoder::new()),
            CompressAlgorithm::Lzma => DecompressCodec::Lzma(LzmaDecoder::new()),
            CompressAlgorithm::Xz => DecompressCodec::Xz(XzDecoder::new()),
            CompressAlgorithm::Zlib => DecompressCodec::Zlib(ZlibDecoder::new()),
            CompressAlgorithm::Zstd => DecompressCodec::Zstd(ZstdDecoder::new()),
        }
    }
}

#[derive(Debug)]
pub enum DecompressCodec {
    /// Decoder for [`CompressAlgorithm::Brotli`]
    ///
    /// BrotliDecoder is too large that is 2592 bytes
    /// Wrap into box to reduce the total size of the enum
    Brotli(Box<BrotliDecoder>),
    /// Decoder for [`CompressAlgorithm::Bz2`]
    Bz2(BzDecoder),
    /// Decoder for [`CompressAlgorithm::Deflate`]
    Deflate(DeflateDecoder),
    /// Decoder for [`CompressAlgorithm::Gzip`]
    Gzip(GzipDecoder),
    /// Decoder for [`CompressAlgorithm::Lzma`]
    Lzma(LzmaDecoder),
    /// Decoder for [`CompressAlgorithm::Xz`]
    Xz(XzDecoder),
    /// Decoder for [`CompressAlgorithm::Zlib`]
    Zlib(ZlibDecoder),
    /// Decoder for [`CompressAlgorithm::Zstd`]
    Zstd(ZstdDecoder),
}

impl Decode for DecompressCodec {
    fn reinit(&mut self) -> Result<()> {
        match self {
            DecompressCodec::Brotli(v) => v.reinit(),
            DecompressCodec::Bz2(v) => v.reinit(),
            DecompressCodec::Deflate(v) => v.reinit(),
            DecompressCodec::Gzip(v) => v.reinit(),
            DecompressCodec::Lzma(v) => v.reinit(),
            DecompressCodec::Xz(v) => v.reinit(),
            DecompressCodec::Zlib(v) => v.reinit(),
            DecompressCodec::Zstd(v) => v.reinit(),
        }
    }

    fn decode(
        &mut self,
        input: &mut PartialBuffer<impl AsRef<[u8]>>,
        output: &mut PartialBuffer<impl AsRef<[u8]> + AsMut<[u8]>>,
    ) -> Result<bool> {
        match self {
            DecompressCodec::Brotli(v) => v.decode(input, output),
            DecompressCodec::Bz2(v) => v.decode(input, output),
            DecompressCodec::Deflate(v) => v.decode(input, output),
            DecompressCodec::Gzip(v) => v.decode(input, output),
            DecompressCodec::Lzma(v) => v.decode(input, output),
            DecompressCodec::Xz(v) => v.decode(input, output),
            DecompressCodec::Zlib(v) => v.decode(input, output),
            DecompressCodec::Zstd(v) => v.decode(input, output),
        }
    }

    fn flush(
        &mut self,
        output: &mut PartialBuffer<impl AsRef<[u8]> + AsMut<[u8]>>,
    ) -> Result<bool> {
        match self {
            DecompressCodec::Brotli(v) => v.flush(output),
            DecompressCodec::Bz2(v) => v.flush(output),
            DecompressCodec::Deflate(v) => v.flush(output),
            DecompressCodec::Gzip(v) => v.flush(output),
            DecompressCodec::Lzma(v) => v.flush(output),
            DecompressCodec::Xz(v) => v.flush(output),
            DecompressCodec::Zlib(v) => v.flush(output),
            DecompressCodec::Zstd(v) => v.flush(output),
        }
    }

    fn finish(
        &mut self,
        output: &mut PartialBuffer<impl AsRef<[u8]> + AsMut<[u8]>>,
    ) -> Result<bool> {
        match self {
            DecompressCodec::Brotli(v) => v.finish(output),
            DecompressCodec::Bz2(v) => v.finish(output),
            DecompressCodec::Deflate(v) => v.finish(output),
            DecompressCodec::Gzip(v) => v.finish(output),
            DecompressCodec::Lzma(v) => v.finish(output),
            DecompressCodec::Xz(v) => v.finish(output),
            DecompressCodec::Zlib(v) => v.finish(output),
            DecompressCodec::Zstd(v) => v.finish(output),
        }
    }
}

/// DecompressState is that decode state during decompress.
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum DecompressState {
    /// Reading means there is no data to be consume, we need to fetch more.
    ///
    /// We need to call `DecompressReader::fetch()`.
    Reading,
    /// Decoding means data is ready.
    ///
    /// We need to call `DecompressReader::decode()`
    Decoding,
    /// Finishing means all data has been consumed.
    ///
    /// We need to call `DecompressReader::finish()` to flush them into output.
    Flushing,
    /// Done means the whole process of decompress is done.
    ///
    /// We should not call any function of `DecompressReader` anymore.
    Done,
}

/// DecompressDecoder provides blocking decompress support for opendal: `decode` happen
/// inside a blocking thread (user need to handle the decompress logic)
///
/// Note: please handle state carefully!
#[derive(Debug)]
pub struct DecompressDecoder {
    /// TODO: Should be replace by [ReadBuf](https://doc.rust-lang.org/std/io/struct.ReadBuf.html)
    buf: BytesMut,
    decoder: DecompressCodec,
    state: DecompressState,
    multiple_members: bool,
}

impl DecompressDecoder {
    /// Create a new DecompressDecoder with given CompressAlgorithm
    pub fn new(algo: CompressAlgorithm) -> Self {
        Self {
            buf: BytesMut::new(),
            decoder: algo.into(),
            state: DecompressState::Reading,
            multiple_members: false,
        }
    }

    /// Get decompress state
    pub fn state(&self) -> DecompressState {
        self.state
    }

    /// Fetch more data from underlying reader.
    ///
    /// # Notes
    ///
    /// For now, we will read all content into internal buffer. But in the future,
    /// we may change the implementation to only read part of input data.
    ///
    /// So it's required to check returning read size and advance the reader's amt.
    pub fn fill(&mut self, bs: &[u8]) -> usize {
        debug_assert_eq!(self.state, DecompressState::Reading);

        let len = bs.len();
        self.buf.extend_from_slice(bs);
        self.state = DecompressState::Decoding;

        trace!(
            "fill: read {len} bytes from src, next state {:?}",
            self.state
        );
        len
    }

    /// Decode data into output.
    /// Returns the data that has been written.
    pub fn decode(&mut self, output: &mut [u8]) -> Result<usize> {
        debug_assert_eq!(self.state, DecompressState::Decoding);

        // If input is empty, inner reader must reach EOF, return directly.
        if self.buf.is_empty() {
            trace!("input is empty, return directly");
            // Avoid attempting to reinitialise the decoder if the reader
            // has returned EOF.
            self.multiple_members = false;
            self.state = DecompressState::Flushing;
            return Ok(0);
        }

        let mut input = PartialBuffer::new(&self.buf);
        let mut output = PartialBuffer::new(output);
        let done = self.decoder.decode(&mut input, &mut output)?;
        let read_len = input.written().len();
        let written_len = output.written().len();
        self.buf.advance(read_len);

        if done {
            self.state = DecompressState::Flushing;
        } else if self.buf.is_empty() {
            self.state = DecompressState::Reading;
        } else {
            self.state = DecompressState::Decoding;
        }
        trace!(
            "decode: consume {read_len} bytes from src, write {written_len} bytes into dst, next state {:?}",
            self.state
        );
        Ok(written_len)
    }

    /// Finish a decompress press, flushing remaining data into output.
    /// Return the data that has been written.
    pub fn finish(&mut self, output: &mut [u8]) -> Result<usize> {
        debug_assert_eq!(self.state, DecompressState::Flushing);

        let mut output = PartialBuffer::new(output);
        let done = self.decoder.finish(&mut output)?;
        if done {
            if self.multiple_members {
                self.decoder.reinit()?;
                self.state = DecompressState::Reading;
            } else {
                self.state = DecompressState::Done;
            }
        } else {
            self.state = DecompressState::Flushing;
        }

        let len = output.written().len();
        trace!(
            "finish: flush {len} bytes into dst, next state {:?}",
            self.state
        );
        Ok(len)
    }
}

#[derive(Debug)]
#[pin_project]
pub struct DecompressReader<R: AsyncRead> {
    #[pin]
    reader: BufReader<R>,
    decoder: DecompressDecoder,
}

impl<R: AsyncRead> DecompressReader<R> {
    /// Create a new DecompressReader.
    pub fn new(reader: R, algo: CompressAlgorithm) -> Self {
        Self {
            reader: BufReader::new(reader),
            decoder: DecompressDecoder::new(algo),
        }
    }
}

impl<R: AsyncRead> AsyncRead for DecompressReader<R> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<Result<usize>> {
        if buf.is_empty() {
            return Poll::Ready(Ok(0));
        }

        let mut this = self.project();

        loop {
            match this.decoder.state() {
                DecompressState::Reading => {
                    let read = {
                        // TODO: after decoder adopt ReadBuf, we can do IO on
                        // BytesRead directly.
                        let input = ready!(this.reader.as_mut().poll_fill_buf(cx))?;
                        this.decoder.fill(input);
                        input.len()
                    };

                    this.reader.as_mut().consume(read);
                }
                DecompressState::Decoding => {
                    let written = this.decoder.decode(buf)?;
                    if written != 0 {
                        return Poll::Ready(Ok(written));
                    }
                }
                DecompressState::Flushing => {
                    let written = this.decoder.finish(buf)?;
                    if written != 0 {
                        return Poll::Ready(Ok(written));
                    }
                }
                DecompressState::Done => return Poll::Ready(Ok(0)),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::cmp::min;
    use std::env;
    use std::fs;
    use std::io::Result;

    use async_compression::futures::bufread::GzipEncoder;
    use async_compression::futures::bufread::ZlibEncoder;
    use futures::io::Cursor;
    use futures::AsyncReadExt;
    use rand::prelude::*;
    use sha2::Digest;
    use sha2::Sha256;

    use super::*;

    #[tokio::test]
    async fn test_decompress_bytes_zlib() -> Result<()> {
        let _ = env_logger::try_init();

        let mut rng = ThreadRng::default();
        let size = rng.gen_range(1..16 * 1024 * 1024);
        let mut content = vec![0; size];
        rng.fill_bytes(&mut content);

        let mut e = ZlibEncoder::new(Cursor::new(content.clone()));
        let mut compressed_content = vec![];
        e.read_to_end(&mut compressed_content).await?;

        let mut cr = DecompressDecoder::new(CompressAlgorithm::Zlib);

        let mut result = vec![0; size];
        let mut cnt = 0;
        loop {
            let (_, output) = result.split_at_mut(cnt);

            match cr.state {
                DecompressState::Reading => {
                    cr.fill(&compressed_content);
                }
                DecompressState::Decoding => {
                    let written = cr.decode(output)?;
                    cnt += written;
                }
                DecompressState::Flushing => {
                    let written = cr.finish(output)?;
                    cnt += written;
                }
                DecompressState::Done => {
                    break;
                }
            }
        }

        assert_eq!(result, content);

        Ok(())
    }

    #[tokio::test]
    async fn test_decompress_bytes_zlib_read_multiple() -> Result<()> {
        let _ = env_logger::try_init();

        let mut rng = ThreadRng::default();
        let size = rng.gen_range(1..16 * 1024 * 1024);
        let mut content = vec![0; size];
        rng.fill_bytes(&mut content);

        let mut e = ZlibEncoder::new(Cursor::new(content.clone()));
        let mut compressed_content = vec![];
        e.read_to_end(&mut compressed_content).await?;

        let mut cr = DecompressDecoder::new(CompressAlgorithm::Zlib);

        let mut result = Vec::with_capacity(content.len());
        let mut buf = vec![0; 1024 * 1024];
        let mut read = 0;
        loop {
            match cr.state {
                DecompressState::Reading => {
                    if read == compressed_content.len() {
                        break;
                    }

                    // Simulate read in 4k.
                    let size = min(read + 4 * 1024 * 1024, compressed_content.len());
                    let n = cr.fill(&compressed_content[read..size]);
                    read += n;
                }
                DecompressState::Decoding => {
                    let n = cr.decode(&mut buf)?;
                    result.extend_from_slice(&buf[..n])
                }
                DecompressState::Flushing => {
                    let n = cr.finish(&mut buf)?;
                    result.extend_from_slice(&buf[..n])
                }
                DecompressState::Done => {
                    break;
                }
            }
        }

        assert_eq!(result.len(), content.len());
        assert_eq!(
            format!("{:x}", Sha256::digest(&result)),
            format!("{:x}", Sha256::digest(&content))
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_decompress_bytes_gzip_read_multiple() -> Result<()> {
        let _ = env_logger::try_init();

        let mut rng = ThreadRng::default();
        let size = rng.gen_range(1..16 * 1024 * 1024);
        let mut content = vec![0; size];
        rng.fill_bytes(&mut content);

        let mut e = GzipEncoder::new(Cursor::new(content.clone()));
        let mut compressed_content = vec![];
        e.read_to_end(&mut compressed_content).await?;

        let mut cr = DecompressDecoder::new(CompressAlgorithm::Gzip);

        let mut result = Vec::with_capacity(content.len());
        let mut buf = vec![0; 1024 * 1024];
        let mut read = 0;
        loop {
            match cr.state {
                DecompressState::Reading => {
                    if read == compressed_content.len() {
                        break;
                    }

                    // Simulate read in 4k.
                    let size = min(read + 4 * 1024 * 1024, compressed_content.len());
                    let n = cr.fill(&compressed_content[read..size]);
                    read += n;
                }
                DecompressState::Decoding => {
                    let n = cr.decode(&mut buf)?;
                    result.extend_from_slice(&buf[..n])
                }
                DecompressState::Flushing => {
                    let n = cr.finish(&mut buf)?;
                    result.extend_from_slice(&buf[..n])
                }
                DecompressState::Done => {
                    break;
                }
            }
        }

        assert_eq!(result.len(), content.len());
        assert_eq!(
            format!("{:x}", Sha256::digest(&result)),
            format!("{:x}", Sha256::digest(&content))
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_decompress_reader_zlib() -> Result<()> {
        let _ = env_logger::try_init();

        let mut rng = ThreadRng::default();
        let mut content = vec![0; 16 * 1024 * 1024];
        rng.fill_bytes(&mut content);

        let mut e = ZlibEncoder::new(Cursor::new(content.clone()));
        let mut compressed_content = vec![];
        e.read_to_end(&mut compressed_content).await?;

        let mut cr =
            DecompressReader::new(Cursor::new(compressed_content), CompressAlgorithm::Zlib);

        let mut result = vec![];
        cr.read_to_end(&mut result).await?;

        assert_eq!(result, content);

        Ok(())
    }

    #[tokio::test]
    async fn test_decompress_reader_gzip() -> Result<()> {
        let _ = env_logger::try_init();

        let mut rng = ThreadRng::default();
        let mut content = vec![0; 16 * 1024 * 1024];
        rng.fill_bytes(&mut content);

        let mut e = GzipEncoder::new(Cursor::new(content.clone()));
        let mut compressed_content = vec![];
        e.read_to_end(&mut compressed_content).await?;

        let mut cr =
            DecompressReader::new(Cursor::new(compressed_content), CompressAlgorithm::Gzip);

        let mut result = vec![];
        cr.read_to_end(&mut result).await?;

        assert_eq!(result, content);

        Ok(())
    }

    #[tokio::test]
    async fn test_decompress_reader_ontime_gzip() -> Result<()> {
        let _ = env_logger::try_init();

        let content = fs::read(format!(
            "{}/tests/data/ontime_200.csv",
            env::current_dir()?.to_string_lossy()
        ))?;
        let compressed_content = fs::read(format!(
            "{}/tests/data/ontime_200.csv.gz",
            env::current_dir()?.to_string_lossy()
        ))?;

        let mut cr =
            DecompressReader::new(Cursor::new(compressed_content), CompressAlgorithm::Gzip);

        let mut result = vec![];
        cr.read_to_end(&mut result).await?;

        assert_eq!(result, content);

        Ok(())
    }

    #[tokio::test]
    async fn test_decompress_reader_ontime_bz2() -> Result<()> {
        let _ = env_logger::try_init();

        let content = fs::read(format!(
            "{}/tests/data/ontime_200.csv",
            env::current_dir()?.to_string_lossy()
        ))?;
        let compressed_content = fs::read(format!(
            "{}/tests/data/ontime_200.csv.bz2",
            env::current_dir()?.to_string_lossy()
        ))?;

        let mut cr = DecompressReader::new(Cursor::new(compressed_content), CompressAlgorithm::Bz2);

        let mut result = vec![];
        cr.read_to_end(&mut result).await?;

        assert_eq!(result, content);

        Ok(())
    }

    #[tokio::test]
    async fn test_decompress_reader_ontime_zstd() -> Result<()> {
        let _ = env_logger::try_init();

        let content = fs::read(format!(
            "{}/tests/data/ontime_200.csv",
            env::current_dir()?.to_string_lossy()
        ))?;
        let compressed_content = fs::read(format!(
            "{}/tests/data/ontime_200.csv.zst",
            env::current_dir()?.to_string_lossy()
        ))?;

        let mut cr =
            DecompressReader::new(Cursor::new(compressed_content), CompressAlgorithm::Zstd);

        let mut result = vec![];
        cr.read_to_end(&mut result).await?;

        assert_eq!(result, content);

        Ok(())
    }
}
