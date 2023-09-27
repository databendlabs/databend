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

use std::path::PathBuf;

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
