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

use databend_common_compress::CompressAlgorithm;
use databend_common_exception::ErrorCode;
use databend_common_meta_app::principal::StageFileCompression;

pub fn get_compression_alg_copy(
    compress_option: StageFileCompression,
    path: &str,
) -> databend_common_exception::Result<Option<CompressAlgorithm>> {
    let compression_algo = match compress_option {
        StageFileCompression::Auto => CompressAlgorithm::from_path(path),
        StageFileCompression::Gzip => Some(CompressAlgorithm::Gzip),
        StageFileCompression::Bz2 => Some(CompressAlgorithm::Bz2),
        StageFileCompression::Brotli => Some(CompressAlgorithm::Brotli),
        StageFileCompression::Zstd => Some(CompressAlgorithm::Zstd),
        StageFileCompression::Deflate => Some(CompressAlgorithm::Zlib),
        StageFileCompression::RawDeflate => Some(CompressAlgorithm::Deflate),
        StageFileCompression::Xz => Some(CompressAlgorithm::Xz),
        StageFileCompression::Lzo => {
            return Err(ErrorCode::Unimplemented(
                "compress type lzo is unimplemented",
            ));
        }
        StageFileCompression::Snappy => {
            return Err(ErrorCode::Unimplemented(
                "compress type snappy is unimplemented",
            ));
        }
        StageFileCompression::None => None,
    };
    Ok(compression_algo)
}
