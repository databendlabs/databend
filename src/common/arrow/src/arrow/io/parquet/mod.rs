// Copyright 2020-2022 Jorge C. Leitão
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

//! APIs to read from and write to Parquet format.
use crate::arrow::error::Error;

pub mod read;
pub mod write;

#[cfg(feature = "io_parquet_bloom_filter")]
#[cfg_attr(docsrs, doc(cfg(feature = "io_parquet_bloom_filter")))]
pub use parquet2::bloom_filter;

const ARROW_SCHEMA_META_KEY: &str = "ARROW:schema";

impl From<parquet2::error::Error> for Error {
    fn from(error: parquet2::error::Error) -> Self {
        match error {
            parquet2::error::Error::FeatureNotActive(_, _) => {
                let message = "Failed to read a compressed parquet file. \
                    Use the cargo feature \"io_parquet_compression\" to read compressed parquet files."
                    .to_string();
                Error::ExternalFormat(message)
            }
            _ => Error::ExternalFormat(error.to_string()),
        }
    }
}

impl From<Error> for parquet2::error::Error {
    fn from(error: Error) -> Self {
        parquet2::error::Error::OutOfSpec(error.to_string())
    }
}
