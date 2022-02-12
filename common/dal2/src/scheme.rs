// Copyright 2022 Datafuse Labs.
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
use std::str::FromStr;

use super::error::Error;

#[derive(Clone, Debug, PartialEq)]
pub enum Scheme {
    // TODO: Although we don't have azblob support for now, but we need to add it for compatibility. We will implement azblob support as soon as possible.
    Azblob,
    Fs,
    S3,
}

impl FromStr for Scheme {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let s = s.to_lowercase();
        match s.as_str() {
            "azblob" => Ok(Scheme::Azblob),
            "fs" => Ok(Scheme::Fs),
            "s3" => Ok(Scheme::S3),

            // TODO: it's used for compatibility with dal1, should be removed in the future
            "local" | "disk" => Ok(Scheme::Fs),
            "azurestorageblob" => Ok(Scheme::Azblob),

            _ => Err(Error::BackendNotSupported(s)),
        }
    }
}
