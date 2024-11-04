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

use databend_common_meta_sled_store::openraft;
use databend_common_meta_stoerr::MetaStorageError;
use databend_common_meta_types::ErrorSubject;
use databend_common_meta_types::StorageError;
use openraft::ErrorVerb;

/// Convert MetaStorageError to openraft::StorageError;
pub trait ToStorageError<T> {
    fn map_to_sto_err(self, subject: ErrorSubject, verb: ErrorVerb) -> Result<T, StorageError>;
}

impl<T> ToStorageError<T> for Result<T, MetaStorageError> {
    fn map_to_sto_err(self, subject: ErrorSubject, verb: ErrorVerb) -> Result<T, StorageError> {
        match self {
            Ok(x) => Ok(x),
            Err(e) => {
                let io_err = StorageError::new(subject, verb, &e);
                Err(io_err)
            }
        }
    }
}
