// Copyright 2021 Datafuse Labs.
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

use anyerror::AnyError;
use common_meta_sled_store::openraft;
use common_meta_types::MetaStorageError;
use openraft::ErrorSubject;
use openraft::ErrorVerb;
use openraft::StorageError;
use openraft::StorageIOError;

/// Convert MetaStorageError to openraft::StorageError;  
pub trait ToStorageError<T> {
    fn map_to_sto_err(self, subject: ErrorSubject, verb: ErrorVerb) -> Result<T, StorageError>;
}

impl<T> ToStorageError<T> for Result<T, MetaStorageError> {
    fn map_to_sto_err(self, subject: ErrorSubject, verb: ErrorVerb) -> Result<T, StorageError> {
        match self {
            Ok(x) => Ok(x),
            Err(e) => {
                let ae = AnyError::new(&e);
                let io_err = StorageIOError::new(subject, verb, ae);
                Err(io_err.into())
            }
        }
    }
}
