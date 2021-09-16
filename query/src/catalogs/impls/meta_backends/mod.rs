//  Copyright 2021 Datafuse Labs.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
//

// TODO move this mod to catalogs

pub use embedded_meta_backend::EmbeddedMetaBackend;
pub use remote_meta_backend::RemoteMeteStoreClient;

//pub use crate::catalogs::metastore_client::DatabaseInfo;
//pub use crate::catalogs::metastore_client::MetaBackend;
//pub use crate::catalogs::metastore_client::TableInfo;

mod embedded_meta_backend;
mod remote_meta_backend;
