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

mod blob_accessor;
mod impls;

pub use blob_accessor::AsyncSeekableReader;
pub use blob_accessor::Bytes;
pub use blob_accessor::DataAccessor;
pub use blob_accessor::InputStream;
pub use blob_accessor::SeekableReader;
pub use impls::Local;
pub use impls::StorageScheme;
pub use impls::S3;
