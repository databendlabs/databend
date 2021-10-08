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

mod data_accessor;
mod impls;
mod schemes;

#[cfg(test)]
mod schemes_test;

pub use data_accessor::AsyncSeekableReader;
pub use data_accessor::Bytes;
pub use data_accessor::DataAccessor;
pub use data_accessor::DataAccessorBuilder;
pub use data_accessor::DefaultDataAccessorBuilder;
pub use data_accessor::InputStream;
pub use data_accessor::ObjectAccessor;
pub use data_accessor::SeekableReader;
pub use impls::aws_s3::S3InputStream;
pub use impls::aws_s3::S3;
pub use impls::local::Local;
pub use schemes::StorageScheme;
