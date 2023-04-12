use std::io::Cursor;
use std::marker::PhantomData;

use common_exception::Result;
use common_io::prelude::BinaryRead;
use futures_util::AsyncRead;
use futures_util::AsyncReadExt;
use serde::de::DeserializeOwned;
use storages_common_table_meta::meta::Encoding;
use storages_common_table_meta::meta::MetaCompression;
use storages_common_table_meta::meta::TableSnapshot;
use storages_common_table_meta::meta::Versioned;

use crate::io::read::meta::meta_readers::read_and_deserialize;

// Copyright 2023 Datafuse Labs.
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
//

pub async fn load_snapshot_v3<R, T>(mut reader: R, _v: &PhantomData<T>) -> Result<TableSnapshot>
where
    T: DeserializeOwned,
    R: AsyncRead + Unpin + Send,
{
    let mut buffer: Vec<u8> = vec![];
    reader.read_to_end(&mut buffer).await?;

    let mut cursor = Cursor::new(buffer);
    let version = cursor.read_scalar::<u64>()?;
    assert_eq!(version, TableSnapshot::VERSION);
    let encoding = Encoding::try_from(cursor.read_scalar::<u8>()?)?;
    let compression = MetaCompression::try_from(cursor.read_scalar::<u8>()?)?;
    let snapshot_size: u64 = cursor.read_scalar::<u64>()?;

    read_and_deserialize(&mut cursor, snapshot_size, &encoding, &compression)
}
