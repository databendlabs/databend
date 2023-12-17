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

use borsh::BorshDeserialize;
use borsh::BorshSerialize;
use databend_common_exception::Result;

/// bincode serialize_into wrap with optimized config
#[inline]
pub fn borsh_serialize_into_buf<W: std::io::Write, T: BorshSerialize>(
    writer: &mut W,
    value: &T,
) -> Result<()> {
    borsh::to_writer(writer, value)?;
    Ok(())
}

/// bincode deserialize_from wrap with optimized config
#[inline]
pub fn borsh_deserialize_from_slice<T: BorshDeserialize>(slice: &[u8]) -> Result<T> {
    let value = borsh::from_slice::<T>(slice)?;
    Ok(value)
}

#[inline]
pub fn borsh_deserialize_from_stream<T: BorshDeserialize>(stream: &mut &[u8]) -> Result<T> {
    // `T::deserialize` will updates the buffer to point at the remaining bytes.
    let value = T::deserialize(stream)?;
    Ok(value)
}
