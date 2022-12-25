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

use std::fmt::Debug;
use std::io::Read;
use std::io::Write;
use std::sync::Arc;

use byteorder::BigEndian;
use byteorder::ReadBytesExt;
use byteorder::WriteBytesExt;
use common_exception::ErrorCode;
use common_exception::Result;
use common_exception::ToErrorCode;
use common_expression::Chunk;
use common_expression::ChunkMetaInfoPtr;
use common_expression::DataSchemaRef;

use crate::sessions::QueryContext;
use crate::sessions::TableContext;

// PrecommitBlock only use block.meta for data transfer.
#[derive(Debug, Eq, PartialEq, Clone)]
pub struct PrecommitChunk(pub Chunk);

impl PrecommitChunk {
    pub fn precommit(&self, ctx: &Arc<QueryContext>) {
        ctx.push_precommit_chunk(self.0.clone());
    }

    pub fn write<T: Write>(self, bytes: &mut T) -> Result<()> {
        let data_block = self.0;
        let serialized_meta = bincode::serialize(&data_block.meta()?).map_err_to_code(
            ErrorCode::BadBytes,
            || "precommit block serialize error when exchange",
        )?;

        bytes.write_u64::<BigEndian>(serialized_meta.len() as u64)?;
        bytes.write_all(&serialized_meta)?;
        Ok(())
    }

    pub fn read<T: Read>(bytes: &mut T) -> Result<PrecommitChunk> {
        let meta_len = bytes.read_u64::<BigEndian>()? as usize;
        let mut meta = vec![0; meta_len];

        bytes.read_exact(&mut meta)?;
        let block_meta: Option<ChunkMetaInfoPtr> = bincode::deserialize(&meta).map_err_to_code(
            ErrorCode::BadBytes,
            || "precommit block deserialize error when exchange",
        )?;

        Ok(PrecommitChunk(Chunk::empty_with_meta(block_meta)))
    }
}
