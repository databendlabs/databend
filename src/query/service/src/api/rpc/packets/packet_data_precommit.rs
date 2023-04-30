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
use common_expression::BlockMetaInfoPtr;
use common_expression::DataBlock;

use crate::sessions::QueryContext;
use crate::sessions::TableContext;

// PrecommitBlock only use block.meta for data transfer.
#[derive(Clone, Debug)]
pub struct PrecommitBlock(pub DataBlock);

impl PrecommitBlock {
    pub fn precommit(&self, ctx: &Arc<QueryContext>) {
        ctx.push_precommit_block(self.0.clone());
    }

    pub fn write<T: Write>(self, bytes: &mut T) -> Result<()> {
        let data_block = self.0;
        let serialized_meta = bincode::serialize(&data_block.get_meta()).map_err_to_code(
            ErrorCode::BadBytes,
            || "precommit block serialize error when exchange",
        )?;

        bytes.write_u64::<BigEndian>(serialized_meta.len() as u64)?;
        bytes.write_all(&serialized_meta)?;
        Ok(())
    }

    pub fn read<T: Read>(bytes: &mut T) -> Result<PrecommitBlock> {
        let meta_len = bytes.read_u64::<BigEndian>()? as usize;
        let mut meta = vec![0; meta_len];

        bytes.read_exact(&mut meta)?;
        let block_meta: Option<BlockMetaInfoPtr> = bincode::deserialize(&meta).map_err_to_code(
            ErrorCode::BadBytes,
            || "precommit block deserialize error when exchange",
        )?;

        Ok(PrecommitBlock(DataBlock::new_with_meta(
            vec![],
            0,
            block_meta,
        )))
    }
}
