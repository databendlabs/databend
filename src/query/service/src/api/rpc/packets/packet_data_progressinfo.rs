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
use databend_common_base::base::ProgressValues;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;

use crate::sessions::QueryContext;
use crate::sessions::TableContext;

#[allow(clippy::enum_variant_names)]
#[derive(Debug)]
pub enum ProgressInfo {
    ScanProgress(ProgressValues),
    WriteProgress(ProgressValues),
    ResultProgress(ProgressValues),
}

impl ProgressInfo {
    pub fn inc(&self, ctx: &Arc<QueryContext>) {
        match self {
            ProgressInfo::ScanProgress(values) => ctx.get_scan_progress().incr(values),
            ProgressInfo::WriteProgress(values) => ctx.get_write_progress().incr(values),
            ProgressInfo::ResultProgress(values) => ctx.get_result_progress().incr(values),
        };
    }

    pub fn write<T: Write>(self, bytes: &mut T) -> Result<()> {
        let (info_type, values) = match self {
            ProgressInfo::ScanProgress(values) => (1_u8, values),
            ProgressInfo::WriteProgress(values) => (2_u8, values),
            ProgressInfo::ResultProgress(values) => (3_u8, values),
        };

        bytes.write_u8(info_type)?;
        bytes.write_u64::<BigEndian>(values.rows as u64)?;
        bytes.write_u64::<BigEndian>(values.bytes as u64)?;
        Ok(())
    }

    pub fn read<T: Read>(bytes: &mut T) -> Result<ProgressInfo> {
        let info_type = bytes.read_u8()?;
        let rows = bytes.read_u64::<BigEndian>()? as usize;
        let bytes = bytes.read_u64::<BigEndian>()? as usize;

        match info_type {
            1 => Ok(ProgressInfo::ScanProgress(ProgressValues { rows, bytes })),
            2 => Ok(ProgressInfo::WriteProgress(ProgressValues { rows, bytes })),
            3 => Ok(ProgressInfo::ResultProgress(ProgressValues { rows, bytes })),
            _ => Err(ErrorCode::Unimplemented(format!(
                "Unimplemented progress info type, {}",
                info_type
            ))),
        }
    }
}
