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

use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;

use databend_common_exception::Result;
use databend_common_expression::ColumnBuilder;
use databend_common_meta_app::principal::OnErrorMode;
use databend_common_storage::FileParseError;
use databend_common_storage::FileStatus;

pub struct ErrorHandler {
    pub on_error_mode: OnErrorMode,
    pub on_error_count: AtomicU64,
}

impl ErrorHandler {
    pub fn on_error(
        &self,
        e: FileParseError,
        columns: Option<(&mut [ColumnBuilder], usize)>,
        file_status: &mut FileStatus,
        file_path: &str,
        line: usize,
    ) -> Result<()> {
        if let Some((columns, num_rows)) = columns {
            columns.iter_mut().for_each(|c| {
                // the whole record is invalid, so we need to pop all the values
                // not necessary if this function returns error, still do it for code simplicity
                if c.len() > num_rows {
                    c.pop().expect("must success");
                    assert_eq!(c.len(), num_rows);
                }
            });
        }

        match &self.on_error_mode {
            OnErrorMode::Continue => {
                file_status.add_error(e, line);
                Ok(())
            }
            OnErrorMode::AbortNum(abort_num) => {
                if *abort_num <= 1
                    || self.on_error_count.fetch_add(1, Ordering::Relaxed) >= *abort_num - 1
                {
                    Err(e.to_error_code(&self.on_error_mode, file_path, line))
                } else {
                    Ok(())
                }
            }
            _ => Err(e.to_error_code(&self.on_error_mode, file_path, line)),
        }
    }
}
