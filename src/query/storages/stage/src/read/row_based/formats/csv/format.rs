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

use std::sync::Arc;

use databend_common_exception::Result;
use databend_common_meta_app::principal::CsvFileFormatParams;

use crate::read::load_context::LoadContext;
use crate::read::row_based::format::RowBasedFileFormat;
use crate::read::row_based::format::RowDecoder;
use crate::read::row_based::format::SeparatorState;
use crate::read::row_based::formats::csv::block_builder::CsvDecoder;
use crate::read::row_based::formats::csv::separator::CsvReader;

#[derive(Clone)]
pub struct CsvInputFormat {
    pub(crate) params: CsvFileFormatParams,
}

impl RowBasedFileFormat for CsvInputFormat {
    fn try_create_separator(
        &self,
        load_ctx: Arc<LoadContext>,
        path: &str,
    ) -> Result<Box<dyn SeparatorState>> {
        Ok(Box::new(CsvReader::try_create(load_ctx, path, self)?))
    }

    fn try_create_decoder(&self, load_ctx: Arc<LoadContext>) -> Result<Arc<dyn RowDecoder>> {
        Ok(Arc::new(CsvDecoder::create(self.clone(), load_ctx.clone())))
    }
}
