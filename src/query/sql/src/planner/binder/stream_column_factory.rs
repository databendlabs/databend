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

use std::collections::BTreeMap;

use ctor::ctor;
use databend_common_catalog::plan::StreamColumn;
use databend_common_catalog::plan::StreamColumnType;
use databend_common_expression::ORIGIN_BLOCK_ID_COL_NAME;
use databend_common_expression::ORIGIN_BLOCK_ROW_NUM_COL_NAME;
use databend_common_expression::ORIGIN_VERSION_COL_NAME;

#[ctor]
pub static STREAM_COLUMN_FACTORY: StreamColumnFactory = StreamColumnFactory::init();

pub struct StreamColumnFactory {
    stream_columns: BTreeMap<String, StreamColumn>,
}

impl StreamColumnFactory {
    pub fn init() -> StreamColumnFactory {
        let mut stream_columns = BTreeMap::new();

        stream_columns.insert(
            ORIGIN_VERSION_COL_NAME.to_string(),
            StreamColumn::new(ORIGIN_VERSION_COL_NAME, StreamColumnType::OriginVersion),
        );
        stream_columns.insert(
            ORIGIN_BLOCK_ID_COL_NAME.to_string(),
            StreamColumn::new(ORIGIN_BLOCK_ID_COL_NAME, StreamColumnType::OriginBlockId),
        );
        stream_columns.insert(
            ORIGIN_BLOCK_ROW_NUM_COL_NAME.to_string(),
            StreamColumn::new(
                ORIGIN_BLOCK_ROW_NUM_COL_NAME,
                StreamColumnType::OriginRowNum,
            ),
        );

        StreamColumnFactory { stream_columns }
    }

    pub fn get_stream_column(&self, name: &str) -> Option<StreamColumn> {
        self.stream_columns
            .get(name)
            .map(|stream_column| stream_column.to_owned())
    }
}
