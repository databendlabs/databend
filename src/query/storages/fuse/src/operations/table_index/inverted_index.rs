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
use std::sync::Arc;
use std::time::Instant;

use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_expression::DataSchema;
use databend_common_expression::TableSchemaRef;
use databend_common_meta_app::schema::TableIndex;
use databend_common_meta_app::schema::TableIndexType;
use databend_common_metrics::storage::metrics_inc_block_inverted_index_generate_milliseconds;
use databend_common_metrics::storage::metrics_inc_block_inverted_index_write_bytes;
use databend_common_metrics::storage::metrics_inc_block_inverted_index_write_milliseconds;
use databend_common_metrics::storage::metrics_inc_block_inverted_index_write_nums;
use databend_common_pipeline::core::Pipeline;
use databend_storages_common_index::INVERTED_INDEX_FILE_FORMAT_VERSION;
use databend_storages_common_table_meta::meta::Location;
use lindera::dictionary::UserDictionary;
use opendal::Operator;

use crate::FuseTable;
use crate::io::InvertedIndexWriter;
use crate::io::TableMetaLocationGenerator;
use crate::io::block_index::WrittenInvertedIndex;
use crate::io::resolve_inverted_index_user_dictionary;
use crate::operations::table_index::check_index_schema;
use crate::operations::table_index::get_refresh_index;

impl FuseTable {
    pub async fn do_refresh_inverted_index(
        &self,
        ctx: Arc<dyn TableContext>,
        index_name: String,
        index_version: String,
        index_options: &BTreeMap<String, String>,
        index_schema: TableSchemaRef,
        segment_locs: Option<Vec<Location>>,
        pipeline: &mut Pipeline,
    ) -> Result<u64> {
        let index = get_refresh_index(self, &index_name)?;
        if index.index_type != TableIndexType::Inverted {
            return Err(ErrorCode::RefreshIndexError(format!(
                "Index {index_name} is not an INVERTED index"
            )));
        }
        if index.version != index_version || &index.options != index_options {
            return Err(ErrorCode::RefreshIndexError(format!(
                "Index {index_name} definition changed before refresh"
            )));
        }
        check_index_schema(&index_name, index, &index_schema)?;
        self.do_refresh_indexes(ctx, &[index_name], segment_locs, pipeline)
            .await
    }
}

pub struct RefreshInvertedIndex {
    index: TableIndex,
    dictionary: Option<Arc<UserDictionary>>,
}

impl RefreshInvertedIndex {
    pub async fn try_create(operator: &Operator, index: &TableIndex) -> Result<Self> {
        Ok(Self {
            index: index.clone(),
            dictionary: resolve_inverted_index_user_dictionary(operator, &index.options).await?,
        })
    }

    /// Tokenizing and the blocking upload belong on an executor thread, not on
    /// the IO runtime whose workers drive the upload itself.
    pub fn build(
        &self,
        operator: &Operator,
        locations: &TableMetaLocationGenerator,
        schema: &TableSchemaRef,
        block: &DataBlock,
    ) -> Result<WrittenInvertedIndex> {
        let location = locations.gen_inverted_index_v2_location(&self.index.version);
        let start = Instant::now();
        let mut writer = InvertedIndexWriter::try_create(
            Arc::new(DataSchema::from(schema.as_ref())),
            &self.index.options,
            operator.clone(),
            location.clone(),
            self.dictionary.clone(),
        )?;
        writer.add_block(schema, block)?;
        let sizes = writer.finalize()?;
        let elapsed = start.elapsed().as_millis() as u64;
        metrics_inc_block_inverted_index_generate_milliseconds(elapsed);
        metrics_inc_block_inverted_index_write_nums(1);
        metrics_inc_block_inverted_index_write_bytes(sizes.bundle + sizes.siblings);
        metrics_inc_block_inverted_index_write_milliseconds(elapsed);
        Ok(WrittenInvertedIndex {
            index_name: self.index.name.clone(),
            index_version: self.index.version.clone(),
            location: (location, INVERTED_INDEX_FILE_FORMAT_VERSION),
            bundle_size: sizes.bundle,
            total_size: sizes.bundle + sizes.siblings,
        })
    }
}
