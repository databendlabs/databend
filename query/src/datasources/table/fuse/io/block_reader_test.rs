//  Copyright 2021 Datafuse Labs.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
//

use std::sync::Arc;

use common_base::tokio;
use common_dal::DataAccessor;
use common_datablocks::assert_blocks_sorted_eq;
use common_datablocks::pretty_format_blocks;
use common_datablocks::DataBlock;
//  Copyright 2021 Datafuse Labs.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
//
use common_datavalues::prelude::SeriesFrom;
use common_datavalues::series::Series;
use common_datavalues::DataField;
use common_datavalues::DataSchemaRefExt;
use common_datavalues::DataType;
use common_planners::Part;
use tempfile::TempDir;

use super::super::util;
use super::block_appender::BlockAppender;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_block_reader_read() -> common_exception::Result<()> {
    let tmp_dir = TempDir::new().unwrap();
    let local_fs = common_dal::Local::with_path(tmp_dir.path().to_owned());
    let da: Arc<dyn DataAccessor> = Arc::new(local_fs);
    let schema = DataSchemaRefExt::create(vec![DataField::new("a", DataType::Int32, false)]);
    let block = DataBlock::create_by_array(schema.clone(), vec![Series::new(vec![1, 2, 3])]);
    let arrow_scheme = block.schema().to_arrow();
    let location = util::gen_unique_block_location();

    let _r = BlockAppender::save_block(&arrow_scheme, block.clone(), &da, &location).await?;

    let part = Part {
        name: location.to_string(),
        version: 0,
    };

    let proj = (0..arrow_scheme.fields().len()).collect();
    let got = super::block_reader::do_read(part, da, proj, arrow_scheme).await;
    assert!(got.is_ok(), "{:?}", got);

    let input_block_as_string = pretty_format_blocks(&[block]).unwrap();
    let lines_of_input_block = input_block_as_string.lines().collect();

    assert_blocks_sorted_eq(lines_of_input_block, &[got.unwrap()]);
    Ok(())
}
