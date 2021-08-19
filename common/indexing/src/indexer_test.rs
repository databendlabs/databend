// Copyright 2021 Datafuse Labs.
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

use common_datablocks::DataBlock;
use common_datavalues::prelude::*;
use common_datavalues::DataField;
use common_datavalues::DataSchemaRefExt;
use common_datavalues::DataType;
use common_exception::Result;

use crate::Index;
use crate::Indexer;

#[test]
fn test_indexer() -> Result<()> {
    let schema = DataSchemaRefExt::create(vec![
        DataField::new("name", DataType::Utf8, true),
        DataField::new("age", DataType::Int32, false),
    ]);

    let block1 = DataBlock::create_by_array(schema.clone(), vec![
        Series::new(vec!["jack", "ace", "bohu"]),
        Series::new(vec![11, 6, 24]),
    ]);

    let block2 = DataBlock::create_by_array(schema.clone(), vec![
        Series::new(vec!["xjack", "xace", "xbohu"]),
        Series::new(vec![11, 6, 24]),
    ]);

    let indexer = Indexer::create();
    indexer.create_index(&[block1, block2])?;
    Ok(())
}
