// Copyright 2020 Datafuse Labs.
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

use common_datavalues::prelude::*;
use common_exception::Result;

use crate::DataBlock;

#[test]
fn test_data_block() -> Result<()> {
    let schema = DataSchemaRefExt::create(vec![DataField::new("a", DataType::Int64, false)]);

    let block = DataBlock::create_by_array(schema.clone(), vec![Series::new(vec![1, 2, 3])]);
    assert_eq!(&schema, block.schema());

    assert_eq!(3, block.num_rows());
    assert_eq!(1, block.num_columns());
    assert_eq!(3, block.try_column_by_name("a")?.len());
    assert_eq!(3, block.column(0).len());

    assert_eq!(true, block.try_column_by_name("a").is_ok());
    assert_eq!(true, block.try_column_by_name("a_not_found").is_err());

    Ok(())
}
