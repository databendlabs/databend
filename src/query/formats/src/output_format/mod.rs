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

use databend_common_exception::Result;
use databend_common_expression::DataBlock;
pub mod csv;
pub mod json;
pub mod ndjson;
pub mod parquet;
pub mod tsv;

pub use csv::CSVOutputFormat;
pub use csv::CSVWithNamesAndTypesOutputFormat;
pub use csv::CSVWithNamesOutputFormat;
pub use json::JSONOutputFormat;
pub use ndjson::NDJSONOutputFormatBase;
pub use parquet::ParquetOutputFormat;
pub use tsv::TSVOutputFormat;
pub use tsv::TSVWithNamesAndTypesOutputFormat;
pub use tsv::TSVWithNamesOutputFormat;

pub trait OutputFormat: Send {
    fn serialize_block(&mut self, data_block: &DataBlock) -> Result<Vec<u8>>;

    fn serialize_prefix(&self) -> Result<Vec<u8>> {
        Ok(vec![])
    }

    fn buffer_size(&mut self) -> usize {
        0
    }

    fn finalize(&mut self) -> Result<Vec<u8>>;
}
