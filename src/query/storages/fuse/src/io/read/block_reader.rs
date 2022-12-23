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

use common_arrow::parquet::metadata::SchemaDescriptor;
use common_catalog::plan::Projection;
use common_datavalues::DataSchemaRef;
use common_storage::ColumnLeaves;
use opendal::Operator;

// TODO: make BlockReader as a trait.
#[derive(Clone)]
pub struct BlockReader {
    pub(crate) operator: Operator,
    pub(crate) projection: Projection,
    pub(crate) projected_schema: DataSchemaRef,
    pub(crate) column_leaves: ColumnLeaves,
    pub(crate) parquet_schema_descriptor: SchemaDescriptor,
}
