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

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::TableSchemaRef;
use databend_common_pipeline::core::Pipeline;
use databend_storages_common_stage::CopyIntoLocationInfo;
use opendal::Operator;

pub(crate) fn append_data_to_lance_dataset(
    _pipeline: &mut Pipeline,
    _info: CopyIntoLocationInfo,
    _schema: TableSchemaRef,
    _op: Operator,
    _query_id: String,
    _mem_limit: usize,
    _max_threads: usize,
) -> Result<()> {
    Err(ErrorCode::Unimplemented(
        "LANCE unload support is disabled, rebuild with cargo feature 'storage-stage-lance'",
    ))
}
