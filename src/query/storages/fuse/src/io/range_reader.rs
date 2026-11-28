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
use databend_storages_common_io::OperatorRangeReader;
use databend_storages_common_io::RangeReader;
use opendal::Operator;

/// The ranged-read chain of one file: storage only. A chunk cache link over the disk cache
/// was set aside when the cache crate was rewritten upstream; see the branch notes.
pub(crate) fn create_file_range_reader(
    operator: Operator,
    path: String,
    max_prefetch: usize,
) -> Result<Box<dyn RangeReader>> {
    let tail = OperatorRangeReader::new(operator, path, max_prefetch.saturating_add(1));
    Ok(Box::new(tail))
}
