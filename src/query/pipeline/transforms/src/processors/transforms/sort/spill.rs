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

use databend_common_expression::local_block_meta_serde;
use databend_common_expression::BlockMetaInfo;

use super::super::SortSpillParams;

/// Mark a partially sorted [`DataBlock`] as a block needs to be spilled.
#[derive(Debug)]
pub struct SortSpillMetaWithParams(pub SortSpillParams);

local_block_meta_serde!(SortSpillMetaWithParams);

#[typetag::serde(name = "sort_spill")]
impl BlockMetaInfo for SortSpillMetaWithParams {}

/// Mark a partially sorted [`DataBlock`] as a block needs to be spilled.
#[derive(Debug)]
pub struct SortSpillMeta {}

local_block_meta_serde!(SortSpillMeta);

#[typetag::serde(name = "sort_spill")]
impl BlockMetaInfo for SortSpillMeta {}
