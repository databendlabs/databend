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

#[cfg(feature = "storage-stage")]
pub use databend_common_storages_stage::BytesBatch;
#[cfg(feature = "storage-stage")]
pub use databend_common_storages_stage::BytesReader;
#[cfg(feature = "storage-stage")]
pub use databend_common_storages_stage::Decompressor;
#[cfg(feature = "storage-stage")]
pub use databend_common_storages_stage::InferSchemaPartInfo;
#[cfg(feature = "storage-stage")]
pub use databend_common_storages_stage::LoadContext;
#[cfg(feature = "storage-stage")]
pub use databend_common_storages_stage::StageSinkTable;
#[cfg(feature = "storage-stage")]
pub use databend_common_storages_stage::StageTable;
#[cfg(feature = "storage-stage")]
pub use databend_common_storages_stage::build_streaming_load_pipeline;
#[cfg(feature = "storage-stage")]
pub use databend_common_storages_stage::parse_tsv_records_for_infer_schema;
