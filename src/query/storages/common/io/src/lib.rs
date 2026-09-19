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

mod blocking_write;
mod buffer_reader;
mod files;
mod merge_io_reader;
mod merge_io_result;
mod range_read;
mod read_settings;

pub use blocking_write::BLOCKING_WRITE_CHUNK_SIZE;
pub use blocking_write::BLOCKING_WRITE_MAX_CHUNKS;
pub use blocking_write::OpenDalBlockingWrite;
pub use blocking_write::blocking_write_retained_bytes;
pub use blocking_write::create_blocking_write;
pub use buffer_reader::BufferReader;
pub use files::Files;
pub use files::dedup_file_locations;
pub use merge_io_reader::MergeIOReader;
pub use merge_io_result::MergeIOReadResult;
pub use merge_io_result::OwnerMemory;
pub use range_read::ChunkGrid;
pub use range_read::ChunkedRangeReader;
pub use range_read::DiskCacheRangeReader;
pub use range_read::OperatorRangeReader;
pub use range_read::RangeReader;
pub use read_settings::ReadSettings;

#[cfg(test)]
pub(crate) fn init_test_runtime() {
    use std::sync::Once;

    use databend_common_base::base::GlobalInstance;
    use databend_common_base::runtime::GlobalIORuntime;

    static INIT: Once = Once::new();
    INIT.call_once(|| {
        GlobalInstance::init_production();
        GlobalIORuntime::init(2).unwrap();
    });
}
