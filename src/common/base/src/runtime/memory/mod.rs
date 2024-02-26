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

mod alloc_error_hook;
mod mem_stat;
mod stat_buffer;

pub use alloc_error_hook::set_alloc_error_hook;
pub use mem_stat::MemStat;
pub use mem_stat::OutOfLimit;
pub use mem_stat::GLOBAL_MEM_STAT;
pub use stat_buffer::StatBuffer;
