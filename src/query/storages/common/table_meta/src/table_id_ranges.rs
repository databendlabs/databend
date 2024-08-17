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

// min id for system database
pub const SYS_DB_ID_BEGIN: u64 = 1 << 62;
// min id for system tables (inclusive)
pub const SYS_TBL_ID_BEGIN: u64 = 1 << 62;
// max id for system tables (exclusive)
pub const SYS_TBL_ID_END: u64 = SYS_TBL_ID_BEGIN + 10000;

// min id for table funcs (inclusive)
pub const SYS_TBL_FUNC_ID_BEGIN: u64 = SYS_TBL_ID_END;
// max id for table tables (exclusive)
pub const SYS_TBL_FUC_ID_END: u64 = SYS_TBL_FUNC_ID_BEGIN + 10000;

// min id for temp tables (inclusive)
pub const TEMP_TBL_ID_BEGIN: u64 = SYS_TBL_FUC_ID_END;
// max id for temp tables (exclusive)
pub const TEMP_TBL_ID_END: u64 = TEMP_TBL_ID_BEGIN + 10000;
