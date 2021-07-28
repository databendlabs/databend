// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.
//

// min id for system tables (inclusive)
pub(crate) const SYS_TBL_ID_BEGIN: u64 = 1 << 62;
// max id for system tables (exclusive)
pub(crate) const SYS_TBL_ID_END: u64 = SYS_TBL_ID_BEGIN + 10000;

// min id for system tables (inclusive)
// max id for local tables is u64:MAX
pub(crate) const LOCAL_TBL_ID_BEGIN: u64 = SYS_TBL_ID_END;
