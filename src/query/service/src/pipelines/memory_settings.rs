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

use databend_common_base::runtime::GLOBAL_MEM_STAT;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::Result;
use databend_common_pipeline_transforms::MemorySettings;
use databend_common_settings::MemoryExceededBehavior;

use crate::sessions::QueryContext;

pub trait MemorySettingsExt: Sized {
    fn from_join_settings(ctx: &QueryContext) -> Result<Self>;

    fn from_sort_settings(ctx: &QueryContext) -> Result<Self>;

    fn from_window_settings(ctx: &QueryContext) -> Result<Self>;

    fn from_aggregate_settings(ctx: &QueryContext) -> Result<Self>;
}

impl MemorySettingsExt for MemorySettings {
    fn from_join_settings(ctx: &QueryContext) -> Result<Self> {
        let settings = ctx.get_settings();
        let mut enable_global_level_spill = false;
        let mut max_memory_usage = settings.get_max_memory_usage()? as usize;

        let max_memory_ratio = settings.get_join_spilling_memory_ratio()?;
        if max_memory_usage != 0 && max_memory_ratio != 0 {
            enable_global_level_spill = true;
            let max_memory_ratio = (max_memory_ratio as f64 / 100_f64).min(1_f64);
            max_memory_usage = (max_memory_usage as f64 * max_memory_ratio) as usize;
        }

        let max_query_memory_usage = settings.get_max_query_memory_usage()? as usize;
        let enable_query_level_spill = match settings.get_query_out_of_memory_behavior()? {
            MemoryExceededBehavior::ThrowOOM => false,
            MemoryExceededBehavior::EnableSpill => max_query_memory_usage != 0,
        };

        let spilling_batch_bytes = settings.get_sort_spilling_batch_bytes()?;

        Ok(MemorySettings {
            max_memory_usage,
            max_query_memory_usage,
            enable_query_level_spill,
            enable_global_level_spill,
            query_memory_tracking: ctx.get_query_memory_tracking(),
            spill_unit_size: spilling_batch_bytes,
            global_memory_tracking: &GLOBAL_MEM_STAT,
        })
    }

    fn from_sort_settings(ctx: &QueryContext) -> Result<Self> {
        if !ctx.get_enable_sort_spill() {
            return Ok(MemorySettings::disable_spill());
        }

        let settings = ctx.get_settings();
        let mut enable_global_level_spill = false;
        let mut max_memory_usage = settings.get_max_memory_usage()? as usize;

        let max_memory_ratio = settings.get_sort_spilling_memory_ratio()?;
        if max_memory_usage != 0 && max_memory_ratio != 0 {
            enable_global_level_spill = true;
            let max_memory_ratio = (max_memory_ratio as f64 / 100_f64).min(1_f64);
            max_memory_usage = (max_memory_usage as f64 * max_memory_ratio) as usize;
        }

        let max_query_memory_usage = settings.get_max_query_memory_usage()? as usize;
        let enable_query_level_spill = match settings.get_query_out_of_memory_behavior()? {
            MemoryExceededBehavior::ThrowOOM => false,
            MemoryExceededBehavior::EnableSpill => max_query_memory_usage != 0,
        };

        let spilling_batch_bytes = settings.get_sort_spilling_batch_bytes()?;

        Ok(MemorySettings {
            max_memory_usage,
            max_query_memory_usage,
            enable_query_level_spill,
            enable_global_level_spill,
            query_memory_tracking: ctx.get_query_memory_tracking(),
            spill_unit_size: spilling_batch_bytes,
            global_memory_tracking: &GLOBAL_MEM_STAT,
        })
    }

    fn from_window_settings(ctx: &QueryContext) -> Result<Self> {
        let settings = ctx.get_settings();
        let mut enable_global_level_spill = false;
        let mut max_memory_usage = settings.get_max_memory_usage()? as usize;

        let max_memory_ratio = settings.get_window_partition_spilling_memory_ratio()?;
        if max_memory_usage != 0 && max_memory_ratio != 0 {
            enable_global_level_spill = true;
            let max_memory_ratio = (max_memory_ratio as f64 / 100_f64).min(1_f64);
            max_memory_usage = (max_memory_usage as f64 * max_memory_ratio) as usize;
        }

        let max_query_memory_usage = settings.get_max_query_memory_usage()? as usize;
        let enable_query_level_spill = match settings.get_query_out_of_memory_behavior()? {
            MemoryExceededBehavior::ThrowOOM => false,
            MemoryExceededBehavior::EnableSpill => max_query_memory_usage != 0,
        };

        let spill_unit_size = settings.get_window_spill_unit_size_mb()? * 1024 * 1024;

        Ok(MemorySettings {
            spill_unit_size,
            max_memory_usage,
            max_query_memory_usage,
            enable_query_level_spill,
            enable_global_level_spill,
            query_memory_tracking: ctx.get_query_memory_tracking(),
            global_memory_tracking: &GLOBAL_MEM_STAT,
        })
    }

    fn from_aggregate_settings(ctx: &QueryContext) -> Result<Self> {
        let settings = ctx.get_settings();
        let mut enable_global_level_spill = false;
        let mut max_memory_usage = settings.get_max_memory_usage()? as usize;

        let max_memory_ratio = settings.get_aggregate_spilling_memory_ratio()?;
        if max_memory_usage != 0 && max_memory_ratio != 0 {
            enable_global_level_spill = true;
            let max_memory_ratio = (max_memory_ratio as f64 / 100_f64).min(1_f64);
            max_memory_usage = (max_memory_usage as f64 * max_memory_ratio) as usize;
        }

        let max_query_memory_usage = settings.get_max_query_memory_usage()? as usize;
        let enable_query_level_spill = match settings.get_query_out_of_memory_behavior()? {
            MemoryExceededBehavior::ThrowOOM => false,
            MemoryExceededBehavior::EnableSpill => max_query_memory_usage != 0,
        };

        Ok(MemorySettings {
            max_memory_usage,
            max_query_memory_usage,
            enable_query_level_spill,
            enable_global_level_spill,
            spill_unit_size: 0,
            query_memory_tracking: ctx.get_query_memory_tracking(),
            global_memory_tracking: &GLOBAL_MEM_STAT,
        })
    }
}
