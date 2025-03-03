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
use databend_common_settings::OutofMemoryBehavior;

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

        if settings.get_force_join_data_spill()? {
            return Ok(MemorySettings::always_spill(0));
        }

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
            OutofMemoryBehavior::Throw => false,
            OutofMemoryBehavior::Spilling => max_query_memory_usage != 0,
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

    fn from_sort_settings(ctx: &QueryContext) -> Result<Self> {
        if !ctx.get_enable_sort_spill() {
            return Ok(MemorySettings::disable_spill());
        }

        let settings = ctx.get_settings();

        if settings.get_force_sort_data_spill()? {
            let spilling_batch_bytes = settings.get_sort_spilling_batch_bytes()?;
            return Ok(MemorySettings::always_spill(spilling_batch_bytes));
        }

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
            OutofMemoryBehavior::Throw => false,
            OutofMemoryBehavior::Spilling => max_query_memory_usage != 0,
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

        if settings.get_force_window_data_spill()? {
            let spill_unit_size = settings.get_window_spill_unit_size_mb()? * 1024 * 1024;
            return Ok(MemorySettings::always_spill(spill_unit_size));
        }

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
            OutofMemoryBehavior::Throw => false,
            OutofMemoryBehavior::Spilling => max_query_memory_usage != 0,
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

        if settings.get_force_aggregate_data_spill()? {
            return Ok(MemorySettings::always_spill(0));
        }

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
            OutofMemoryBehavior::Throw => false,
            OutofMemoryBehavior::Spilling => max_query_memory_usage != 0,
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

#[cfg(test)]
mod tests {
    use databend_common_catalog::table_context::TableContext;
    use databend_common_exception::Result;
    use databend_common_pipeline_transforms::MemorySettings;

    use crate::pipelines::memory_settings::MemorySettingsExt;
    use crate::test_kits::TestFixture;

    #[tokio::test(flavor = "multi_thread")]
    async fn test_global_spill_disabled_when_max_memory_zero() -> Result<()> {
        let fixture = TestFixture::setup().await?;
        let ctx = fixture.new_query_ctx().await?;
        let settings = ctx.get_settings();

        settings.set_setting("max_memory_usage".into(), "0".into())?;
        settings.set_setting("join_spilling_memory_ratio".into(), "50".into())?;
        settings.set_setting("query_out_of_memory_behavior".into(), "spilling".into())?;
        settings.set_setting("max_query_memory_usage".into(), "2000".into())?;

        let memory_settings = MemorySettings::from_join_settings(&ctx)?;

        assert!(!memory_settings.enable_global_level_spill);
        assert_eq!(memory_settings.max_memory_usage, 0);
        assert!(memory_settings.enable_query_level_spill);
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_query_spill_disabled_when_max_query_memory_zero() -> Result<()> {
        let fixture = TestFixture::setup().await?;
        let ctx = fixture.new_query_ctx().await?;
        let settings = ctx.get_settings();

        settings.set_setting("max_memory_usage".into(), "1000".into())?;
        settings.set_setting("join_spilling_memory_ratio".into(), "50".into())?;
        settings.set_setting("query_out_of_memory_behavior".into(), "spilling".into())?;
        settings.set_setting("max_query_memory_usage".into(), "0".into())?;

        let memory_settings = MemorySettings::from_join_settings(&ctx)?;

        assert!(!memory_settings.enable_query_level_spill);
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_both_global_and_query_spill_enabled() -> Result<()> {
        let fixture = TestFixture::setup().await?;
        let ctx = fixture.new_query_ctx().await?;
        let settings = ctx.get_settings();

        settings.set_setting("max_memory_usage".into(), "2000".into())?;
        settings.set_setting("join_spilling_memory_ratio".into(), "75".into())?;
        settings.set_setting("query_out_of_memory_behavior".into(), "spilling".into())?;
        settings.set_setting("max_query_memory_usage".into(), "3000".into())?;

        let memory_settings = MemorySettings::from_join_settings(&ctx)?;

        assert!(memory_settings.enable_global_level_spill);
        assert_eq!(memory_settings.max_memory_usage, 1500);
        assert!(memory_settings.enable_query_level_spill);
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_sort_spill_disabled_globally() -> Result<()> {
        let fixture = TestFixture::setup().await?;
        let ctx = fixture.new_query_ctx().await?;

        ctx.set_enable_sort_spill(false);

        let memory_settings = MemorySettings::from_sort_settings(&ctx)?;

        assert!(!memory_settings.enable_global_level_spill);
        assert!(!memory_settings.enable_query_level_spill);
        assert_eq!(memory_settings.max_memory_usage, usize::MAX);
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_global_sort_spill_enabled() -> Result<()> {
        let fixture = TestFixture::setup().await?;
        let ctx = fixture.new_query_ctx().await?;
        let settings = ctx.get_settings();

        ctx.set_enable_sort_spill(true);
        settings.set_setting("max_memory_usage".into(), "1000".into())?;
        settings.set_setting("sort_spilling_memory_ratio".into(), "50".into())?;
        settings.set_setting("query_out_of_memory_behavior".into(), "ThrowOOM".into())?;
        settings.set_setting("max_query_memory_usage".into(), "0".into())?;

        let memory_settings = MemorySettings::from_sort_settings(&ctx)?;

        assert!(memory_settings.enable_global_level_spill);
        assert_eq!(memory_settings.max_memory_usage, 500);
        assert!(!memory_settings.enable_query_level_spill);
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_global_sort_spill_disabled_when_max_memory_zero() -> Result<()> {
        let fixture = TestFixture::setup().await?;
        let ctx = fixture.new_query_ctx().await?;
        let settings = ctx.get_settings();

        ctx.set_enable_sort_spill(true);
        settings.set_setting("max_memory_usage".into(), "0".into())?;
        settings.set_setting("sort_spilling_memory_ratio".into(), "50".into())?;

        let memory_settings = MemorySettings::from_sort_settings(&ctx)?;

        assert!(!memory_settings.enable_global_level_spill);
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_query_level_spill_enabled() -> Result<()> {
        let fixture = TestFixture::setup().await?;
        let ctx = fixture.new_query_ctx().await?;
        let settings = ctx.get_settings();

        ctx.set_enable_sort_spill(true);
        settings.set_setting("query_out_of_memory_behavior".into(), "spilling".into())?;
        settings.set_setting("max_query_memory_usage".into(), "2000".into())?;

        let memory_settings = MemorySettings::from_sort_settings(&ctx)?;

        assert!(memory_settings.enable_query_level_spill);
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_sort_spilling_batch_bytes() -> Result<()> {
        let fixture = TestFixture::setup().await?;
        let ctx = fixture.new_query_ctx().await?;
        let settings = ctx.get_settings();

        ctx.set_enable_sort_spill(true);
        settings.set_setting("sort_spilling_batch_bytes".into(), "8192".into())?;

        let memory_settings = MemorySettings::from_sort_settings(&ctx)?;

        assert_eq!(memory_settings.spill_unit_size, 8192);
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_global_window_spill_enabled() -> Result<()> {
        let fixture = TestFixture::setup().await?;
        let ctx = fixture.new_query_ctx().await?;
        let settings = ctx.get_settings();

        settings.set_setting("max_memory_usage".into(), "2000".into())?;
        settings.set_setting("window_partition_spilling_memory_ratio".into(), "50".into())?;
        settings.set_setting("query_out_of_memory_behavior".into(), "ThrowOOM".into())?;
        settings.set_setting("max_query_memory_usage".into(), "0".into())?;

        let memory_settings = MemorySettings::from_window_settings(&ctx)?;

        assert!(memory_settings.enable_global_level_spill);
        assert_eq!(memory_settings.max_memory_usage, 1000);
        assert!(!memory_settings.enable_query_level_spill);
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_global_window_spill_disabled_when_max_memory_zero() -> Result<()> {
        let fixture = TestFixture::setup().await?;
        let ctx = fixture.new_query_ctx().await?;
        let settings = ctx.get_settings();

        settings.set_setting("max_memory_usage".into(), "0".into())?;
        settings.set_setting("window_partition_spilling_memory_ratio".into(), "50".into())?;

        let memory_settings = MemorySettings::from_window_settings(&ctx)?;

        assert!(!memory_settings.enable_global_level_spill);
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_query_level_window_spill_enabled() -> Result<()> {
        let fixture = TestFixture::setup().await?;
        let ctx = fixture.new_query_ctx().await?;
        let settings = ctx.get_settings();

        settings.set_setting("query_out_of_memory_behavior".into(), "spilling".into())?;
        settings.set_setting("max_query_memory_usage".into(), "3000".into())?;

        let memory_settings = MemorySettings::from_window_settings(&ctx)?;

        assert!(memory_settings.enable_query_level_spill);
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_both_window_spills_enabled() -> Result<()> {
        let fixture = TestFixture::setup().await?;
        let ctx = fixture.new_query_ctx().await?;
        let settings = ctx.get_settings();

        settings.set_setting("max_memory_usage".into(), "4000".into())?;
        settings.set_setting("window_partition_spilling_memory_ratio".into(), "25".into())?;
        settings.set_setting("query_out_of_memory_behavior".into(), "spilling".into())?;
        settings.set_setting("max_query_memory_usage".into(), "5000".into())?;

        let memory_settings = MemorySettings::from_window_settings(&ctx)?;

        assert!(memory_settings.enable_global_level_spill);
        assert_eq!(memory_settings.max_memory_usage, 1000);
        assert!(memory_settings.enable_query_level_spill);
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_window_spill_unit_size_conversion() -> Result<()> {
        let fixture = TestFixture::setup().await?;
        let ctx = fixture.new_query_ctx().await?;
        let settings = ctx.get_settings();

        settings.set_setting("window_spill_unit_size_mb".into(), "3".into())?;

        let memory_settings = MemorySettings::from_window_settings(&ctx)?;

        assert_eq!(memory_settings.spill_unit_size, 3 * 1024 * 1024);
        Ok(())
    }
}
