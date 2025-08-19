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
        let mut builder = MemorySettings::builder();

        if settings.get_force_join_data_spill()? {
            return Ok(builder.with_max_memory_usage(0).build());
        }

        let max_memory_usage = settings.get_max_memory_usage()? as usize;
        let max_memory_ratio = settings.get_join_spilling_memory_ratio()?;
        if max_memory_usage != 0 && max_memory_ratio != 0 {
            let ratio = (max_memory_ratio as f64 / 100.0).min(1.0);
            let global_limit = (max_memory_usage as f64 * ratio) as usize;
            builder = builder.with_max_memory_usage(global_limit);
        }

        let max_query_memory_usage = settings.get_max_query_memory_usage()? as usize;
        let out_of_memory_behavior = settings.get_query_out_of_memory_behavior()?;
        if matches!(out_of_memory_behavior, OutofMemoryBehavior::Spilling)
            && max_query_memory_usage != 0
        {
            builder = builder.with_max_query_memory_usage(
                max_query_memory_usage,
                ctx.get_query_memory_tracking(),
            );
        }

        Ok(builder.build())
    }

    fn from_sort_settings(ctx: &QueryContext) -> Result<Self> {
        let settings = ctx.get_settings();
        let mut builder = MemorySettings::builder()
            .with_spill_unit_size(settings.get_sort_spilling_batch_bytes()?);

        if !ctx.get_enable_sort_spill() {
            return Ok(builder.with_workload_group(false).build());
        }

        if settings.get_force_sort_data_spill()? {
            return Ok(builder.with_max_memory_usage(0).build());
        }

        let max_memory_usage = settings.get_max_memory_usage()? as usize;
        let max_memory_ratio = settings.get_sort_spilling_memory_ratio()?;
        if max_memory_usage != 0 && max_memory_ratio != 0 {
            let ratio = (max_memory_ratio as f64 / 100.0).min(1.0);
            let global_limit = (max_memory_usage as f64 * ratio) as usize;
            builder = builder.with_max_memory_usage(global_limit);
        }

        let max_query_memory_usage = settings.get_max_query_memory_usage()? as usize;
        let out_of_memory_behavior = settings.get_query_out_of_memory_behavior()?;
        if matches!(out_of_memory_behavior, OutofMemoryBehavior::Spilling)
            && max_query_memory_usage != 0
        {
            builder = builder.with_max_query_memory_usage(
                max_query_memory_usage,
                ctx.get_query_memory_tracking(),
            );
        }

        Ok(builder.build())
    }

    fn from_window_settings(ctx: &QueryContext) -> Result<Self> {
        let settings = ctx.get_settings();
        let spill_unit_size = settings.get_window_spill_unit_size_mb()? * 1024 * 1024;
        let mut builder = MemorySettings::builder().with_spill_unit_size(spill_unit_size);

        if settings.get_force_window_data_spill()? {
            return Ok(builder.with_max_memory_usage(0).build());
        }

        let max_memory_usage = settings.get_max_memory_usage()? as usize;
        let max_memory_ratio = settings.get_window_partition_spilling_memory_ratio()?;
        if max_memory_usage != 0 && max_memory_ratio != 0 {
            let ratio = (max_memory_ratio as f64 / 100.0).min(1.0);
            let global_limit = (max_memory_usage as f64 * ratio) as usize;
            builder = builder.with_max_memory_usage(global_limit);
        }

        let max_query_memory_usage = settings.get_max_query_memory_usage()? as usize;
        let out_of_memory_behavior = settings.get_query_out_of_memory_behavior()?;
        if matches!(out_of_memory_behavior, OutofMemoryBehavior::Spilling)
            && max_query_memory_usage != 0
        {
            builder = builder.with_max_query_memory_usage(
                max_query_memory_usage,
                ctx.get_query_memory_tracking(),
            );
        }

        Ok(builder.build())
    }

    fn from_aggregate_settings(ctx: &QueryContext) -> Result<Self> {
        let settings = ctx.get_settings();
        let mut builder = MemorySettings::builder();

        if settings.get_force_aggregate_data_spill()? {
            return Ok(builder.with_max_memory_usage(0).build());
        }

        let max_memory_usage = settings.get_max_memory_usage()? as usize;
        let max_memory_ratio = settings.get_aggregate_spilling_memory_ratio()?;
        if max_memory_usage != 0 && max_memory_ratio != 0 {
            let ratio = (max_memory_ratio as f64 / 100.0).min(1.0);
            let global_limit = (max_memory_usage as f64 * ratio) as usize;
            builder = builder.with_max_memory_usage(global_limit);
        }

        let max_query_memory_usage = settings.get_max_query_memory_usage()? as usize;
        let out_of_memory_behavior = settings.get_query_out_of_memory_behavior()?;
        if matches!(out_of_memory_behavior, OutofMemoryBehavior::Spilling)
            && max_query_memory_usage != 0
        {
            builder = builder.with_max_query_memory_usage(
                max_query_memory_usage,
                ctx.get_query_memory_tracking(),
            );
        }

        Ok(builder.build())
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
        settings.set_setting("query_out_of_memory_behavior".into(), "throw".into())?;
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
        settings.set_setting("sort_spilling_batch_bytes".into(), "1048576".into())?;

        let memory_settings = MemorySettings::from_sort_settings(&ctx)?;

        assert_eq!(memory_settings.spill_unit_size, 1048576);
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_global_window_spill_enabled() -> Result<()> {
        let fixture = TestFixture::setup().await?;
        let ctx = fixture.new_query_ctx().await?;
        let settings = ctx.get_settings();

        settings.set_setting("max_memory_usage".into(), "2000".into())?;
        settings.set_setting("window_partition_spilling_memory_ratio".into(), "50".into())?;
        settings.set_setting("query_out_of_memory_behavior".into(), "throw".into())?;
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
