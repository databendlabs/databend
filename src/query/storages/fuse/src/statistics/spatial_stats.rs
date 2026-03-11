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

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::ScalarRef;
use databend_common_io::ewkb_to_geo;
use databend_storages_common_table_meta::meta::SpatialStatistics;
use geo::BoundingRect;
use geozero::ToGeo;
use geozero::wkb::Ewkb;
use log::debug;

#[derive(Clone)]
pub struct SpatialStatsBuilder {
    min_x: f64,
    min_y: f64,
    max_x: f64,
    max_y: f64,
    srid: Option<i32>,
    has_null: bool,
    has_value: bool,
    srid_mixed: bool,
}

impl Default for SpatialStatsBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl SpatialStatsBuilder {
    pub fn new() -> Self {
        Self {
            min_x: 0.0,
            min_y: 0.0,
            max_x: 0.0,
            max_y: 0.0,
            srid: None,
            has_null: false,
            has_value: false,
            srid_mixed: false,
        }
    }

    pub fn update_value(&mut self, value: ScalarRef) -> Result<()> {
        match value {
            ScalarRef::Geometry(buf) => {
                let (geo, srid) = ewkb_to_geo(&mut Ewkb(buf))?;
                if !self.update_srid(srid.unwrap_or(0)) {
                    return Ok(());
                }
                if let Some(rect) = geo.bounding_rect() {
                    self.update_rect(rect);
                }
            }
            ScalarRef::Geography(buf) => {
                let geo = Ewkb(buf.0)
                    .to_geo()
                    .map_err(|e| ErrorCode::GeometryError(e.to_string()))?;
                if !self.update_srid(4326) {
                    return Ok(());
                }
                if let Some(rect) = geo.bounding_rect() {
                    self.update_rect(rect);
                }
            }
            _ => {
                self.has_null = true;
            }
        }
        Ok(())
    }

    pub fn update_rect_with_srid(&mut self, rect: Option<geo::Rect<f64>>, srid: i32) {
        if !self.update_srid(srid) {
            return;
        }
        if let Some(rect) = rect {
            self.update_rect(rect);
        }
    }

    pub fn mark_null(&mut self) {
        self.has_null = true;
    }

    pub fn srid(&self) -> Option<i32> {
        self.srid
    }

    pub fn is_srid_mixed(&self) -> bool {
        self.srid_mixed
    }

    pub fn finalize(self) -> Option<SpatialStatistics> {
        if self.srid_mixed || !self.has_value {
            return None;
        }
        let srid = self.srid?;
        Some(SpatialStatistics {
            min_x: self.min_x.into(),
            min_y: self.min_y.into(),
            max_x: self.max_x.into(),
            max_y: self.max_y.into(),
            srid,
            has_null: self.has_null,
        })
    }

    fn update_srid(&mut self, srid: i32) -> bool {
        if let Some(prev) = self.srid {
            if prev != srid {
                debug!("Mixed SRID {} and {}", prev, srid);
                self.srid_mixed = true;
                return false;
            }
        } else {
            self.srid = Some(srid);
        }
        true
    }

    fn update_rect(&mut self, rect: geo::Rect<f64>) {
        let min = rect.min();
        let max = rect.max();
        if self.has_value {
            self.min_x = self.min_x.min(min.x);
            self.min_y = self.min_y.min(min.y);
            self.max_x = self.max_x.max(max.x);
            self.max_y = self.max_y.max(max.y);
        } else {
            self.min_x = min.x;
            self.min_y = min.y;
            self.max_x = max.x;
            self.max_y = max.y;
            self.has_value = true;
        }
    }
}

#[cfg(test)]
mod tests {
    use databend_common_expression::ScalarRef;
    use databend_common_io::geometry::geometry_from_str;

    use super::SpatialStatsBuilder;

    #[test]
    fn test_spatial_stats_builder_mixed_srid() -> databend_common_exception::Result<()> {
        let mut builder = SpatialStatsBuilder::new();
        let ewkb_4326 = geometry_from_str("SRID=4326;POINT(1 1)", None)?;
        let ewkb_3857 = geometry_from_str("SRID=3857;POINT(2 2)", None)?;

        builder.update_value(ScalarRef::Geometry(ewkb_4326.as_slice()))?;
        assert!(!builder.is_srid_mixed());

        builder.update_value(ScalarRef::Geometry(ewkb_3857.as_slice()))?;
        assert!(builder.is_srid_mixed());
        assert!(builder.finalize().is_none());
        Ok(())
    }

    #[test]
    fn test_spatial_stats_builder_empty_geometry() -> databend_common_exception::Result<()> {
        let mut builder = SpatialStatsBuilder::new();
        let ewkb_empty = geometry_from_str("POINT EMPTY", None)?;
        builder.update_value(ScalarRef::Geometry(ewkb_empty.as_slice()))?;
        assert!(!builder.is_srid_mixed());
        assert!(builder.finalize().is_none());
        Ok(())
    }
}
