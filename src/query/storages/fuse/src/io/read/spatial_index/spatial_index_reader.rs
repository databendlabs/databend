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
use databend_storages_common_io::ReadSettings;
use geo::algorithm::bounding_rect::BoundingRect;
use geo_index::rtree::RTreeIndex;
use geo_index::rtree::RTreeRef;
use geozero::ToGeo;
use geozero::wkb::Ewkb;
use opendal::Operator;

use crate::io::read::spatial_index::spatial_index_loader::load_spatial_index_files;

#[derive(Clone)]
pub struct SpatialIndexReader {
    operator: Operator,
    settings: ReadSettings,
    columns: Vec<String>,
    query_values: Vec<u8>,
}

impl SpatialIndexReader {
    pub fn create(
        operator: Operator,
        settings: ReadSettings,
        columns: Vec<String>,
        query_values: Vec<u8>,
    ) -> Self {
        Self {
            operator,
            settings,
            columns,
            query_values,
        }
    }

    pub async fn prune(&self, location: &str) -> Result<bool> {
        let geo = Ewkb(&self.query_values)
            .to_geo()
            .map_err(|e| ErrorCode::Internal(format!("Invalid geo ewkb value: {e}")))?;
        let Some(rect) = geo.bounding_rect() else {
            return Ok(false);
        };

        let columns = load_spatial_index_files(
            self.operator.clone(),
            &self.settings,
            &self.columns,
            location,
        )
        .await?;

        if columns.is_empty() {
            return Ok(false);
        }

        let mut checked = false;
        for column in columns {
            let Some(value) = column.index(0) else {
                continue;
            };
            let ScalarRef::Binary(buffer) = value else {
                continue;
            };
            checked = true;

            let tree = RTreeRef::<f64>::try_new(&buffer)
                .map_err(|e| ErrorCode::Internal(format!("Invalid spatial index buffer: {e}")))?;
            if !tree.search_rect(&rect).is_empty() {
                return Ok(false);
            }
        }

        if checked { Ok(true) } else { Ok(false) }
    }
}
