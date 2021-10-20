// Copyright 2020 Datafuse Labs.
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

use std::any::Any;
use std::sync::Arc;

use common_context::IOContext;
use common_context::TableIOContext;
use common_datablocks::DataBlock;
use common_datavalues::series::Series;
use common_datavalues::series::SeriesFrom;
use common_datavalues::DataField;
use common_datavalues::DataSchemaRefExt;
use common_datavalues::DataType;
use common_exception::ErrorCode;
use common_exception::Result;
use common_meta_types::TableInfo;
use common_planners::Extras;
use common_streams::DataBlockStream;
use common_streams::SendableDataBlockStream;
use metrics_exporter_prometheus::PrometheusHandle;
use serde_json;

use crate::catalogs::Table;
use crate::sessions::DatabendQueryContext;

pub struct MetricsTable {
    table_info: TableInfo,
}

impl MetricsTable {
    pub fn create(table_id: u64) -> Self {
        let schema = DataSchemaRefExt::create(vec![
            DataField::new("metric", DataType::String, false),
            DataField::new("kind", DataType::String, false),
            DataField::new("labels", DataType::String, false),
            DataField::new("value", DataType::String, false),
        ]);

        let table_info = TableInfo {
            db: "system".to_string(),
            name: "metrics".to_string(),
            table_id,
            schema,
            engine: "SystemMetrics".to_string(),
            ..Default::default()
        };

        MetricsTable { table_info }
    }

    fn canonicalize_metrics(
        &self,
        handle: PrometheusHandle,
    ) -> Result<Vec<(String, String, String, String)>> {
        let text = handle.render();
        let lines = text.lines().map(|s| Ok(s.to_owned()));
        let samples = prometheus_parse::Scrape::parse(lines)
            .map_err(|err| {
                ErrorCode::UnexpectedError(format!("Dump prometheus metrics failed: {:?}", err))
            })?
            .samples;
        samples
            .into_iter()
            .map(|sample| {
                Ok((
                    sample.metric,
                    self.display_sample_kind(&sample.value),
                    self.display_sample_labels(&sample.labels)?,
                    self.display_sample_value(&sample.value)?,
                ))
            })
            .collect::<Result<Vec<_>>>()
    }

    fn display_sample_labels(&self, labels: &prometheus_parse::Labels) -> Result<String> {
        Ok(format!("{:?}", labels)) // TODO: make it JSON
    }

    fn display_sample_kind(&self, value: &prometheus_parse::Value) -> String {
        use prometheus_parse::Value;
        match value {
            Value::Counter(_) => "counter",
            Value::Gauge(_) => "gauge",
            Value::Untyped(_) => "untyped",
            Value::Histogram(_) => "histogram",
            Value::Summary(_) => "summary",
        }
        .to_string()
    }

    fn display_sample_value(&self, value: &prometheus_parse::Value) -> Result<String> {
        use prometheus_parse::Value;
        match value {
            Value::Counter(v) => serde_json::to_string(v),
            Value::Gauge(v) => serde_json::to_string(v),
            Value::Untyped(v) => serde_json::to_string(v),
            Value::Histogram(v) => Ok(format!("{:?}", v)), // TODO: make it JSON
            Value::Summary(v) => Ok(format!("{:?}", v)),   // TODO: make it JSON
        }
        .map_err(|err| {
            ErrorCode::UnexpectedError(format!(
                "Dump prometheus metrics failed on display values: {}",
                err
            ))
        })
    }
}

#[async_trait::async_trait]
impl Table for MetricsTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn get_table_info(&self) -> &TableInfo {
        &self.table_info
    }

    async fn read(
        &self,
        io_ctx: Arc<TableIOContext>,
        _push_downs: &Option<Extras>,
    ) -> Result<SendableDataBlockStream> {
        let ctx: Arc<DatabendQueryContext> = io_ctx
            .get_user_data()?
            .expect("DatabendQueryContext should not be None");
        let sessions_manager = ctx.get_sessions_manager();
        let prometheus_handle = sessions_manager.get_metric_manager().handle();

        let canonicalized_metrics = self.canonicalize_metrics(prometheus_handle)?;
        let mut metrics: Vec<Vec<u8>> = Vec::with_capacity(canonicalized_metrics.len());
        let mut labels: Vec<Vec<u8>> = Vec::with_capacity(canonicalized_metrics.len());
        let mut kinds: Vec<Vec<u8>> = Vec::with_capacity(canonicalized_metrics.len());
        let mut values: Vec<Vec<u8>> = Vec::with_capacity(canonicalized_metrics.len());
        for (metric_text, kind_text, labels_text, value_text) in canonicalized_metrics.into_iter() {
            metrics.push(metric_text.clone().into_bytes());
            kinds.push(kind_text.clone().into_bytes());
            labels.push(labels_text.clone().into_bytes());
            values.push(value_text.clone().into_bytes());
        }

        let block = DataBlock::create_by_array(self.table_info.schema.clone(), vec![
            Series::new(metrics),
            Series::new(kinds),
            Series::new(labels),
            Series::new(values),
        ]);
        Ok(Box::pin(DataBlockStream::create(
            self.table_info.schema.clone(),
            None,
            vec![block],
        )))
    }
}
