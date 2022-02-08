// Copyright 2021 Datafuse Labs.
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

use std::collections::HashMap;

use common_exception::ErrorCode;
use common_metrics::dump_metric_samples;
use common_metrics::init_default_metrics_recorder;
use common_metrics::try_handle;
use common_metrics::MetricValue;

#[tokio::test]
async fn test_dump_metric_samples() -> common_exception::Result<()> {
    init_default_metrics_recorder();
    metrics::counter!("test.test1_count", 1);
    metrics::counter!("test.test2_count", 2);
    metrics::histogram!("test.test_query_usedtime", 2.0);

    let handle = crate::try_handle().unwrap();
    let samples = dump_metric_samples(handle)
        .unwrap()
        .into_iter()
        .map(|s| (s.name.clone(), s))
        .collect::<HashMap<_, _>>();
    assert_eq!(
        MetricValue::Counter(1.0),
        samples.get("test_test1_count").unwrap().value
    );

    let summaries = match &samples.get("test_test_query_usedtime").unwrap().value {
        MetricValue::Summary(summaries) => summaries,
        _ => return Err(ErrorCode::UnexpectedError("test failed")),
    };
    assert_eq!(7, summaries.len());

    Ok(())
}
