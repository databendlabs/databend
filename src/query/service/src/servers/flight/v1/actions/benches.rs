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

use databend_common_base::runtime::TrySpawn;
use databend_common_exception::Result;
use databend_common_sql::Planner;
use databend_common_storages_system::BenchesArguments;
use databend_common_storages_system::TestMetric;
use futures_util::StreamExt;

use crate::interpreters::InterpreterFactory;
use crate::servers::flight::v1::actions::create_session;

pub async fn benches(arguments: BenchesArguments) -> Result<Vec<TestMetric>> {
    let session = create_session()?;
    let query_context = session.create_query_context().await?;

    let query_context = query_context.clone();
    let handle = query_context.spawn({
        let query_context = query_context.clone();
        async move {
            let query = format!(
                "SELECT * FROM benches('{}')",
                serde_json::to_string(&arguments).unwrap()
            );
            let mut planner = Planner::new(query_context.clone());
            let (plan, _) = planner.plan_sql(&query).await?;
            let executor = InterpreterFactory::get_inner(query_context.clone(), &plan)?;
            let mut stream = executor.execute(query_context.clone()).await?;

            let mut metrics = vec![];
            while let Some(block) = stream.next().await {
                metrics.extend(TestMetric::from_block(block?));
            }

            Ok(metrics)
        }
    });

    handle.await.flatten()
}
