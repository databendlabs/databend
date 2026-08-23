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

use databend_common_expression::DataSchemaRefExt;
use databend_common_sql::executor::physical_plans::FragmentKind;
use databend_query::physical_plans::ConstantTableScan;
use databend_query::physical_plans::Exchange;
use databend_query::physical_plans::ExchangeSink;
use databend_query::physical_plans::ExchangeSource;
use databend_query::physical_plans::PhysicalPlan;
use databend_query::physical_plans::PhysicalPlanCast;
use databend_query::physical_plans::PhysicalPlanMeta;
use databend_query::schedulers::Fragmenter;
use databend_query::schedulers::QueryFragmentsActions;
use databend_query::servers::flight::v1::exchange::DataExchange;
use databend_query::test_kits::ClusterDescriptor;
use databend_query::test_kits::TestFixture;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_coordinator_broadcast_source_keeps_worker_receiver() -> anyhow::Result<()> {
    let fixture = TestFixture::setup().await?;
    let cluster = ClusterDescriptor::new()
        .with_node("coordinator", "127.0.0.1:19001")
        .with_node("worker", "127.0.0.1:19002")
        .with_local_id("coordinator");
    let ctx = fixture.new_query_ctx_with_cluster(cluster).await?;

    let scan = PhysicalPlan::new(ConstantTableScan {
        values: vec![],
        num_rows: 1,
        output_schema: DataSchemaRefExt::create(vec![]),
        meta: PhysicalPlanMeta::new("ConstantTableScan"),
    });
    let merge = PhysicalPlan::new(Exchange {
        input: scan,
        kind: FragmentKind::Merge,
        keys: vec![],
        ignore_exchange: false,
        allow_adjust_parallelism: true,
        source_on_coordinator: false,
        meta: PhysicalPlanMeta::new("Exchange"),
    });
    let broadcast = PhysicalPlan::new(Exchange {
        input: merge,
        kind: FragmentKind::Expansive,
        keys: vec![],
        ignore_exchange: false,
        allow_adjust_parallelism: true,
        source_on_coordinator: true,
        meta: PhysicalPlanMeta::new("Exchange"),
    });

    let fragments = Fragmenter::try_create(ctx.clone())?.build_fragment(&broadcast)?;
    let broadcast_fragment = fragments
        .iter()
        .find(|fragment| matches!(&fragment.exchange, Some(DataExchange::Broadcast(_))))
        .expect("broadcast fragment");
    let broadcast_fragment_id = broadcast_fragment.fragment_id;
    let mut actions = QueryFragmentsActions::create(ctx.clone());
    for fragment in &fragments {
        fragment.get_actions(ctx.clone(), &mut actions)?;
    }

    let broadcast_actions = actions
        .fragments_actions
        .iter()
        .find(|actions| actions.fragment_id == broadcast_fragment_id)
        .expect("broadcast fragment actions");
    assert_eq!(broadcast_actions.fragment_actions.len(), 2);

    let coordinator_action = broadcast_actions
        .fragment_actions
        .iter()
        .find(|action| action.executor == "coordinator")
        .expect("coordinator action");
    let coordinator_sink = ExchangeSink::from_physical_plan(&coordinator_action.physical_plan)
        .expect("coordinator exchange sink");
    assert!(ExchangeSource::check_physical_plan(&coordinator_sink.input));

    let worker_action = broadcast_actions
        .fragment_actions
        .iter()
        .find(|action| action.executor == "worker")
        .expect("worker action");
    let worker_sink = ExchangeSink::from_physical_plan(&worker_action.physical_plan)
        .expect("worker exchange sink");
    let worker_input =
        ConstantTableScan::from_physical_plan(&worker_sink.input).expect("worker receiver input");
    assert_eq!(worker_input.num_rows, 0);
    assert_eq!(worker_input.output_schema, worker_sink.schema);

    Ok(())
}
