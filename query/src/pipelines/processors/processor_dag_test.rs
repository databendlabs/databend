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

use common_base::tokio;
use common_exception::Result;
use pretty_assertions::assert_eq;

use petgraph::dot::{Dot, Config};
use crate::pipelines::processors::*;
use crate::pipelines::processors::processor_dag::ProcessorDAGBuilder;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_processors_dag() -> Result<()> {
    let ctx = crate::tests::try_create_context()?;
    let plan = crate::tests::parse_query("SELECT * FROM numbers(1000)")?;

    let dag_builder = ProcessorDAGBuilder::create(ctx);
    println!("{:?}", dag_builder.build(&plan)?);
    Ok(())
}