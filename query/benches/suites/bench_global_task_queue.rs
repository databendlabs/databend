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

use std::sync::Arc;

use common_exception::Result;
use criterion::criterion_group;
use criterion::criterion_main;
use criterion::Criterion;
use databend_query::configs::Config;
use databend_query::pipelines::new::executor::PipelineExecutor;
use databend_query::pipelines::new::NewPipe;
use databend_query::pipelines::new::NewPipeline;
use databend_query::sessions::SessionManager;
use databend_query::sessions::SessionType;

async fn criterion_benchmark_global_task_queue(c: &mut Criterion) -> Result<()> {
    let sessions = SessionManager::from_conf(Config::default()).await?;
    let executor_session = sessions.create_session(SessionType::Test).await?;
    let ctx = executor_session.create_query_context().await?;
    ctx.get_settings().set_max_threads(8)?;
    let pipeline = create_complex_pipeline();
    let executor = PipelineExecutor::create(ctx.get_storage_runtime(), pipeline)?;
    c.bench_function("execute", |b| {
        b.iter(|| {
            executor.execute();
        })
    });
    Ok(())
}

fn create_complex_pipeline() -> NewPipeline {
    let source_pipe = NewPipe::SimplePipe {
        processors: Vec::with_capacity(10),
        inputs_port: vec![],
        outputs_port: Vec::with_capacity(10),
    };
    let mut pipeline = NewPipeline::create();
    pipeline.add_pipe(source_pipe);
    for _idx in 0..10000 {
        pipeline.add_pipe(NewPipe::SimplePipe {
            processors: Vec::with_capacity(10),
            inputs_port: Vec::with_capacity(10),
            outputs_port: Vec::with_capacity(10),
        })
    }
    let sink_pipe = NewPipe::SimplePipe {
        processors: Vec::with_capacity(10),
        inputs_port: Vec::with_capacity(10),
        outputs_port: vec![],
    };
    pipeline.add_pipe(sink_pipe);
    pipeline
}

criterion_group!(benches, criterion_benchmark_global_task_queue);
criterion_main!(benches);
