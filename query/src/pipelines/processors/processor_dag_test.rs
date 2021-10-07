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

use crate::pipelines::processors::processor_dag::ProcessorDAGBuilder;
use futures::{FutureExt, Future, Stream, StreamExt};
use std::pin::Pin;
use std::task::Context;
use common_base::tokio::macros::support::Poll;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_processors_dag() -> Result<()> {
    struct TestCase {
        name: &'static str,
        query: &'static str,
        expect: &'static str,
    }

    let tests = vec![TestCase {
        name: "Simple query",
        query: "SELECT * FROM numbers(1000)",
        expect: "digraph {\
            \n    0 [ label = \"SourceTransform\" ]\
            \n    1 [ label = \"SourceTransform\" ]\
            \n    2 [ label = \"SourceTransform\" ]\
            \n    3 [ label = \"SourceTransform\" ]\
            \n    4 [ label = \"SourceTransform\" ]\
            \n    5 [ label = \"SourceTransform\" ]\
            \n    6 [ label = \"SourceTransform\" ]\
            \n    7 [ label = \"SourceTransform\" ]\
            \n    8 [ label = \"ProjectionTransform\" ]\
            \n    9 [ label = \"ProjectionTransform\" ]\
            \n    10 [ label = \"ProjectionTransform\" ]\
            \n    11 [ label = \"ProjectionTransform\" ]\
            \n    12 [ label = \"ProjectionTransform\" ]\
            \n    13 [ label = \"ProjectionTransform\" ]\
            \n    14 [ label = \"ProjectionTransform\" ]\
            \n    15 [ label = \"ProjectionTransform\" ]\
            \n    8 -> 0 [ ]\
            \n    9 -> 1 [ ]\
            \n    10 -> 2 [ ]\
            \n    11 -> 3 [ ]\
            \n    12 -> 4 [ ]\
            \n    13 -> 5 [ ]\
            \n    14 -> 6 [ ]\
            \n    15 -> 7 [ ]\
            \n}\n",
    }];

    for test in tests {
        let ctx = crate::tests::try_create_context()?;

        let query_plan = crate::tests::parse_query(test.query)?;
        let processors_graph_builder = ProcessorDAGBuilder::create(ctx);

        let processors_graph = processors_graph_builder.build(&query_plan)?;
        assert_eq!(
            format!("{:?}", processors_graph),
            test.expect,
            "{:#?}",
            test.name
        );
    }

    Ok(())
}
//
// pub struct S {
//     pub count: usize,
// }
//
// impl S {
//     pub async fn get_and_inc(&mut self) -> usize {
//         self.count += 1;
//         self.count
//     }
// }
//
// struct StreamFuture<Fut: Future>(Pin<Box<Fut>>);
//
// impl<Fut: Future> Stream for StreamFuture<Fut> {
//     type Item = Fut::Output;
//
//     fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
//         self.0.poll_unpin(cx).map(|s| Some(s))
//     }
// }
//
// #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
// async fn test_s() {
//     let mut s = S { count: 0 };
//     let future = s.get_and_inc();
//     let mut ss = StreamFuture(Box::pin(future));
//     println!("first: {:?}", ss.next().await);
//     println!("seconds: {:?}", ss.next().await);
// }
// fn main() {
//     let mut s = S { count: 0 };
//     let future = s.get_and_inc();
//
// }

