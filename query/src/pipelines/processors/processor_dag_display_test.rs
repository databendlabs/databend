use common_exception::Result;
use crate::pipelines::processors::processor_dag::{ProcessorsDAG, ProcessorDAGBuilder};

use common_base::tokio;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_graph_indent_display() -> Result<()> {
    let ctx = crate::tests::try_create_context()?;
    let query_plan = crate::tests::parse_query("select sum(number+1)+2 as sumx from numbers_mt(80000) where (number+1)=4 limit 1")?;

    let builder = ProcessorDAGBuilder::create(ctx);
    let query_processors_graph = builder.build(&query_plan)?;

    assert_eq!(
        format!("{}", query_processors_graph.display_indent()),
        "LimitTransform × 1 processor\
        \n  ProjectionTransform × 1 processor\
        \n    ExpressionTransform × 1 processor\
        \n      AggregatorFinalTransform × 1 processor\
        \n        Merge (AggregatorPartialTransform × 8 processors) to (AggregatorFinalTransform × 1)\
        \n          AggregatorPartialTransform × 8 processors\
        \n            ExpressionTransform × 8 processors\
        \n              FilterTransform × 8 processors\
        \n                SourceTransform × 8 processors\n"
    );
    Ok(())
}
