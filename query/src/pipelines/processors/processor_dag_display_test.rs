use common_base::tokio;
use common_exception::Result;

use crate::pipelines::processors::processor_dag::ProcessorsDAG;
use crate::pipelines::processors::processor_dag::ProcessorsDAGBuilder;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_processors_display_indent() -> Result<()> {
    struct TestCase {
        name: &'static str,
        query: &'static str,
        expect: &'static str,
    }

    let tests = vec![
        TestCase {
            name: "Simple query",
            query: "SELECT * FROM numbers(1000)",
            expect: "\
                ProjectionTransform × 8 processors\
                \n  SourceTransform × 8 processors\n",
        },
        TestCase {
            name: "Group by query without group keys",
            query: "SELECT SUM(number+1)+2 as sumx FROM numbers_mt(80000) WHERE (number+1)=4 LIMIT 1",
            expect: "\
            LimitTransform × 1 processor\
            \n  Merge (ProjectionTransform × 8 processors) to (LimitTransform × 1)\
            \n    ProjectionTransform × 8 processors\
            \n      ExpressionTransform × 8 processors\
            \n        Mixed (AggregatorFinalTransform × 1 processor) to (ExpressionTransform × 8 processors)\
            \n          AggregatorFinalTransform × 1 processor\
            \n            Merge (AggregatorPartialTransform × 8 processors) to (AggregatorFinalTransform × 1)\
            \n              AggregatorPartialTransform × 8 processors\
            \n                ExpressionTransform × 8 processors\
            \n                  FilterTransform × 8 processors\
            \n                    SourceTransform × 8 processors\n",
        },
        TestCase {
            name: "Mixed graph query without order by",
            query: "SELECT AVG(number) c FROM numbers(100000) GROUP BY number % 1000 HAVING c > 100 LIMIT 1;",
            expect: "\
            LimitTransform × 1 processor\
            \n  Merge (ProjectionTransform × 8 processors) to (LimitTransform × 1)\
            \n    ProjectionTransform × 8 processors\
            \n      HavingTransform × 8 processors\
            \n        Mixed (GroupByFinalTransform × 1 processor) to (HavingTransform × 8 processors)\
            \n          GroupByFinalTransform × 1 processor\
            \n            Merge (GroupByPartialTransform × 8 processors) to (GroupByFinalTransform × 1)\
            \n              GroupByPartialTransform × 8 processors\
            \n                ExpressionTransform × 8 processors\
            \n                  SourceTransform × 8 processors\n",
        }];

    for test in tests {
        let ctx = crate::tests::try_create_context()?;

        let query_plan = crate::tests::parse_query(test.query)?;
        let processors_graph_builder = ProcessorsDAGBuilder::create(ctx);

        let processors_graph = processors_graph_builder.build(&query_plan)?;
        assert_eq!(
            format!("{}", processors_graph.display_indent()),
            test.expect,
            "{:#?}",
            test.name
        );
    }

    Ok(())
}
