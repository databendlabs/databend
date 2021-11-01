---
id: development-tracing
title: Tracing in Databend
---

## Tracing In Databend

Databend using Rust's tracing ecosystem [tokio-tracing](https://github.com/tokio-rs/tracing) to do log and profile.

Databend default log level is `INFO`.

## Enable Tracing

```
QUERY_LOG_LEVEL="DEBUG" ./databend-query
```

If we want to track the execution of a query:
```
set max_threads=1;
select sum(number+1)+1 from numbers(10000) where number>0 group by number%3;
```

Tracing log:
```
[2021-06-10T08:40:36Z DEBUG clickhouse_srv::cmd] Got packet Query(QueryRequest { query_id: "bac2b254-6245-4cae-910d-3e5e979c8b68", client_info: QueryClientInfo { query_kind: 1, initial_user: "", initial_query_id: "", initial_address: "0.0.0.0:0", interface: 1, os_user: "bohu", client_hostname: "thinkpad", client_name: "ClickHouse ", client_version_major: 21, client_version_minor: 4, client_version_patch: 6, client_revision: 54447, http_method: 0, http_user_agent: "", quota_key: "" }, stage: 2, compression: 1, query: "select sum(number+1)+1 from numbers(10000) where number>0 group by number%3;" })
Jun 10 16:40:36.131 DEBUG ThreadId(16) databend_query::sql::plan_parser: query="select sum(number+1)+1 from numbers(10000) where number>0 group by number%3;"
[2021-06-10T08:40:36Z DEBUG sqlparser::parser] parsing expr
[2021-06-10T08:40:36Z DEBUG sqlparser::parser] parsing expr
[2021-06-10T08:40:36Z DEBUG sqlparser::parser] prefix: Identifier(Ident { value: "number", quote_style: None })
[2021-06-10T08:40:36Z DEBUG sqlparser::parser] get_next_precedence() Plus
[2021-06-10T08:40:36Z DEBUG sqlparser::parser] next precedence: 30
[2021-06-10T08:40:36Z DEBUG sqlparser::parser] parsing expr
[2021-06-10T08:40:36Z DEBUG sqlparser::parser] prefix: Value(Number("1", false))
[2021-06-10T08:40:36Z DEBUG sqlparser::parser] get_next_precedence() RParen
[2021-06-10T08:40:36Z DEBUG sqlparser::parser] next precedence: 0
[2021-06-10T08:40:36Z DEBUG sqlparser::parser] get_next_precedence() RParen
[2021-06-10T08:40:36Z DEBUG sqlparser::parser] next precedence: 0
[2021-06-10T08:40:36Z DEBUG sqlparser::parser] prefix: Function(Function { name: ObjectName([Ident { value: "sum", quote_style: None }]), args: [Unnamed(BinaryOp { left: Identifier(Ident { value: "number", quote_style: None }), op: Plus, right: Value(Number("1", false)) })], over: None, distinct: false })
[2021-06-10T08:40:36Z DEBUG sqlparser::parser] get_next_precedence() Plus
[2021-06-10T08:40:36Z DEBUG sqlparser::parser] next precedence: 30
[2021-06-10T08:40:36Z DEBUG sqlparser::parser] parsing expr
[2021-06-10T08:40:36Z DEBUG sqlparser::parser] prefix: Value(Number("1", false))
[2021-06-10T08:40:36Z DEBUG sqlparser::parser] get_next_precedence() Word(Word { value: "from", quote_style: None, keyword: FROM })
[2021-06-10T08:40:36Z DEBUG sqlparser::parser] next precedence: 0
[2021-06-10T08:40:36Z DEBUG sqlparser::parser] get_next_precedence() Word(Word { value: "from", quote_style: None, keyword: FROM })
[2021-06-10T08:40:36Z DEBUG sqlparser::parser] next precedence: 0
[2021-06-10T08:40:36Z DEBUG sqlparser::parser] parsing expr
[2021-06-10T08:40:36Z DEBUG sqlparser::parser] prefix: Value(Number("10000", false))
[2021-06-10T08:40:36Z DEBUG sqlparser::parser] get_next_precedence() RParen
[2021-06-10T08:40:36Z DEBUG sqlparser::parser] next precedence: 0
[2021-06-10T08:40:36Z DEBUG sqlparser::parser] parsing expr
[2021-06-10T08:40:36Z DEBUG sqlparser::parser] prefix: Identifier(Ident { value: "number", quote_style: None })
[2021-06-10T08:40:36Z DEBUG sqlparser::parser] get_next_precedence() Gt
[2021-06-10T08:40:36Z DEBUG sqlparser::parser] next precedence: 20
[2021-06-10T08:40:36Z DEBUG sqlparser::parser] parsing expr
[2021-06-10T08:40:36Z DEBUG sqlparser::parser] prefix: Value(Number("0", false))
[2021-06-10T08:40:36Z DEBUG sqlparser::parser] get_next_precedence() Word(Word { value: "group", quote_style: None, keyword: GROUP })
[2021-06-10T08:40:36Z DEBUG sqlparser::parser] next precedence: 0
[2021-06-10T08:40:36Z DEBUG sqlparser::parser] get_next_precedence() Word(Word { value: "group", quote_style: None, keyword: GROUP })
[2021-06-10T08:40:36Z DEBUG sqlparser::parser] next precedence: 0
[2021-06-10T08:40:36Z DEBUG sqlparser::parser] parsing expr
[2021-06-10T08:40:36Z DEBUG sqlparser::parser] prefix: Identifier(Ident { value: "number", quote_style: None })
[2021-06-10T08:40:36Z DEBUG sqlparser::parser] get_next_precedence() Mod
[2021-06-10T08:40:36Z DEBUG sqlparser::parser] next precedence: 40
[2021-06-10T08:40:36Z DEBUG sqlparser::parser] parsing expr
[2021-06-10T08:40:36Z DEBUG sqlparser::parser] prefix: Value(Number("3", false))
[2021-06-10T08:40:36Z DEBUG sqlparser::parser] get_next_precedence() SemiColon
[2021-06-10T08:40:36Z DEBUG sqlparser::parser] next precedence: 0
[2021-06-10T08:40:36Z DEBUG sqlparser::parser] get_next_precedence() SemiColon
[2021-06-10T08:40:36Z DEBUG sqlparser::parser] next precedence: 0
Jun 10 16:40:36.135  INFO ThreadId(16) sql_statement_to_plan: databend_query::sql::plan_parser: new
Jun 10 16:40:36.136  INFO ThreadId(16) sql_statement_to_plan: databend_query::sql::plan_parser: enter
Jun 10 16:40:36.136  INFO ThreadId(16) sql_statement_to_plan:select_to_plan: databend_query::sql::plan_parser: new
Jun 10 16:40:36.136  INFO ThreadId(16) sql_statement_to_plan:select_to_plan: databend_query::sql::plan_parser: enter
Jun 10 16:40:36.139  INFO ThreadId(16) sql_statement_to_plan:select_to_plan: databend_query::sql::plan_parser: exit
Jun 10 16:40:36.139  INFO ThreadId(16) sql_statement_to_plan:select_to_plan: databend_query::sql::plan_parser: close time.busy=2.65ms time.idle=457µs
Jun 10 16:40:36.139  INFO ThreadId(16) sql_statement_to_plan: databend_query::sql::plan_parser: exit
Jun 10 16:40:36.139  INFO ThreadId(16) sql_statement_to_plan: databend_query::sql::plan_parser: close time.busy=3.57ms time.idle=453µs
Jun 10 16:40:36.140  INFO ThreadId(309) execute{ctx.id="1c651744-3e73-4b94-9df0-dc031b73c626"}: databend_query::interpreters::interpreter_select: new
Jun 10 16:40:36.141  INFO ThreadId(309) execute{ctx.id="1c651744-3e73-4b94-9df0-dc031b73c626"}: databend_query::interpreters::interpreter_select: enter
Jun 10 16:40:36.141 DEBUG ThreadId(309) execute{ctx.id="1c651744-3e73-4b94-9df0-dc031b73c626"}: databend_query::optimizers::optimizer: Before ProjectionPushDown
Projection: (sum((number + 1)) + 1):UInt64
  Expression: (sum((number + 1)) + 1):UInt64 (Before Projection)
    AggregatorFinal: groupBy=[[(number % 3)]], aggr=[[sum((number + 1))]]
      AggregatorPartial: groupBy=[[(number % 3)]], aggr=[[sum((number + 1))]]
        Expression: (number % 3):UInt8, (number + 1):UInt64 (Before GroupBy)
          Filter: (number > 0)
            ReadDataSource: scan partitions: [1], scan schema: [number:UInt64], statistics: [read_rows: 10000, read_bytes: 80000]
Jun 10 16:40:36.142 DEBUG ThreadId(309) execute{ctx.id="1c651744-3e73-4b94-9df0-dc031b73c626"}: databend_query::optimizers::optimizer: After ProjectionPushDown
Projection: (sum((number + 1)) + 1):UInt64
  Expression: (sum((number + 1)) + 1):UInt64 (Before Projection)
    AggregatorFinal: groupBy=[[(number % 3)]], aggr=[[sum((number + 1))]]
      AggregatorPartial: groupBy=[[(number % 3)]], aggr=[[sum((number + 1))]]
        Expression: (number % 3):UInt8, (number + 1):UInt64 (Before GroupBy)
          Filter: (number > 0)
            ReadDataSource: scan partitions: [1], scan schema: [number:UInt64], statistics: [read_rows: 10000, read_bytes: 80000]
Jun 10 16:40:36.142 DEBUG ThreadId(309) execute{ctx.id="1c651744-3e73-4b94-9df0-dc031b73c626"}: databend_query::optimizers::optimizer: Before Scatters
Projection: (sum((number + 1)) + 1):UInt64
  Expression: (sum((number + 1)) + 1):UInt64 (Before Projection)
    AggregatorFinal: groupBy=[[(number % 3)]], aggr=[[sum((number + 1))]]
      AggregatorPartial: groupBy=[[(number % 3)]], aggr=[[sum((number + 1))]]
        Expression: (number % 3):UInt8, (number + 1):UInt64 (Before GroupBy)
          Filter: (number > 0)
            ReadDataSource: scan partitions: [1], scan schema: [number:UInt64], statistics: [read_rows: 10000, read_bytes: 80000]
Jun 10 16:40:36.143 DEBUG ThreadId(309) execute{ctx.id="1c651744-3e73-4b94-9df0-dc031b73c626"}: databend_query::optimizers::optimizer: After Scatters
Projection: (sum((number + 1)) + 1):UInt64
  Expression: (sum((number + 1)) + 1):UInt64 (Before Projection)
    AggregatorFinal: groupBy=[[(number % 3)]], aggr=[[sum((number + 1))]]
      AggregatorPartial: groupBy=[[(number % 3)]], aggr=[[sum((number + 1))]]
        Expression: (number % 3):UInt8, (number + 1):UInt64 (Before GroupBy)
          Filter: (number > 0)
            ReadDataSource: scan partitions: [1], scan schema: [number:UInt64], statistics: [read_rows: 10000, read_bytes: 80000]
Jun 10 16:40:36.143  INFO ThreadId(309) execute{ctx.id="1c651744-3e73-4b94-9df0-dc031b73c626"}:reschedule: databend_query::interpreters::plan_scheduler: new
Jun 10 16:40:36.143  INFO ThreadId(309) execute{ctx.id="1c651744-3e73-4b94-9df0-dc031b73c626"}:reschedule: databend_query::interpreters::plan_scheduler: enter
Jun 10 16:40:36.143  INFO ThreadId(309) execute{ctx.id="1c651744-3e73-4b94-9df0-dc031b73c626"}:reschedule: databend_query::interpreters::plan_scheduler: exit
Jun 10 16:40:36.143  INFO ThreadId(309) execute{ctx.id="1c651744-3e73-4b94-9df0-dc031b73c626"}:reschedule: databend_query::interpreters::plan_scheduler: close time.busy=145µs time.idle=264µs
Jun 10 16:40:36.144  INFO ThreadId(309) execute{ctx.id="1c651744-3e73-4b94-9df0-dc031b73c626"}:build: databend_query::pipelines::processors::pipeline_builder: new
Jun 10 16:40:36.144  INFO ThreadId(309) execute{ctx.id="1c651744-3e73-4b94-9df0-dc031b73c626"}:build: databend_query::pipelines::processors::pipeline_builder: enter
Jun 10 16:40:36.144 DEBUG ThreadId(309) execute{ctx.id="1c651744-3e73-4b94-9df0-dc031b73c626"}:build: databend_query::pipelines::processors::pipeline_builder: Received plan:
Projection: (sum((number + 1)) + 1):UInt64
  Expression: (sum((number + 1)) + 1):UInt64 (Before Projection)
    AggregatorFinal: groupBy=[[(number % 3)]], aggr=[[sum((number + 1))]]
      AggregatorPartial: groupBy=[[(number % 3)]], aggr=[[sum((number + 1))]]
        Expression: (number % 3):UInt8, (number + 1):UInt64 (Before GroupBy)
          Filter: (number > 0)
            ReadDataSource: scan partitions: [1], scan schema: [number:UInt64], statistics: [read_rows: 10000, read_bytes: 80000]
Jun 10 16:40:36.145 DEBUG ThreadId(309) execute{ctx.id="1c651744-3e73-4b94-9df0-dc031b73c626"}:build: databend_query::pipelines::processors::pipeline_builder: Pipeline:
ProjectionTransform × 1 processor
  ExpressionTransform × 1 processor
    GroupByFinalTransform × 1 processor
      GroupByPartialTransform × 1 processor
        ExpressionTransform × 1 processor
          FilterTransform × 1 processor
            SourceTransform × 1 processor
Jun 10 16:40:36.145  INFO ThreadId(309) execute{ctx.id="1c651744-3e73-4b94-9df0-dc031b73c626"}:build: databend_query::pipelines::processors::pipeline_builder: exit
Jun 10 16:40:36.145  INFO ThreadId(309) execute{ctx.id="1c651744-3e73-4b94-9df0-dc031b73c626"}:build: databend_query::pipelines::processors::pipeline_builder: close time.busy=1.07ms time.idle=215µs
Jun 10 16:40:36.145 DEBUG ThreadId(309) execute{ctx.id="1c651744-3e73-4b94-9df0-dc031b73c626"}: databend_query::pipelines::transforms::transform_projection: execute...
Jun 10 16:40:36.145 DEBUG ThreadId(309) execute{ctx.id="1c651744-3e73-4b94-9df0-dc031b73c626"}: databend_query::pipelines::transforms::transform_groupby_final: execute...
Jun 10 16:40:36.146 DEBUG ThreadId(309) execute{ctx.id="1c651744-3e73-4b94-9df0-dc031b73c626"}: databend_query::pipelines::transforms::transform_groupby_partial: execute...
Jun 10 16:40:36.146 DEBUG ThreadId(309) execute{ctx.id="1c651744-3e73-4b94-9df0-dc031b73c626"}: databend_query::pipelines::transforms::transform_source: execute, table:system.numbers, is_remote:false...
Jun 10 16:40:36.148 DEBUG ThreadId(309) execute{ctx.id="1c651744-3e73-4b94-9df0-dc031b73c626"}: databend_query::pipelines::transforms::transform_filter: execute...
Jun 10 16:40:36.148 DEBUG ThreadId(309) execute{ctx.id="1c651744-3e73-4b94-9df0-dc031b73c626"}: databend_query::pipelines::transforms::transform_expression_executor: (filter executor) execute, actions: [Input(ActionInput { name: "number", return_type: UInt64 }), Constant(ActionConstant { name: "0", value: 0 }), Function(ActionFunction { name: "(number > 0)", func_name: ">", return_type: Boolean, is_aggregated: false, arg_names: ["number", "0"], arg_types: [UInt64, UInt64], arg_fields: [] })]
Jun 10 16:40:36.150 DEBUG ThreadId(309) execute{ctx.id="1c651744-3e73-4b94-9df0-dc031b73c626"}: databend_query::pipelines::transforms::transform_filter: Filter cost: 1.678104ms
Jun 10 16:40:36.150 DEBUG ThreadId(309) execute{ctx.id="1c651744-3e73-4b94-9df0-dc031b73c626"}: databend_query::pipelines::transforms::transform_expression_executor: (expression executor) execute, actions: [Input(ActionInput { name: "number", return_type: UInt64 }), Constant(ActionConstant { name: "3", value: 3 }), Function(ActionFunction { name: "(number % 3)", func_name: "%", return_type: UInt64, is_aggregated: false, arg_names: ["number", "3"], arg_types: [UInt64, UInt64], arg_fields: [] }), Input(ActionInput { name: "number", return_type: UInt64 }), Constant(ActionConstant { name: "1", value: 1 }), Function(ActionFunction { name: "(number + 1)", func_name: "+", return_type: UInt64, is_aggregated: false, arg_names: ["number", "1"], arg_types: [UInt64, UInt64], arg_fields: [] })]
Jun 10 16:40:36.165 DEBUG ThreadId(309) execute{ctx.id="1c651744-3e73-4b94-9df0-dc031b73c626"}: databend_query::pipelines::transforms::transform_groupby_partial: Group by partial cost: 18.822193ms
Jun 10 16:40:36.166 DEBUG ThreadId(309) execute{ctx.id="1c651744-3e73-4b94-9df0-dc031b73c626"}: databend_query::pipelines::transforms::transform_groupby_final: Group by final cost: 20.170851ms
Jun 10 16:40:36.167  INFO ThreadId(309) execute{ctx.id="1c651744-3e73-4b94-9df0-dc031b73c626"}: databend_query::interpreters::interpreter_select: exit
Jun 10 16:40:36.167  INFO ThreadId(309) execute{ctx.id="1c651744-3e73-4b94-9df0-dc031b73c626"}: databend_query::interpreters::interpreter_select: close time.busy=26.1ms time.idle=592µs
Jun 10 16:40:36.167 DEBUG ThreadId(309) databend_query::pipelines::transforms::transform_expression_executor: (expression executor) execute, actions: [Input(ActionInput { name: "sum((number + 1))", return_type: UInt64 }), Constant(ActionConstant { name: "1", value: 1 }), Function(ActionFunction { name: "(sum((number + 1)) + 1)", func_name: "+", return_type: UInt64, is_aggregated: false, arg_names: ["sum((number + 1))", "1"], arg_types: [UInt64, UInt64], arg_fields: [] })]
Jun 10 16:40:36.168 DEBUG ThreadId(309) databend_query::pipelines::transforms::transform_expression_executor: (projection executor) execute, actions: [Input(ActionInput { name: "(sum((number + 1)) + 1)", return_type: UInt64 })]
Jun 10 16:40:36.168 DEBUG ThreadId(309) databend_query::pipelines::transforms::transform_projection: Projection cost: 241.864µs
```