// Copyright 2025 RisingWave Labs
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

#![cfg(all(feature = "javascript-runtime", feature = "javascript-fetch"))]

use std::sync::Arc;

use arrow_array::{ArrayRef, Int32Array, RecordBatch};
use arrow_cast::pretty::{pretty_format_batches, pretty_format_columns};
use arrow_schema::{DataType, Field, Schema};
use arrow_udf_runtime::javascript::{AggregateOptions, FunctionOptions, Runtime};
use expect_test::{expect, Expect};
use mockito::Server;
use rquickjs::{async_with, AsyncContext};

async fn run_async_js_code(context: &AsyncContext, js_code: &str) {
    async_with!(context => |ctx| {
        ctx.eval_promise::<_>(js_code)
        .inspect_err(|e| inspect_error(e, &ctx))
        .unwrap()
        .into_future::<()>()
        .await
        .inspect_err(|e| inspect_error(e, &ctx))
        .unwrap();
    })
    .await;
}

#[tokio::test]
async fn run_javascript_tests() {
    let runtime = Runtime::new().await.unwrap();
    runtime.enable_fetch().await.unwrap();

    run_async_js_code(
        runtime.context(),
        &std::fs::read_to_string("src/javascript/fetch/fetch.test.js").unwrap(),
    )
    .await;
    run_async_js_code(
        runtime.context(),
        &std::fs::read_to_string("src/javascript/fetch/headers.test.js").unwrap(),
    )
    .await;
}

#[tokio::test]
async fn test_fetch_in_udf() {
    let mut server = Server::new_async().await;

    let mock_hello = server
        .mock("POST", "/echo")
        .match_header("authorization", "Bearer dummy-token")
        .match_body(r#"{"input":"hello"}"#)
        .with_status(200)
        .with_header("content-type", "application/json")
        .with_body(r#"{"output":"hello"}"#)
        .create();
    let mock_buddy = server
        .mock("POST", "/echo")
        .match_header("authorization", "Bearer dummy-token")
        .match_body(r#"{"input":"buddy"}"#)
        .with_status(200)
        .with_header("content-type", "application/json")
        .with_body(r#"{"output":"buddy"}"#)
        .create();
    let mock_bad_request = server
        .mock("POST", "/echo")
        .match_header("authorization", "Bearer dummy-token")
        .match_body(r#"{"input":null}"#)
        .with_status(400)
        .with_header("content-type", "application/json")
        .with_body(r#"{"error":"Bad Request"}"#)
        .create();

    let mut runtime = Runtime::new().await.unwrap();
    runtime.enable_fetch().await.unwrap();

    let js_code = r#"
    export async function test_fetch(input) {
        const body = JSON.stringify({
            input: input
        });
        const response = await fetch("$URL/echo", {
            method: 'POST',
            headers: {
                'Authorization': 'Bearer dummy-token'
            },
            body
        });
        if (!response.ok) {
            const m = await response.json();
            throw new Error(m.error);
        }
        const m = await response.json();
        return m.output;
    }
"#
    .replace("$URL", &server.url());

    runtime
        .add_function(
            "test_fetch",
            DataType::Utf8,
            &js_code,
            FunctionOptions::default().async_mode(),
        )
        .await
        .unwrap();

    let schema = Schema::new(vec![Field::new("input", DataType::Utf8, true)]);
    let arg0 = arrow_array::StringArray::from(vec![Some("hello"), Some("buddy")]);
    let input = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(arg0)]).unwrap();

    let output = runtime.call("test_fetch", &input).await.unwrap();
    check(
        &[output],
        expect![[r#"
    +------------+
    | test_fetch |
    +------------+
    | hello      |
    | buddy      |
    +------------+"#]],
    );

    let schema = Schema::new(vec![Field::new("input", DataType::Utf8, true)]);
    let arg0 = arrow_array::StringArray::from(vec![None] as Vec<Option<String>>);
    let input = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(arg0)]).unwrap();

    let error = runtime.call("test_fetch", &input).await.unwrap_err();
    assert!(error.source().unwrap().to_string().contains("Bad Request"));

    mock_hello.assert();
    mock_buddy.assert();
    mock_bad_request.assert();
}

#[tokio::test]
async fn test_fetch_in_udaf() {
    let mut server = Server::new_async().await;

    // Mock endpoints for each UDAF operation
    let mock_init = server
        .mock("POST", "/init")
        .with_status(200)
        .with_header("content-type", "application/json")
        .with_body(r#"{"state": 0}"#)
        .create();

    let mock_acc = server
        .mock("POST", "/acc")
        .match_header("content-type", "application/json")
        .match_body(r#"{"state":0,"value":1}"#)
        .with_status(200)
        .with_header("content-type", "application/json")
        .with_body(r#"{"state": 1}"#)
        .create();

    let mock_merge = server
        .mock("POST", "/merge")
        .match_header("content-type", "application/json")
        .match_body(r#"{"state1":1,"state2":1}"#)
        .with_status(200)
        .with_header("content-type", "application/json")
        .with_body(r#"{"state": 2}"#)
        .create();

    let mock_finish = server
        .mock("POST", "/finish")
        .match_header("content-type", "application/json")
        .match_body(r#"{"state":2}"#)
        .with_status(200)
        .with_header("content-type", "application/json")
        .with_body(r#"{"result": 2}"#)
        .create();

    let mut runtime = Runtime::new().await.unwrap();
    runtime.enable_fetch().await.unwrap();

    let js_code = r#"
        export async function create_state() {
            const response = await fetch("$URL/init", {
                method: 'POST'
            });
            const data = await response.json();
            return data.state;
        }

        export async function accumulate(state, value) {
            const response = await fetch("$URL/acc", {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ state, value })
            });
            const data = await response.json();
            return data.state;
        }

        export async function merge(state1, state2) {
            const response = await fetch("$URL/merge", {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ state1, state2 })
            });
            const data = await response.json();
            return data.state;
        }

        export async function finish(state) {
            const response = await fetch("$URL/finish", {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ state })
            });
            const data = await response.json();
            return data.result;
        }
    "#
    .replace("$URL", &server.url());

    // Add the aggregate function
    runtime
        .add_aggregate(
            "fetch_agg",
            DataType::Int32, // state type
            DataType::Int32, // output type
            &js_code,
            AggregateOptions::default()
                .return_null_on_null_input()
                .async_mode(),
        )
        .await
        .unwrap();

    // Create initial state
    let state = runtime.create_state("fetch_agg").await.unwrap();
    check_array(
        std::slice::from_ref(&state),
        expect![[r#"
            +-------+
            | array |
            +-------+
            | 0     |
            +-------+"#]],
    );

    // Test accumulate
    let schema = Schema::new(vec![Field::new("value", DataType::Int32, true)]);
    let arg0 = Int32Array::from(vec![Some(1)]);
    let input = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(arg0)]).unwrap();

    let state = runtime
        .accumulate("fetch_agg", &state, &input)
        .await
        .unwrap();
    check_array(
        std::slice::from_ref(&state),
        expect![[r#"
            +-------+
            | array |
            +-------+
            | 1     |
            +-------+"#]],
    );

    // Test merge
    let states = arrow_select::concat::concat(&[&state, &state]).unwrap();
    let state = runtime.merge("fetch_agg", &states).await.unwrap();
    check_array(
        std::slice::from_ref(&state),
        expect![[r#"
            +-------+
            | array |
            +-------+
            | 2     |
            +-------+"#]],
    );

    // Test finish
    let output = runtime.finish("fetch_agg", &state).await.unwrap();
    check_array(
        &[output],
        expect![[r#"
            +-------+
            | array |
            +-------+
            | 2     |
            +-------+"#]],
    );

    mock_init.assert();
    mock_acc.assert();
    mock_merge.assert();
    mock_finish.assert();
}

#[tokio::test]
async fn test_fetch_in_udtf() {
    let mut server = Server::new_async().await;
    let mock_page1 = server
        .mock("GET", "/items/page/1")
        .with_status(200)
        .with_header("content-type", "application/json")
        .with_body(
            r#"{"items": [
            {"id": 1, "name": "item1"},
            {"id": 2, "name": "item2"}
        ]}"#,
        )
        .create();
    let mock_page2 = server
        .mock("GET", "/items/page/2")
        .with_status(200)
        .with_header("content-type", "application/json")
        .with_body(
            r#"{"items": [
            {"id": 3, "name": "item3"},
            {"id": 4, "name": "item4"}
        ]}"#,
        )
        .create();

    let mut runtime = Runtime::new().await.unwrap();
    runtime.enable_fetch().await.unwrap();

    let js_code = r#"
        export async function* fetch_items(page_count) {
            for (let page = 1; page <= page_count; page++) {
                const response = await fetch("$URL/items/page/" + page);
                const data = await response.json();
                for (const item of data.items) {
                    yield item.name;
                }
            }
        }
    "#
    .replace("$URL", &server.url());

    // Add a table function that fetches and merges pages
    runtime
        .add_function(
            "fetch_items",
            DataType::Utf8,
            &js_code,
            FunctionOptions::default()
                .return_null_on_null_input()
                .async_mode(),
        )
        .await
        .unwrap();

    let schema = Schema::new(vec![Field::new("page_count", DataType::Int32, true)]);
    let arg0 = Int32Array::from(vec![Some(2)]);
    let input = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(arg0)]).unwrap();

    let mut outputs = runtime
        .call_table_function("fetch_items", &input, 10)
        .unwrap();
    let output = outputs.next().await.unwrap().unwrap();
    assert!(outputs.next().await.unwrap().is_none());

    check(
        &[output],
        expect![[r#"
            +-----+-------------+
            | row | fetch_items |
            +-----+-------------+
            | 0   | item1       |
            | 0   | item2       |
            | 0   | item3       |
            | 0   | item4       |
            +-----+-------------+"#]],
    );

    mock_page1.assert();
    mock_page2.assert();
}

/// Auxiliary function to run a test with a given js code and server url
async fn test(server_url: &str, js_code: &str) {
    let runtime = Runtime::new().await.unwrap();
    runtime.enable_fetch().await.unwrap();

    const JS_ASSERT: &str = r#"
        function assert(cond) {
            if (!cond) {
                throw new Error("Assertion failed");
            }
        }"#;
    run_async_js_code(runtime.context(), JS_ASSERT).await;

    let js_code = js_code.replace("$URL", server_url);
    run_async_js_code(runtime.context(), &js_code).await;
}

#[tokio::test]
async fn test_fetch_get() {
    let mut server = Server::new_async().await;
    let mock = server
        .mock("GET", "/todos/1")
        .with_status(200)
        .with_header("content-type", "application/json")
        .with_body(r#"{"todo": "Have fun!"}"#)
        .create();

    test(
        &server.url(),
        r#"
        const response = await fetch("$URL/todos/1");
        assert(response.ok);
        assert(response.status === 200);
        assert(response.statusText === "OK");
        assert(!response.bodyUsed);
        const data = await response.json();
        assert(response.bodyUsed);
        assert(data.todo === "Have fun!");"#,
    )
    .await;

    mock.assert();
}

#[tokio::test]
async fn test_fetch_get_with_headers() {
    let mut server = Server::new_async().await;
    let mock = server
        .mock("GET", "/todos/1")
        .match_header("authorization", "Bearer dummy-token")
        .with_status(401)
        .with_header("content-type", "application/json")
        .with_body(r#"{"todo": "Have fun!"}"#)
        .create();

    test(
        &server.url(),
        r#"
        const headers = {
            'Authorization': 'Bearer dummy-token'
        };
        const response = await fetch("$URL/todos/1", { headers });
        assert(response.status === 401);
        const data = await response.json();
        assert(data.todo === "Have fun!");"#,
    )
    .await;

    mock.assert();
}

#[tokio::test]
async fn test_fetch_post_with_body() {
    let mut server = Server::new_async().await;

    let mock_hello = server
        .mock("POST", "/echo")
        .match_header("authorization", "Bearer dummy-token")
        .match_body(r#"{"input":"hello"}"#)
        .with_status(200)
        .with_header("content-type", "application/json")
        .with_body(r#"{"output":"hello"}"#)
        .create();
    let mock_buddy = server
        .mock("POST", "/echo")
        .match_header("authorization", "Bearer dummy-token")
        .match_body(r#"{"input":"buddy"}"#)
        .with_status(200)
        .with_header("content-type", "application/json")
        .with_body(r#"{"output":"buddy"}"#)
        .create();

    test(
        &server.url(),
        r#"
        const inputs = ["hello", "buddy"];
        for (const input of inputs) {
            const body = JSON.stringify({ input });
            const headers = { 'Authorization': 'Bearer dummy-token' };
            const response = await fetch("$URL/echo", {
                method: 'POST',
                headers,
                body
            });
            assert(response.ok);
            const data = await response.json();
            assert(data.output === input);
        }"#,
    )
    .await;

    mock_hello.assert();
    mock_buddy.assert();
}

#[tokio::test]
async fn test_fetch_get_503() {
    let mut server = Server::new_async().await;
    let mock = server
        .mock("GET", "/todos/1")
        .with_status(503)
        .with_header("content-type", "application/json")
        .with_body(r#"{"error": "Oops! Service Unavailable"}"#)
        .create();

    test(
        &server.url(),
        r#"
        const response = await fetch("$URL/todos/1");
        assert(!response.ok);
        assert(response.status === 503);
        assert(response.statusText === "Service Unavailable");
        assert(!response.bodyUsed);
        const data = await response.json();
        assert(response.bodyUsed);
        assert(data.error === "Oops! Service Unavailable");"#,
    )
    .await;

    mock.assert();
}

#[tokio::test]
async fn test_fetch_timeout() {
    test(
        "",
        r#"
            try {
                const response = await fetch("https://httpbin.org/delay/1", { timeout_ms: 500 });
                assert(false); // should not reach here
            } catch (e) {
                assert(e.message.includes("operation timed out"));
            }
        "#,
    )
    .await;
}

/// Compare the actual output with the expected output.
#[track_caller]
fn check(actual: &[RecordBatch], expect: Expect) {
    expect.assert_eq(&pretty_format_batches(actual).unwrap().to_string());
}

/// Compare the actual output with the expected output.
#[track_caller]
fn check_array(actual: &[ArrayRef], expect: Expect) {
    expect.assert_eq(&pretty_format_columns("array", actual).unwrap().to_string());
}

fn inspect_error(err: &rquickjs::Error, ctx: &rquickjs::Ctx) {
    match err {
        rquickjs::Error::Exception => {
            eprintln!("exception generated by QuickJS: {:?}", ctx.catch())
        }
        e => eprintln!("{:?}", e),
    }
}
