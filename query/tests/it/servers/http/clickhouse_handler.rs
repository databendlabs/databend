// Copyright 2022 Datafuse Labs.
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

use common_base::base::tokio;
use databend_query::servers::http::middleware::HTTPSessionEndpoint;
use databend_query::servers::http::middleware::HTTPSessionMiddleware;
use databend_query::servers::http::v1::clickhouse_router;
use databend_query::servers::HttpHandlerKind;
use http::Uri;
use poem::error::Result as PoemResult;
use poem::http::Method;
use poem::http::StatusCode;
use poem::web::headers::Authorization;
use poem::Body;
use poem::Endpoint;
use poem::EndpointExt;
use poem::Request;
use poem::Route;
use pretty_assertions::assert_eq;

use crate::tests::SessionManagerBuilder;

macro_rules! assert_error {
    ($body:expr, $msg:expr$(,)?) => {{
        assert!($body.contains($msg), "{}", $body);
    }};
}

macro_rules! assert_ok {
    ($status:expr, $body:expr) => {{
        assert_eq!($status, StatusCode::OK, "{}: {}", $status, $body);
    }};
}

#[tokio::test]
async fn test_select() -> PoemResult<()> {
    let server = Server::new();

    {
        let (status, body) = server.get("bad sql").await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_error!(body, "sql parser error");
    }

    {
        let (status, body) = server.post("sel", "ect 1").await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_error!(body, "sql parser error");
    }

    {
        let (status, body) = server.post("", "bad sql").await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_error!(body, "sql parser error");
    }

    {
        let (status, body) = server.get("").await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(&body, "");
    }

    {
        let (status, body) = server.get("select 1").await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(&body, "1\n");
    }

    {
        let (status, body) = server.post("", "select 1").await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(&body, "1\n");
    }

    {
        let (status, body) = server.post("select 1", "").await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(&body, "1\n");
    }

    {
        // basic tsv format
        let (status, body) = server
            .get(r#"select number, 'a' from numbers(2) order by number"#)
            .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(&body, "0\ta\n1\ta\n");
    }
    Ok(())
}

#[tokio::test]
async fn test_insert_values() -> PoemResult<()> {
    let server = Server::new();
    {
        let (status, body) = server.post("create table t1(a int, b string)", "").await;
        assert_eq!(status, StatusCode::OK);
        assert_error!(body, "");
    }

    {
        let (status, body) = server
            .post("insert into table t1 values (0, 'a'), (1, 'b')", "")
            .await;
        assert_eq!(status, StatusCode::OK);
        assert_error!(body, "");
    }

    {
        // basic tsv format
        let (status, body) = server.get(r#"select * from t1"#).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(&body, "0\ta\n1\tb\n");
    }
    Ok(())
}

#[tokio::test]
async fn test_output_formats() -> PoemResult<()> {
    let server = Server::new();
    {
        let (status, body) = server
            .post("create table t1(a int, b string null)", "")
            .await;
        assert_ok!(status, body);
    }

    {
        let (status, body) = server
            .post(
                "insert into table t1(a, b) format values",
                "(0, 'a'), (1, 'b')",
            )
            .await;
        assert_ok!(status, body);
        assert_error!(body, "");
    }

    let cases = [
        ("CSV", "0,\"a\"\n1,\"b\"\n"),
        ("TSV", "0\ta\n1\tb\n"),
        ("TSVWithNames", "a\tb\n0\ta\n1\tb\n"),
        (
            "TSVWithNamesAndTypes",
            "a\tb\nInt32\tNullable(String)\n0\ta\n1\tb\n",
        ),
    ];

    for (fmt, exp) in cases {
        let sql = format!(r#"select * from t1 order by a format {}"#, fmt);
        let (status, body) = server.get(&sql).await;
        assert_ok!(status, body);
        assert_eq!(&body, exp);
    }
    Ok(())
}

#[tokio::test]
async fn test_output_format_compress() -> PoemResult<()> {
    let server = Server::new();
    let sql = "select 1 format TabSeparated";
    let (status, body) = server
        .get_response_bytes(
            QueryBuilder::new("")
                .compress(true)
                .body(sql.to_string())
                .build(),
        )
        .await;
    let body = hex::encode_upper(body);
    assert_ok!(status, body);
    let exp = "DE79CF087FB635049DB816DF195B016B820C0000000200000020310A";
    assert_eq!(&body, exp);
    Ok(())
}

#[tokio::test]
async fn test_insert_format_values() -> PoemResult<()> {
    let server = Server::new();
    {
        let (status, body) = server.post("create table t1(a int, b string)", "").await;
        assert_eq!(status, StatusCode::OK);
        assert_error!(body, "");
    }

    {
        let (status, body) = server
            .post("insert into table t1 format values", "(0, 'a'), (1, 'b')")
            .await;
        assert_ok!(status, body);
        assert_error!(body, "");
    }

    {
        // basic tsv format
        let (status, body) = server.get(r#"select * from t1"#).await;
        assert_eq!(status, StatusCode::OK, "{} {}", status, body);
        assert_eq!(&body, "0\ta\n1\tb\n");
    }
    Ok(())
}

#[tokio::test]
async fn test_insert_format_ndjson() -> PoemResult<()> {
    let server = Server::new();
    {
        let (status, body) = server
            .post("create table t1(a int, b string null)", "")
            .await;
        assert_ok!(status, body);
    }

    {
        let jsons = vec![r#"{"a": 0, "b": "a"}"#, r#"{"a": 1, "b": "b"}"#];
        let body = jsons.join("\n");
        let (status, body) = server
            .post("insert into table t1 format JSONEachRow", &body)
            .await;
        assert_ok!(status, body);
    }

    {
        let (status, body) = server.get(r#"select * from t1 order by a"#).await;
        assert_ok!(status, body);
        assert_eq!(&body, "0\ta\n1\tb\n");
    }

    {
        let jsons = vec![r#"{"a": 2}"#];
        let body = jsons.join("\n");
        let (status, body) = server
            .post("insert into table t1 format JSONEachRow", &body)
            .await;
        assert_ok!(status, body);
    }

    {
        let (status, body) = server.get(r#"select * from t1 order by a"#).await;
        assert_ok!(status, body);
        assert_eq!(&body, "0\ta\n1\tb\n2\tNULL\n");
    }

    {
        let jsons = vec![r#"{"b": 0}"#];
        let body = jsons.join("\n");
        let (status, body) = server
            .post("insert into table t1 format JSONEachRow", &body)
            .await;
        assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
        assert_error!(body, "column a");
    }
    Ok(())
}

#[tokio::test]
async fn test_multi_partition() -> PoemResult<()> {
    let server = Server::new();
    {
        let sql = "create table tb2(id int, c1 varchar) Engine=Fuse;";
        let (status, body) = server.get(sql).await;
        assert_ok!(status, body);
        assert_eq!(&body, "");
    }
    {
        for _ in 0..3 {
            let sql = "insert into tb2 format values ";
            let data = "(1, 'mysql'),(2,'databend')";
            let (status, body) = server.post(sql, data).await;
            assert_ok!(status, body);
            assert_eq!(&body, "");
        }
    }
    {
        let sql = "select * from tb2 format tsv;";
        let (status, body) = server.get(sql).await;
        assert_ok!(status, body);
        assert_eq!(
            &body,
            "1\tmysql\n2\tdatabend\n1\tmysql\n2\tdatabend\n1\tmysql\n2\tdatabend\n"
        );
    }

    Ok(())
}

struct QueryBuilder {
    sql: String,
    body: Option<Body>,
    compress: bool,
}

impl QueryBuilder {
    pub fn new(sql: &str) -> Self {
        QueryBuilder {
            sql: sql.to_string(),
            body: None,
            compress: false,
        }
    }

    pub fn body(self, body: impl Into<Body>) -> Self {
        Self {
            body: Some(body.into()),
            ..self
        }
    }

    pub fn compress(self, compress: bool) -> Self {
        Self { compress, ..self }
    }

    pub fn build(self) -> Request {
        let mut uri = url::form_urlencoded::Serializer::new(String::new());
        uri.append_pair("query", &self.sql);
        if self.compress {
            uri.append_pair("compress", "1");
        }
        let uri = uri.finish();
        let uri = "/?".to_string() + &uri;
        let uri = uri.parse::<Uri>().unwrap();
        let (method, body) = match self.body {
            None => (Method::GET, Body::empty()),
            Some(body) => (Method::POST, body),
        };

        let basic = Authorization::basic("root", "");
        Request::builder()
            .uri(uri)
            .method(method)
            .typed_header(basic)
            .body(body)
    }
}

struct Server {
    endpoint: HTTPSessionEndpoint<Route>,
}

impl Server {
    pub fn new() -> Self {
        let session_manager = SessionManagerBuilder::create().build().unwrap();
        let endpoint = Route::new()
            .nest("/", clickhouse_router())
            .with(HTTPSessionMiddleware {
                kind: HttpHandlerKind::Clickhouse,
                session_manager,
            });
        Server { endpoint }
    }

    pub async fn get_response_bytes(&self, req: Request) -> (StatusCode, Vec<u8>) {
        let response = self.endpoint.get_response(req).await;
        let status = response.status();
        let body = response.into_body().into_vec().await.unwrap();
        (status, body)
    }

    pub async fn get_response(&self, req: Request) -> (StatusCode, String) {
        let response = self.endpoint.get_response(req).await;
        let status = response.status();
        let body = response.into_body().into_string().await.unwrap();
        (status, body)
    }

    pub async fn get(&self, sql: &str) -> (StatusCode, String) {
        self.get_response(QueryBuilder::new(sql).build()).await
    }

    pub async fn post(&self, sql: &str, body: &str) -> (StatusCode, String) {
        self.get_response(QueryBuilder::new(sql).body(body.to_string()).build())
            .await
    }
}
