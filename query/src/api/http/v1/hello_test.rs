use common_runtime::tokio;

#[tokio::test]
async fn test_hello() -> common_exception::Result<()> {
    use axum::body::Body;
    use axum::handler::get;
    use axum::http::Request;
    use axum::http::StatusCode;
    use axum::http::{self};
    use axum::AddExtensionLayer;
    use axum::Router;
    use pretty_assertions::assert_eq;
    use tower::ServiceExt;

    use crate::api::http::v1::config::config_handler;
    use crate::api::http::v1::hello::hello_handler;
    use crate::configs::Config; // for `app.oneshot()`

    let conf = Config::default();
    let cluster_router = Router::new()
        .route("/v1/hello", get(hello_handler))
        .route("/v1/config", get(config_handler))
        .layer(AddExtensionLayer::new(conf.clone()));
    // health check
    {
        let response = cluster_router
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/v1/hello")
                    .method(http::Method::GET)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
    }
    // health check(config)
    {
        let response = cluster_router
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/v1/config")
                    .method(http::Method::GET)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
    }
    Ok(())
}
