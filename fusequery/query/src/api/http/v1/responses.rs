use common_exception::ErrorCode;
use serde::Serialize;
use warp::http::StatusCode;
use warp::hyper::Body;
use warp::reply::Response;

pub trait JSONResponseHelper {
    fn into_json_response(&self) -> Response;
}

pub trait ErrorCodeResponseHelper {
    fn into_response(&self) -> Response;
}

pub trait StatusCodeResponseHelper {
    fn into_with_body_response(&self, body: String) -> Response;
}

impl<T> JSONResponseHelper for T
where T: Serialize
{
    fn into_json_response(&self) -> Response {
        match serde_json::to_vec(self).map_err(ErrorCode::from) {
            Err(error) => error.into_response(),
            Ok(serialized_json) => {
                let body: Body = serialized_json.into();
                let mut response = Response::new(body);
                *response.status_mut() = StatusCode::OK;
                response.headers_mut().insert(
                    warp::http::header::CONTENT_TYPE,
                    warp::http::header::HeaderValue::from_static("application/json"),
                );
                response
            }
        }
    }
}

impl ErrorCodeResponseHelper for ErrorCode {
    fn into_response(&self) -> Response {
        StatusCode::INTERNAL_SERVER_ERROR.into_with_body_response(format!("{}", self))
    }
}

impl StatusCodeResponseHelper for StatusCode {
    fn into_with_body_response(&self, body: String) -> Response {
        let body: Body = body.into();
        let mut response = Response::new(body);
        *response.status_mut() = self.clone();
        response.headers_mut().insert(
            warp::http::header::CONTENT_TYPE,
            warp::http::header::HeaderValue::from_static("text/plain; charset=utf-8"),
        );
        response
    }
}
