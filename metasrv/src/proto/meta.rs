/// TODO(zbr): keep it or remove
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Db {
    #[prost(int64, tag = "1")]
    pub db_id: i64,
    /// Every modification has a corresponding unique ver.
    #[prost(int64, tag = "20")]
    pub ver: i64,
    #[prost(map = "string, int64", tag = "2")]
    pub table_name_to_id: ::std::collections::HashMap<::prost::alloc::string::String, i64>,
    #[prost(map = "int64, message", tag = "3")]
    pub tables: ::std::collections::HashMap<i64, Table>,
}
/// TODO(zbr): keep it or remove
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Table {
    #[prost(int64, tag = "1")]
    pub table_id: i64,
    /// Every modification has a corresponding unique ver.
    #[prost(int64, tag = "20")]
    pub ver: i64,
    #[prost(bytes = "vec", tag = "5")]
    pub schema: ::prost::alloc::vec::Vec<u8>,
    #[prost(map = "string, string", tag = "30")]
    pub options:
        ::std::collections::HashMap<::prost::alloc::string::String, ::prost::alloc::string::String>,
    /// a func(string, Vec<ReplicationGroupId>) mapping PartitionBy expr to
    /// replication group. A DatabendQuery process should consider this to determine
    /// where to send the read or write operations.
    ///
    /// repeated ReplicationGroupId ReplicationGroupIds
    #[prost(bytes = "vec", tag = "10")]
    pub placement_policy: ::prost::alloc::vec::Vec<u8>,
}
// A Cmd serves as a raft log entry to commit an atomic operation into meta data
// storage.

/// TODO(zbr): keep it or remove
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CmdCreateDatabase {
    #[prost(string, tag = "20")]
    pub db_name: ::prost::alloc::string::String,
    #[prost(message, optional, tag = "50")]
    pub db: ::core::option::Option<Db>,
}
/// TODO(zbr): keep it or remove
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CmdCreateTable {
    #[prost(string, tag = "20")]
    pub db_name: ::prost::alloc::string::String,
    #[prost(string, tag = "30")]
    pub table_name: ::prost::alloc::string::String,
    #[prost(message, optional, tag = "40")]
    pub table: ::core::option::Option<Table>,
}
// meta service

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GetReq {
    #[prost(string, tag = "1")]
    pub key: ::prost::alloc::string::String,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GetReply {
    #[prost(bool, tag = "1")]
    pub ok: bool,
    #[prost(string, tag = "2")]
    pub key: ::prost::alloc::string::String,
    #[prost(string, tag = "3")]
    pub value: ::prost::alloc::string::String,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct RaftRequest {
    #[prost(string, tag = "1")]
    pub data: ::prost::alloc::string::String,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct RaftReply {
    #[prost(string, tag = "1")]
    pub data: ::prost::alloc::string::String,
    #[prost(string, tag = "2")]
    pub error: ::prost::alloc::string::String,
}
#[doc = r" Generated client implementations."]
pub mod meta_service_client {
    #![allow(unused_variables, dead_code, missing_docs, clippy::let_unit_value)]
    use tonic::codegen::*;
    #[derive(Debug, Clone)]
    pub struct MetaServiceClient<T> {
        inner: tonic::client::Grpc<T>,
    }
    impl MetaServiceClient<tonic::transport::Channel> {
        #[doc = r" Attempt to create a new client by connecting to a given endpoint."]
        pub async fn connect<D>(dst: D) -> Result<Self, tonic::transport::Error>
        where
            D: std::convert::TryInto<tonic::transport::Endpoint>,
            D::Error: Into<StdError>,
        {
            let conn = tonic::transport::Endpoint::new(dst)?.connect().await?;
            Ok(Self::new(conn))
        }
    }
    impl<T> MetaServiceClient<T>
    where
        T: tonic::client::GrpcService<tonic::body::BoxBody>,
        T::ResponseBody: Body + Send + Sync + 'static,
        T::Error: Into<StdError>,
        <T::ResponseBody as Body>::Error: Into<StdError> + Send,
    {
        pub fn new(inner: T) -> Self {
            let inner = tonic::client::Grpc::new(inner);
            Self { inner }
        }
        pub fn with_interceptor<F>(
            inner: T,
            interceptor: F,
        ) -> MetaServiceClient<InterceptedService<T, F>>
        where
            F: tonic::service::Interceptor,
            T: tonic::codegen::Service<
                http::Request<tonic::body::BoxBody>,
                Response = http::Response<
                    <T as tonic::client::GrpcService<tonic::body::BoxBody>>::ResponseBody,
                >,
            >,
            <T as tonic::codegen::Service<http::Request<tonic::body::BoxBody>>>::Error:
                Into<StdError> + Send + Sync,
        {
            MetaServiceClient::new(InterceptedService::new(inner, interceptor))
        }
        #[doc = r" Compress requests with `gzip`."]
        #[doc = r""]
        #[doc = r" This requires the server to support it otherwise it might respond with an"]
        #[doc = r" error."]
        pub fn send_gzip(mut self) -> Self {
            self.inner = self.inner.send_gzip();
            self
        }
        #[doc = r" Enable decompressing responses with `gzip`."]
        pub fn accept_gzip(mut self) -> Self {
            self.inner = self.inner.accept_gzip();
            self
        }
        pub async fn write(
            &mut self,
            request: impl tonic::IntoRequest<super::RaftRequest>,
        ) -> Result<tonic::Response<super::RaftReply>, tonic::Status> {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static("/meta.MetaService/Write");
            self.inner.unary(request.into_request(), path, codec).await
        }
        pub async fn get(
            &mut self,
            request: impl tonic::IntoRequest<super::GetReq>,
        ) -> Result<tonic::Response<super::GetReply>, tonic::Status> {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static("/meta.MetaService/Get");
            self.inner.unary(request.into_request(), path, codec).await
        }
        #[doc = "/ Forward a request to other"]
        pub async fn forward(
            &mut self,
            request: impl tonic::IntoRequest<super::RaftRequest>,
        ) -> Result<tonic::Response<super::RaftReply>, tonic::Status> {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static("/meta.MetaService/Forward");
            self.inner.unary(request.into_request(), path, codec).await
        }
        pub async fn append_entries(
            &mut self,
            request: impl tonic::IntoRequest<super::RaftRequest>,
        ) -> Result<tonic::Response<super::RaftReply>, tonic::Status> {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static("/meta.MetaService/AppendEntries");
            self.inner.unary(request.into_request(), path, codec).await
        }
        pub async fn install_snapshot(
            &mut self,
            request: impl tonic::IntoRequest<super::RaftRequest>,
        ) -> Result<tonic::Response<super::RaftReply>, tonic::Status> {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static("/meta.MetaService/InstallSnapshot");
            self.inner.unary(request.into_request(), path, codec).await
        }
        pub async fn vote(
            &mut self,
            request: impl tonic::IntoRequest<super::RaftRequest>,
        ) -> Result<tonic::Response<super::RaftReply>, tonic::Status> {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static("/meta.MetaService/vote");
            self.inner.unary(request.into_request(), path, codec).await
        }
    }
}
#[doc = r" Generated server implementations."]
pub mod meta_service_server {
    #![allow(unused_variables, dead_code, missing_docs, clippy::let_unit_value)]
    use tonic::codegen::*;
    #[doc = "Generated trait containing gRPC methods that should be implemented for use with MetaServiceServer."]
    #[async_trait]
    pub trait MetaService: Send + Sync + 'static {
        async fn write(
            &self,
            request: tonic::Request<super::RaftRequest>,
        ) -> Result<tonic::Response<super::RaftReply>, tonic::Status>;
        async fn get(
            &self,
            request: tonic::Request<super::GetReq>,
        ) -> Result<tonic::Response<super::GetReply>, tonic::Status>;
        #[doc = "/ Forward a request to other"]
        async fn forward(
            &self,
            request: tonic::Request<super::RaftRequest>,
        ) -> Result<tonic::Response<super::RaftReply>, tonic::Status>;
        async fn append_entries(
            &self,
            request: tonic::Request<super::RaftRequest>,
        ) -> Result<tonic::Response<super::RaftReply>, tonic::Status>;
        async fn install_snapshot(
            &self,
            request: tonic::Request<super::RaftRequest>,
        ) -> Result<tonic::Response<super::RaftReply>, tonic::Status>;
        async fn vote(
            &self,
            request: tonic::Request<super::RaftRequest>,
        ) -> Result<tonic::Response<super::RaftReply>, tonic::Status>;
    }
    #[derive(Debug)]
    pub struct MetaServiceServer<T: MetaService> {
        inner: _Inner<T>,
        accept_compression_encodings: (),
        send_compression_encodings: (),
    }
    struct _Inner<T>(Arc<T>);
    impl<T: MetaService> MetaServiceServer<T> {
        pub fn new(inner: T) -> Self {
            let inner = Arc::new(inner);
            let inner = _Inner(inner);
            Self {
                inner,
                accept_compression_encodings: Default::default(),
                send_compression_encodings: Default::default(),
            }
        }
        pub fn with_interceptor<F>(inner: T, interceptor: F) -> InterceptedService<Self, F>
        where F: tonic::service::Interceptor {
            InterceptedService::new(Self::new(inner), interceptor)
        }
    }
    impl<T, B> tonic::codegen::Service<http::Request<B>> for MetaServiceServer<T>
    where
        T: MetaService,
        B: Body + Send + Sync + 'static,
        B::Error: Into<StdError> + Send + 'static,
    {
        type Response = http::Response<tonic::body::BoxBody>;
        type Error = Never;
        type Future = BoxFuture<Self::Response, Self::Error>;
        fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }
        fn call(&mut self, req: http::Request<B>) -> Self::Future {
            let inner = self.inner.clone();
            match req.uri().path() {
                "/meta.MetaService/Write" => {
                    #[allow(non_camel_case_types)]
                    struct WriteSvc<T: MetaService>(pub Arc<T>);
                    impl<T: MetaService> tonic::server::UnaryService<super::RaftRequest> for WriteSvc<T> {
                        type Response = super::RaftReply;
                        type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::RaftRequest>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).write(request).await };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = WriteSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec).apply_compression_config(
                            accept_compression_encodings,
                            send_compression_encodings,
                        );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/meta.MetaService/Get" => {
                    #[allow(non_camel_case_types)]
                    struct GetSvc<T: MetaService>(pub Arc<T>);
                    impl<T: MetaService> tonic::server::UnaryService<super::GetReq> for GetSvc<T> {
                        type Response = super::GetReply;
                        type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
                        fn call(&mut self, request: tonic::Request<super::GetReq>) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).get(request).await };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = GetSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec).apply_compression_config(
                            accept_compression_encodings,
                            send_compression_encodings,
                        );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/meta.MetaService/Forward" => {
                    #[allow(non_camel_case_types)]
                    struct ForwardSvc<T: MetaService>(pub Arc<T>);
                    impl<T: MetaService> tonic::server::UnaryService<super::RaftRequest> for ForwardSvc<T> {
                        type Response = super::RaftReply;
                        type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::RaftRequest>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).forward(request).await };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = ForwardSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec).apply_compression_config(
                            accept_compression_encodings,
                            send_compression_encodings,
                        );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/meta.MetaService/AppendEntries" => {
                    #[allow(non_camel_case_types)]
                    struct AppendEntriesSvc<T: MetaService>(pub Arc<T>);
                    impl<T: MetaService> tonic::server::UnaryService<super::RaftRequest> for AppendEntriesSvc<T> {
                        type Response = super::RaftReply;
                        type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::RaftRequest>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).append_entries(request).await };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = AppendEntriesSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec).apply_compression_config(
                            accept_compression_encodings,
                            send_compression_encodings,
                        );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/meta.MetaService/InstallSnapshot" => {
                    #[allow(non_camel_case_types)]
                    struct InstallSnapshotSvc<T: MetaService>(pub Arc<T>);
                    impl<T: MetaService> tonic::server::UnaryService<super::RaftRequest> for InstallSnapshotSvc<T> {
                        type Response = super::RaftReply;
                        type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::RaftRequest>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).install_snapshot(request).await };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = InstallSnapshotSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec).apply_compression_config(
                            accept_compression_encodings,
                            send_compression_encodings,
                        );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/meta.MetaService/vote" => {
                    #[allow(non_camel_case_types)]
                    struct voteSvc<T: MetaService>(pub Arc<T>);
                    impl<T: MetaService> tonic::server::UnaryService<super::RaftRequest> for voteSvc<T> {
                        type Response = super::RaftReply;
                        type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::RaftRequest>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).vote(request).await };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = voteSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec).apply_compression_config(
                            accept_compression_encodings,
                            send_compression_encodings,
                        );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                _ => Box::pin(async move {
                    Ok(http::Response::builder()
                        .status(200)
                        .header("grpc-status", "12")
                        .header("content-type", "application/grpc")
                        .body(empty_body())
                        .unwrap())
                }),
            }
        }
    }
    impl<T: MetaService> Clone for MetaServiceServer<T> {
        fn clone(&self) -> Self {
            let inner = self.inner.clone();
            Self {
                inner,
                accept_compression_encodings: self.accept_compression_encodings,
                send_compression_encodings: self.send_compression_encodings,
            }
        }
    }
    impl<T: MetaService> Clone for _Inner<T> {
        fn clone(&self) -> Self {
            Self(self.0.clone())
        }
    }
    impl<T: std::fmt::Debug> std::fmt::Debug for _Inner<T> {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "{:?}", self.0)
        }
    }
    impl<T: MetaService> tonic::transport::NamedService for MetaServiceServer<T> {
        const NAME: &'static str = "meta.MetaService";
    }
}
