// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use opentelemetry::global;
use opentelemetry::propagation::Extractor;
use opentelemetry::propagation::Injector;
use tracing_opentelemetry::OpenTelemetrySpanExt;

/// Inject tracing info into tonic request meta.
struct MetadataMapInjector<'a>(&'a mut tonic::metadata::MetadataMap);

impl<'a> Injector for MetadataMapInjector<'a> {
    /// Set a key and value in the MetadataMap.  Does nothing if the key or value are not valid inputs
    fn set(&mut self, key: &str, value: String) {
        if let Ok(key) = tonic::metadata::MetadataKey::from_bytes(key.as_bytes()) {
            if let Ok(val) = tonic::metadata::MetadataValue::from_str(&value) {
                self.0.insert(key, val);
            }
        }
    }
}

/// Extract tracing info from tonic request meta.
struct MetadataMapExtractor<'a>(&'a tonic::metadata::MetadataMap);

impl<'a> Extractor for MetadataMapExtractor<'a> {
    /// Get a value for a key from the MetadataMap.  If the value can't be converted to &str, returns None
    fn get(&self, key: &str) -> Option<&str> {
        self.0.get(key).and_then(|metadata| metadata.to_str().ok())
    }

    /// Collect all the keys from the MetadataMap.
    fn keys(&self) -> Vec<&str> {
        self.0
            .keys()
            .map(|key| match key {
                tonic::metadata::KeyRef::Ascii(v) => v.as_str(),
                tonic::metadata::KeyRef::Binary(v) => v.as_str(),
            })
            .collect::<Vec<_>>()
    }
}

/// Inject current tracing::Span info into tonic request meta
/// before sending request to a tonic server.
/// Then the tonic server will be able to chain a distributed tracing.
///
/// A tonic client should call this function just before sending out the request.
///
/// The global propagater must be installed, e.g. by calling: TODO
pub fn inject_span_to_tonic_request<T>(mes: impl tonic::IntoRequest<T>) -> tonic::Request<T> {
    let curr = tracing::Span::current();
    let cx = curr.context();

    let mut request = mes.into_request();

    global::get_text_map_propagator(|propagator| {
        propagator.inject_context(&cx, &mut MetadataMapInjector(request.metadata_mut()))
    });

    request
}

/// Extract tracing context from tonic request meta
/// and set current tracing::Span parent to the context from remote,
/// to chain the client side span with current server side span.
///
/// A tonic request handler should call this before doing anything else.
///
/// The global propagater must be installed, e.g. by calling: TODO
pub fn extract_remote_span_as_parent<T>(request: &tonic::Request<T>) {
    let parent_cx = global::get_text_map_propagator(|prop| {
        prop.extract(&MetadataMapExtractor(request.metadata()))
    });

    let span = tracing::Span::current();
    span.set_parent(parent_cx);
}
