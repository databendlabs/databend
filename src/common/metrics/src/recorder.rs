// Copyright 2021 Datafuse Labs
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

use std::mem::ManuallyDrop;
use std::sync::Arc;
use std::sync::Once;

use log::warn;
use metrics::Counter;
use metrics::CounterFn;
use metrics::Gauge;
use metrics::GaugeFn;
use metrics::Histogram;
use metrics::HistogramFn;
use metrics::Key;
use metrics::KeyName;
use metrics::Recorder;
use metrics::SharedString;
use metrics::Unit;
use metrics_exporter_prometheus::PrometheusBuilder;
use metrics_exporter_prometheus::PrometheusHandle;
use metrics_exporter_prometheus::PrometheusRecorder;
use once_cell::sync::Lazy;
use parking_lot::RwLock;

static PROMETHEUS_HANDLE: Lazy<Arc<RwLock<Option<ClearableRecorder>>>> =
    Lazy::new(|| Arc::new(RwLock::new(None)));

pub const LABEL_KEY_TENANT: &str = "tenant";
pub const LABEL_KEY_CLUSTER: &str = "cluster_name";

pub fn init_default_metrics_recorder() {
    static START: Once = Once::new();
    START.call_once(init_prometheus_recorder)
}

/// Init prometheus recorder.
fn init_prometheus_recorder() {
    let recorder = ClearableRecorder::create();
    let mut h = PROMETHEUS_HANDLE.as_ref().write();
    *h = Some(recorder.clone());
    unsafe {
        metrics::clear_recorder();
    }
    match metrics::set_boxed_recorder(Box::new(recorder)) {
        Ok(_) => (),
        Err(err) => warn!("Install prometheus recorder failed, cause: {}", err),
    };
}

pub fn try_handle() -> Option<PrometheusHandle> {
    let read_guard = PROMETHEUS_HANDLE.as_ref().read();
    read_guard.as_ref().map(ClearableRecorder::handle)
}

pub fn try_get_record() -> Option<ClearableRecorder> {
    let read_guard = PROMETHEUS_HANDLE.as_ref().read();
    read_guard.as_ref().cloned()
}

struct CounterFnWrap<Holder> {
    pub counter: ManuallyDrop<Counter>,
    pub holder: ManuallyDrop<Arc<Holder>>,
}

impl<Holder> Drop for CounterFnWrap<Holder> {
    fn drop(&mut self) {
        unsafe {
            ManuallyDrop::drop(&mut self.counter);
            ManuallyDrop::drop(&mut self.holder);
        }
    }
}

impl<Holder> CounterFn for CounterFnWrap<Holder> {
    fn increment(&self, value: u64) {
        self.counter.increment(value)
    }

    fn absolute(&self, value: u64) {
        self.counter.absolute(value)
    }
}

struct GaugeFnWrap<Holder> {
    pub gauge: ManuallyDrop<Gauge>,
    pub holder: ManuallyDrop<Arc<Holder>>,
}

impl<Holder> Drop for GaugeFnWrap<Holder> {
    fn drop(&mut self) {
        unsafe {
            ManuallyDrop::drop(&mut self.gauge);
            ManuallyDrop::drop(&mut self.holder);
        }
    }
}

impl<Holder> GaugeFn for GaugeFnWrap<Holder> {
    fn increment(&self, value: f64) {
        self.gauge.increment(value)
    }

    fn decrement(&self, value: f64) {
        self.gauge.decrement(value)
    }

    fn set(&self, value: f64) {
        self.gauge.set(value)
    }
}

struct HistogramFnWrap<Holder> {
    pub histogram: std::mem::ManuallyDrop<Histogram>,
    pub holder: ManuallyDrop<Arc<Holder>>,
}

impl<Holder> Drop for HistogramFnWrap<Holder> {
    fn drop(&mut self) {
        unsafe {
            ManuallyDrop::drop(&mut self.histogram);
            ManuallyDrop::drop(&mut self.holder);
        }
    }
}

impl<Holder> HistogramFn for HistogramFnWrap<Holder> {
    fn record(&self, value: f64) {
        self.histogram.record(value)
    }
}

// It will be ensured that the recorder will be destroyed after all counters, gauge, histogram are destroyed
struct ArcRecorder<T: Recorder> {
    pub inner: Arc<T>,
}

impl<T: Recorder + Send + Sync + 'static> Recorder for ArcRecorder<T> {
    #[inline]
    fn describe_counter(&self, key: KeyName, unit: Option<Unit>, description: SharedString) {
        self.inner.describe_counter(key, unit, description)
    }

    #[inline]
    fn describe_gauge(&self, key: KeyName, unit: Option<Unit>, description: SharedString) {
        self.inner.describe_gauge(key, unit, description)
    }

    #[inline]
    fn describe_histogram(&self, key: KeyName, unit: Option<Unit>, description: SharedString) {
        self.inner.describe_histogram(key, unit, description)
    }

    fn register_counter(&self, key: &Key) -> Counter {
        Counter::from_arc(Arc::new(CounterFnWrap {
            counter: ManuallyDrop::new(self.inner.register_counter(key)),
            holder: ManuallyDrop::new(self.inner.clone()),
        }))
    }

    fn register_gauge(&self, key: &Key) -> Gauge {
        Gauge::from_arc(Arc::new(GaugeFnWrap {
            gauge: ManuallyDrop::new(self.inner.register_gauge(key)),
            holder: ManuallyDrop::new(self.inner.clone()),
        }))
    }

    fn register_histogram(&self, key: &Key) -> Histogram {
        Histogram::from_arc(Arc::new(HistogramFnWrap {
            histogram: ManuallyDrop::new(self.inner.register_histogram(key)),
            holder: ManuallyDrop::new(self.inner.clone()),
        }))
    }
}

// TODO: use atomic refactor rwlock
#[derive(Clone)]
pub struct ClearableRecorder {
    inner: Arc<RwLock<ArcRecorder<PrometheusRecorder>>>,
}

impl ClearableRecorder {
    pub fn create() -> ClearableRecorder {
        let recorder = PrometheusBuilder::new().build_recorder();
        ClearableRecorder {
            inner: Arc::new(RwLock::new(ArcRecorder {
                inner: Arc::new(recorder),
            })),
        }
    }

    pub fn clear(&self) {
        let mut inner = self.inner.write();
        let recorder = PrometheusBuilder::new().build_recorder();
        *inner = ArcRecorder {
            inner: Arc::new(recorder),
        };
    }

    pub fn handle(&self) -> PrometheusHandle {
        self.inner.read().inner.handle()
    }
}

impl Recorder for ClearableRecorder {
    fn describe_counter(&self, key: KeyName, unit: Option<Unit>, description: SharedString) {
        self.inner.read().describe_counter(key, unit, description)
    }

    fn describe_gauge(&self, key: KeyName, unit: Option<Unit>, description: SharedString) {
        self.inner.read().describe_gauge(key, unit, description)
    }

    fn describe_histogram(&self, key: KeyName, unit: Option<Unit>, description: SharedString) {
        self.inner.read().describe_histogram(key, unit, description)
    }

    fn register_counter(&self, key: &Key) -> Counter {
        self.inner.read().register_counter(key)
    }

    fn register_gauge(&self, key: &Key) -> Gauge {
        self.inner.read().register_gauge(key)
    }

    fn register_histogram(&self, key: &Key) -> Histogram {
        self.inner.read().register_histogram(key)
    }
}
