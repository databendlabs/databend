use std::sync::Mutex;
use std::sync::MutexGuard;

use lazy_static::lazy_static;
use prometheus_client::registry::Registry;

lazy_static! {
    pub static ref REGISTRY: Mutex<Registry> = Mutex::new(Registry::default());
}

pub fn load_global_registry() -> MutexGuard<'static, Registry> {
    REGISTRY.lock().unwrap()
}
