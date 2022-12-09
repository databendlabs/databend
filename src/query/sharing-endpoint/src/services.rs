use std::cell::UnsafeCell;
use std::sync::Arc;

use common_base::base::GlobalIORuntime;
use common_base::base::Runtime;
use common_base::base::SingletonImpl;
use common_exception::Result;

use crate::accessor::SharingAccessor;
use crate::configs::Config;

// hold singleton services.
pub struct SharingServices {
    // by default, operator init needs the global_runtime singleton instance
    // thus we keep it in the sharing services, alternatively, we may consider expose no layer operator
    global_runtime: UnsafeCell<Option<Arc<Runtime>>>,
    // storage_accessor is the accessor for the shared tenant data.
    storage_accessor: UnsafeCell<Option<Arc<SharingAccessor>>>,
}

unsafe impl Send for SharingServices {}

unsafe impl Sync for SharingServices {}

impl SharingServices {
    pub async fn init(config: Config) -> Result<()> {
        let sharing_services = Arc::new(SharingServices {
            global_runtime: UnsafeCell::new(None),
            storage_accessor: UnsafeCell::new(None),
        });
        GlobalIORuntime::init(config.storage.num_cpus as usize, sharing_services.clone())?;
        SharingAccessor::init(&config, sharing_services.clone()).await
    }
}

impl SingletonImpl<Arc<SharingAccessor>> for SharingServices {
    fn get(&self) -> Arc<SharingAccessor> {
        unsafe {
            match &*self.storage_accessor.get() {
                None => panic!("Sharing Accessor is not init"),
                Some(storage_accessor) => storage_accessor.clone(),
            }
        }
    }

    fn init(&self, value: Arc<SharingAccessor>) -> Result<()> {
        unsafe {
            *(self.storage_accessor.get() as *mut Option<Arc<SharingAccessor>>) = Some(value);
            Ok(())
        }
    }
}

impl SingletonImpl<Arc<Runtime>> for SharingServices {
    fn get(&self) -> Arc<Runtime> {
        unsafe {
            match &*self.global_runtime.get() {
                None => panic!("Global Runtime is not init"),
                Some(runtime) => runtime.clone(),
            }
        }
    }

    fn init(&self, value: Arc<Runtime>) -> Result<()> {
        unsafe {
            *(self.global_runtime.get() as *mut Option<Arc<Runtime>>) = Some(value);
            Ok(())
        }
    }
}
