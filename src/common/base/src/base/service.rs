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

use std::any::Any;
use std::any::TypeId;
use std::any::type_name;
use std::ops::Deref;
use std::sync::Arc;

pub use databend_common_base_macros::Service;
pub use databend_common_base_macros::service_symbol;

pub trait AnyServiceProvider: Send + Sync {
    fn get_service_any(&self, type_id: TypeId) -> Option<Arc<dyn Any + Send + Sync>>;
}

macro_rules! define_service_symbol {
    ($vis:vis $name:ident) => {
        #[derive(Debug, Clone, Copy, Default)]
        $vis struct $name;

        impl ServiceSymbol for $name {}
    };
}

define_service_symbol!(pub CacheManagerSymbol);
define_service_symbol!(pub CatalogManagerSymbol);
define_service_symbol!(pub BuildInfoRefSymbol);
define_service_symbol!(pub ClientSessionManagerSymbol);
define_service_symbol!(pub CloudControlApiProviderSymbol);
define_service_symbol!(pub ClusterDiscoverySymbol);
define_service_symbol!(pub DataExchangeManagerSymbol);
define_service_symbol!(pub DataOperatorSymbol);
define_service_symbol!(pub HttpQueryManagerSymbol);
define_service_symbol!(pub InnerConfigSymbol);
define_service_symbol!(pub LicenseManagerSwitchSymbol);
define_service_symbol!(pub LockManagerSymbol);
define_service_symbol!(pub QueriesQueueManagerSymbol);
define_service_symbol!(pub RoleCacheManagerSymbol);
define_service_symbol!(pub SessionManagerSymbol);
define_service_symbol!(pub UserApiProviderSymbol);
define_service_symbol!(pub AuthMgrSymbol);
define_service_symbol!(pub WorkloadGroupResourceManagerSymbol);
define_service_symbol!(pub WorkloadMgrSymbol);

pub trait ServiceSymbol: Send + Sync + 'static {}

pub trait ServiceRegistry: AnyServiceProvider {
    fn insert_service_any(&mut self, type_id: TypeId, service: Arc<dyn Any + Send + Sync>);

    fn insert_service<T>(&mut self, service: Arc<T>)
    where
        Self: Sized,
        T: Service,
    {
        self.insert_service_any(TypeId::of::<T>(), service);
    }
}

pub trait ServiceProvider<S: ServiceSymbol>: AnyServiceProvider {}

pub trait Service: Send + Sync + 'static + Sized {
    type Symbol: ServiceSymbol;

    fn try_get_service<P>(provider: &impl Deref<Target = P>) -> Option<Arc<Self>>
    where P: ServiceProvider<Self::Symbol> + ?Sized {
        provider
            .get_service_any(TypeId::of::<Self>())
            .and_then(|service| service.downcast::<Self>().ok())
    }

    fn get_service<P>(provider: &impl Deref<Target = P>) -> Arc<Self>
    where P: ServiceProvider<Self::Symbol> + ?Sized {
        Self::try_get_service(provider)
            .unwrap_or_else(|| panic!("{} service is not configured", type_name::<Self>()))
    }
}
