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

pub trait ServiceProvider: Send + Sync {
    fn get_service_any(&self, type_id: TypeId) -> Option<Arc<dyn Any + Send + Sync>>;
}

pub trait ServiceRegistry: ServiceProvider {
    fn insert_service_any(&mut self, type_id: TypeId, service: Arc<dyn Any + Send + Sync>);

    fn insert_service<T>(&mut self, service: Arc<T>)
    where
        Self: Sized,
        T: Service,
    {
        self.insert_service_any(TypeId::of::<T>(), service);
    }
}

pub trait Service: Send + Sync + 'static + Sized {
    fn try_get_service<P>(provider: &impl Deref<Target = P>) -> Option<Arc<Self>>
    where P: ServiceProvider + ?Sized {
        provider
            .get_service_any(TypeId::of::<Self>())
            .and_then(|service| service.downcast::<Self>().ok())
    }

    fn get_service<P>(provider: &impl Deref<Target = P>) -> Arc<Self>
    where P: ServiceProvider + ?Sized {
        Self::try_get_service(provider)
            .unwrap_or_else(|| panic!("{} service is not configured", type_name::<Self>()))
    }
}
