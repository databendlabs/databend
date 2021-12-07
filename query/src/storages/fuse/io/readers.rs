//  Copyright 2021 Datafuse Labs.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
//

use common_dal::DataAccessor;
use common_exception::Result;
use serde::de::DeserializeOwned;

pub async fn read_obj<T: DeserializeOwned>(
    da: &dyn DataAccessor,
    loc: impl AsRef<str>,
) -> Result<T> {
    let bytes = da.read(loc.as_ref()).await?;
    let r = serde_json::from_slice::<T>(&bytes)?;
    Ok(r)
}
