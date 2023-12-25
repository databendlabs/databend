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

mod settings;
mod settings_default;
mod settings_getter_setter;
mod settings_global;

pub use settings::ChangeValue;
pub use settings::ScopeLevel;
pub use settings::Settings;
pub use settings_default::ReplaceIntoShuffleStrategy;
pub use settings_default::SettingMode;
pub use settings_default::SettingRange;
pub use settings_getter_setter::FlightCompression;
