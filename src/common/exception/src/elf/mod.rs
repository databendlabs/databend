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

mod dwarf;
mod dwarf_inline_functions;
mod dwarf_subprogram;

#[allow(clippy::collapsible_if)]
mod dwarf_unit;
mod library_loader;
mod library_manager;
mod library_symbol;

#[cfg(target_pointer_width = "32")]
type ElfFile = object::read::elf::ElfFile32<'static, object::NativeEndian, &'static [u8]>;

#[cfg(target_pointer_width = "64")]
type ElfFile = object::read::elf::ElfFile64<'static, object::NativeEndian, &'static [u8]>;

pub use library_manager::LibraryManager;
