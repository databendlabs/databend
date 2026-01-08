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

#![feature(int_roundings)]
#![allow(internal_features)]
#![allow(
    clippy::derivable_impls,
    clippy::cloned_ref_to_slice_refs,
    clippy::collapsible_if,
    clippy::iter_kv_map,
    clippy::let_and_return,
    clippy::manual_is_multiple_of,
    clippy::needless_return,
    clippy::unnecessary_unwrap,
    clippy::uninlined_format_args,
    clippy::useless_asref
)]
#![feature(iter_map_windows)]
#![feature(core_intrinsics)]
#![feature(arbitrary_self_types)]
#![feature(type_alias_impl_trait)]
#![feature(assert_matches)]
#![feature(trusted_len)]
#![feature(box_patterns)]
#![feature(sync_unsafe_cell)]
#![allow(mismatched_lifetime_syntaxes)]
#![feature(iterator_try_reduce)]
#![feature(cursor_split)]
#![allow(clippy::large_enum_variant)]
#![feature(impl_trait_in_assoc_type)]
#![feature(iterator_try_collect)]
#![feature(try_blocks)]
#![feature(variant_count)]
#![feature(duration_constructors)]
#![feature(get_mut_unchecked)]
#![feature(box_into_inner)]
#![allow(clippy::diverging_sub_expression)]
#![allow(clippy::arc_with_non_send_sync)]
#![feature(debug_closure_helpers)]

extern crate core;

pub mod auth;
pub mod builtin;
pub mod catalogs;
pub mod clusters;
pub mod databases;
pub mod history_tables;
pub mod interpreters;
pub mod locks;
pub mod pipelines;
pub mod schedulers;
pub mod servers;
pub mod sessions;
pub mod spillers;
pub mod stream;
pub mod table_functions;
pub mod test_kits;

mod global_services;
pub mod task;

pub mod physical_plans;

pub use databend_common_sql as sql;
pub use databend_common_storages_factory as storages;
pub use global_services::GlobalServices;
pub use table_functions::get_fuse_table_snapshot;
pub use table_functions::get_fuse_table_statistics;

/// Initialize custom stack management for StackSafe crate.
///
/// Stack size configuration:
/// - Debug mode: Uses larger stack sizes (10x) to catch stack overflow issues early
/// - Release mode: Uses normal stack sizes for production
///
/// Default StackSafe thresholds:
/// - Red zone: 128 KiB
/// - Stack allocation: 2 MiB
///
/// Our custom settings:
/// - Debug: 2.5 MiB red zone, 20 MiB allocation (10x default)
/// - Release: 256 KiB red zone, 2 MiB allocation (same as default)
pub fn init_stack_management() {
    use std::sync::atomic::AtomicUsize;

    // The stack in debugging mode is 10x than in release
    #[cfg(debug_assertions)]
    let minimum_stack_size = AtomicUsize::new(256 * 1024 * 10); // 2.5 MiB for debug
    #[cfg(debug_assertions)]
    let stack_alloc_size = AtomicUsize::new(2 * 1024 * 1024 * 10); // 20 MiB for debug

    #[cfg(not(debug_assertions))]
    let minimum_stack_size = AtomicUsize::new(256 * 1024); // 256 KiB for release
    #[cfg(not(debug_assertions))]
    let stack_alloc_size = AtomicUsize::new(2 * 1024 * 1024); // 2 MiB for release

    stacksafe::set_minimum_stack_size(minimum_stack_size);
    stacksafe::set_stack_allocation_size(stack_alloc_size);

    #[cfg(debug_assertions)]
    log::info!(
        "StackSafe configured for debug: minimum_stack_size={} KiB, stack_alloc_size={} MiB",
        minimum_stack_size.load(std::sync::atomic::Ordering::Relaxed) / 1024,
        stack_alloc_size.load(std::sync::atomic::Ordering::Relaxed) / (1024 * 1024)
    );

    #[cfg(not(debug_assertions))]
    log::info!(
        "StackSafe configured for release: minimum_stack_size={} KiB, stack_alloc_size={} MiB",
        minimum_stack_size.load(std::sync::atomic::Ordering::Relaxed) / 1024,
        stack_alloc_size.load(std::sync::atomic::Ordering::Relaxed) / (1024 * 1024)
    );
}
