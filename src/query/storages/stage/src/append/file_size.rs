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

use databend_common_ast::ast::CopyIntoLocationOptions;

pub(crate) fn resolve_file_size_options(
    mem_limit: usize,
    max_threads: usize,
    options: &CopyIntoLocationOptions,
) -> (Option<usize>, usize) {
    if options.single {
        return (None, 1);
    }

    let mem_limit = if mem_limit == 0 {
        usize::MAX
    } else {
        mem_limit
    };
    let mem_limit = (mem_limit / 2).max(1);
    let target_file_size = if options.max_file_size == 0 {
        64 * 1024 * 1024
    } else {
        options.max_file_size.min(mem_limit)
    }
    .max(1);
    let max_threads = max_threads.min(mem_limit / target_file_size).max(1);

    (Some(target_file_size), max_threads)
}
