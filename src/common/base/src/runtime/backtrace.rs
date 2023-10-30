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

use std::fmt::Write;

#[derive(Debug)]
pub struct AsyncTaskItem {
    pub stack_frames: Vec<String>,
}

pub fn dump_backtrace(wait_for_running_tasks: bool) -> String {
    let (tasks, polling_tasks) = get_all_tasks(wait_for_running_tasks);

    let mut output = String::new();
    for mut tasks in [tasks, polling_tasks] {
        tasks.sort_by(|l, r| Ord::cmp(&l.stack_frames.len(), &r.stack_frames.len()));

        for item in tasks.into_iter().rev() {
            for frame in item.stack_frames {
                writeln!(output, "{}", frame).unwrap();
            }

            writeln!(output).unwrap();
        }
    }

    output
}

pub fn get_all_tasks(wait_for_running_tasks: bool) -> (Vec<AsyncTaskItem>, Vec<AsyncTaskItem>) {
    let tree = async_backtrace::taskdump_tree(wait_for_running_tasks);

    let mut tasks = vec![];
    let mut polling_tasks = vec![];
    let mut current_stack_frames = vec![];

    let mut first = true;
    let mut is_polling = false;
    for line in tree.lines() {
        if line.starts_with(|x: char| !x.is_ascii_whitespace()) {
            if !first {
                match is_polling {
                    true => polling_tasks.push(AsyncTaskItem {
                        stack_frames: std::mem::take(&mut current_stack_frames),
                    }),
                    false => tasks.push(AsyncTaskItem {
                        stack_frames: std::mem::take(&mut current_stack_frames),
                    }),
                };

                is_polling = false;
            }

            first = false;
        }

        if line.ends_with("[POLLING]") {
            is_polling = true;
        }

        current_stack_frames.push(line.to_string());
    }

    match is_polling {
        true => polling_tasks.push(AsyncTaskItem {
            stack_frames: std::mem::take(&mut current_stack_frames),
        }),
        false => tasks.push(AsyncTaskItem {
            stack_frames: std::mem::take(&mut current_stack_frames),
        }),
    };
    (tasks, polling_tasks)
}
