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

use crate::ErrorCode;

#[derive(Debug, Clone)]
pub struct ErrorFrame {
    pub file: String,
    pub line: u32,
    pub col: u32,
    pub message: String,
}

impl<C> ErrorCode<C> {
    #[track_caller]
    pub fn with_context<C2>(self, ctx: impl ToString) -> ErrorCode<C2> {
        let location = std::panic::Location::caller();
        let frame = ErrorFrame {
            file: location.file().to_string(),
            line: location.line(),
            col: location.column(),
            message: ctx.to_string(),
        };
        let mut stacks = self.stacks;
        stacks.push(frame);
        ErrorCode {
            code: self.code,
            name: self.name,
            display_text: self.display_text,
            detail: self.detail,
            span: self.span,
            cause: self.cause,
            backtrace: self.backtrace,
            stacks,
            _phantom: std::marker::PhantomData,
        }
    }
}

pub trait ResultExt<T, C> {
    fn with_context<C2>(self, ctx: impl ToString) -> std::result::Result<T, ErrorCode<C2>>;
}

impl<T, C> ResultExt<T, C> for std::result::Result<T, ErrorCode<C>> {
    fn with_context<C2>(self, ctx: impl ToString) -> std::result::Result<T, ErrorCode<C2>> {
        self.map_err(|e| e.with_context(ctx))
    }
}

pub fn display_error_stack(stacks: &[ErrorFrame]) -> String {
    let mut buf = String::new();
    for (i, stack) in stacks.iter().enumerate() {
        writeln!(
            &mut buf,
            "{:<2} {} at {}:{}:{}",
            i, stack.message, stack.file, stack.line, stack.col,
        )
        .unwrap();
    }
    buf
}
