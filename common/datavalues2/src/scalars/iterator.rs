// Copyright 2021 Datafuse Labs.
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

use std::iter::TrustedLen;
use std::marker::PhantomData;

use crate::prelude::*;

pub struct ScalarViewerIter<'a, Viewer, T> {
    pub(crate) viewer: &'a Viewer,
    pub(crate) size: usize,
    pub(crate) pos: usize,
    pub(crate) _t: PhantomData<T>,
}

impl<'a, Viewer, T> Iterator for ScalarViewerIter<'a, Viewer, T>
where
    Viewer: ScalarViewer<'a, ScalarItem = T>,
    T: Scalar,
{
    type Item = T::RefType<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.pos >= self.size {
            None
        } else {
            let item = self.viewer.value_at(self.pos);
            self.pos += 1;
            Some(item)
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.size - self.pos, Some(self.size - self.pos))
    }
}

impl<'a, Viewer, T> ExactSizeIterator for ScalarViewerIter<'a, Viewer, T>
where
    Viewer: ScalarViewer<'a, ScalarItem = T>,
    T: Scalar,
{
    fn len(&self) -> usize {
        self.size - self.pos
    }
}

unsafe impl<'a, Viewer, T> TrustedLen for ScalarViewerIter<'a, Viewer, T>
where
    Viewer: ScalarViewer<'a, ScalarItem = T>,
    T: Scalar,
{
}
