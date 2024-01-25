// Copyright 2020-2022 Jorge C. Leitão
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

use databend_common_arrow::arrow::bitmap::utils::BitmapIter;
use databend_common_arrow::arrow::bitmap::utils::ZipValidity;
use databend_common_arrow::arrow::bitmap::Bitmap;

#[test]
fn basic() {
    let a = Bitmap::from([true, false]);
    let a = Some(a.iter());
    let values = vec![0, 1];
    let zip = ZipValidity::new(values.into_iter(), a);

    let a = zip.collect::<Vec<_>>();
    assert_eq!(a, vec![Some(0), None]);
}

#[test]
fn complete() {
    let a = Bitmap::from([true, false, true, false, true, false, true, false]);
    let a = Some(a.iter());
    let values = vec![0, 1, 2, 3, 4, 5, 6, 7];
    let zip = ZipValidity::new(values.into_iter(), a);

    let a = zip.collect::<Vec<_>>();
    assert_eq!(a, vec![
        Some(0),
        None,
        Some(2),
        None,
        Some(4),
        None,
        Some(6),
        None
    ]);
}

#[test]
fn slices() {
    let a = Bitmap::from([true, false]);
    let a = Some(a.iter());
    let offsets = [0, 2, 3];
    let values = [1, 2, 3];
    let iter = offsets.windows(2).map(|x| {
        let start = x[0];
        let end = x[1];
        &values[start..end]
    });
    let zip = ZipValidity::new(iter, a);

    let a = zip.collect::<Vec<_>>();
    assert_eq!(a, vec![Some([1, 2].as_ref()), None]);
}

#[test]
fn byte() {
    let a = Bitmap::from([true, false, true, false, false, true, true, false, true]);
    let a = Some(a.iter());
    let values = vec![0, 1, 2, 3, 4, 5, 6, 7, 8];
    let zip = ZipValidity::new(values.into_iter(), a);

    let a = zip.collect::<Vec<_>>();
    assert_eq!(a, vec![
        Some(0),
        None,
        Some(2),
        None,
        None,
        Some(5),
        Some(6),
        None,
        Some(8)
    ]);
}

#[test]
fn offset() {
    let a = Bitmap::from([true, false, true, false, false, true, true, false, true]).sliced(1, 8);
    let a = Some(a.iter());
    let values = vec![0, 1, 2, 3, 4, 5, 6, 7];
    let zip = ZipValidity::new(values.into_iter(), a);

    let a = zip.collect::<Vec<_>>();
    assert_eq!(a, vec![
        None,
        Some(1),
        None,
        None,
        Some(4),
        Some(5),
        None,
        Some(7)
    ]);
}

#[test]
fn none() {
    let values = vec![0, 1, 2];
    let zip = ZipValidity::new(values.into_iter(), None::<BitmapIter>);

    let a = zip.collect::<Vec<_>>();
    assert_eq!(a, vec![Some(0), Some(1), Some(2)]);
}

#[test]
fn rev() {
    let a = Bitmap::from([true, false, true, false, false, true, true, false, true]).sliced(1, 8);
    let a = Some(a.iter());
    let values = vec![0, 1, 2, 3, 4, 5, 6, 7];
    let zip = ZipValidity::new(values.into_iter(), a);

    let result = zip.rev().collect::<Vec<_>>();
    let expected = vec![None, Some(1), None, None, Some(4), Some(5), None, Some(7)]
        .into_iter()
        .rev()
        .collect::<Vec<_>>();
    assert_eq!(result, expected);
}
