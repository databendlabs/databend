// Copyright [2021] [Jorge C Leitao]
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

mod decoder;
mod encoder;

pub use decoder::Decoder;
pub use encoder::encode;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::Error;

    #[test]
    fn basic() -> Result<(), Error> {
        let data = vec![1, 3, 1, 2, 3];

        let mut buffer = vec![];
        encode(data.clone().into_iter(), &mut buffer);
        let iter = Decoder::try_new(&buffer)?;

        let result = iter.collect::<Result<Vec<_>, _>>()?;
        assert_eq!(result, data);
        Ok(())
    }

    #[test]
    fn negative_value() -> Result<(), Error> {
        let data = vec![1, 3, -1, 2, 3];

        let mut buffer = vec![];
        encode(data.clone().into_iter(), &mut buffer);
        let iter = Decoder::try_new(&buffer)?;

        let result = iter.collect::<Result<Vec<_>, _>>()?;
        assert_eq!(result, data);
        Ok(())
    }

    #[test]
    fn some() -> Result<(), Error> {
        let data = vec![
            -2147483648,
            -1777158217,
            -984917788,
            -1533539476,
            -731221386,
            -1322398478,
            906736096,
        ];

        let mut buffer = vec![];
        encode(data.clone().into_iter(), &mut buffer);
        let iter = Decoder::try_new(&buffer)?;

        let result = iter.collect::<Result<Vec<_>, Error>>()?;
        assert_eq!(result, data);
        Ok(())
    }

    #[test]
    fn more_than_one_block() -> Result<(), Error> {
        let mut data = vec![1, 3, -1, 2, 3, 10, 1];
        for x in 0..128 {
            data.push(x - 10)
        }

        let mut buffer = vec![];
        encode(data.clone().into_iter(), &mut buffer);
        let iter = Decoder::try_new(&buffer)?;

        let result = iter.collect::<Result<Vec<_>, _>>()?;
        assert_eq!(result, data);
        Ok(())
    }

    #[test]
    fn test_another() -> Result<(), Error> {
        let data = vec![2, 3, 1, 2, 1];

        let mut buffer = vec![];
        encode(data.clone().into_iter(), &mut buffer);
        let len = buffer.len();
        let mut iter = Decoder::try_new(&buffer)?;

        let result = iter.by_ref().collect::<Result<Vec<_>, _>>()?;
        assert_eq!(result, data);

        assert_eq!(iter.consumed_bytes(), len);
        Ok(())
    }
}
