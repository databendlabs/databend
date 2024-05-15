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
        let data = vec!["aa", "bbb", "a", "aa", "b"];

        let mut buffer = vec![];
        encode(data.into_iter().map(|x| x.as_bytes()), &mut buffer);

        let mut iter = Decoder::try_new(&buffer)?;

        let result = iter.by_ref().collect::<Result<Vec<_>, _>>()?;
        assert_eq!(result, vec![2, 3, 1, 2, 1]);

        let result = iter.values();
        assert_eq!(result, b"aabbbaaab".as_ref());
        Ok(())
    }

    #[test]
    fn many_numbers() -> Result<(), Error> {
        let mut data = vec![];
        for i in 0..136 {
            data.push(format!("a{}", i))
        }
        let expected_values = data.join("");
        let expected_lengths = data.iter().map(|x| x.len() as i32).collect::<Vec<_>>();

        let mut buffer = vec![];
        encode(data.into_iter(), &mut buffer);

        let mut iter = Decoder::try_new(&buffer)?;

        let result = iter.by_ref().collect::<Result<Vec<_>, _>>()?;
        assert_eq!(result, expected_lengths);

        let result = iter.into_values();
        assert_eq!(result, expected_values.as_str().as_bytes());
        Ok(())
    }
}
