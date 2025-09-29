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

use std::io;

use databend_common_meta_types::protobuf::KeysCount;
use databend_common_meta_types::protobuf::KeysLayoutRequest;
use futures::Stream;
use futures::StreamExt;

/// Convert a stream of keys into hierarchical prefix counts
/// Based on the Python algorithm provided in the reference
#[futures_async_stream::try_stream(boxed, ok = KeysCount, error = io::Error)]
pub async fn count_prefix(
    keys_stream: impl Stream<Item = Result<String, io::Error>> + Send + 'static,
    layout_request: KeysLayoutRequest,
) {
    let max_depth = layout_request.depth.unwrap_or(u32::MAX) as usize;

    let mut total = 0;
    let mut last_key: String = String::new(); // The last processed directory path

    // (part end index, prefix count)
    // [i].0: Ending positions of each part in last_key
    // [i].1 = count for prefix ending at part_ends[i]
    let mut last_parts: Vec<(usize, u64)> = Vec::new();

    let mut new_part_ends: Vec<usize> = Vec::new();

    let mut keys_stream = std::pin::pin!(keys_stream);

    // Process keys one by one, yielding results immediately
    while let Some(key_result) = keys_stream.next().await {
        let key = key_result?;

        total += 1;

        {
            // Split key by '/' and remove the last part (filename)
            let mut parts: Vec<&str> = key.split('/').collect();
            if !parts.is_empty() {
                parts.pop(); // Remove filename part
            }

            // Calculate new part ends
            new_part_ends.clear();

            let mut pos = 0;
            for part in parts.iter() {
                pos += part.len();
                new_part_ends.push(pos); // End position of this part (inclusive)
                pos += 1; // +1 for the '/' separator (except for last part)
            }
        }

        // Find common prefix count by comparing string slices
        let common_prefix_count = {
            let mut i = last_parts.len().min(new_part_ends.len());

            while i > 0 && last_key[..last_parts[i - 1].0] != key[..new_part_ends[i - 1]] {
                i -= 1;
            }

            i
        };

        // Yield directories that are no longer in common prefix
        while last_parts.len() > common_prefix_count {
            let (end_pos, count) = last_parts.pop().unwrap();

            if last_parts.len() < max_depth {
                yield KeysCount {
                    prefix: last_key[..end_pos].to_string(),
                    count,
                };
            }
        }

        // Extend dir_counts if needed and add counts for all parent directories of current key
        #[allow(clippy::needless_range_loop)]
        for i in last_parts.len()..new_part_ends.len() {
            last_parts.push((new_part_ends[i], 0));
        }
        for (_, count) in &mut last_parts {
            *count += 1;
        }

        last_key = key;
    }

    // Yield remaining directories
    while let Some((end_pos, count)) = last_parts.pop() {
        if last_parts.len() < max_depth {
            yield KeysCount {
                prefix: last_key[..end_pos].to_string(),
                count,
            };
        }
    }

    yield KeysCount {
        prefix: "".to_string(),
        count: total,
    };
}

#[cfg(test)]
mod tests {
    use std::io;

    use databend_common_meta_types::protobuf::KeysCount;
    use databend_common_meta_types::protobuf::KeysLayoutRequest;
    use futures::stream::BoxStream;
    use futures::stream::{self};
    use futures::StreamExt;

    use super::count_prefix;

    fn kc(prefix: impl ToString, count: u64) -> KeysCount {
        KeysCount::new(prefix, count)
    }

    fn no_depth_limit() -> KeysLayoutRequest {
        KeysLayoutRequest { depth: None }
    }

    fn depth_limit(depth: u32) -> KeysLayoutRequest {
        KeysLayoutRequest { depth: Some(depth) }
    }

    async fn keys_to_stream(keys: Vec<&str>) -> BoxStream<'static, Result<String, io::Error>> {
        let keys: Vec<String> = keys.into_iter().map(|s| s.to_string()).collect();
        Box::pin(stream::iter(keys.into_iter().map(Ok)))
    }

    async fn error_stream() -> BoxStream<'static, Result<String, io::Error>> {
        Box::pin(stream::iter(vec![Err(io::Error::other("Test error"))]))
    }

    async fn collect_results(
        stream: BoxStream<'static, Result<String, io::Error>>,
        layout_request: KeysLayoutRequest,
    ) -> Result<Vec<KeysCount>, io::Error> {
        let mut results = Vec::new();
        let mut count_stream = count_prefix(stream, layout_request);
        while let Some(result) = count_stream.next().await {
            results.push(result?);
        }
        Ok(results)
    }

    #[tokio::test]
    async fn test_empty_stream() {
        let stream = keys_to_stream(vec![]).await;
        let results = collect_results(stream, no_depth_limit()).await.unwrap();
        let expected = vec![kc("", 0)];
        assert_eq!(results, expected);
    }

    #[tokio::test]
    async fn test_single_file_root() {
        let stream = keys_to_stream(vec!["file.txt"]).await;
        let results = collect_results(stream, no_depth_limit()).await.unwrap();
        let expected = vec![kc("", 1)];
        assert_eq!(results, expected);
    }

    #[tokio::test]
    async fn test_single_file_in_directory() {
        let stream = keys_to_stream(vec!["dir/file.txt"]).await;
        let results = collect_results(stream, no_depth_limit()).await.unwrap();
        let expected = vec![kc("dir", 1), kc("", 1)];
        assert_eq!(results, expected);
    }

    #[tokio::test]
    async fn test_multiple_files_same_directory() {
        let stream = keys_to_stream(vec!["dir/file1.txt", "dir/file2.txt", "dir/file3.txt"]).await;
        let results = collect_results(stream, no_depth_limit()).await.unwrap();
        let expected = vec![kc("dir", 3), kc("", 3)];
        assert_eq!(results, expected);
    }

    #[tokio::test]
    async fn test_nested_directories() {
        let stream = keys_to_stream(vec!["a/b/file1.txt", "a/b/file2.txt", "a/c/file3.txt"]).await;
        let results = collect_results(stream, no_depth_limit()).await.unwrap();
        let expected = vec![kc("a/b", 2), kc("a/c", 1), kc("a", 3), kc("", 3)];
        assert_eq!(results, expected);
    }

    #[tokio::test]
    async fn test_complex_hierarchy() {
        let stream = keys_to_stream(vec![
            "root/dir1/subdir1/file1.txt",
            "root/dir1/subdir1/file2.txt",
            "root/dir1/subdir2/file3.txt",
            "root/dir2/file4.txt",
            "root/dir2/file5.txt",
        ])
        .await;
        let results = collect_results(stream, no_depth_limit()).await.unwrap();
        let expected = vec![
            kc("root/dir1/subdir1", 2),
            kc("root/dir1/subdir2", 1),
            kc("root/dir1", 3),
            kc("root/dir2", 2),
            kc("root", 5),
            kc("", 5),
        ];
        assert_eq!(results, expected);
    }

    #[tokio::test]
    async fn test_different_root_directories() {
        let stream = keys_to_stream(vec![
            "dir1/file1.txt",
            "dir1/file2.txt",
            "dir2/file3.txt",
            "dir3/subdir/file4.txt",
        ])
        .await;
        let results = collect_results(stream, no_depth_limit()).await.unwrap();
        let expected = vec![
            kc("dir1", 2),
            kc("dir2", 1),
            kc("dir3/subdir", 1),
            kc("dir3", 1),
            kc("", 4),
        ];
        assert_eq!(results, expected);
    }

    #[tokio::test]
    async fn test_sorted_keys_with_various_depths() {
        // Test properly sorted keys with various nesting depths
        let stream = keys_to_stream(vec![
            "app/deep/nested/file1.txt",
            "app/shallow/file2.txt",
            "config/file3.txt",
        ])
        .await;
        let results = collect_results(stream, no_depth_limit()).await.unwrap();
        let expected = vec![
            kc("app/deep/nested", 1),
            kc("app/deep", 1),
            kc("app/shallow", 1),
            kc("app", 2),
            kc("config", 1),
            kc("", 3),
        ];
        assert_eq!(results, expected);
    }

    #[tokio::test]
    async fn test_sorted_keys_different_depths() {
        // Test with sorted keys at different hierarchy levels
        let stream = keys_to_stream(vec![
            "a/deep/nested/dir/file1.txt",
            "a/deep/nested/file2.txt",
            "a/file3.txt",
            "b/file4.txt",
        ])
        .await;
        let results = collect_results(stream, no_depth_limit()).await.unwrap();
        let expected = vec![
            kc("a/deep/nested/dir", 1),
            kc("a/deep/nested", 2),
            kc("a/deep", 2),
            kc("a", 3),
            kc("b", 1),
            kc("", 4),
        ];
        assert_eq!(results, expected);
    }

    #[tokio::test]
    async fn test_files_with_no_directory() {
        let stream = keys_to_stream(vec!["file1.txt", "file2.txt", "file3.txt"]).await;
        let results = collect_results(stream, no_depth_limit()).await.unwrap();
        let expected = vec![kc("", 3)];
        assert_eq!(results, expected);
    }

    #[tokio::test]
    async fn test_mixed_root_and_directory_files() {
        // Keys must be sorted
        let stream = keys_to_stream(vec!["dir/file2.txt", "file1.txt", "file3.txt"]).await;
        let results = collect_results(stream, no_depth_limit()).await.unwrap();
        let expected = vec![kc("dir", 1), kc("", 3)];
        assert_eq!(results, expected);
    }

    #[tokio::test]
    async fn test_stream_error_propagation() {
        let error_stream = error_stream().await;
        let result = collect_results(error_stream, no_depth_limit()).await;
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().to_string(), "Test error");
    }

    #[tokio::test]
    async fn test_very_deep_nesting() {
        let stream =
            keys_to_stream(vec!["a/b/c/d/e/f/g/file1.txt", "a/b/c/d/e/f/h/file2.txt"]).await;
        let results = collect_results(stream, no_depth_limit()).await.unwrap();
        let expected = vec![
            kc("a/b/c/d/e/f/g", 1),
            kc("a/b/c/d/e/f/h", 1),
            kc("a/b/c/d/e/f", 2),
            kc("a/b/c/d/e", 2),
            kc("a/b/c/d", 2),
            kc("a/b/c", 2),
            kc("a/b", 2),
            kc("a", 2),
            kc("", 2),
        ];
        assert_eq!(results, expected);
    }

    #[tokio::test]
    async fn test_depth_limit_1() {
        // depth=1 should only include prefixes with 0 slashes
        let stream = keys_to_stream(vec![
            "a/b/c/file1.txt",
            "a/b/file2.txt",
            "a/file3.txt",
            "d/file4.txt",
        ])
        .await;
        let results = collect_results(stream, depth_limit(1)).await.unwrap();
        let expected = vec![
            kc("a", 3), // Only top-level "a" included
            kc("d", 1), // Only top-level "d" included
            kc("", 4),  // Total count always included
        ];
        assert_eq!(results, expected);
    }

    #[tokio::test]
    async fn test_depth_limit_2() {
        // depth=2 should include prefixes with 0 or 1 slash
        let stream = keys_to_stream(vec![
            "a/b/c/file1.txt",
            "a/b/file2.txt",
            "a/file3.txt",
            "d/e/file4.txt",
        ])
        .await;
        let results = collect_results(stream, depth_limit(2)).await.unwrap();
        let expected = vec![
            kc("a/b", 2), // 1 slash - included
            kc("a", 3),   // 0 slashes - included
            kc("d/e", 1), // 1 slash - included
            kc("d", 1),   // 0 slashes - included
            kc("", 4),    // Total count always included
        ];
        assert_eq!(results, expected);
    }

    #[tokio::test]
    async fn test_depth_limit_3() {
        // depth=3 should include prefixes with 0, 1, or 2 slashes
        let stream = keys_to_stream(vec![
            "a/b/c/d/file1.txt",
            "a/b/c/file2.txt",
            "a/b/file3.txt",
        ])
        .await;
        let results = collect_results(stream, depth_limit(3)).await.unwrap();
        let expected = vec![
            kc("a/b/c", 2), // 2 slashes - included
            kc("a/b", 3),   // 1 slash - included
            kc("a", 3),     // 0 slashes - included
            kc("", 3),      // Total count always included
        ];
        assert_eq!(results, expected);
    }

    #[tokio::test]
    async fn test_depth_limit_filters_deep_paths() {
        // Very deep paths should be filtered out with low depth limit
        let stream = keys_to_stream(vec!["root/level1/level2/level3/level4/file.txt"]).await;
        let results = collect_results(stream, depth_limit(2)).await.unwrap();
        let expected = vec![
            kc("root/level1", 1), // 1 slash - included
            kc("root", 1),        // 0 slashes - included
            kc("", 1),            // Total count always included
        ];
        assert_eq!(results, expected);
    }
}
