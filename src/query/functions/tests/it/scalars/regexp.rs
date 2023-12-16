// Copyright 2022 Datafuse Labs.
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

use std::io::Write;

use databend_common_expression::types::number::Int64Type;
use databend_common_expression::types::StringType;
use databend_common_expression::FromData;
use goldenfile::Mint;

use super::run_ast;

#[test]
fn test_string() {
    let mut mint = Mint::new("tests/it/scalars/testdata");
    let regexp_file = &mut mint.new_goldenfile("regexp.txt").unwrap();

    test_regexp_instr(regexp_file);
    test_regexp_like(regexp_file);
    test_regexp_replace(regexp_file);
    test_regexp_substr(regexp_file);
}

fn test_regexp_instr(file: &mut impl Write) {
    run_ast(file, "regexp_instr('dog cat dog', 'dog', 1)", &[]);
    run_ast(
        file,
        "regexp_instr('aa aaa aaaa aa aaa aaaa', 'a{2}', 1)",
        &[],
    );
    run_ast(file, "regexp_instr('aa aaa aaaa aa aaa aaaa', NULL, 2)", &[
    ]);
    run_ast(file, "regexp_instr('', '', 1)", &[]);
    run_ast(file, "regexp_instr('', '', 0)", &[]);

    let two_columns = &[
        (
            "source",
            StringType::from_data(vec!["dog cat dog", "aa aaa aaaa aa aaa aaaa", ""]),
        ),
        ("pat", StringType::from_data(vec!["dog", "a{2}", ""])),
    ];
    run_ast(file, "regexp_instr(source, pat)", two_columns);

    let three_columns = &[
        (
            "source",
            StringType::from_data(vec!["dog cat dog", "aa aaa aaaa aa aaa aaaa", ""]),
        ),
        ("pat", StringType::from_data(vec!["dog", "a{2}", ""])),
        ("pos", Int64Type::from_data(vec![1_i64, 2, 1])),
    ];
    run_ast(file, "regexp_instr(source, pat, pos)", three_columns);

    let four_columns = &[
        (
            "source",
            StringType::from_data(vec![
                "dog cat dog",
                "aa aaa aaaa aa aaa aaaa",
                "aa aa aa aaaa aaaa aaaa",
            ]),
        ),
        ("pat", StringType::from_data(vec!["dog", "a{2}", "a{4}"])),
        ("pos", Int64Type::from_data(vec![1_i64, 1, 9])),
        ("occur", Int64Type::from_data(vec![2_i64, 3, 2])),
    ];
    run_ast(file, "regexp_instr(source, pat, pos, occur)", four_columns);

    let five_columns = &[
        (
            "source",
            StringType::from_data(vec![
                "dog cat dog",
                "aa aaa aaaa aa aaa aaaa",
                "aa aa aa aaaa aaaa aaaa",
            ]),
        ),
        ("pat", StringType::from_data(vec!["dog", "a{2}", "a{4}"])),
        ("pos", Int64Type::from_data(vec![1_i64, 2, 1])),
        ("occur", Int64Type::from_data(vec![2_i64, 2, 2])),
        ("ro", Int64Type::from_data(vec![0_i64, 1, 1])),
    ];
    run_ast(
        file,
        "regexp_instr(source, pat, pos, occur, ro)",
        five_columns,
    );

    let six_columns = &[
        (
            "source",
            StringType::from_data(vec![
                "dog cat dog",
                "aa aaa aaaa aa aaa aaaa",
                "aa aa aa aaaa aaaa aaaa",
            ]),
        ),
        ("pat", StringType::from_data(vec!["dog", "A{2}", "A{4}"])),
        ("pos", Int64Type::from_data(vec![1_i64, 2, 1])),
        ("occur", Int64Type::from_data(vec![2_i64, 2, 2])),
        ("ro", Int64Type::from_data(vec![0_i64, 1, 1])),
        ("mt", StringType::from_data(vec!["i", "c", "i"])),
    ];
    run_ast(
        file,
        "regexp_instr(source, pat, pos, occur, ro, mt)",
        six_columns,
    );

    let nullable_five_columns = &[
        (
            "source",
            StringType::from_data_with_validity(
                vec![
                    "dog cat dog",
                    "aa aaa aaaa aa aaa aaaa",
                    "",
                    "aa aa aa aaaa aaaa aaaa",
                ],
                vec![true, true, false, true],
            ),
        ),
        (
            "pat",
            StringType::from_data_with_validity(vec!["dog", "", "", "A{4}"], vec![
                true, false, false, true,
            ]),
        ),
        ("pos", Int64Type::from_data(vec![1_i64, 2, 1, 1])),
        ("occur", Int64Type::from_data(vec![2_i64, 2, 2, 1])),
        ("ro", Int64Type::from_data(vec![0_i64, 1, 1, 1])),
    ];
    run_ast(
        file,
        "regexp_instr(source, pat, pos, occur, ro)",
        nullable_five_columns,
    );

    let nullable_six_columns = &[
        (
            "source",
            StringType::from_data_with_validity(
                vec![
                    "dog cat dog",
                    "aa aaa aaaa aa aaa aaaa",
                    "",
                    "aa aa aa aaaa aaaa aaaa",
                ],
                vec![true, true, false, true],
            ),
        ),
        (
            "pat",
            StringType::from_data_with_validity(vec!["dog", "", "", "A{4}"], vec![
                true, false, false, true,
            ]),
        ),
        ("pos", Int64Type::from_data(vec![1_i64, 2, 1, 1])),
        ("occur", Int64Type::from_data(vec![2_i64, 2, 2, 1])),
        ("ro", Int64Type::from_data(vec![0_i64, 1, 1, 1])),
        ("mt", StringType::from_data(vec!["i", "c", "i", "i"])),
    ];
    run_ast(
        file,
        "regexp_instr(source, pat, pos, occur, ro, mt)",
        nullable_six_columns,
    );

    let multi_byte_five_columns = &[
        (
            "source",
            StringType::from_data(vec![
                "周 周周 周周周 周周周周",
                "周 周周 周周周 周周周周",
                "周 周周 周周周 周周周周",
                "周 周周 周周周 周周周周",
            ]),
        ),
        (
            "pat",
            StringType::from_data(vec!["周+", "周+", "周+", "周+"]),
        ),
        ("pos", Int64Type::from_data(vec![1_i64, 2, 3, 5])),
        ("occur", Int64Type::from_data(vec![2_i64, 2, 3, 1])),
        ("ro", Int64Type::from_data(vec![0_i64, 1, 1, 1])),
    ];
    run_ast(
        file,
        "regexp_instr(source, pat, pos, occur, ro)",
        multi_byte_five_columns,
    );

    let pos_error_five_columns = &[
        ("source", StringType::from_data(vec!["dog cat dog"])),
        ("pat", StringType::from_data(vec!["dog"])),
        ("pos", Int64Type::from_data(vec![0_i64])),
        ("occur", Int64Type::from_data(vec![1_i64])),
        ("ro", Int64Type::from_data(vec![0_i64])),
    ];
    run_ast(
        file,
        "regexp_instr(source, pat, pos, occur, ro)",
        pos_error_five_columns,
    );

    let return_option_error_five_columns = &[
        (
            "source",
            StringType::from_data(vec![
                "dog cat dog",
                "aa aaa aaaa aa aaa aaaa",
                "aa aaa aaaa aa aaa aaaa",
            ]),
        ),
        ("pat", StringType::from_data(vec!["dog", "A{2}", "A{4}"])),
        ("pos", Int64Type::from_data(vec![2_i64, 2, 2])),
        ("occur", Int64Type::from_data(vec![1_i64, 2, 1])),
        ("ro", Int64Type::from_data(vec![0_i64, 2, 1])),
    ];
    run_ast(
        file,
        "regexp_instr(source, pat, pos, occur, ro)",
        return_option_error_five_columns,
    );

    let match_type_error_six_columns = &[
        (
            "source",
            StringType::from_data(vec![
                "dog cat dog",
                "aa aaa aaaa aa aaa aaaa",
                "aa aaa aaaa aa aaa aaaa",
            ]),
        ),
        ("pat", StringType::from_data(vec!["dog", "A{2}", "A{4}"])),
        ("pos", Int64Type::from_data(vec![1_i64, 2, 1])),
        ("occur", Int64Type::from_data(vec![2_i64, 2, 1])),
        ("ro", Int64Type::from_data(vec![0_i64, 1, 1])),
        ("mt", StringType::from_data(vec!["i", "c", "-i"])),
    ];
    run_ast(
        file,
        "regexp_instr(source, pat, pos, occur, ro, mt)",
        match_type_error_six_columns,
    );
}

fn test_regexp_like(file: &mut impl Write) {
    run_ast(file, "regexp_like('Michael!', '.*')", &[]);
    run_ast(file, "regexp_like('a', '^[a-d]')", &[]);
    run_ast(file, "regexp_like('abc', 'ABC')", &[]);
    run_ast(file, "regexp_like('abc', 'ABC', 'c')", &[]);
    run_ast(file, "regexp_like('abc', 'ABC', NULL)", &[]);
    run_ast(file, "regexp_like('', '', 'c')", &[]);

    let two_columns = &[
        (
            "source",
            StringType::from_data(vec!["abc", "abd", "Abe", "new*\n*line", "fo\nfo", ""]),
        ),
        (
            "pat",
            StringType::from_data(vec!["^a", "Ab", "abe", "new\\*.\\*line", "^fo$", ""]),
        ),
    ];
    run_ast(file, "regexp_like(source, pat)", two_columns);

    let three_columns = &[
        (
            "source",
            StringType::from_data(vec!["abc", "abd", "Abe", "new*\n*line", "fo\nfo", ""]),
        ),
        (
            "pat",
            StringType::from_data(vec!["^a", "Ab", "abe", "new\\*.\\*line", "^fo$", ""]),
        ),
        (
            "mt",
            StringType::from_data(vec!["", "c", "i", "n", "m", "c"]),
        ),
    ];
    run_ast(file, "regexp_like(source, pat, mt)", three_columns);

    let nullable_three_columns = &[
        (
            "source",
            StringType::from_data_with_validity(vec!["abc", "abc", "", "abc"], vec![
                true, true, false, true,
            ]),
        ),
        (
            "pat",
            StringType::from_data_with_validity(vec!["abc", "", "", "abc"], vec![
                true, false, false, true,
            ]),
        ),
        (
            "mt",
            StringType::from_data_with_validity(vec!["", "i", "i", ""], vec![
                true, true, true, false,
            ]),
        ),
    ];
    run_ast(file, "regexp_like(source, pat, mt)", nullable_three_columns);

    let pat_type_error_two_columns = &[
        ("source", StringType::from_data(vec!["abc", "abd"])),
        ("pat", Int64Type::from_data(vec![2, 3])),
    ];
    run_ast(file, "regexp_like(source, pat)", pat_type_error_two_columns);

    let match_type_error_three_columns = &[
        ("source", StringType::from_data(vec!["abc"])),
        ("pat", StringType::from_data(vec!["abc"])),
        ("mt", StringType::from_data(vec!["x"])),
    ];
    run_ast(
        file,
        "regexp_like(source, pat, mt)",
        match_type_error_three_columns,
    );

    let match_type_error2_three_columns = &[
        ("source", StringType::from_data(vec!["abc"])),
        ("pat", StringType::from_data(vec!["abc"])),
        ("mt", StringType::from_data(vec!["u"])),
    ];
    run_ast(
        file,
        "regexp_like(source, pat, mt)",
        match_type_error2_three_columns,
    );

    let match_type_join_error_three_columns = &[
        ("source", StringType::from_data(vec!["Abc-", "Abc-"])),
        ("pat", StringType::from_data(vec!["abc-", "abc"])),
        ("mt", StringType::from_data(vec!["i", "-i"])),
    ];
    run_ast(
        file,
        "regexp_like(source, pat, mt)",
        match_type_join_error_three_columns,
    );

    let match_type_join_error2_three_columns = &[
        ("source", StringType::from_data(vec!["Abc--", "Abc--"])),
        ("pat", StringType::from_data(vec!["abc--", "abc-"])),
        ("mt", StringType::from_data(vec!["", "-"])),
    ];
    run_ast(
        file,
        "regexp_like(source, pat, mt)",
        match_type_join_error2_three_columns,
    );
}

fn test_regexp_replace(file: &mut impl Write) {
    run_ast(file, "regexp_replace('a b c', 'b', 'X')", &[]);
    run_ast(file, "regexp_replace('a b c', '', 'X')", &[]);
    run_ast(file, "regexp_replace('', 'b', 'X')", &[]);
    run_ast(file, "regexp_replace('a b c', 'b', NULL)", &[]);
    run_ast(
        file,
        "regexp_replace('abc def ghi', '[a-z]+', 'X', 1, 3)",
        &[],
    );

    let three_columns = &[
        (
            "source",
            StringType::from_data(vec!["a b c", "a b c", "a b c", ""]),
        ),
        ("pat", StringType::from_data(vec!["b", "x", "", "b"])),
        ("repl", StringType::from_data(vec!["X", "X", "X", "X"])),
    ];
    run_ast(file, "regexp_replace(source, pat, repl)", three_columns);

    let four_columns = &[
        (
            "source",
            StringType::from_data(vec![
                "abc def ghi",
                "abc def ghi",
                "abc def ghi",
                "abc def ghi",
            ]),
        ),
        (
            "pat",
            StringType::from_data(vec!["[a-z]+", "[a-z]+", "[a-z]+", "[a-z]+"]),
        ),
        ("repl", StringType::from_data(vec!["X", "X", "X", "X"])),
        ("pos", Int64Type::from_data(vec![1_i64, 4, 8, 12])),
    ];
    run_ast(file, "regexp_replace(source, pat, repl, pos)", four_columns);

    let five_columns = &[
        (
            "source",
            StringType::from_data(vec![
                "abc def ghi",
                "abc def ghi",
                "abc def ghi",
                "abc def ghi",
            ]),
        ),
        (
            "pat",
            StringType::from_data(vec!["[a-z]+", "[a-z]+", "[a-z]+", "[a-z]+"]),
        ),
        ("repl", StringType::from_data(vec!["X", "X", "X", "X"])),
        ("pos", Int64Type::from_data(vec![1_i64, 1, 4, 4])),
        ("occur", Int64Type::from_data(vec![0_i64, 1, 2, 3])),
    ];
    run_ast(
        file,
        "regexp_replace(source, pat, repl, pos, occur)",
        five_columns,
    );

    let six_columns = &[
        (
            "source",
            StringType::from_data(vec!["abc def ghi", "abc DEF ghi", "abc DEF ghi"]),
        ),
        (
            "pat",
            StringType::from_data(vec!["[a-z]+", "[a-z]+", "[a-z]+"]),
        ),
        ("repl", StringType::from_data(vec!["X", "X", "X"])),
        ("pos", Int64Type::from_data(vec![1_i64, 1, 4])),
        ("occur", Int64Type::from_data(vec![0_i64, 2, 1])),
        ("mt", StringType::from_data(vec!["", "c", "i"])),
    ];
    run_ast(
        file,
        "regexp_replace(source, pat, repl, pos, occur, mt)",
        six_columns,
    );

    let nullable_five_columns = &[
        (
            "source",
            StringType::from_data_with_validity(
                vec!["abc def ghi", "abc DEF ghi", "", "abc DEF ghi"],
                vec![true, true, false, true],
            ),
        ),
        (
            "pat",
            StringType::from_data_with_validity(vec!["[a-z]+", "", "", "[a-z]+"], vec![
                true, false, false, true,
            ]),
        ),
        ("repl", StringType::from_data(vec!["X", "X", "X", "X"])),
        ("pos", Int64Type::from_data(vec![1_i64, 1, 4, 4])),
        ("occur", Int64Type::from_data(vec![0_i64, 2, 1, 1])),
    ];
    run_ast(
        file,
        "regexp_replace(source, pat, repl, pos, occur)",
        nullable_five_columns,
    );

    let nullable_six_columns = &[
        (
            "source",
            StringType::from_data_with_validity(
                vec!["abc def ghi", "abc DEF ghi", "", "abc DEF ghi"],
                vec![true, true, false, true],
            ),
        ),
        (
            "pat",
            StringType::from_data_with_validity(vec!["[a-z]+", "", "", "[a-z]+"], vec![
                true, false, false, true,
            ]),
        ),
        ("repl", StringType::from_data(vec!["X", "X", "X", "X"])),
        ("pos", Int64Type::from_data(vec![1_i64, 1, 4, 4])),
        ("occur", Int64Type::from_data(vec![0_i64, 2, 1, 1])),
        ("mt", StringType::from_data(vec!["", "c", "i", "i"])),
    ];
    run_ast(
        file,
        "regexp_replace(source, pat, repl, pos, occur, mt)",
        nullable_six_columns,
    );

    let multi_byte_five_columns = &[
        (
            "source",
            StringType::from_data(vec![
                "周 周周 周周周 周周周周",
                "周 周周 周周周 周周周周",
                "周 周周 周周周 周周周周",
                "周 周周 周周周 周周周周",
            ]),
        ),
        (
            "pat",
            StringType::from_data(vec!["周+", "周+", "周+", "周+"]),
        ),
        ("repl", StringType::from_data(vec!["唐", "唐", "唐", "唐"])),
        ("pos", Int64Type::from_data(vec![1_i64, 2, 3, 5])),
        ("occur", Int64Type::from_data(vec![0_i64, 1, 3, 1])),
    ];
    run_ast(
        file,
        "regexp_replace(source, pat, repl, pos, occur)",
        multi_byte_five_columns,
    );

    let position_error_four_columns = &[
        ("source", StringType::from_data(vec!["abc"])),
        ("pat", StringType::from_data(vec!["b"])),
        ("repl", StringType::from_data(vec!["X"])),
        ("pos", Int64Type::from_data(vec![0_i64])),
    ];
    run_ast(
        file,
        "regexp_replace(source, pat, repl, pos)",
        position_error_four_columns,
    );

    let occurrence_error_five_columns = &[
        ("source", StringType::from_data(vec!["a b c"])),
        ("pat", StringType::from_data(vec!["b"])),
        ("repl", StringType::from_data(vec!["X"])),
        ("pos", Int64Type::from_data(vec![1_i64])),
        ("occur", Int64Type::from_data(vec![-1_i64])),
    ];
    run_ast(
        file,
        "regexp_replace(source, pat, repl, pos, occur)",
        occurrence_error_five_columns,
    );

    let match_type_error_six_columns = &[
        ("source", StringType::from_data(vec!["a b c"])),
        ("pat", StringType::from_data(vec!["b"])),
        ("repl", StringType::from_data(vec!["X"])),
        ("pos", Int64Type::from_data(vec![1_i64])),
        ("occur", Int64Type::from_data(vec![0_i64])),
        ("mt", StringType::from_data(vec!["-c"])),
    ];
    run_ast(
        file,
        "regexp_replace(source, pat, repl, pos, occur, mt)",
        match_type_error_six_columns,
    );
}

fn test_regexp_substr(file: &mut impl Write) {
    run_ast(file, "regexp_substr('abc def ghi', '[a-z]+')", &[]);
    run_ast(file, "regexp_substr('abc def ghi', '[a-z]+', 1, 3)", &[]);
    run_ast(file, "regexp_substr('abc def ghi', '[a-z]+', NULL)", &[]);
    run_ast(file, "regexp_substr('abc def ghi', '')", &[]);
    run_ast(file, "regexp_substr('', NULL)", &[]);
    run_ast(file, "regexp_substr('', '', 1, 3)", &[]);

    let two_columns = &[
        (
            "source",
            StringType::from_data(vec!["abc def ghi", "abc def ghi", ""]),
        ),
        ("pat", StringType::from_data(vec!["[a-z]+", "xxx", ""])),
    ];
    run_ast(file, "regexp_substr(source, pat)", two_columns);

    let three_columns = &[
        (
            "source",
            StringType::from_data(vec!["abc def ghi", "abc def ghi", "abc def ghi"]),
        ),
        (
            "pat",
            StringType::from_data(vec!["[a-z]+", "[a-z]+", "[a-z]+"]),
        ),
        ("pos", Int64Type::from_data(vec![1_i64, 4, 12])),
    ];
    run_ast(file, "regexp_substr(source, pat, pos)", three_columns);

    let four_columns = &[
        (
            "source",
            StringType::from_data(vec!["abc def ghi", "abc def ghi", "abc def ghi"]),
        ),
        (
            "pat",
            StringType::from_data(vec!["[a-z]+", "[a-z]+", "[a-z]+"]),
        ),
        ("pos", Int64Type::from_data(vec![1_i64, 4, 12])),
        ("occur", Int64Type::from_data(vec![3_i64, 2, 3])),
    ];
    run_ast(file, "regexp_substr(source, pat, pos, occur)", four_columns);

    let five_columns = &[
        (
            "source",
            StringType::from_data(vec!["ABC def ghi", "abc def GHI", "abc DEF ghi"]),
        ),
        (
            "pat",
            StringType::from_data(vec!["[a-z]+", "[a-z]+", "[a-z]+"]),
        ),
        ("pos", Int64Type::from_data(vec![1_i64, 4, 12])),
        ("occur", Int64Type::from_data(vec![3_i64, 2, 3])),
        ("mt", StringType::from_data(vec!["c", "i", "i"])),
    ];
    run_ast(
        file,
        "regexp_substr(source, pat, pos, occur, mt)",
        five_columns,
    );

    let nullable_five_columns = &[
        (
            "source",
            StringType::from_data_with_validity(
                vec!["abc def ghi", "abc DEF ghi", "", "abc DEF ghi"],
                vec![true, true, false, true],
            ),
        ),
        (
            "pat",
            StringType::from_data_with_validity(vec!["[a-z]+", "", "", "[a-z]+"], vec![
                true, false, false, true,
            ]),
        ),
        ("pos", Int64Type::from_data(vec![1_i64, 1, 4, 4])),
        ("occur", Int64Type::from_data(vec![1_i64, 2, 1, 1])),
        ("mt", StringType::from_data(vec!["", "c", "i", "i"])),
    ];
    run_ast(
        file,
        "regexp_substr(source, pat, pos, occur, mt)",
        nullable_five_columns,
    );

    let multi_byte_four_columns = &[
        (
            "source",
            StringType::from_data(vec![
                "周 周周 周周周 周周周周",
                "周 周周 周周周 周周周周",
                "周 周周 周周周 周周周周",
            ]),
        ),
        ("pat", StringType::from_data(vec!["周+", "周+", "周+"])),
        ("pos", Int64Type::from_data(vec![1_i64, 2, 14])),
        ("occur", Int64Type::from_data(vec![1_i64, 2, 1])),
    ];
    run_ast(
        file,
        "regexp_substr(source, pat, pos, occur)",
        multi_byte_four_columns,
    );

    let match_type_error_five_columns = &[
        ("source", StringType::from_data(vec!["a b c"])),
        ("pat", StringType::from_data(vec!["b"])),
        ("pos", Int64Type::from_data(vec![1_i64])),
        ("occur", Int64Type::from_data(vec![0_i64])),
        ("mt", StringType::from_data(vec!["-c"])),
    ];
    run_ast(
        file,
        "regexp_substr(source, pat, pos, occur, mt)",
        match_type_error_five_columns,
    );
}
