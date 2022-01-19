#[test]
fn {name}() {{
    let test = Test {{
        name: "{name}".to_string(),
        input: "{input}".to_string(),
        input_data: include_str!("{input}"),
        result: "{result}".to_string(),
        result_data: include_str!("{result}"),
    }};

    test.run()
}}
