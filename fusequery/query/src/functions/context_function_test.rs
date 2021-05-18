// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

#[test]
fn test_context_function_build_arg_from_ctx() -> anyhow::Result<()> {
    use pretty_assertions::assert_eq;

    use crate::functions::*;

    let ctx = crate::tests::try_create_context()?;

    // Ok.
    {
        let args = ContextFunction::build_args_from_ctx("database".clone())?;
        assert_eq!("default", format!("{:?}", args[0]));
    }

    // Error.
    {
        let result = ContextFunction::build_args_from_ctx("databasexx").is_err();
        assert_eq!(true, result);
    }

    Ok(())
}
