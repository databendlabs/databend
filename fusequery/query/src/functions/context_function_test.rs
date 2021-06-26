// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use common_exception::Result;

#[test]
fn test_context_function_build_arg_from_ctx() -> Result<()> {
    use pretty_assertions::assert_eq;

    use crate::functions::*;

    let ctx = crate::tests::try_create_context()?;

    // Ok.
    {
        let args = ContextFunction::build_args_from_ctx("database".clone(), ctx.clone())?;
        assert_eq!("default", format!("{:?}", args[0]));
    }

    // Error.
    {
        let result = ContextFunction::build_args_from_ctx("databasexx", ctx.clone()).is_err();
        assert_eq!(true, result);
    }

    Ok(())
}
