import functools
import difflib
import pytest


def comparison_output(want):
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            import io
            from contextlib import redirect_stdout

            with io.StringIO() as buf, redirect_stdout(buf):
                result = func(*args, **kwargs)
                got = buf.getvalue().strip()

                if got != want.strip():
                    diff = "\n".join(
                        difflib.unified_diff(
                            want.strip().splitlines(),
                            got.splitlines(),
                        )
                    )
                    pytest.fail(f"{diff}")

            return result

        return wrapper

    return decorator
