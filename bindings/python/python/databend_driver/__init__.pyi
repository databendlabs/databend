class AsyncDatabendDriver:
    def __init__(self, dsn: str): ...  # NOQA
    async def exec(self, sql: str) -> int: ...  # NOQA
