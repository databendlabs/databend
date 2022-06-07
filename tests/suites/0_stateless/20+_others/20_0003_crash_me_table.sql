SELECT * FROM sync_crash_me('async crash me'); -- {ErrorCode 1104}
SELECT * FROM async_crash_me('async crash me'); -- {ErrorCode 1104}
