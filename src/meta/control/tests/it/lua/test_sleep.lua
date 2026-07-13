-- `metactl.sleep` completes and takes at least roughly the requested time.
--
-- Runs under the metactl Lua test harness (tests/it/lua.rs). `os.time()` has
-- whole-second resolution, so sleep a bit over one second and require the
-- clock to advance. Failures are raised via `assert`.

print("sleep: metactl.sleep(1.1) should advance the wall clock")

local before = os.time()
metactl.sleep(1.1)
local elapsed = os.difftime(os.time(), before)

assert(elapsed >= 1, "sleep(1.1) returned after " .. elapsed .. "s")
print("sleep OK: " .. elapsed .. "s")
