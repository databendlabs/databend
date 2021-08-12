  # Datafuse CLI

All-in-one tool for setting up, managing with Datafuse.

Build:
```
$ make cli
```

Usage:
``` 
$ ./target/release/datafuse-cli 
[test] > #version
#version
[test] > version
Datafuse CLI         0.1.0
Datafuse CLI SHA256  841f74c8efd556229732142ea400863b7a71e640bc7112f43b658777bcf4ce19
Git commit           f15e6ab
Build date           2021-08-05T09:55:03.581162876+00:00
OS version           thinkpad 20.04 (kernel 5.10.0-1038-oem)
[test] > help
version              Datafuse CLI version
comment              # your comments
package              Package command

[test] > package -h
package 

USAGE:
    package [SUBCOMMAND]

FLAGS:
    -h, --help    Prints help information

SUBCOMMANDS:
    fetch     Fetch the latest version package
    list      List all the packages
    switch    Switch the active datafuse to a specified version

[test] > package fetch
✅ [ok] Arch x86_64-unknown-linux-gnu
✅ [ok] Tag v0.4.69-nightly
✅ [ok] Binary /home/bohu/.datafuse/test/downloads/v0.4.69-nightly/datafuse--x86_64-unknown-linux-gnu.tar.gz
✅ [ok] Unpack /home/bohu/.datafuse/test/bin/v0.4.69-nightly

[test] > package list
+-----------------+-----------------------------------------------+---------+
| Version         | Path                                          | Current |
+-----------------+-----------------------------------------------+---------+
| v0.4.69-nightly | /home/bohu/.datafuse/test/bin/v0.4.69-nightly | ✅      |
+-----------------+-----------------------------------------------+---------+
| v0.4.68-nightly | /home/bohu/.datafuse/test/bin/v0.4.68-nightly |         |
+-----------------+-----------------------------------------------+---------+
[test] > package switch v0.4.68-nightly
✅ [ok] Package switch to v0.4.68-nightly
[test] > 

```
