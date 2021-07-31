# Datafuse CLI

All-in-one tool for setting up, managing with Datafuse.

Build:
```
$ make cli
```

Usage:
``` 
$ ./target/release/datafuse-cli 
[test] > help
version              Datafuse CLI version
comment              # your comments
update               Check and download the package to local path
cluster              Start the fuse-query and fuse-store service
[test] > #update to latest version
#update to latest version 
[test] > update
Arch                 x86_64-unknown-linux-gnu
Tag                  v0.4.66-nightly
Binary               /home/bohu/.datafuse/test/downloads/datafuse--x86_64-unknown-linux-gnu.tar.gz
Download             https://github.com/datafuselabs/datafuse/releases/download/v0.4.66-nightly/datafuse--x86_64-unknown-linux-gnu.tar.gz
Unpack to            /home/bohu/.datafuse/test/bin/v0.4.66-nightly
[test] > #start fuse-query and fuse-store on local
#start fuse-query and fuse-store on local 
[test] > cluster start
Start [fuse-query] /home/bohu/.datafuse/test/bin/v0.4.66-nightly/fuse-query > /tmp/query-log.txt 2>&1 &
[OK]        
Start [fuse-store] /home/bohu/.datafuse/test/bin/v0.4.66-nightly/fuse-store> /tmp/store-log.txt 2>&1 &
[OK]        
[test] > 
```
