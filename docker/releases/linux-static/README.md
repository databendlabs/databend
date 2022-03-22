# Build fully static binaries

## How to use?

> It's OK to use both docker and podman.

All example is running under the root dir of databend.

### Build image

```shell
podman build --network=host ./docker/releases/linux-static/ -t databend-static-builder
```

- `--network=host` makes sure that the network is available during build.
- `-t` is the name of this image.

### Build databend

```shell
podman run --network=host -it -v .:/code localhost/databend-static-builder sh
```

- `--network=host` makes sure that the network is available during build.
- `-it` opens an interactive shell for operation.
- `-v .:/code` mount current dir into container `/code`
- `localhost/databend-static-builder` the name of the previously built image
- `sh` shell that provided by `alpine`.

Inside the container, we can build our static binaries:

```shell
cargo build --target=x86_64-unknown-linux-musl
```

After the build is finished, we can use `ldd` and `readelf` to ensure.

```shell
> ldd target/x86_64-unknown-linux-musl/debug/databend-query
    /lib/ld-musl-x86_64.so.1 (0x7f67838ae000)
> readelf -d target/x86_64-unknown-linux-musl/debug/databend-query | grep NEEDED
```

There is no `NEEDED` dynamic lib here. We can use this lib in different linux distributions now!

## Explain

In short: use `musl`.

We use musl native linux distribution `Alpine` to make it easier for the static build.

The only tricky part is `openssl`. To make OpenSSL works for `openssl-sys`, we need to install `openssl-libs-static` and make sure `OPENSSL_STATIC`, `OPENSSL_LIB_DIR`, `OPENSSL_INCLUDE_DIR` are set up correctly.

> NOTE: trust Linux distribution maintainers, don't try to compile openssl by hands or other trick parts. They often make things much more complicated.

We have done those parts in the builder's image, so users don't need to care about them.
