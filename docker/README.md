# Multi-architecture Docker image support

Support cross platform docker image build

## How to use

## Install docker ![Buildkit](https://github.com/moby/buildkit)

For macOS

```bash
brew install buildkit
```

For linux

```bash
docker run --name buildkit -d --privileged -p 1234:1234 moby/buildkit --addr tcp://0.0.0.0:1234
export BUILDKIT_HOST=tcp://0.0.0.0:1234
docker cp buildkit:/usr/bin/buildctl /usr/local/bin/
buildctl build --help
```

## Check on available platforms given your host machine

```bash
docker run --privileged --rm tonistiigi/binfmt --install all
```

## Build and push container for supported platforms

### initialize host networking

```bash
docker buildx create --name host --use --buildkitd-flags '--allow-insecure-entitlement network.host'
```

### update qemu static link

```bash
 docker run --rm --privileged multiarch/qemu-user-static --reset -p yes
```

### build with given buildx builder

return to databend root directory

```bash
make dockerx PLATFORM=<platform your host machine supports>
```