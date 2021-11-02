---
id: development-release-channels
title: Databend release channels
---

## What's Databend release channels?

Databend release process following the 'release train' model used by e.g. Rust, Firefox and Chrome, as well as 'feature staging'.

**But in the early stage Databend will upgrade the nightly version number only, and when a nightly version is ready for beta, we will leave a beta version getting off of the nightly version.**

Ok, Let's start by understanding how Databend do the releases.
The following is mainly from the Rust documentation [How Rust is Made and “Nightly Rust”](https://github.com/rust-lang/book/blob/main/src/appendix-07-nightly-rust.md).

There are three release channels for Databend(this is same as Rust):
- Nightly
- Beta
- Stable

So as time passes, our releases look like this, once a night:
```
nightly: * - - * - - *
```

Every six weeks, it’s time to prepare a new release! The beta branch of the Databend repository branches off from the main branch used by nightly. Now, there are two releases:
```
nightly: * - - * - - *
                     |
beta:                *
```

Six weeks after the first beta was created, it’s time for a stable release! The stable branch is produced from the beta branch:
```
nightly: * - - * - - * - - * - - * - - * - * - *
                     |
beta:                * - - - - - - - - *
                                       |
stable:                                *
```

This is called the “train model” because every six weeks, a release “leaves the station”, but still has to take a journey through the beta channel before it arrives as a stable release.


## Notes

[Roadmap 2021](https://github.com/datafuselabs/databend/issues/746)

[https://databend.rs](https://databend.rs/)