---
name: Build issue
about: Report failed Databend build from master
title: ''
labels: build
assignees: ''

---

Make sure that `git diff` result is empty and you've just pulled fresh master, and run first:
```text
$ make setup
```

**Operating system**

OS kind or distribution, specific version/release, non-standard kernel if any. If you are trying to build inside virtual machine, please mention it too.

**rust version**
```text
$ rustc --version
$ cargo --version
```

**Databend git version**
