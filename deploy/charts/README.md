## Run in Kubernetes using Helm
### Dependencies

1. Clone:

```text
git clone https://github.com/datafuselabs/datafuse.git
```

2. Make sure you have [Kubernetes](https://kubernetes.io/) cluster running

### Build Image

`make docker` to build image `datafuselabs/fuse-query`

###  Run Helm 

`make run-helm` in project root directory,

when successful install you will get a note like this,

```
NOTES:
1. connect to fuse-query mysql port:
export FUSE_MYSQL_PORT=$(kubectl get --namespace default -o jsonpath="{.spec.ports[0].nodePort}" services datafuse)
mysql -h127.0.0.1 -P$FUSE_MYSQL_PORT

export FUSE_HTTP_PORT=$(kubectl get --namespace default -o jsonpath="{.spec.ports[2].nodePort}" services datafuse)
curl http://127.0.0.1:$FUSE_HTTP_PORT/v1/configs
```

More to see [building-and-running.md](../../docs/overview/building-and-running.md)
