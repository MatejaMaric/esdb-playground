## Requirements

- [Go 1.21 or higher](https://go.dev/dl/)
- [Docker](https://www.docker.com/products/docker-desktop)

## Setup steps

### EventStoreDB (taken from the [official docs for setting up a secure cluster](https://developers.eventstore.com/server/v20.10/installation.html#use-docker-compose))

Docker containers will use the shared volume using the local ./certs directory
for certificates. However, if you let Docker to create the directory on
startup, the container won't be able to get write access to it. Therefore,
create the certs directory manually. You only need to do it once.

```bash
mkdir certs
```

Now you are ready to start the cluster.

```bash
docker-compose up
```

Check the log messages, after some time the elections process completes, and
you'd be able to connect to each node using the Admin UI. Nodes should be
accessible on the loopback address (127.0.0.1 or localhost) over HTTP and TCP,
using ports specified below:

| Node  | TCP port | HTTP port |
|-------|----------|-----------|
| node1 | 1111     | 2111      |
| node2 | 1112     | 2112      |
| node3 | 1113     | 2113      |

You have to tell your client to use secure connection for both TCP and gRPC.

| Protocol | Connection string                                                                                   |
|----------|-----------------------------------------------------------------------------------------------------|
| TCP      | GossipSeeds=localhost:1111,localhost:1112,localhost:1113;ValidateServer=False;UseSslConnection=True |
| gRPC     | esdb://localhost:2111,localhost:2112,localhost:2113?tls=true&tlsVerifyCert=false                    |
