## Requirements

- [Go 1.21 or higher](https://go.dev/dl/)
- [Docker](https://www.docker.com/products/docker-desktop)

## Setup steps

### EventStoreDB

If you are on an x86_64 system you should go to `docker-compose.yaml` and change the container image to `buster-slim`.
To start the single instance cluster run:

```bash
docker-compose up
```

### The project itself

```bash
go build
./esdb-playground
```
