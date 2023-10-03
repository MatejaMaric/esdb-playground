## Requirements

- [Go 1.21 or higher](https://go.dev/dl/)
- [Docker](https://www.docker.com/products/docker-desktop)

## Setup steps

### EventStoreDB & MariaDB

If you are on an x86_64 system you should go to `docker-compose.yaml` and change the EventStoreDBs container image to `buster-slim`.
To start the databases run:

```bash
docker-compose up
```

To access EventStoreDB web interface you can type `localhost:2113` in your web browser.

To access MariaDB, you use the MySQL CLI client (if you have it installed):

```bash
mysql --host=127.0.0.1 --user=playground_user --password=playground_user_password projected_models
```

Or you can access the Docker container to connect, using it's MariaDB CLI client:

```bash
docker exec -it esdb-playground-mariadb-1 /bin/sh
/bin/mariadb --user=playground_user --password=playground_user_password projected_models
```

### The project itself

```bash
go build
./esdb-playground
```
