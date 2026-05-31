## Group Members:

- Kenton Rhoden
- Sam Phan

## Usage:

- in the root directory, build the containers with:

```
docker compose build
```

- run the tester and bank servers with:

```
docker compose up tester bankserver1 bankserver2 bankserver3
```

- run the teller with:

```
docker compose run --rm teller
```

- run the client with:

```
docker compose run --rm customer
```

## NOTE

- the leader automatically calls `Tester.CompareLogs` after a successful append where all replicas ACK
