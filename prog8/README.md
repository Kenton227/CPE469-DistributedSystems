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

## TESTING
```
docker compose run --rm teller -auto-open -user teller

docker compose run --rm customer -auto -user alice -n 1000 -delay-ms 10

docker compose run --rm customer -auto -user bob -n 1000 -delay-ms 10

docker compose run --rm customer -auto -user carol -n 1000 -delay-ms 10

docker compose run --rm customer -auto -user dave -n 1000 -delay-ms 10

docker compose run --rm customer -auto -user erin -n 1000 -delay-ms 10
```

## NOTE

- the leader automatically calls `Tester.CompareLogs` after a successful append where all replicas ACK
