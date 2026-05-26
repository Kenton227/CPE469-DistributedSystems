## Group Members:

- Kenton Rhoden
- Sam Phan

## Usage:

- in the root directory, create the database with:

```
sqlite3 db/bank.db < db/schema.sql
```

- run the bank server with:

```
go run bankServer/bankServer.go
```

- run the teller client with:

```
go run client/teller/teller.go
```

- run the customer client with:

```
go run client/customer/customer.go
```
