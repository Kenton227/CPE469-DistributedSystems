## Group Members:

- Kenton Rhoden
- Sam Phan

## Usage:

- use the `.env` to specify an M, R, INPUT_FILE, P, and BATCH_SIZE
    - **M**: number of map tasks
    - **R**: number of reduce tasks
    - **INPUT_FILE**: file containing urls to scrape separated by new lines
    - **P**: number of threads that searcher will use
    - **BATCH_SIZE**: size of each split of data

- to run the docker containers:

```
docker compose up --build coordinator worker1 worker2 worker3 worker4
```

- to run the searcher:

```
docker compose up --build searcher
```

## Output Files

- `logs/runtime.txt`: gives runtime of map phase, reduce phase, and total runtime
- `logs/logfile.txt`: list of log entries for last run
