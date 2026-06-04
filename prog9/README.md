## Group Members:

- Kenton Rhoden
- Sam Phan

## Usage:

- download the dataset on the host first:

```
cd ./dataset/
./downloadDataset.sh
cd ../
```

- build the Spark image:

```
docker compose build
```

- run the analysis on a Spark master with two worker containers:

```
docker compose up app
```

- stop the cluster containers when finished:

```
docker compose down
```

## Notes:

- results are written to `./output/analysisOutput.txt`.
