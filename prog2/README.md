## Group Members:
- Kenton RHoden
- Sam Phan

## Usage:
- to run the driver for the coordinator do:
```
> cd prog2/
> go run . 9 3 <someTextFile.txt>
```
- to run an instance of a worker do:
```
> cd prog2/
> go run ./worker/
```

**Output Files**
- `splits/split-*` - directory of splits from input file named split-*splitId*
- `intermediates/intermediate-*` - directory of intermediate files named intermediate-*splitId*-*reducerId*.json
- `mr-outs/mr-out-*` - directory of map reduce output files named mr-out-*reduceId*.txt
