## Group Members:
- Kenton Rhoden
- Sam Phan

## Usage:
- compose.yaml uses the `.env` file to specify an M, R, and input filename
- INPUT_FILENAME should include filepath prefix `'app/[FILENAME]`
- Make sure input file is in the same directory as compose.yaml and .env
- to run the docker container:
```
> docker compose up --build
```

* Note: Docker takes a while to build the context if the INPUT file is too large

**Output Files**
- `outputs/mr-out-[n].txt*` - output from reducer n