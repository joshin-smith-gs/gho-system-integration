# GHO Child Mortality Data Integration

## Overview

This process pulls mortality rates for children under the age for all African countries. The script allows for either pulling the latest missing years or pulling data for all time.
It combines the number of deaths with the mortality rate per 1000 births as well as the percentage. This data is aggregate by year, country and sex.

## Exploring the GHO data

This is how I recommended exploring the GHO data using the API. It is also how I went about investigating the data. I would begin by exploring the GHO Indicators. You can do this by browsing the alphabetic list of indicatiors [here](https://www.who.int/data/gho/data/indicators/indicators-index) and choosing something that seems interesting. This will give you sample views of the possible data. It also show related indicators and metadata. Once you choose something that you want to investigate, you can pull the relevant indicators using the API and explore usings pandas or importing the data into SQL for exploration. During this time you can identify the dimensions that you would like values for as well.

You can also figure out related indicators join together. Pulling the indicators and dimensions data into python will also allow you to determine how the data is structured and how to join the two. Note that many indicators do not have the same dimensions even if they seem related.

## Running the script

The script has three possible arguements:
1. `delta`: specifies whether or not to only get the latest missing data
2. `schema`: destination schema of the data
3. `table`: destination table of the data

The script can be run either with python or using the provided container (recommended). Either option requires postgres credentials to be stored as an environment variable in the following format:
```
{"host": host, "port": port, "dbname": dbname, "user": username, "password":password}
```

Alternatively, the credentials can be stored in a file (if using docker), e.g. `~/env.list` with the below contents
```
PG_CREDS={"host": host, "port": port, "dbname": dbname, "user": username, "password":password}
```

Docker:
1. Build the container
```bash
 docker build -t mortality_rates:latest .
```
2. Run the container
```bash
docker run --rm -e PG_CREDS mortality_rates:latest python main.py --delta=True --schema="mortality_data" --table="mortality_rates_africa"
```
or
```bash
docker run --rm --env-file ~/env.list  mortality_rates:latest  python main.py --delta=True --schema="mortality_data" --table="mortality_rates_africa"
```

Python
1. Install requirements (preferrably within a virtual environment)
```bash
python -m pip install -r requirements.txt
```
2. Run the script
```bash
python main.py  --delta=True --schema="mortality_data" --table="mortality_rates_africa"
```

## Future Improvements

1. Add some tests:
    * check for new data before running
    * validate inputs
    * add more error handling on known possible errors
2. Expand functionality in order to reduce hardcoded values as well as improve efficiency
    * change the api call function to be async
    * allow for pulling for more expansive data. e.g. specific dieases, additional age groups
    * add config files to make the code more dynamic. For example a yaml that specifies filters and other paramaters
    * expand the code to allow for pulling in different types of data
    * move some of the data processing to Postgres to improve efficiency
