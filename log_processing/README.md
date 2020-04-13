# AGS logs with SQLITE -- Setup

## Create the environment

1. Install Anaconda.
2. Clone this repo.
3. `cd log-processing`
4. `git checkout old_sqlite`
5. Create the proper environment using

```
conda env create -f environment.yml
```

6. Activate the environment with `conda activate sqlite`
7. Install the package with `python setup.py install`
8. Install a script for getting the akamai logs with `cd ../akamai`
9. `python setup.py install`
10.  `cd ../log_processing`

## Initialize the database

So, for example, to setup for nowcoast and idpgis, use

```
initialize-arcgis-apache-database nowcoast
initialize-arcgis-apache-database idpgis
```

# Let 'er rip!

```
./run process_daily_logs.sh
```

This first time, it will take a while for this to run, as it has to get thru
several days of logs.  Subsequent runs over the next few days will be quicker.

