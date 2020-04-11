# AGS logs with SQLITE -- Setup

## Create the environment

1. Install Anaconda.
2. Clone this repo.
3. `cd log-processing`
4. `git checkout sqlite`
5. Create the proper environment using

```
conda env create -f environment.yml
```

6. Activate the environment with `conda activate sqlite`
7. Install the package with `python setup.py install`

## Initialize the database

```
usage: initialize-ags-database [-h] [--dbname DBNAME] {idpgis,nowcoast}
initialize-ags-database: error: the following arguments are required: project
```

So, for example, to setup for nowcoast, use

