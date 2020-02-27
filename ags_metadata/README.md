# Installation
Change directories into `gis-monitoring/ags_metadata`.

Create the anaconda environment with

```shell
conda env create -f environment.yml
```

Install the command line scripts with

```shell
python setup.py develop
```

This will install 3 command line scripts, but you only need to concern yourself with `rest2iso`.

# rest2iso

## Usage
```bash
usage: rest2iso [-h] [--verbose {debug,info,warning,error,critical}]
                [--output OUTPUT]
                config

Build ISO 19115-2 metadata from ArcGIS REST directory.

positional arguments:
  config                YAML configuration file

optional arguments:
  -h, --help            show this help message and exit
  --verbose {debug,info,warning,error,critical}
                        Log level
  --output OUTPUT       Output directory (default is the current directory).
                        Underneath this directory, a subdirectory
                        corresponding to the server name will be created (see
                        the config file for that), and under that, 'xml' and
                        'html' directories will be created. And under THOSE,
                        the service folders are created, which house the XML
                        documents and their HTML counterparts.
```

You can run the metadata generation for, say, nowCOAST from the UNIX shell with

```shell
rest2iso config/nowcoast.yml
```

This will create a directory `nowcoast.noaa.gov` in the your current directory
and write the XML metadata documents (and human-readable HTML counterparts)
to that directory.
