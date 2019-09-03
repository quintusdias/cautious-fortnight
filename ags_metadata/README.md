# Installation

```shell
python setup.py install --user
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

If you are in the `gis-monitoring/ags_metadata` directory, then try it with

```shell
rest2iso --output=tmp config/nowcoast.yml
```

This will write metadata documents (and human-readable HTML counterparts)
to the `tmp` subdirectory of the current directory.
