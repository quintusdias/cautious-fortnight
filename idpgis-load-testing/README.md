# Running a load test in College Park

1. Install anaconda from http://continuum.io .  You must get version 3.6 for linux.
2. Clone the repository

```bash
cd $HOME
mkdir git
cd git
git clone https://gitlab.ncep.noaa.gov:jevans/idp-gis-jmeter.git
```

2. We need to run a load test from the VM specifically setup for this.

```bash
ssh vm-lnx-gis-loadtest
```

3. The location from which a load test is run is not set in stone.  For now, however, assume that they will be run from somewhere in the <tt>/gis_loadtest_logs</tt> directory structure.  The directory must should have `input` and `output` subdirectories.  It may be advisable to link to a previously existing `input` subdirectory rather than copying it.

4. Edit a file called config.yml appropriately.  This configuration file determines what services will be tests.  The simplest possible configuration file would specify a single service to be tested.

```YAML
folders:
- name: NWS_Forecasts_Guidance_Warnings
  services:
  - input: input/watch_warn_adv.csv
    name: watch_warn_adv
    num_threads: [46, 92, 138, 184, 230, 276, 322, 368, 414, 460]
    type: MapServer
intervals: [5, 5, 5, 5, 5, 5, 5, 5, 5, 5]
output_root: output
server: idpgisqa.ncep.noaa.gov
```

There will be 10 levels of load (5 minutes each level) specified to the
WWA service on <tt>idpgisqa.ncep.noaa.gov</tt>.  The input CSV file is
expected to be relative to the location of the configuration file.
5. Run the load test with

```shell
python $HOME/git/idp-gis-jmeter/bin/run_export_load_test.py
```

6. After the test has completed, the results will be found
under a directory called `output`.  Under `output` will be found
numerically-named subdirectories according to the load level.
The easiest way of determining the throughput at each level is to
examine the `stdout.txt`.  Additional summaries may be computed with
`idp-gis-jmeter/bin/summarize.py` which computes the throughput, number
of transactions, mean elapsed times, errors, and total bytes transferred
by each service at each load level.  This is stored as an HDF5 file that
should be read by the `pandas` package.

# Troubleshooting

It's advisable to run a preliminary test before initiating a full load
test.  A good way to do this is to use just a couple of short intervals,
e.g. a 6-minute test with 3 load levels might look like

```YAML
folders:
- name: NWS_Forecasts_Guidance_Warnings
  services:
  - input: input/watch_warn_adv.csv
    name: watch_warn_adv
    num_threads: [46, 92, 138, 184, 230, 276, 322, 368, 414, 460]
    type: MapServer
intervals: [2, 2, 2]
output_root: output
server: idpgisqa.ncep.noaa.gov
```

It can be helpful to enable a `Summary Report` module in a service element that is causing trouble.  Saving the URL is also useful.
