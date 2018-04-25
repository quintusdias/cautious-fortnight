# Third party library imports
from setuptools import setup

# Define the bin scripts.
units = [
    'generate-geoserver-test-plans',
    'generate-geoserver-wms-input',
    'run-geoserver-loadtest',
    'summarize-geoserver-loadtest',
]
console_scripts = [
    f"{item}=geoserver_load_testing.commandline:{item.replace('-', '_')}"
    for item in units
]

kwargs = {
    'name': 'geoserver-load-testing',
    'description': 'Tools for running load tests against GeoServer',
    'author': 'John Evans',
    'author_email': 'john.g.evans@noaa.gov',
    'packages': ['geoserver_load_testing'],
    'package_data': {'geoserver_load_testing': ['etc/plan.jmx']},
    'version': '0.1.3',
    'install_requires': [
        'apache_log_parser>=1.7.0',
        'PyYAML>=3.12',
        'lxml>=3.7.0',
        'pandas>=0.21.0',
        'pyproj>=1.9.5',
        'seaborn>=0.8.1',
    ],
    'entry_points': {
        'console_scripts': console_scripts,
    },
}

setup(**kwargs)
