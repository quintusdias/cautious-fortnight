# Third party library imports
from setuptools import setup

# Define the bin scripts.
units = [
    'generate-idpgis-test-plans',
    'generate-idpgis-rest-input',
    'generate-idpgis-wms-input',
    'run-idpgis-loadtest',
    'summarize-idpgis-loadtest',
]
console_scripts = [
    f"{item}=idpgis_load_testing.commandline:{item.replace('-', '_')}"
    for item in units
]

kwargs = {
    'name': 'idpgis-load-testing',
    'description': 'Tools for running load tests against IDP-GIS',
    'author': 'John Evans',
    'author_email': 'john.g.evans@noaa.gov',
    'packages': ['idpgis_load_testing'],
    'package_data': {'idpgis_load_testing': ['etc/plan.jmx']},
    'version': '0.1.3',
    'install_requires': [
        'apache_log_parser>=1.7.0',
        'PyYAML>=3.12',
        'lxml>=3.7.0',
        'pandas>=0.21.0',
        'pyproj>=1.9.5',
    ],
    'entry_points': {
        'console_scripts': console_scripts,
    },
}

setup(**kwargs)
