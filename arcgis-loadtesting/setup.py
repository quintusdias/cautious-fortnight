# Third party library imports
from setuptools import setup

# Define the bin scripts.
units = [
    'generate-arcgis-test-plans',
    'generate-arcgis-rest-input',
    'generate-arcgis-wms-input',
    'run-arcgis-loadtest',
    'summarize-arcgis-loadtest',
]
console_scripts = [
    f"{item}=arcgis_loadtesting.commandline:{item.replace('-', '_')}"
    for item in units
]

kwargs = {
    'name': 'arcgis-loadtesting',
    'description': 'Tools for running load tests against nowCOAST/IDP-GIS',
    'author': 'John Evans',
    'author_email': 'john.g.evans@noaa.gov',
    'packages': ['arcgis_loadtesting'],
    'package_data': {'arcgis_loadtesting': ['etc/plan.jmx']},
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
