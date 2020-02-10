# Standard library imports ...

# Third party library imports ...
from setuptools import setup

cmdline = 'arcgis_apache_logs.commandline'
console_scripts = [
    f'initialize-arcgis-apache-database={cmdline}:init_db',
    f'initialize-ag-pg-database={cmdline}:initialize_ag_pg_database',
    f'parse-arcgis-apache-logs={cmdline}:parse_arcgis_apache_logs',
    f'prune-arcgis-apache-database={cmdline}:prune_arcgis_apache_database',
    f'produce-arcgis-apache-graphics={cmdline}:produce_arcgis_apache_graphics',
],

kwargs = {
    'name': 'ArcGIS-Apache-Logs',
    'description': 'Tools for processing ArcGIS Apache Logs',
    'author': 'John Evans',
    'author_email': 'john.g.evans.ne@gmail.com',
    'url': 'https://github.com/quintusdias/gis-monitoring',
    'packages': ['arcgis_apache_logs'],
    'entry_points': {
        'console_scripts': console_scripts,
    },
    'license': 'MIT',
    'install_requires': ['pandas', 'lxml', 'setuptools'],
    'version': '0.0.8',
}

kwargs['classifiers'] = [
    "Programming Language :: Python",
    "Programming Language :: Python :: 3.7",
    "Programming Language :: Python :: Implementation :: CPython",
    "License :: OSI Approved :: MIT License",
    "Development Status :: 5 - Production/Stable",
    "Operating System :: MacOS",
    "Operating System :: POSIX :: Linux",
    "Operating System :: Microsoft :: Windows :: Windows XP",
    "Intended Audience :: Science/Research",
    "Intended Audience :: Information Technology",
    "Topic :: Software Development :: Libraries :: Python Modules"
]

setup(**kwargs)
