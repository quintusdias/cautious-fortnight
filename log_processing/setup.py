# Standard library imports ...

# Third party library imports ...
from setuptools import setup

cmdline = 'arcgis_apache_logs.commandline'
console_scripts = [
    f'agp-initialize-database={cmdline}:initialize_ag_ap_pg_database',
    f'agp-check-services={cmdline}:check_ag_ap_pg_services',
    f'agp-update-database={cmdline}:update_ag_ap_pg_database',
    f'agp-parse-logs={cmdline}:parse_arcgis_apache_logs',
    f'agp-prune-database={cmdline}:prune_arcgis_apache_database',
    f'agp-produce-graphics={cmdline}:produce_arcgis_apache_graphics',
],

kwargs = {
    'name': 'ArcGIS-Apache-Postgres-Logs',
    'description': 'Tools for processing ArcGIS Apache Logs',
    'author': 'John Evans',
    'author_email': 'john.g.evans.ne@gmail.com',
    'url': 'https://github.com/quintusdias/gis-monitoring',
    'packages': ['arcgis_apache_postgres_logs'],
    'entry_points': {
        'console_scripts': console_scripts,
    },
    'license': 'MIT',
    'install_requires': ['pandas', 'lxml', 'setuptools'],
    'version': '0.0.9',
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
