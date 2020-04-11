# Standard library imports ...

# Third party library imports ...
from setuptools import setup

cmdline = 'arcgis_apache_logs.commandline'
console_scripts = [
    f'initialize-ags-database={cmdline}:initialize_ags_database',
    f'check-ags-services={cmdline}:check_ags_services',
    f'update-ags-database={cmdline}:update_ags_database',
    f'parse-ags-logs={cmdline}:parse_ags_logs',
    f'prune-ags-database={cmdline}:prune_ags_database',
    f'produce-ags-graphics={cmdline}:produce_ags_graphics',
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
