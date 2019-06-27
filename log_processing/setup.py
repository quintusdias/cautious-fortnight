# Standard library imports ...
import pathlib
import re

# Third party library imports ...
from setuptools import setup

kwargs = {
    'name': 'ArcGIS-Apache-Logs',
    'description': 'Tools for processing ArcGIS Apache Logs',
    'author': 'John Evans',
    'author_email': 'john.g.evans.ne@gmail.com',
    'url': 'https://github.com/quintusdias/gis-monitoring',
    'packages': ['arcgis_apache_logs'],
    'entry_points': {
        'console_scripts': [
            'parse-arcgis-apache-logs=arcgis_apache_logs.commandline:parse_arcgis_apache_logs',
        ],
    },
    'license': 'MIT',
    'install_requires': ['pandas', 'lxml', 'setuptools'],
    'version': '0.0.1',
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
