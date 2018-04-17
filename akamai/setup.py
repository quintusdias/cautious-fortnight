from setuptools import setup

scripts = [
    'get_akamai_logs',
]
console_scripts = [f"{item}=nco_akamai.commandline:{item}"
                   for item in scripts]

kwargs = {
    'name': 'nco_akamai',
    'author': 'John Evans',
    'author_email': 'john.g.evans@noaa.gov',
    'url': 'https://vlab.ncep.noaa.gov/redmine/projects/gis-monitoring',
    'description': 'Tools for working with Akamai',
    'entry_points': {
        'console_scripts': console_scripts,
    },
    'packages': ['nco_akamai'],
    'install_requires': [
        'apache_log_parser',
        'openpyxl>=2.4.0',
        'pandas',
        'tables>=3.3.0',
    ],
    'version': '0.1.0',
}

setup(**kwargs)
