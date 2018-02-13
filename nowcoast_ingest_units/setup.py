from setuptools import setup

scripts = [
    'summarize_nc_ingest_units'
]
console_scripts = [f"{item}=nc_ingest_units.commandline:{item}"
                   for item in scripts]

kwargs = {
    'name': 'nc_ingest_units',
    'author': 'John Evans',
    'author_email': 'john.g.evans@noaa.gov',
    'description': 'Graphically summarize nowCOAST ingest process loads',
    'entry_points': {
        'console_scripts': console_scripts,
    },
    'packages': ['nc_ingest_units'],
    'install_requires': [
        'pandas',
    ],
    'version': '0.0.1',
}

setup(**kwargs)

