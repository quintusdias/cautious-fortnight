from setuptools import setup

kwargs = {
    'name': 'ags_metadata',
    'author': 'John Evans',
    'author_email': 'john.g.evans@noaa.gov',
    'url': 'https://gitlab.ncep.noaa.gov/jevans/nowcoast-metadata',
    'description': 'Tools for generating ISO19115-2 metadata',
    'entry_points': {
        'console_scripts': [
            'rest2iso=ags_metadata.command_line:rest2iso',
            'iso2html=ags_metadata.command_line:iso191152_to_html',
            'update-iso=ags_metadata.command_line:update_iso',
        ]
    },
    'packages': ['ags_metadata'],
    'package_data': {
        'ags_metadata': [
            'data/*.xml',
            'data/schema/gmd/*.xsd',
            'data/schema/gmi/*.xsd',
            'data/schema/srv/*.xsd',
            'data/xsl/*.xsl',
            'data/*.txt',
        ]
    },
    'install_requires': [
        'gdal>=2.1.0',
        'lxml>=3.5.0',
        'requests>=2.12.1',
        'setuptools>=27.2.0',
        'pyyaml>=3.12',
    ],
    'version': '0.1.0',
}

setup(**kwargs)
