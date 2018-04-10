from setuptools import setup

kwargs = {
    'name': 'test_suite_documentation_generation',
    'author': 'John Evans',
    'author_email': 'john.g.evans@noaa.gov',
    'url': 'https://gitlab.ncep.noaa.gov/jevans/test-suite-documentation-generation',
    'description': 'Tools for generating HTML documentation from test suites',
    'entry_points': {
        'console_scripts': [
            'ts2html=test_suite_documentation_generation.commandline:ts2html',
        ]
    },
    'packages': ['test_suite_documentation_generation'],
    'install_requires': ['lxml>=3.5.0'],
    'version': '0.0.1',
}

setup(**kwargs)
