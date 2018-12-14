from setuptools import setup

scripts = [
    'collect_ags_stats',
    'get_ags_requests',
    'heatmap',
    'plot_mpl_ags_stats',
    'set_ags',
    'summarize_ags_logs'
]
console_scripts = [f"{item}=ags_user.commandline:{item}"
                   for item in scripts]

kwargs = {
    'name': 'ags_user',
    'author': 'John Evans',
    'author_email': 'john.g.evans@noaa.gov',
    'url': 'https://gitlab.ncep.noaa.gov/jevans/abusive-user-detection',
    'description': 'Tools for detecting abusive users',
    'entry_points': {
        'console_scripts': console_scripts,
    },
    'packages': ['ags_user'],
    'install_requires': [
        'apache_log_parser',
        'openpyxl>=2.4.0',
        'matplotlib>=2.1.0',
        'pandas',
        'tables>=3.3.0',
    ],
    'version': '0.0.2rc1',
}

setup(**kwargs)
