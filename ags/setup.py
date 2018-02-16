from setuptools import setup

scripts = [
    'bin_daily_apache_log_file',
    'collect_ags_stats',
    'collect_arcsoc_counts',
    'count_nco_log_items',
    'get_ags_requests',
    'get_akamai_logs',
    'heatmap',
    'plot_mpl_ags_stats',
    'plot_nco_hits',
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
        'pandas',
        'tables>=3.3.0',
        'scikit-image',
    ],
    'version': '0.0.1',
}

setup(**kwargs)
