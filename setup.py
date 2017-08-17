from setuptools import setup, find_packages

setup(
    name='geozones',
    version='1.0.0',
    packages=find_packages(),
    include_package_data=True,
    install_requires=[
        'click>=4.0',
        'colorama>=0.3.3',
        'Fiona==1.7.5',
        'msgpack-python==0.4.7',
        'pymongo>=3.0',
        'Shapely==1.5.17',
        'Flask',
        'requests',
    ],
    extras_require={
        'i18n': ['transifex-client==0.11.1b0', 'Babel>=1.3']
    },
    entry_points='''
        [console_scripts]
        geozones=geozones.__main__:cli
    ''',
)
