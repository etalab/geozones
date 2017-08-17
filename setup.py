from setuptools import setup, find_packages

setup(
    name='geozones',
    version='1.0.0',
    packages=find_packages(),
    include_package_data=True,
    install_requires=[
        'click==6.7',
        'colorama==0.3.9',
        'Fiona==1.7.8',
        'msgpack-python==0.4.8',
        'pymongo==3.5.0',
        'Shapely==1.6b4',
        'Flask==0.12.2',
        'requests==2.18.4',
    ],
    extras_require={
        'i18n': ['transifex-client==0.12.4', 'Babel==2.4.0']
    },
    entry_points='''
        [console_scripts]
        geozones=geozones.__main__:cli
    ''',
)
