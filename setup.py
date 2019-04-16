from setuptools import setup, find_packages

setup(
    name='geozones',
    version='1.0.0',
    packages=find_packages(),
    include_package_data=True,
    zip_safe=False,
    install_requires=[
        'Fiona==1.7.13',
        'Flask==1.0.2',
        'Shapely==1.6.4.post2',
        'click==6.7',
        'colorhash==1.0.2',
        'colorama==0.3.9',
        'msgpack==0.5.6',
        'pymongo==3.7.1',
        'requests==2.18.4',
    ],
    extras_require={
        'i18n': ['transifex-client==0.13.4', 'Babel==2.6.0']
    },
    entry_points='''
        [console_scripts]
        geozones=geozones.__main__:cli
    ''',
)
