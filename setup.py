from setuptools import setup, find_packages

setup(
    name='geozones',
    version='1.0.0',
    packages=find_packages(),
    include_package_data=True,
    zip_safe=False,
    install_requires=[
        'Fiona==1.8.13',
        'Flask==1.0.2',
        'Shapely==1.6.4.post2',
        'click==7.0',
        'colorama==0.4.1',
        'colorhash==1.0.2',
        'msgpack==0.6.1',
        'pymongo==3.8.0',
        'requests==2.21.0',
    ],
    extras_require={
        'i18n': ['Babel==2.6.0']
    },
    entry_points='''
        [console_scripts]
        geozones=geozones.__main__:cli
    ''',
)
