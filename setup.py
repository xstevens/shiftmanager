# -*- coding: utf-8 -*-
import imp

from setuptools import setup, find_packages

# reqs = ["boto>=2.38.0",
#         "botocore>=1.1.4",
#         "docutils>=0.12",
#         "flake8>=2.2.5",
#         "funcsigs>=0.4",
#         "futures>=2.2.0",
#         "gdata>=2.0.18",
#         "gnureadline>=6.3.3",
#         "httplib2>=0.9.1",
#         "jmespath>=0.7.1",
#         "mccabe>=0.2.1",
#         "mock>=1.3.0",
#         "pbr>=1.3.0",
#         "pep8>=1.5.7",
#         "psycopg2>=2.5.4",
#         "py>=1.4.30",
#         "pyasn1>=0.1.8",
#         "pyasn1-modules>=0.0.7",
#         "pyflakes>=0.8.1",
#         "pytest>=2.7.2",
#         "python-dateutil>=2.4.2",
#         "sqlalchemy-redshift>=0.3.0",
#         "sqlalchemy-views>=0.2",
#         "rsa>=3.2",
#         "simplejson>=3.8.0",
#         "six>=1.9.0",
#         "uritemplate>=0.6"]

# Import metadata. Normally this would just be:
#
#     from shiftmanager import metadata
#
# However, when we do this, we also import `shiftmanager/__init__.py'. If this
# imports names from some other modules and these modules have third-party
# dependencies that need installing (which happens after this file is run), the
# script will crash. What we do instead is to load the metadata module by path
# instead, effectively side-stepping the dependency problem. Please make sure
# metadata has no dependencies, otherwise they will need to be added to
# the setup_requires keyword.
metadata = imp.load_source(
    'metadata', 'shiftmanager/metadata.py')

setup(
    name=metadata.package,
    version=metadata.version,
    author=metadata.authors[0],
    author_email=metadata.emails[0],
    maintainer=metadata.authors[0],
    maintainer_email=metadata.emails[0],
    url=metadata.url,
    description=metadata.description,
    #long_description=read('README.rst'),
    # Find a list of classifiers here:
    # <http://pypi.python.org/pypi?%3Aaction=list_classifiers>
    classifiers=[
        'Development Status :: 4 - Beta',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 2.6',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3.3',
        'Programming Language :: Python :: Implementation :: PyPy',
    ],
    packages=find_packages(),
    package_data={'shiftmanager': ['*.json']},
    install_requires=[
        "psycopg2>=2.5.4",
        "sqlalchemy-redshift>=0.3.0",
        "sqlalchemy-views>=0.2",
    ]
)
