# -*- coding: utf-8 -*-
try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

reqs = ["boto>=2.38.0",
        "botocore>=1.1.4",
        "docutils>=0.12",
        "flake8>=2.2.5",
        "funcsigs>=0.4",
        "futures>=2.2.0",
        "gdata>=2.0.18",
        "gnureadline>=6.3.3",
        "google-api-python-client>=1.4.1",
        "httplib2>=0.9.1",
        "jmespath>=0.7.1",
        "mccabe>=0.2.1",
        "mock>=1.3.0",
        "oauth2client>=1.4.12",
        "pbr>=1.3.0",
        "pep8>=1.5.7",
        "psycopg2>=2.5.4",
        "py>=1.4.30",
        "pyasn1>=0.1.8",
        "pyasn1-modules>=0.0.7",
        "pyflakes>=0.8.1",
        "pytest>=2.7.2",
        "python-dateutil>=2.4.2",
        "python-gnupg>=0.3.6",
        "rsa>=3.2",
        "simplejson>=3.8.0",
        "six>=1.9.0",
        "uritemplate>=0.6"]

setup(
    name='shiftmanager',
    version='0.0.1',
    description='Redshift Management Tools',
    author='Jeff Klukas',
    author_email='klukas@simple.com',
    classifiers=['Development Status :: 4 - Beta',
                 'Programming Language :: Python',
                 'Programming Language :: Python :: 2',
                 'Programming Language :: Python :: 3'],
    packages=['shiftmanager'],
    package_data={'shiftmanager': ['*.json']},
    install_requires=reqs
)
