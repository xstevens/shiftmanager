# -*- coding: utf-8 -*-

try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

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
)
