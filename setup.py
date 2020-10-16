#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""The setup script."""

from setuptools import setup, find_packages

##------------------ VERSIONING BEST PRACTICES --------------------------##
import versioneer

with open('README.rst') as readme_file:
    readme = readme_file.read()

with open('HISTORY.rst') as history_file:
    history = history_file.read()

requirements = ['Click>=6.0','networkx']

setup_requirements = ['pytest-runner>=5.0', ]

test_requirements = ['pytest>=5.0', ]

setup(
    author="Nicky Sandhu",
    author_email='psandhu@water.ca.gov',
    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Natural Language :: English',
        'Programming Language :: Python :: 3.7',
    ],
    description="For loading data from DSM2 hdf5 files into pandas DataFrame",
    entry_points={
        'console_scripts': [
            'pydsm=pydsm.cli:main'
        ],
    },
    install_requires=requirements,
    license="MIT license",
    long_description=readme + '\n\n' + history,
    include_package_data=True,
    keywords='pydsm',
    name='pydsm',
    packages=find_packages(include=['pydsm','pydsm.*']),
    setup_requires=setup_requirements,
    test_suite='tests',
    tests_require=test_requirements,
    url='https://github.com/CADWRDeltaModeling/pydsm',
    version=versioneer.get_version(),
    cmdclass=versioneer.get_cmdclass(),
    zip_safe=False,
)
