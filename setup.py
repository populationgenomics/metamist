#!/usr/bin/env python

"""
Setup script for the Python package. Dependencies for server are listed separately
in requirements.txt, dev dependencies are listed in requirements-dev.txt.
"""

from setuptools import find_packages, setup

PKG = 'metamist'

all_packages = ['metamist']
all_packages.extend('metamist.' + p for p in sorted(find_packages('./metamist')))

with open('README.md', encoding='utf-8') as f:
    readme = f.read()


setup(
    name=PKG,
    # This tag is automatically updated by bump2version
    version='6.3.1',
    description='Python API for interacting with the Sample API system',
    long_description=readme,
    long_description_content_type='text/markdown',
    url='https://github.com/populationgenomics/metamist',
    license='MIT',
    packages=all_packages,
    install_requires=[
        'click',
        'google-auth',
        'google-api-core',  # dependency to google-auth that however is not
        # pulled automatically: https://github.com/googleapis/google-auth-library-python/blob/main/setup.py#L22-L27
        'urllib3 >= 1.25.3',
        'python-dateutil',
        'requests',
        'typing-extensions',
        # for get id-token
        'cpg-utils >= 4.9.4',
        'gql[aiohttp,requests]',
    ],
    entry_points={
        'metamist_parser': [
            'GenericMetadataParser = metamist.parser.generic_metadata_parser:GenericMetadataParser',
            'SampleFileMapParser = metamist.parser.sample_file_map_parser:SampleFileMapParser',
        ],
    },
    include_package_data=True,
    zip_safe=False,
    keywords='bioinformatics',
    classifiers=[
        'Environment :: Console',
        'Intended Audience :: Science/Research',
        'License :: OSI Approved :: MIT License',
        'Natural Language :: English',
        'Operating System :: MacOS :: MacOS X',
        'Operating System :: POSIX',
        'Operating System :: Unix',
        'Programming Language :: Python',
        'Topic :: Scientific/Engineering',
        'Topic :: Scientific/Engineering :: Bio-Informatics',
    ],
)
