#!/usr/bin/env python

try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

setup(
    name='micron',
    version='0.0.2',
    author='Chris Villa',
    author_email='chrisvilla@me.com',
    url='https://github.com/chrisvxd/micron',
    license='MIT',
    description='Microservice message library',
    classifiers=[
        'Environment :: Web Environment',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Natural Language :: English',
        'Operating System :: OS Independent',
        'Topic :: Internet :: WWW/HTTP',
    ],
    packages=[
        'micron'
    ]
)
