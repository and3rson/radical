#!/usr/bin/env python3.6

# coding=utf-8

from setuptools import setup
from radical.meta import VERSION

setup(
    name='radical-rpc',
    version=VERSION,
    description='Multi-transport RPC with asyncio & Django support.',
    long_description=open('README.rst', 'r').read(),
    author="Andrew Dunai",
    author_email='andrew@dun.ai',
    url='https://github.com/and3rson/radical',
    license='GPLv3',
    packages=[
        'radical',
        'radical.transports',
        'radical.management',
        'radical.management.commands',
        'radical.serialization',
        'radical.contrib',
        'radical.contrib.django',
    ],
    include_package_data=True,
    install_requires=['aioredis'],
    zip_safe=False,
    entry_points={
        'console_scripts': [
            'radical=radical.worker:main'
        ]
    },
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Environment :: Web Environment',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: GNU General Public License v3 (GPLv3)',
        'Operating System :: OS Independent',
        'Programming Language :: Python :: 3',
        'Framework :: Django',
        'Framework :: AsyncIO',
        'Topic :: Software Development :: Libraries',
    ],
    keywords='rpc,python2,python3,python,asyncio,aio,redis,django',
)
