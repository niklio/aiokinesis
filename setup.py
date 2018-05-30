import os
import re
from setuptools import setup


def read(f):
    return open(os.path.join(os.path.dirname(__file__), f)).read().strip()


def read_version():
    regexp = re.compile(r"^__version__\W*=\W*'([\d.abrcdev]+)'")
    init_py = os.path.join(
        os.path.dirname(__file__),
        'aiokinesis',
        '__init__.py'
    )
    with open(init_py) as f:
        for line in f:
            match = regexp.match(line)
            if match is not None:
                return match.group(1)
        else:
            raise RuntimeError('Cannot find version in aiokinesis/__init__.py')


install_req = list(filter(
    None,
    map(str.strip, read('requirements/production.txt').split('\n'))
))

classifiers = [
    'License :: OSI Approved :: MIT License',
    'Intended Audience :: Developers',
    'Programming Language :: Python :: 3',
    'Programming Language :: Python :: 3.3',
    'Programming Language :: Python :: 3.4',
    'Programming Language :: Python :: 3.5',
    'Programming Language :: Python :: 3.6',
    'Operating System :: OS Independent',
    'Topic :: System :: Networking',
    'Topic :: System :: Distributed Computing',
    'Framework :: AsyncIO',
    'Development Status :: 4 - Beta',
]

setup(
    name='aiokinesis',
    description='Asyncio kinesis client',
    long_description=read('README.md'),
    classifiers=classifiers,
    author='Nik Liolios',
    author_email='nik@asktetra.com',
    version=read_version(),
    packages=['aiokinesis'],
    install_requires=install_req,
)
