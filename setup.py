import os
from setuptools import setup, find_packages

try:
    import pypandoc
    long_description = pypandoc.convert('README.md', 'rst')
except (IOError, ImportError):
    long_description = 'Thumbor redis-tornado storage adapters'


def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()


setup(
    name="tc_redis_tornado",
    version="1.0.1",
    author="Thumbor Community",
    description=("Thumbor redis-tornado storage adapters"),
    license="MIT",
    keywords="thumbor redis tornado",
    url="https://github.com/thumbor-community/redis-tornado",
    packages=find_packages(),
    long_description=long_description,
    classifiers=[
        'Development Status :: 5 - Production/Stable',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 2.7',
    ],
    install_requires=[
        'thumbor>=5.2.0',
        'redis'
    ]
)
