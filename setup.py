from distribute_setup import use_setuptools
use_setuptools()

from setuptools import setup
setup(
    name='poqa',
    version='0.8',
    packages=['poqa',],
    license='BSD',
    long_description=open('README.rst').read(),
)
