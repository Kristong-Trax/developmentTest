__author__ = 'yoava'

from setuptools import setup, find_packages
from pkg_resources import get_distribution


def get_version(package_name):
    ve = get_distribution(package_name).version
    return str(float(ve) + 0.1)


setup(
  name='Trax',
  version=get_version(package_name='Trax'),
  packages=find_packages(),
  package_data={'': ['secret_keys.conf']},
  include_package_data=True,
  description='all the dependencies in ace_factory',
  author='Yoav Amir',
  author_email='yoava@traxretail.com',
  keywords=['trax', 'deps'],
  long_description = open('README.txt').read(),
 # url = 'https://s3.console.aws.amazon.com/s3/buckets/trax-pypi/Trax'
)
