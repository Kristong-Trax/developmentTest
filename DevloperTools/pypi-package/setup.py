__author__ = 'yoava'

from setuptools import setup, find_packages

setup(
  name='Trax',
  version='0.2',
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
