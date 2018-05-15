from setuptools import setup , find_packages
setup(
  name = 'KPIUtils',
  version = '0.4',
  packages= find_packages() ,
  description = 'all the tools you need to build a kpi',
  author = 'Ilan Pinto',
  author_email = 'ilanp@traxretail.com',
  download_url = 'http://trax-cloud.com.s3-website-us-east-1.amazonaws.com/kpiutils/archive/0.1.tar.gz', # I'll explain this in a second
  # url = 'https://github.com/peterldowns/mypackage', # use the URL to the github repo
  # download_url = 'https://github.com/peterldowns/mypackage/archive/0.1.tar.gz', # I'll explain this in a second
  keywords = ['trax', 'kpi'], # arbitrary keywords
  classifiers = []
)