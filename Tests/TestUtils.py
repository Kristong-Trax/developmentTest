import os

import shutil
import tempfile

from Trax.Cloud.Providers.Local.Storage.StorageConnector import LocalStorageConnector
from Trax.Data.Testing.TestProjects import TestProjectsNames

__author__ = 'yoava'


def remove_cached_products():
    project_name = TestProjectsNames().TEST_PROJECT_1
    if os.path.exists(os.path.join(tempfile.gettempdir(), project_name)):
        shutil.rmtree(os.path.join(tempfile.gettempdir(), project_name))


def remove_local_storage():
    local_storage = LocalStorageConnector('fake_bucket')
    if os.path.exists(os.path.join(local_storage.bucket, TestProjectsNames().TEST_PROJECT_1)):
        shutil.rmtree(os.path.join(local_storage.bucket, TestProjectsNames().TEST_PROJECT_1))


def remove_cache_and_storage():
    remove_cached_products()
    remove_local_storage()
