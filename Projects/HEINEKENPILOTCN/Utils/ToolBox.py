from Trax.Algo.Calculations.Core.DataProvider import Data
from Trax.Aws.S3Connector import BucketConnector
from Trax.Algo.Calculations.Core.Shortcuts import BaseCalculationsGroup
from Trax.Utils.Logging.Logger import Log
import os
from datetime import datetime
import json

CACHE_PATH = os.path.join(os.path.dirname(os.path.realpath(__file__)), '..', 'CacheData')
UPDATED_DATE_FILE = 'LastUpdated'
UPDATED_DATE_FORMAT = '%Y-%m-%d'
BUCKET = 'traxuscalc'


class HEINEKENPILOTCNHandleTemplate():

    TEMPLATES_PATH = 'Heineken_templates/'

    def __init__(self, data_provider, output, **data):
        self.k_engine = BaseCalculationsGroup(data_provider, output)
        self.data_provider = data_provider
        self.project_name = self.data_provider.project_name
        self.session_uid = self.data_provider.session_uid
        self.scif = self.data_provider[Data.SCENE_ITEM_FACTS]
        self.all_products = self.data_provider[Data.ALL_PRODUCTS]
        self.survey_response = self.data_provider[Data.SURVEY_RESPONSES]
        self.cloud_templates_path = '{}{}/{}'.format(self.TEMPLATES_PATH, self.project_name, {})
        self.local_templates_path = os.path.join(CACHE_PATH, 'templates')

    @property
    def amz_conn(self):
        if not hasattr(self, '_amz_conn'):
            self._amz_conn = BucketConnector(BUCKET)
        return self._amz_conn

    @staticmethod
    def get_latest_directory_date_from_cloud(cloud_path, amz_conn):
        """
        This function reads all files from a given path (in the Cloud), and extracts the dates of their mother dirs
        by their name. Later it returns the latest date (up to today).
        """
        files = amz_conn.bucket.list(cloud_path)
        files = [f.key.replace(cloud_path, '') for f in files]
        files = [f for f in files if len(f.split('/')) > 1]
        files = [f.split('/')[0] for f in files]
        files = [f for f in files if f.isdigit()]
        if not files:
            return
        dates = [datetime.strptime(f, '%y%m%d') for f in files]
        for date in sorted(dates, reverse=True):
            if date.date() <= datetime.utcnow().date():
                return date.strftime("%y%m%d")
        return

    def download_template(self, set_name):
        """
        This function receives a KPI set name and return its relevant template as a JSON.
        """
        with open(os.path.join(self.local_templates_path, set_name), 'rb') as f:
            json_data = json.load(f)
        return json_data

    def update_templates(self):
        """
        This function checks whether the recent templates are updated.
        If they're not, it downloads them from the Cloud and saves them in a local path.
        """
        if not os.path.exists(self.local_templates_path):
            os.makedirs(self.local_templates_path)
            self.save_latest_templates()
        else:
            files_list = os.listdir(self.local_templates_path)
            if files_list and UPDATED_DATE_FILE in files_list:
                with open(os.path.join(self.local_templates_path, UPDATED_DATE_FILE), 'rb') as f:
                    date = datetime.strptime(f.read(), UPDATED_DATE_FORMAT)
                if date.date() == datetime.utcnow().date():
                    return
                else:
                    self.save_latest_templates()
            else:
                self.save_latest_templates()

    def save_latest_templates(self):
        """
        This function reads the latest templates from the Cloud, and saves them in a local path.
        """
        if not os.path.exists(self.local_templates_path):
            os.makedirs(self.local_templates_path)
        dir_name = self.get_latest_directory_date_from_cloud(self.cloud_templates_path.format(''),
                                                             self.amz_conn)
        files = [f.key for f in self.amz_conn.bucket.list(self.cloud_templates_path.format(dir_name))]
        for file_path in files:
            file_name = file_path.split('/')[-1]
            with open(os.path.join(self.local_templates_path, file_name), 'wb') as f:
                self.amz_conn.download_file(file_path, f)
        with open(os.path.join(self.local_templates_path, UPDATED_DATE_FILE), 'wb') as f:
            f.write(datetime.utcnow().strftime(UPDATED_DATE_FORMAT))
        Log.info('Latest version of templates has been saved to cache')
