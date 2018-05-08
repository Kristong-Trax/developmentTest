
import os
import json
import pandas as pd
from datetime import datetime, timedelta

from Trax.Aws.S3Connector import BucketConnector

from Projects.DIAGEOIE.Utils.GeneralToolBox import DIAGEOIEDIAGEOIEGENERALToolBox
from Projects.DIAGEOIE.Utils.ParseTemplates import parse_template


__author__ = 'Nimrod'

BUCKET = 'traxuscalc'

UPDATED_DATE_FILE = 'LastUpdated'
UPDATED_DATE_FORMAT = '%Y-%m-%d'
CLOUD_FOLDER_NAME_FORMAT = '%y%m%d'
TEMPLATES_TEMP_PATH = os.getcwd()


class DIAGEOIEDIAGEOIETemplateAsCache:

    JSON = 'Json'
    DATA_FRAME = 'DataFrame'
    EXCEL = 'Excel'

    def __init__(self, project, templates_path_in_cloud='KPITemplates', template_type=JSON,
                 cache_path_in_project_directory='CacheData', time_difference_from_utc=0):
        self.project = project.upper().replace('-', '_')
        self.cloud_path = os.path.join(self.project, templates_path_in_cloud)
        self.cache_path = os.path.join(self.get_projects_path(), self.project, cache_path_in_project_directory)
        self.template_type = template_type
        self.time_difference = time_difference_from_utc

    @property
    def amz_conn(self):
        if not hasattr(self, '_amz_conn'):
            self._amz_conn = BucketConnector(BUCKET)
        return self._amz_conn

    @staticmethod
    def get_projects_path():
        current_path = os.path.dirname(os.path.realpath(__file__))
        index = 0
        for index, name in enumerate(current_path.split('/')):
            if name in ('Deployment', 'KPIUtils', 'Projects'):
                break
        projects_path = os.path.join('/'.join(current_path.split('/')[:index]), 'Projects')
        return projects_path

    def convert_excel_file(self, file_path):
        if self.template_type == self.DATA_FRAME:
            data = parse_template(file_path).to_json()
        elif self.template_type == self.JSON:
            data = DIAGEOIEDIAGEOIEGENERALToolBox.get_json_data(file_path)
        else:
            with open(file_path, 'rb') as f:
                data = f.read()
        return data

    def upload_templates(self, templates_path, templates_names, align_with_existing=True, immediate_change=False):
        new_folder_name = datetime.utcnow().date()
        if not immediate_change:
            new_folder_name += timedelta(1)
        new_folder_name = new_folder_name.strftime(CLOUD_FOLDER_NAME_FORMAT)
        new_folder_path = os.path.join(self.cloud_path, new_folder_name)
        if align_with_existing:
            existing_templates = self.get_latest_templates()
        else:
            existing_templates = {}

        for name in templates_names:
            data = self.convert_excel_file(os.path.join(templates_path, name))
            temp_file_path = os.path.join(TEMPLATES_TEMP_PATH, '{}_temp'.format(name))
            with open(temp_file_path, 'wb') as f:
                f.write(data)
            self.amz_conn.save_file(new_folder_path, name, temp_file_path)
            existing_templates.pop(name, None)
            os.remove(temp_file_path)
        for name in existing_templates.keys():
            temp_file_path = os.path.join(TEMPLATES_TEMP_PATH, '{}_temp'.format(name))
            with open(temp_file_path, 'wb') as f:
                self.amz_conn.download_file(existing_templates[name], f)
            self.amz_conn.save_file(new_folder_path, name, temp_file_path)
            os.remove(temp_file_path)

    def get_latest_templates(self):
        """
        This function returns the paths of the latest documented templates in the Cloud.
        """
        latest_templates = {}
        latest_date = self.get_latest_directory_date_from_cloud()
        if latest_date:
            for file_path in [f.key for f in self.amz_conn.bucket.list(os.path.join(self.cloud_path, latest_date))]:
                file_name = file_path.split('/')[-1]
                latest_templates[file_name] = file_path
        return latest_templates

    def get_latest_directory_date_from_cloud(self):
        """
        This function reads all files from a given path (in the Cloud), and extracts the dates of their mother dirs
        by their name. Later it returns the latest date (up to today).
        """
        files = self.amz_conn.bucket.list(self.cloud_path)
        files = [f.key.replace(self.cloud_path, '') for f in files]
        files = [f for f in files if len(f.split('/')) > 1]
        files = [f.split('/')[0] for f in files]
        files = [f for f in files if f.isdigit()]
        if files:
            dates = [datetime.strptime(f, CLOUD_FOLDER_NAME_FORMAT) for f in files]
            for date in sorted(dates, reverse=True):
                if date.date() <= datetime.utcnow().date():
                    return date.strftime(CLOUD_FOLDER_NAME_FORMAT)
        return None

    def update_templates(self):
        """
        This function checks whether the recent templates are updated.
        If they're not, it downloads them from the Cloud and saves them in a local path.
        """
        if os.path.exists(self.cache_path):
            files_list = os.listdir(self.cache_path)
            if files_list and UPDATED_DATE_FILE in files_list:
                with open(os.path.join(self.cache_path, UPDATED_DATE_FILE), 'rb') as f:
                    date = datetime.strptime(f.read(), UPDATED_DATE_FORMAT)
                current_time = datetime.utcnow() + timedelta(hours=self.time_difference)
                if date.date() > current_time.date():
                    self.save_latest_templates()
            else:
                self.save_latest_templates()
        else:
            self.save_latest_templates()

    def save_latest_templates(self):
        """
        This function reads the latest templates from the Cloud, and saves them in a local path.
        """
        if not os.path.exists(self.cache_path):
            os.makedirs(self.cache_path)
        latest_date = self.get_latest_directory_date_from_cloud()
        files = [f.key for f in self.amz_conn.bucket.list(os.path.join(self.cloud_path, latest_date))]
        for file_path in files:
            file_name = file_path.split('/')[-1]
            with open(os.path.join(self.cache_path, file_name), 'wb') as f:
                self.amz_conn.download_file(file_path, f)
        with open(os.path.join(self.cache_path, UPDATED_DATE_FILE), 'wb') as f:
            current_time = datetime.utcnow() + timedelta(hours=self.time_difference)
            f.write(current_time.strftime(UPDATED_DATE_FORMAT))

    def get_template(self, name):
        """
        This function receives a KPI set name and return its relevant template as a JSON.
        """
        template_path = os.path.join(self.cache_path, name)
        if self.template_type == self.EXCEL:
            data = template_path
        else:
            with open(template_path, 'rb') as f:
                if self.template_type == self.DATA_FRAME:
                    data = pd.read_json(f)
                else:
                    data = json.load(f)
        return data
