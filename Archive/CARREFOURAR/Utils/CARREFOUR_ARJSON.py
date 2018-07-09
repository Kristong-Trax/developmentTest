# - *- coding: utf- 8 - *-
import json
import sys
from datetime import datetime
import pandas as pd
import os
import pysftp
from Trax.Utils.Logging.Logger import Log
# from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Trax.Utils.Conf.Configuration import Config
from Trax.Utils.Files.FilesServices import create_folder
from Trax.Utils.Files.GlobalResources import local_repository_path
import gzip
import shutil
sys.path.append('.')

__author__ = 'ortal'


class CARREFOUR_ARJsonGenerator:
    def __init__(self, project, visit_date, store_number, base_path):
        self.store = store_number
        self.visit_date = visit_date
        self.project = project
        self.base_path = base_path
        self.project_kpi_dict = {'project': self.project, 'author': 'urid', 'cat_targets_by_region': '', 'kpi_data': []}

    def create_json(self):

        sftp_conn = FileTransferConnector('secure-exchange.trax-cloud.com', user_name='carrefourar',
                                          password='4sdK6dZg', port=23, store_number=self.store)
        updated_file_name = sftp_conn.download_file('/incoming', self.base_path, self.store, self.visit_date)
        if updated_file_name:
            file_input = pd.read_csv(os.path.join(self.base_path, updated_file_name), index_col=False, prefix='X')
            return file_input
        else:
            return None

    # def create_targets_json(self, file_name):
    #     file_input = pd.read_excel(os.path.join(self.base_path, file_name))
    #     output = file_input.to_json(orient='records')
    #     final_json = json.loads(output)
    #     self.project_kpi_dict['cat_targets_by_region'] = final_json
    #
    #     return

class FileTransferConnector:
    def __init__(self, host, user_name, password, port=22, store_number=None):
        # if Config.get_environment() in Config.LOCAL_ENVIRONMENTS:
        #     self._connector = LocalFileTransferConnector()
        # else:
        self._connector = SFTPConnector(host, user_name, password, port)

    def zipfile(self, file_to_zip):
        return self._connector.zipfile(file_to_zip)

    def save_file(self, file_path, file_to_save):
        # type: (object, object) -> object
        self._connector.save_file(file_path, file_to_save)

    def download_file(self, source_path, dest_file, store_number, session_date):
        file_name = self._connector.download_file(source_path, dest_file, store_number, session_date=session_date)
        return file_name


class SFTPConnector:
    def __init__(self, host, user_name, password, port=22):
        cnopts = pysftp.CnOpts()
        cnopts.hostkeys = None
        self.sftp = pysftp.Connection(host, username=user_name, password=None, cnopts=cnopts, port=port,
                                      private_key=os.path.join(os.path.dirname(os.path.realpath(__file__)), 'carrefourar.pem'))
        Log.init('connected to SFTP')

    def zipfile(self, file_to_zip):
        f_in = open(file_to_zip)
        zip_file_to_save = file_to_zip + '.gz'
        f_out = gzip.open(zip_file_to_save, 'wb')
        f_out.writelines(f_in)
        f_out.close()
        f_in.close()
        return zip_file_to_save

    def save_file(self, file_path, file_to_zip):
        with self.sftp.cd(file_path):
            self.sftp.put(file_to_zip)
            Log.init('upload file success')

    def download_file(self, source_path, dest_file_path, store_number, session_date=None):
        file_list = self.sftp.listdir(source_path)
        file_name = ''
        store_files = []
        for file in file_list:
            if file.split('_')[0] == store_number:
                if session_date >= datetime.strptime(file.split('_')[1].strip('.csv'), "%y-%m-%d").date():
                    store_files.append(file)
        dates = [datetime.strptime(f.split('_')[1].strip('.csv'), "%y-%m-%d") for f in store_files]
        session_datetime = datetime(session_date.year, session_date.month, session_date.day)
        for date in sorted(dates, reverse=True):
            if session_datetime >= date:
                file_name = date.strftime("%y-%m-%d")
                break
        for f in store_files:
            if f.split('_')[1].strip('.csv') == file_name:
                file_name = f
                break
        if file_name:
            self.sftp.get(source_path + '/' + file_name, dest_file_path + file_name)
            return file_name
        else:
            Log.warning('unable to download file for store {}'.format(store_number))
            return None


class LocalFileTransferConnector:
    def __init__(self):
        env = Config.get_environment()
        self._base_file_path = os.path.join(local_repository_path(), env, 'sftp')
        create_folder(self._base_file_path)

    def save_file(self, file_path, file_to_save):
        file_local_path = self._combine_path(self._base_file_path, file_path)
        create_folder(file_local_path)
        shutil.copyfile(file_to_save, file_local_path)

    def download_file(self, source_path, dest_file):
        pass

    def _combine_path(self, file_path, file_name):
        if file_name:
            if file_name[0] == os.path.sep:
                file_name = file_name[1:]
                file_path = os.path.join(file_path, file_name)
            return file_path








