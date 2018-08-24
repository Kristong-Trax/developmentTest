import os
import json
import argparse
import datetime
import pandas as pd
from Trax.Cloud.Services.Connector.Keys import DbUsers
from Trax.Data.Projects.Connector import ProjectConnector
from Trax.Utils.Logging.Logger import Log
# from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Storage.Factory import StorageFactory


__author__ = 'Nimrod'


PROJECT = 'ccru-sand'
BUCKET = 'traxuscalc'
CLOUD_BASE_PATH = 'CCRU_SAND/KPIData/Contract/'
TEMPLATES_TEMP_PATH = os.getcwd()


class CCRU_SANDContract:
    STORE_NUMBER = 'Store Number'
    START_DATE = 'Start Date'
    END_DATE = 'End Date'

    def __init__(self, rds_conn=None):
        self.cloud_path = CLOUD_BASE_PATH
        self.temp_path = os.path.join(TEMPLATES_TEMP_PATH, 'TempFile')
        self.stores = {}
        self.invalid_stores = []

    def __del__(self):
        if os.path.exists(self.temp_path):
            os.remove(self.temp_path)

    @property
    def amz_conn(self):
        if not hasattr(self, '_amz_conn'):
            self._amz_conn = StorageFactory.get_connector(BUCKET)
        return self._amz_conn

    @property
    def rds_conn(self):
        if not hasattr(self, '_rds_conn'):
            self._rds_conn = ProjectConnector(PROJECT, DbUsers.CalculationEng)
        try:
            pd.read_sql_query('select pk from probedata.session limit 1', self._rds_conn.db)
        except Exception as e:
            self._rds_conn.disconnect_rds()
            self._rds_conn = ProjectConnector(PROJECT, DbUsers.CalculationEng)
        return self._rds_conn

    @property
    def store_data(self):
        if not hasattr(self, '_store_data'):
            query = "select pk as store_fk, store_number_1 as store_number from static.stores"
            self._store_data = pd.read_sql_query(query, self.rds_conn.db)
        return self._store_data

    def get_store_fk(self, store_number):
        store_number = str(store_number)
        if store_number in self.stores:
            store_fk = self.stores[store_number]
        else:
            store_fk = self.store_data[self.store_data['store_number'] == store_number]
            if not store_fk.empty:
                store_fk = store_fk['store_fk'].values[0]
                self.stores[store_number] = store_fk
            else:
                store_fk = None
        return store_fk

    def get_json_file_content(self, file_name):
        """
        This function receives a KPI set name and return its relevant template as a JSON.
        """
        cloud_path = os.path.join(CLOUD_BASE_PATH, file_name)
        with open(self.temp_path, 'wb') as f:
            try:
                self.amz_conn.download_file(cloud_path, f)
            except:
                f.write('{}')
        with open(self.temp_path, 'rb') as f:
            data = json.load(f)
        os.remove(self.temp_path)
        return data

    def parse_and_upload_file(self):
        parsed_args = self.parse_arguments()
        file_path = parsed_args.file

        kpi_weights = zip(list(pd.read_excel(file_path, header=1).columns)[3:],
                          list(pd.read_excel(file_path, skipcols=3).iloc[1].values))
        kpi_weights = {x[0]: x[1] for x in kpi_weights}

        raw_data = pd.read_excel(file_path, skiprows=2).fillna('')
        if self.STORE_NUMBER not in raw_data.columns:
            Log.error('File must contain a {} column header'.format(self.STORE_NUMBER))
            return
        if self.START_DATE not in raw_data.columns:
            Log.error('File must contain a {} column header'.format(self.START_DATE))
            return
        if self.END_DATE not in raw_data.columns:
            Log.error('File must contain a {} column header'.format(self.END_DATE))
            return
        raw_data[self.START_DATE] = raw_data[self.START_DATE].astype(str)
        raw_data[self.END_DATE] = raw_data[self.END_DATE].astype(str)

        data_per_store = {}
        store_number = None
        for x, row in raw_data.iterrows():
            store_number = row[self.STORE_NUMBER]
            store_id = self.get_store_fk(store_number)
            if store_id is None:
                Log.warning('Store number {} does not exist in the DB'.format(store_number))
                self.invalid_stores.append(store_number)
                continue
            if store_id not in data_per_store.keys():
                data_per_store[store_id] = []
            row = row.to_dict()
            for key in row.keys():
                if key in kpi_weights:
                    row[key] = (row[key], kpi_weights[key])
            data_per_store[store_id].append(row)

        for x, store_id in enumerate(data_per_store.keys()):

            target_data_raw = self.get_json_file_content(str(store_id))
            if target_data_raw:
                Log.info('Relevant Contract Execution target file for Store ID {} / Number {} is found'
                         .format(store_id, store_number))

            target_data = None
            for data in target_data_raw:
                start_date = datetime.datetime.strptime(data['Start Date'], '%Y-%m-%d').date()
                end_date = datetime.datetime.now().date() if not data['End Date'] else \
                    datetime.datetime.strptime(data['End Date'], '%Y-%m-%d').date()
                if start_date <= self.visit_date <= end_date:
                    if target_data is None or start_date >= target_data[1]:
                        target_data = (data, start_date)

            with open(self.temp_path, 'wb') as f:
                f.write(json.dumps(data_per_store[store_id]))
            self.amz_conn.save_file(self.cloud_path, str(store_id), self.temp_path)
            Log.info('File for Store ID {} was uploaded {}/{}'.format(store_id, x+1, len(data_per_store)))

        if os.path.exists(self.temp_path):
            os.remove(self.temp_path)

        if not self.invalid_stores:
            Log.warning('The following Store numbers are not invalid: {}'.format(self.invalid_stores))

    @staticmethod
    def parse_arguments():
        """
        This function gets the arguments from the command line / configuration in case of a local run and manages them.
        To run it locally just copy: -e prod --file **your file path** to the configuration parameters
        :return:
        """
        parser = argparse.ArgumentParser(description='Execution Contract')
        parser.add_argument('--env', '-e', type=str, help='The environment - dev/int/prod')
        parser.add_argument('--file', type=str, required=True, help='The targets template')
        return parser.parse_args()


if __name__ == '__main__':
    # LoggerInitializer.init(PROJECT)
    Log.init(PROJECT, 'CCRU_SAND Execution Contract targets upload')
    Config.init()
    CCRU_SANDContract().parse_and_upload_file()
