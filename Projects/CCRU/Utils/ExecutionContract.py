
import os
import json
import pandas as pd
import argparse

from Trax.Utils.Logging.Logger import Log
# from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Storage.Factory import StorageFactory

from Projects.CCRU.Utils.TopSKU import CCRUTopSKUAssortment


__author__ = 'Nimrod'

BUCKET = 'traxuscalc'
CLOUD_BASE_PATH = 'CCRU/KPIData/Contract/'
TEMPLATES_TEMP_PATH = os.getcwd()


class CCRUContract:

    def __init__(self, rds_conn=None):
        self.static_data_extractor = CCRUTopSKUAssortment(rds_conn=rds_conn)
        self.cloud_path = CLOUD_BASE_PATH
        self.temp_path = os.path.join(TEMPLATES_TEMP_PATH, 'TempFile')
        self.invalid_stores = []

    def __del__(self):
        if os.path.exists(self.temp_path):
            os.remove(self.temp_path)

    @property
    def amz_conn(self):
        if not hasattr(self, '_amz_conn'):
            self._amz_conn = StorageFactory.get_connector(BUCKET)
        return self._amz_conn

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

    @staticmethod
    def parse_template(file_path, rows_to_skip):
        """
        This function is taking care of cases that the template has different number of rows to skip.
        It tries to skip one row and than 2 rows in order to read the data in both cases.
        :return: A dataframe with the relevant data
        """
        data = pd.read_excel(file_path, skiprows=rows_to_skip-1).fillna('')
        if 'Start Date' not in data.columns:
            new_columns = data.values[0]
            data = data[1:]
            data.columns = new_columns
        data['Start Date'] = data['Start Date'].astype(str)
        data['End Date'] = data['End Date'].astype(str)
        return data

    def parse_and_upload_file(self, skiprows=2):
        parsed_args = self.parse_arguments()
        file_path = parsed_args.file
        kpi_weights = self.get_kpi_weights(file_path, kpi_row=skiprows, weight_row=skiprows-1)
        raw_data = self.parse_template(file_path, skiprows)
        if self.static_data_extractor.STORE_NUMBER not in raw_data.columns:
            Log.warning('File must '
                        'contain a {} header'.format(self.static_data_extractor.STORE_NUMBER))
        data_per_store = {}
        for x, row in raw_data.iterrows():
            store_number = row[self.static_data_extractor.STORE_NUMBER]
            store_id = self.static_data_extractor.get_store_fk(store_number)
            if store_id is None:
                Log.warning('Store number {} does not exist'.format(store_number))
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
            with open(self.temp_path, 'wb') as f:
                f.write(json.dumps(data_per_store[store_id]))
            self.amz_conn.save_file(self.cloud_path, str(store_id), self.temp_path)
            Log.info('File for store {} was uploaded {}/{}'.format(store_id, x+1, len(data_per_store)))
        if os.path.exists(self.temp_path):
            os.remove(self.temp_path)
        Log.warning('The following stores were not invalid: {}'.format(self.invalid_stores))

    @staticmethod
    def get_kpi_weights(file_path, kpi_row, weight_row):
        conversion = zip(list(pd.read_excel(file_path, header=kpi_row).columns)[3:],
                         list(pd.read_excel(file_path, skipcols=3).iloc[weight_row-1].values))
        conversion = {x[0]: x[1] for x in conversion}
        return conversion

    @staticmethod
    def parse_arguments():
        """
        This function gets the arguments from the command line / configuration in case of a local run and manage them.
        :return:
        """
        parser = argparse.ArgumentParser(description='Execution Contract CCRU')
        parser.add_argument('--env', '-e', type=str, help='The environment - dev/int/prod')
        parser.add_argument('--file', type=str, required=True, help='The assortment template')
        return parser.parse_args()


if __name__ == '__main__':
    # LoggerInitializer.init('ccru')
    Log.init('ccru', 'Execution Contract')
    Config.init()
    CCRUContract().parse_and_upload_file()
