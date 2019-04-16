import os
import json
import argparse
import datetime as dt
import pandas as pd

from Trax.Utils.Logging.Logger import Log
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Storage.Factory import StorageFactory
from Trax.Cloud.Services.Connector.Keys import DbUsers
from KPIUtils_v2.DB.PsProjectConnector import PSProjectConnector


__author__ = 'Nimrod'


PROJECT = 'ccru'
BUCKET = 'traxuscalc'
CLOUD_BASE_PATH = 'CCRU/KPIData/Contract/'
TEMPLATES_TEMP_PATH = os.getcwd()
TARGETS_SHEET_NAME = 'targets'


class CCRUContract:
    STORE_NUMBER = 'Store Number'
    START_DATE = 'Start Date'
    END_DATE = 'End Date'

    def __init__(self, rds_conn=None):
        self.cloud_path = CLOUD_BASE_PATH
        self.temp_path = os.path.join(TEMPLATES_TEMP_PATH, 'TempFile')
        self.stores = {}
        self.stores_processed = []
        self.invalid_stores = []
        self.stores_with_invalid_dates = []
        self.stores_with_invalid_targets = []

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
            self._rds_conn = PSProjectConnector(PROJECT, DbUsers.CalculationEng)
        try:
            pd.read_sql_query('select pk from probedata.session limit 1', self._rds_conn.db)
        except Exception as e:
            self._rds_conn.disconnect_rds()
            self._rds_conn = PSProjectConnector(PROJECT, DbUsers.CalculationEng)
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

        Log.info("Starting template parsing and validation")
        parsed_args = self.parse_arguments()
        file_path = parsed_args.file

        kpi_weights = zip(list(pd.read_excel(file_path, header=2,
                                             sheetname=TARGETS_SHEET_NAME, sheet_name=TARGETS_SHEET_NAME).columns)[3:],
                          list(pd.read_excel(file_path, skipcols=3,
                                             sheetname=TARGETS_SHEET_NAME, sheet_name=TARGETS_SHEET_NAME).iloc[0].values))
        kpi_weights = {x[0]: x[1] for x in kpi_weights}

        raw_data = pd.read_excel(file_path, skiprows=2,
                                 sheetname=TARGETS_SHEET_NAME, sheet_name=TARGETS_SHEET_NAME).fillna('')
        if self.STORE_NUMBER not in raw_data.columns:
            Log.error('File must contain a {} column header'.format(self.STORE_NUMBER))
            return
        if self.START_DATE not in raw_data.columns:
            Log.error('File must contain a {} column header'.format(self.START_DATE))
            return
        if self.END_DATE not in raw_data.columns:
            Log.error('File must contain a {} column header'.format(self.END_DATE))
            return
        raw_data[self.STORE_NUMBER] = raw_data[self.STORE_NUMBER].astype(str)
        raw_data[self.START_DATE] = raw_data[self.START_DATE].astype(str)
        raw_data[self.END_DATE] = raw_data[self.END_DATE].astype(str)

        Log.info("Starting Stores validation")
        target_data_new = {}
        count_stores_total = raw_data.shape[0]
        for x, row in raw_data.iterrows():

            store_number = row[self.STORE_NUMBER]
            store_id = self.get_store_fk(store_number)
            if store_id is None:

                self.invalid_stores.append(store_number)

            else:

                if store_id not in target_data_new.keys():
                    target_data_new[store_id] = []
                row = row.to_dict()
                row_to_append = {
                    self.STORE_NUMBER: row[self.STORE_NUMBER],
                    self.START_DATE: row[self.START_DATE],
                    self.END_DATE: row[self.END_DATE]
                }
                for key in row.keys():
                    if key in kpi_weights:
                        row_to_append[str(key)] = [row[key], kpi_weights[key]]
                target_data_new[store_id].append(row_to_append)

            count_stores_processed = x + 1
            if count_stores_processed % 1000 == 0 or count_stores_processed == count_stores_total:
                Log.info("Number of stores validated: {}/{}".format(count_stores_processed, count_stores_total))

        if self.invalid_stores:
            Log.warning("The following stores do not exist in the DB and will be ignored ({}): "
                        "{}".format(len(self.invalid_stores), self.invalid_stores))

        Log.info("Starting data processing")
        count_stores_total = len(target_data_new.keys())
        for x, store_id in enumerate(target_data_new.keys()):

            data_new = target_data_new[store_id][0]
            start_date_new = dt.datetime.strptime(data_new[self.START_DATE], '%Y-%m-%d').date()
            end_date_new = dt.datetime.strptime(data_new[self.END_DATE], '%Y-%m-%d').date()
            store_number = data_new[self.STORE_NUMBER]

            if not start_date_new <= end_date_new:

                self.stores_with_invalid_dates += [store_number]

            else:

                target_data = []
                target_data_cur = self.get_json_file_content(str(store_id))
                for data_cur in target_data_cur:
                    try:
                        start_date_cur = dt.datetime.strptime(
                            data_cur[self.START_DATE], '%Y-%m-%d').date()
                        end_date_cur = dt.datetime.strptime(
                            data_cur[self.END_DATE], '%Y-%m-%d').date()
                        store_number_cur = data_cur[self.STORE_NUMBER]
                    except:
                        self.stores_with_invalid_targets += [store_number]
                        continue
                    if store_number_cur == store_number \
                            and start_date_cur <= end_date_new \
                            and end_date_cur >= start_date_new:
                        details_new = data_new.copy()
                        del details_new[self.START_DATE]
                        del details_new[self.END_DATE]
                        details_cur = data_cur.copy()
                        del details_cur[self.START_DATE]
                        del details_cur[self.END_DATE]
                        if details_cur == details_new:
                            data_new[self.START_DATE] = str(
                                start_date_cur) if start_date_cur <= start_date_new else str(start_date_new)
                        else:
                            end_date_cur = start_date_new - dt.timedelta(days=1)
                            if start_date_cur <= end_date_cur:
                                data_cur[self.END_DATE] = str(end_date_cur)
                                target_data += [data_cur]
                    else:
                        target_data += [data_cur]
                target_data += [data_new]

                try:
                    with open(self.temp_path, 'wb') as f:
                        f.write(json.dumps(target_data))
                    self.amz_conn.save_file(self.cloud_path, str(store_id), self.temp_path)
                except Exception as e:
                    Log.error("Store Seq/ID/Number: {}/{}/{}. Error: {}".format(x, store_id, store_number, e))
                    Log.error("target_data: {}".format(target_data))

            count_stores_processed = x + 1
            self.stores_processed += [store_number]
            if count_stores_processed % 1000 == 0 or count_stores_processed == count_stores_total:
                Log.info("Number of stores processed: {}/{}".format(count_stores_processed, count_stores_total))
                # Log.debug("Stores processed: {}".format(self.stores_processed))
                self.stores_processed = []

        if os.path.exists(self.temp_path):
            os.remove(self.temp_path)

        if self.invalid_stores:
            Log.warning("The following stores do not exist in the DB and were ignored ({}): "
                        "{}".format(len(self.invalid_stores), self.invalid_stores))

        if self.stores_with_invalid_dates:
            Log.warning("The following stores have invalid date period and were ignored ({}): "
                        "{}".format(len(self.stores_with_invalid_dates), self.stores_with_invalid_dates))

        if self.stores_with_invalid_targets:
            Log.warning("The following stores have invalid target format and were ignored ({}): "
                        "{}".format(len(self.stores_with_invalid_dates), self.stores_with_invalid_dates))

        Log.info("Execution targets are uploaded successfully. " +
                  ("Incorrect template data were ignored (see above)" if self.invalid_stores or self.stores_with_invalid_dates or self.stores_with_invalid_targets else ""))

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
    Log.init(PROJECT, 'CCRU Contract Execution targets upload')
    Config.init()
    CCRUContract().parse_and_upload_file()
