import argparse
import numpy as np
import pandas as pd

from Trax.Cloud.Services.Connector.Keys import DbUsers
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Trax.Data.Utils.MySQLservices import get_table_insertion_query as insert
from Trax.Utils.Logging.Logger import Log
from KPIUtils_v2.DB.PsProjectConnector import PSProjectConnector


PROJECT = 'ccru-sand'

VISIT_PLAN_TABLE = 'pservice.planned_visits'

STORE_NUMBER = 'store'
USER_NAME = 'sales_rep base'
VISIT_DATE = 'visit_date'
PLANNED_FLAG = 'planned'
PLANNED_FLAG_LIST = ['0', '1', '0.0', '1.0']  # 1 and 0 values both for int and float formats
START_DATE = 'Start Date'
END_DATE = 'End Date'


class CCRU_SANDVisitPlan:

    def __init__(self, rds_conn=None):
        if rds_conn is not None:
            self._rds_conn = rds_conn
        self.stores = {}
        self.users = {}
        self.invalid_stores = []
        self.invalid_users = []
        self.invalid_dates = 0
        self.invalid_flags = 0
        self.delete_queries = []
        self.insert_queries = []
        self.insert_queries_count = 0
        self.merged_insert_queries = []

    @staticmethod
    def parse_arguments():
        """
        This function gets the arguments from the command line / configuration in case of a local run and manage them.
        :return:
        """
        parser = argparse.ArgumentParser(description='Visit Plan CCRU')
        parser.add_argument('--env', '-e', type=str, help='The environment - dev/int/prod')
        parser.add_argument('--file', type=str, required=True, help='The visit plan template')
        return parser.parse_args()

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

    @property
    def user_data(self):
        if not hasattr(self, '_user_data'):
            query = "select max(pk) as user_fk, login_name as user_name from static.sales_reps " \
                    "group by login_name"
            # "where delete_date is null " \  # TODO uncomment and insert into query
            self._user_data = pd.read_sql_query(query, self.rds_conn.db)
        return self._user_data

    @staticmethod
    def get_delete_query(start_date, end_date):
        query = """
                delete from {}
                where visit_date >= '{}' and visit_date <= '{}';
                """ \
            .format(VISIT_PLAN_TABLE, start_date, end_date)
        return query

    @staticmethod
    def get_insert_query(store_fk, user_fk, visit_date, planned_flag):
        attributes = pd.DataFrame([(store_fk, user_fk, visit_date, planned_flag)],
                                  columns=['store_fk', 'sales_rep_fk', 'visit_date', 'planned_flag'])
        query = insert(attributes.to_dict(), VISIT_PLAN_TABLE)
        return query

    @staticmethod
    def merge_insert_queries(insert_queries):
        query_groups = {}
        for query in insert_queries:
            static_data, inserted_data = query.split('VALUES ')
            if static_data not in query_groups:
                query_groups[static_data] = []
            query_groups[static_data].append(inserted_data)
        merged_queries = []
        for group in query_groups:
            for group_index in xrange(0, len(query_groups[group]), 10 ** 4):
                merged_queries.append('{0} VALUES {1}'.format(group, ',\n'.join(query_groups[group]
                                                                                [group_index:group_index + 10 ** 4])))
        return merged_queries

    def check_upload_query(self, start_date, end_date):
        query = """
                select count(*) from {}
                where visit_date >= '{}' and visit_date <= '{}';
                """ \
            .format(VISIT_PLAN_TABLE, start_date, end_date)
        result = pd.read_sql_query(query, self.rds_conn.db)
        return result.values[0][0]

    def upload_visit_plan_file(self):
        # parsed_args = self.parse_arguments()  # TODO uncomment
        # file_path = parsed_args.file
        file_path = "/home/sergey/Documents/CCRU/visit_plan.xlsx"

        Log.debug("Starting template parsing and validation")
        plan_data = pd.read_excel(file_path)
        plan_data[[STORE_NUMBER, USER_NAME, PLANNED_FLAG]] = plan_data[[STORE_NUMBER, USER_NAME, PLANNED_FLAG]].astype(str)
        plan_data = plan_data.drop_duplicates(subset=[STORE_NUMBER, USER_NAME, VISIT_DATE], keep='first')
        plan_data = plan_data.merge(self.store_data, how='left', left_on=STORE_NUMBER, right_on='store_number')
        plan_data = plan_data.merge(self.user_data, how='left', left_on=USER_NAME, right_on='user_name')
        plan_data = plan_data.where((pd.notnull(plan_data)), None)

        try:
            start_date = np.datetime64(plan_data[plan_data[STORE_NUMBER] == START_DATE][VISIT_DATE].values[0], 'D')
            end_date = np.datetime64(plan_data[plan_data[STORE_NUMBER] == END_DATE][VISIT_DATE].values[0], 'D')
        except:
            start_date = None
            end_date = None

        if isinstance(start_date, np.datetime64) and isinstance(start_date, np.datetime64):
            Log.warning("Uploading period: {} - {}"
                        "".format(np.datetime64(start_date, 'D'), np.datetime64(end_date, 'D')))
        else:
            Log.debug("The period Start Date and End Date are not specified properly. The template is not uploaded.")
            return

        plan_data = plan_data[~plan_data[STORE_NUMBER].isin([START_DATE, END_DATE])]
        total_rows = plan_data.shape[0]

        self.invalid_dates = plan_data[~((plan_data[VISIT_DATE] >= start_date) &
                                         (plan_data[VISIT_DATE] <= end_date))].shape[0]
        self.invalid_flags = plan_data[~plan_data[PLANNED_FLAG].isin(PLANNED_FLAG_LIST)].shape[0]
        self.invalid_stores = plan_data[plan_data['store_fk'].isnull()][STORE_NUMBER].unique().tolist()
        self.invalid_users = plan_data[plan_data['user_fk'].isnull()][USER_NAME].unique().tolist()

        plan_data = plan_data[((plan_data[VISIT_DATE] >= start_date) & (plan_data[VISIT_DATE] <= end_date))]
        plan_data = plan_data[plan_data[PLANNED_FLAG].isin(PLANNED_FLAG_LIST)]
        plan_data = plan_data[~plan_data['store_fk'].isnull()]
        plan_data = plan_data[~plan_data['user_fk'].isnull()]
        plan_data = plan_data.reset_index()

        if plan_data.empty:

            Log.warning("No relevant data to upload out of {} rows for the period of {} - {}"
                        "".format(total_rows, np.datetime64(start_date, 'D'), np.datetime64(end_date, 'D')))

        else:

            self.delete_queries.append(self.get_delete_query(start_date, end_date))

            for i, data in plan_data.iterrows():

                self.insert_queries.append(self.get_insert_query(data['store_fk'], data['user_fk'], data['visit_date'],
                                                                 data[PLANNED_FLAG]))

                if (i + 1) % 1000 == 0 or (i + 1) == plan_data.shape[0]:
                    Log.debug("Number of rows processed: {}/{}".format(i + 1, plan_data.shape[0]))

            Log.warning("Committing to DB")
            self.commit_results(self.delete_queries)
            self.commit_results(self.merge_insert_queries(self.insert_queries))

            uploaded_rows = self.check_upload_query(start_date, end_date)

            Log.warning("{} of {} rows are uploaded for the period of {} - {}"
                        "".format(uploaded_rows, total_rows, np.datetime64(start_date, 'D'), np.datetime64(end_date, 'D')))

        if self.invalid_dates:
            Log.warning("{} rows are ignored due to visit date out of the uploading period"
                        "".format(self.invalid_dates))
        if self.invalid_flags:
            Log.warning("{} rows are ignored due to incorrect planned flag in the template"
                        "".format(self.invalid_flags))
        if self.invalid_stores:
            Log.warning("The following stores do not exist in the DB and are ignored ({}): "
                        "{}".format(len(self.invalid_stores), self.invalid_stores))
        if self.invalid_users:
            Log.warning("The following users do not exist in the DB and are ignored ({}): "
                        "{}".format(len(self.invalid_users), self.invalid_users))

        return

    def commit_results(self, queries):
        batch_size = 1000
        rds_conn, cur = self.connection_ritual()
        query_num = 0
        for query in queries:
            # print query
            try:
                cur.execute(query)
            except Exception as e:
                Log.debug('DB update failed due to: {}'.format(e))
                rds_conn, cur = self.connection_ritual()
                cur.execute(query)
                continue
            if query_num > batch_size:
                query_num = 0
                rds_conn, cur = self.connection_ritual()
                rds_conn.db.commit()
            query_num += 1
        rds_conn.db.commit()
        return

    def connection_ritual(self):
        """
        This function connects to the DB and cursor
        :return: rds connection and cursor connection
        """
        self.rds_conn.disconnect_rds()
        rds_conn = PSProjectConnector(PROJECT, DbUsers.CalculationEng)
        cur = rds_conn.db.cursor()
        return rds_conn, cur


if __name__ == '__main__':
    LoggerInitializer.init('Visit Plan CCRU')
    ts = CCRU_SANDVisitPlan()
    ts.upload_visit_plan_file()
# # # To run it locally just copy: -e prod --file **your file path** to the configuration
# # # At the end of the script there are logs with all of the invalid store numbers, users and dates
