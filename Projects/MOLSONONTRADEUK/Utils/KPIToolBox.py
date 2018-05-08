
import pandas as pd
import numpy as np
from datetime import datetime

from Trax.Algo.Calculations.Core.DataProvider import Data
from Trax.Utils.Conf.Keys import DbUsers
from Trax.Data.Projects.Connector import ProjectConnector
from Trax.Utils.Logging.Logger import Log
from Trax.Data.Utils.MySQLservices import get_table_insertion_query as insert

from Projects.MOLSONONTRADEUK.Utils.Fetcher import MOLSONONTRADEUKQueries
from Projects.MOLSONONTRADEUK.Utils.GeneralToolBox import MOLSONONTRADEUKGENERALToolBox

__author__ = 'nissand'

KPI_NEW_TABLE = 'report.kpi_level_2_results'


def log_runtime(description, log_start=False):
    def decorator(func):
        def wrapper(*args, **kwargs):
            calc_start_time = datetime.utcnow()
            if log_start:
                Log.info('{} started at {}'.format(description, calc_start_time))
            result = func(*args, **kwargs)
            calc_end_time = datetime.utcnow()
            Log.info('{} took {}'.format(description, calc_end_time - calc_start_time))
            return result
        return wrapper
    return decorator


class MOLSONONTRADEUKToolBox:
    LEVEL1 = 1
    LEVEL2 = 2
    LEVEL3 = 3

    def __init__(self, data_provider, output):
        self.output = output
        self.data_provider = data_provider
        self.project_name = self.data_provider.project_name
        self.session_uid = self.data_provider.session_uid
        self.session_id = self.data_provider.session_id
        self.products = self.data_provider[Data.PRODUCTS]
        self.all_products = self.data_provider[Data.ALL_PRODUCTS]
        self.match_product_in_scene = self.data_provider[Data.MATCHES]
        self.visit_date = self.data_provider[Data.VISIT_DATE]
        self.session_info = self.data_provider[Data.SESSION_INFO]
        self.scene_info = self.data_provider[Data.SCENES_INFO]
        self.store_id = self.data_provider[Data.STORE_FK]
        self.scif = self.data_provider[Data.SCENE_ITEM_FACTS]
        self.rds_conn = ProjectConnector(self.project_name, DbUsers.CalculationEng)
        self.tools = MOLSONONTRADEUKGENERALToolBox(self.data_provider, self.output, rds_conn=self.rds_conn)
        self.tap_type_data = self.get_tap_type_per_session_data()
        self.New_kpi_static_data = self.get_new_kpi_static_data()
        self.kpi_results_queries = []

    def get_new_kpi_static_data(self):
        """
            This function extracts the static new KPI data (new tables) and saves it into one global data frame.
            The data is taken from static.kpi_level_2.
            """
        query = MOLSONONTRADEUKQueries.get_new_kpi_data()
        kpi_static_data = pd.read_sql_query(query, self.rds_conn.db)
        return kpi_static_data

    def get_tap_type_per_session_data(self):
        """
            This function extracts the static new KPI data (new tables) and saves it into one global data frame.
            The data is taken from static.kpi_level_2.
            """
        query = MOLSONONTRADEUKQueries.get_tap_type_data(self.session_uid)
        tap_type_data = pd.read_sql_query(query, self.rds_conn.db)
        return tap_type_data

    def sum_of_facings(self, products):
        facing_sum = {}
        for index, row in products.iterrows():
            pks = products[products['product_fk'] == row['product_fk']]['match_product_in_probe_fk'].values
            facing_sum[row['product_fk']] = len(self.match_product_in_scene[self.match_product_in_scene['probe_match_fk'].isin(pks)].values)
        return facing_sum

    def calculate_availability_kpi(self, *args, **kwargs):
        """
        This function calculates the KPI results.
        """
        results = {}
        plaque_products = self.scif[(self.scif['att1'] == 'Plaque')]['product_fk'].unique()
        t_bar_products = self.tap_type_data[(self.tap_type_data['match_product_in_probe_state_fk'] == 3) & (
            self.tap_type_data['product_fk'].isin(plaque_products))][['match_product_in_probe_fk', 'product_fk']]
        results['T Bar'] = self.sum_of_facings(t_bar_products)

        stand_alone_products = self.tap_type_data[(self.tap_type_data['match_product_in_probe_state_fk'] != 3) & (
            self.tap_type_data['product_fk'].isin(plaque_products))][['match_product_in_probe_fk', 'product_fk']]
        results['Stand Alone'] = self.sum_of_facings(stand_alone_products)
        for kpi_type in results:
            kpi_fk_new_table = self.New_kpi_static_data[self.New_kpi_static_data['client_name'] == kpi_type]['pk'].values[0]
            for product in results[kpi_type]:
                self.write_to_db_result_new_tables(kpi_fk_new_table, numerator_id=product,
                                                   numerator_result=results[kpi_type][product],
                                                   result=results[kpi_type][product])
        return

    def calculate_placing_kpi(self, *args, **kwargs):
        """
        This function calculates the KPI results.
        """
        plaque_products = self.scif[(self.scif['att1'] == 'Plaque')]['product_fk'].unique()
        scenes = self.scif['scene_id'].unique()
        results = {
            '0%-33%': {},
            '33%-66%': {},
            '66%-100%': {},
        }
        for scene in scenes:
            products_per_scene = self.match_product_in_scene[(self.match_product_in_scene['scene_fk'] == scene) &
                                                                    (self.match_product_in_scene['product_fk'].isin(plaque_products))][
                ['product_fk', 'bay_number', 'shelf_number', 'facing_sequence_number']].sort_values(by=['bay_number', 'shelf_number', 'facing_sequence_number'])
            products_per_scene = products_per_scene['product_fk'].values
            scene_len = len(products_per_scene)
            counter = 1
            for product in products_per_scene:
                placing = np.divide(float(counter), float(scene_len))*100
                if scene_len == 1:
                    results['0%-33%'][product] = 1
                    results['33%-66%'][product] = 1
                    results['66%-100%'][product] = 1
                    counter += 1
                    continue
                if scene_len == 2:
                    if placing <= 50:
                        results['0%-33%'][product] = 1
                        results['33%-66%'][product] = 1
                    else:
                        results['33%-66%'][product] = 1
                        results['66%-100%'][product] = 1
                    counter += 1
                    continue
                if placing <= 33:
                    results['0%-33%'][product] = 1
                elif 33 < placing <= 66:
                    results['33%-66%'][product] = 1
                else:
                    results['66%-100%'][product] = 1
                counter += 1
        for kpi_type in results:
            missing_products = list(set(plaque_products) - set(results[kpi_type].keys()))
            for product in missing_products:
                results[kpi_type][product] = 0
            kpi_fk_new_table = self.New_kpi_static_data[self.New_kpi_static_data['client_name'] == kpi_type]['pk'].values[0]
            for product in results[kpi_type]:
                self.write_to_db_result_new_tables(kpi_fk_new_table, numerator_id=product,
                                                   numerator_result=results[kpi_type][product],
                                                   result=results[kpi_type][product])
        return

    def write_to_db_result_new_tables(self, fk, numerator_id, numerator_result, result, denominator_id=None,
                                      denominator_result=None, score=None):
        """
            This function creates the result data frame of new rables KPI,
            and appends the insert SQL query into the queries' list, later to be written to the DB.
            """
        table = KPI_NEW_TABLE
        attributes = self.create_attributes_dict_new_tables(fk, numerator_id, numerator_result, denominator_id,
                                                            denominator_result, result, score)
        query = insert(attributes, table)
        self.kpi_results_queries.append(query)

    def create_attributes_dict_new_tables(self, kpi_fk, numerator_id, numerator_result, denominator_id,
                                          denominator_result, result, score):
        """
        This function creates a data frame with all attributes needed for saving in KPI results new tables.
        """
        attributes = pd.DataFrame([(kpi_fk, self.session_id, numerator_id, numerator_result, denominator_id,
                                    denominator_result, result, score)], columns=['kpi_level_2_fk', 'session_fk',
                                                                                  'numerator_id',
                                                                                  'numerator_result',
                                                                                  'denominator_id',
                                                                                  'denominator_result', 'result',
                                                                                  'score'])
        return attributes.to_dict()


    @log_runtime('Saving to DB')
    def commit_results_data(self):
        """
        This function writes all KPI results to the DB, and commits the changes.
        """
        insert_queries = self.merge_insert_queries(self.kpi_results_queries)
        cur = self.rds_conn.db.cursor()
        delete_query = MOLSONONTRADEUKQueries.get_delete_session_results_query(self.session_uid, self.session_id)
        # for query in delete_queries:
        cur.execute(delete_query)
        for query in insert_queries:
            cur.execute(query)
        self.rds_conn.db.commit()

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
            merged_queries.append('{0} VALUES {1}'.format(group, ',\n'.join(query_groups[group])))
        return merged_queries
