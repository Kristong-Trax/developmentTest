from datetime import datetime, timedelta
import pandas as pd
import numpy as np
from KPIUtils_v2.GlobalDataProvider.PSAssortmentProvider import PSAssortmentDataProvider

from Trax.Algo.Calculations.Core.DataProvider import Data
from Trax.Utils.Conf.Keys import DbUsers
from KPIUtils_v2.DB.PsProjectConnector import PSProjectConnector
from Trax.Utils.Logging.Logger import Log
from Trax.Data.Utils.MySQLservices import get_table_insertion_query as insert
from KPIUtils_v2.Calculations.AssortmentCalculations import Assortment
from Projects.RISPARKWINEDE_SAND.Utils.Fetcher import RISPARKWINEDEQueries
# from Projects.RISPARKWINEDE_SAND.Utils.GeneralToolBox import RISPARKWINEDEGENERALToolBox
from KPIUtils_v2.DB.CommonV2 import Common
from KPIUtils_v2.Utils.Consts.DB import StaticKpis
from Projects.RISPARKWINEDE_SAND.Utils.LocalConsts import Consts as LocalConst
from KPIUtils_v2.Utils.Consts.DataProvider import ScifConsts


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


class RISPARKWINEDEToolBox:
    LEVEL1 = 1
    LEVEL2 = 2
    LEVEL3 = 3
    TIME_DELTA = 90
    # OOS_SKU_KPI = 'OOS-SKU'
    # OOS_KPI = 'OOS'
    # DIST = 'Distribution'

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
        self.rds_conn = PSProjectConnector(
            self.project_name, DbUsers.CalculationEng)
        # self.tools = RISPARKWINEDEGENERALToolBox(
        #     self.data_provider, self.output, rds_conn=self.rds_conn)
        # self.New_kpi_static_data = self.get_new_kpi_static_data()
        self.kpi_results_new_tables_queries = []
        self.store_assortment = PSAssortmentDataProvider(self.data_provider).execute()
        self.store_info = self.data_provider[Data.STORE_INFO]
        self.current_date = datetime.now()
        self.assortment = Assortment(self.data_provider, self.output)
        self.common = Common(self.data_provider)

    def main_calculation(self):
        lvl3_result = self.assortment.calculate_lvl3_assortment()
        if not lvl3_result.empty:
            wine_kpi_fk = self.common.get_kpi_fk_by_kpi_type(LocalConst.WINE_AVAILABILITY)
            wine_assortment_res = lvl3_result[lvl3_result['kpi_fk_lvl2'] == wine_kpi_fk]
            self.wine_assortment_calculation(lvl3_result=wine_assortment_res)

            # existing assortment
            self.main_assortment_calculation(lvl3_result=lvl3_result)
        self.common.commit_results_data()

    def wine_assortment_calculation(self, lvl3_result):
        if not lvl3_result.empty:
            lvl3_result['in_store'] = 0
            filtered_scif = self.scif[self.scif[ScifConsts.TEMPLATE_NAME].isin(LocalConst.WINE_SHELVES)]
            products_in_session = filtered_scif.loc[filtered_scif[ScifConsts.FACINGS] > 0][ScifConsts.PRODUCT_FK].values
            lvl3_result.loc[lvl3_result[ScifConsts.PRODUCT_FK].isin(products_in_session), 'in_store'] = 1

            products_facings = filtered_scif.groupby([ScifConsts.PRODUCT_FK], as_index=False).agg({ScifConsts.FACINGS:
                                                                                                                np.sum})
            lvl3_result = lvl3_result.merge(products_facings, on=ScifConsts.PRODUCT_FK, how='left')
            for result in lvl3_result.itertuples():
                score = result.in_store * 100
                self.common.write_to_db_result(fk=result.kpi_fk_lvl3, numerator_id=result.product_fk,
                                               numerator_result=result.in_store, result=score,
                                               denominator_id=result.assortment_fk, denominator_result=1,
                                               score=score)

    # def get_new_kpi_static_data(self):
    #     """
    #         This function extracts the static new KPI data (new tables) and saves it into one global data frame.
    #         The data is taken from static.kpi_level_2.
    #         """
    #     query = RISPARKWINEDEQueries.get_new_kpi_data()
    #     kpi_static_data = pd.read_sql_query(query, self.rds_conn.db)
    #     return kpi_static_data

    def main_assortment_calculation(self, lvl3_result):
        """
        This function calculates the KPI results.
        """
        # lvl3_result = self.assortment.calculate_lvl3_assortment()
        if not lvl3_result.empty:
            for result in lvl3_result.itertuples():
                # start_new_date = datetime(2018, 2, 26) - timedelta(self.TIME_DELTA)
                start_new_date = self.visit_date - timedelta(self.TIME_DELTA)
                is_new = 0
                # ass_start_date = datetime(2017, 2, 9)
                ass_start_date = self.store_assortment[(self.store_assortment['product_fk'] == result.product_fk) &
                                                       (self.store_assortment['assortment_fk'] == result.assortment_fk) &
                                                       (self.store_assortment['assortment_group_fk'] ==
                                                        result.assortment_group_fk)]['start_date'].values[0]
                if np.datetime64(start_new_date) <= ass_start_date:
                    is_new = 1
                score = result.in_store * 100
                self.common.write_to_db_result(fk=result.kpi_fk_lvl3, numerator_id=result.product_fk,
                                               numerator_result=result.in_store, result=score,
                                               denominator_id=result.assortment_fk, denominator_result=1,
                                               score=score, score_after_actions=is_new)
                # self.write_to_db_result_new_tables(fk=result.kpi_fk_lvl3, numerator_id=result.product_fk,
                #                                    numerator_result=result.in_store, result=score,
                #                                    denominator_id=result.assortment_fk, denominator_result=1,
                #                                    score=score, score_after_actions=is_new)
                kpi_fk = self.common.get_kpi_fk_by_kpi_name(LocalConst.OOS_SKU_KPI)
                parent_kpi_fk = self.common.get_kpi_fk_by_kpi_name(LocalConst.OOS_KPI)
                is_oos = 1
                if result.in_store:
                    is_oos = 0
                self.common.write_to_db_result(fk=kpi_fk, numerator_id=result.product_fk, numerator_result=is_oos,
                                               result=is_oos, denominator_id=parent_kpi_fk)
                # self.write_to_db_result_new_tables(fk=kpi_fk, numerator_id=result.product_fk, numerator_result=is_oos,
                #                                    result=is_oos, denominator_id=parent_kpi_fk)
            lvl2_result = self.assortment.calculate_lvl2_assortment(lvl3_result)
            for result in lvl2_result.itertuples():
                denominator_res = result.total
                if not pd.isnull(result.target) and not pd.isnull(result.group_target_date) and result.group_target_date <= self.visit_date:
                    denominator_res = result.target
                res = np.divide(float(result.passes), float(denominator_res)) * 100
                if res >= 100:
                    score = 100
                else:
                    score = 0
                self.common.write_to_db_result(fk=result.kpi_fk_lvl2, numerator_id=result.assortment_fk,
                                               numerator_result=result.passes, result=res,
                                               denominator_id=result.assortment_group_fk,
                                               denominator_result=denominator_res, score=score)
                # self.write_to_db_result_new_tables(fk=result.kpi_fk_lvl2, numerator_id=result.assortment_fk,
                #                                    numerator_result=result.passes, result=res,
                #                                    denominator_id=result.assortment_group_fk,
                #                                    denominator_result=denominator_res, score=score)
            oos_kpi_fk = self.common.get_kpi_fk_by_kpi_name(LocalConst.OOS_KPI)
            # oos_kpi_fk = self.New_kpi_static_data[self.New_kpi_static_data['client_name']
            #                                       == self.OOS_KPI]['pk'].values[0]
            dist_kpi_fk = self.common.get_kpi_fk_by_kpi_name(LocalConst.DIST)
            # dist_kpi_fk = self.New_kpi_static_data[self.New_kpi_static_data['client_name']
            #                                        == self.DIST]['pk'].values[0]
            oos_numerator = len(lvl3_result[lvl3_result['in_store'] == 0])
            dist_numerator = len(lvl3_result[lvl3_result['in_store'] == 1])
            denominator = len(lvl3_result['in_store'])
            oos_res = np.divide(float(oos_numerator), float(denominator)) * 100
            dist_res = np.divide(float(dist_numerator), float(denominator)) * 100
            self.common.write_to_db_result(fk=oos_kpi_fk, numerator_id=oos_kpi_fk, numerator_result=oos_numerator,
                                           result=oos_res, denominator_result=denominator, score=oos_res)
            # self.write_to_db_result_new_tables(fk=oos_kpi_fk, numerator_id=oos_kpi_fk, numerator_result=oos_numerator,
            #                                    result=oos_res, denominator_result=denominator,
            #                                    score=oos_res)
            self.common.write_to_db_result(fk=dist_kpi_fk, numerator_id=dist_kpi_fk,
                                           numerator_result=dist_numerator, result=dist_res,
                                           denominator_result=denominator, score=dist_res)
            # self.write_to_db_result_new_tables(fk=dist_kpi_fk, numerator_id=dist_kpi_fk,
            #                                    numerator_result=dist_numerator, result=dist_res,
            #                                    denominator_result=denominator, score=dist_res)
        # return

    # def write_to_db_result_new_tables(self, fk, numerator_id, numerator_result, result, denominator_id=None,
    #                                   denominator_result=None, score=None, score_after_actions=None):
    #     """
    #         This function creates the result data frame of new rables KPI,
    #         and appends the insert SQL query into the queries' list, later to be written to the DB.
    #         """
    #     table = KPI_NEW_TABLE
    #     attributes = self.create_attributes_dict_new_tables(fk, numerator_id, numerator_result, denominator_id,
    #                                                         denominator_result, result, score, score_after_actions)
    #     query = insert(attributes, table)
    #     self.kpi_results_new_tables_queries.append(query)
    #
    # def create_attributes_dict_new_tables(self, kpi_fk, numerator_id, numerator_result, denominator_id,
    #                                       denominator_result, result, score, score_after_actions):
    #     """
    #     This function creates a data frame with all attributes needed for saving in KPI results new tables.
    #     """
    #     attributes = pd.DataFrame([(kpi_fk, self.session_id, numerator_id, numerator_result, denominator_id,
    #                                 denominator_result, result, score, score_after_actions)], columns=['kpi_level_2_fk', 'session_fk',
    #                                                                                                    'numerator_id', 'numerator_result',
    #                                                                                                    'denominator_id',
    #                                                                                                    'denominator_result', 'result',
    #                                                                                                    'score', 'score_after_actions'])
    #     return attributes.to_dict()
    #
    # @log_runtime('Saving to DB')
    # def commit_results_data(self):
    #     """
    #     This function writes all KPI results to the DB, and commits the changes.
    #     """
    #     insert_queries = self.merge_insert_queries(
    #         self.kpi_results_new_tables_queries)
    #     delete_query = RISPARKWINEDEQueries.get_delete_session_results_query(
    #         self.session_uid, self.session_id)
    #     Log.info('Start committing results')
    #     local_con = PSProjectConnector(self.project_name, DbUsers.CalculationEng)
    #     cur = local_con.db.cursor()
    #     # for query in delete_queries:
    #     cur.execute(delete_query)
    #     total_queries = len(insert_queries)
    #     counter = 1
    #     for query in insert_queries:
    #         Log.info('Executing query {} out of {}'.format(
    #             counter, total_queries))
    #         cur.execute(query)
    #         counter += 1
    #     local_con.db.commit()
    #     Log.info('Finish committing results')
    #
    # @staticmethod
    # def merge_insert_queries(insert_queries):
    #     query_groups = {}
    #     for query in insert_queries:
    #         static_data, inserted_data = query.split('VALUES ')
    #         if static_data not in query_groups:
    #             query_groups[static_data] = []
    #         query_groups[static_data].append(inserted_data)
    #     merged_queries = []
    #     for group in query_groups:
    #         merged_queries.append('{0} VALUES {1}'.format(
    #             group, ',\n'.join(query_groups[group])))
    #     return merged_queries