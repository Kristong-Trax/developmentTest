# -*- coding: utf-8 -*-
import datetime

import pandas as pd
import numpy as np
import ast
from Trax.Algo.Calculations.Core.Constants import Fields as Fd
from Trax.Algo.Calculations.Core.DataProvider import Data, Keys
from Trax.Algo.Calculations.Core.Shortcuts import SessionInfo, BaseCalculationsGroup
from Trax.Cloud.Services.Connector.Keys import DbUsers
from Trax.Data.Orm.OrmCore import OrmSession
from Trax.Data.Projects.ProjectConnector import AwsProjectConnector
from Trax.Data.Utils.MySQLservices import get_table_insertion_query as insert
from Trax.Utils.Logging.Logger import Log
from Projects.MARSRU_PROD.MARSRUFetcher import MARSRU_PRODMARSRUKPIFetcher
from Projects.MARSRU_PROD.Utils.MARSRUJSON import MARSRU_PRODMARSRUJsonGenerator
from Projects.MARSRU_PROD.Utils.PositionGraph import MARSRU_PRODPositionGraphs
from KPIUtils_v2.Utils.Decorators.Decorators import kpi_runtime

__author__ = 'urid'

BINARY = 'BINARY'
PROPORTIONAL = 'PROPORTIONAL'
CONDITIONAL_PROPORTIONAL = 'CONDITIONAL PROPORTIONAL'
MARS = 'Mars'
KPI_RESULT = 'report.kpi_results'
KPK_RESULT = 'report.kpk_results'
KPS_RESULT = 'report.kps_results'
EMPTY = 'Empty'
OTHER = 'Other'
ALLOWED_EMPTIES_RATIO = 0.2
ALLOWED_DEVIATION = 2
ALLOWED_DEVIATION_2018 = 3
NEGATIVE_ADJACENCY_RANGE = (2, 1000)
POSITIVE_ADJACENCY_RANGE = (0, 1)

IN_ASSORTMENT = 'in_assortment_osa'
IS_OOS = 'oos_osa'
PSERVICE_CUSTOM_SCIF = 'pservice.custom_scene_item_facts'
PRODUCT_FK = 'product_fk'
SCENE_FK = 'scene_fk'

EXCLUDE_EMPTY = False
INCLUDE_EMPTY = True


class MARSRU_PRODMARSRUKPIToolBox:

    EXCLUDE_FILTER = 0
    INCLUDE_FILTER = 1
    CONTAIN_FILTER = 2

    def __init__(self, data_provider, output, set_name=None, ignore_stacking=True):
        self.data_provider = data_provider
        self.output = output
        self.dict_for_2254 = {2261: 0, 2264: 0, 2265: 0, 2351: 0}
        self.products = self.data_provider[Data.ALL_PRODUCTS]
        self.k_engine = BaseCalculationsGroup(data_provider, output)
        self.project_name = data_provider.project_name
        self.session_uid = self.data_provider.session_uid
        self.products = self.data_provider[Data.ALL_PRODUCTS]
        try:
            self.products['sub_brand'] = self.products['Sub Brand']#the sub_brand column is empty
        except:
            pass
        self.match_product_in_scene = self.data_provider[Data.MATCHES]
        self.templates = self.data_provider[Data.ALL_TEMPLATES]
        self.visit_date = self.data_provider[Data.VISIT_DATE]
        self.scenes_info = self.data_provider[Data.SCENES_INFO]
        self.rds_conn = AwsProjectConnector(self.project_name, DbUsers.CalculationEng)
        self.session_info = SessionInfo(data_provider)
        self.store_id = self.data_provider[Data.STORE_FK]
        self.scif = self.data_provider[Data.SCENE_ITEM_FACTS]
        try:
            self.scif['sub_brand'] = self.scif['Sub Brand']#the sub_brand column is empty
        except:
            pass
        self.set_name = set_name
        self.kpi_fetcher = MARSRU_PRODMARSRUKPIFetcher(self.project_name, self.scif, self.match_product_in_scene,
                                                       self.set_name, self.products, self.session_uid)
        self.survey_response = self.data_provider[Data.SURVEY_RESPONSES]
        self.sales_rep_fk = self.data_provider[Data.SESSION_INFO]['s_sales_rep_fk'].iloc[0]
        self.session_fk = self.data_provider[Data.SESSION_INFO]['pk'].iloc[0]
        self.store_type = self.data_provider[Data.STORE_INFO]['store_type'].iloc[0]
        self.ignore_stacking = ignore_stacking
        self.facings_field = 'facings' if not self.ignore_stacking else 'facings_ign_stack'
        # self.region = self.data_provider[Data.STORE_INFO]['region_name'].iloc[0]
        self.region = self.get_store_Att5()
        self.attr6 = self.get_store_Att6()
        self.store_num_1 = self.get_store_number_1_attribute()
        self.thresholds_and_results = {}
        self.result_df = []
        self.writing_to_db_time = datetime.timedelta(0)
        # self.match_product_in_probe_details = self.kpi_fetcher.get_match_product_in_probe_details(self.session_uid)
        self.kpi_results_queries = []
        self.position_graphs = MARSRU_PRODPositionGraphs(self.data_provider, rds_conn=self.rds_conn)
        self.potential_products = {}
        self.custom_scif_queries = []
        self.shelf_square_boundaries = {}
        self.object_type_conversion = {'SKUs': 'product_ean_code',
                                       'BRAND': 'brand_name',
                                       'BRAND in CAT': 'brand_name',
                                       'CAT': 'category',
                                       'MAN in CAT': 'category',
                                       'MAN': 'manufacturer_name'}

    # def validate_scenes_and_location(self, location, scene_type, sub_location, kpi_data):
    #     if kpi_data.get('Scenes to include'):
    #         if scene_type in kpi_data.get('Scenes to include').split(", "):
    #             #return True
    #             pass
    #         else:
    #             return False
    #     if kpi_data.get('Scenes to exclude'):
    #         if scene_type in kpi_data.get('Scenes to exclude').split(", "):
    #             return False
    #     if kpi_data.get('Sub locations to include'):
    #         if sub_location in str(kpi_data.get('Sub locations to include')).split(", "):
    #             #return True
    #             pass
    #         else:
    #             return False
    #     if kpi_data.get('Sub locations to exclude'):
    #         if sub_location in str(kpi_data.get('Sub locations to exclude')).split(", "):
    #             return False
    #     if kpi_data.get('Locations to include'):
    #         if location in kpi_data.get('Locations to include').split(", "):
    #             #return True
    #             pass
    #         else:
    #             return False
    #     if kpi_data.get('Locations to exclude'):
    #         if location in kpi_data.get('Locations to exclude').split(", "):
    #             return False
    #     return True

    # def convert_kpi_level_1(self, kpi_level_1):
    #     kpi_level_1_df = self.data_provider[Data.KPI_LEVEL_1]
    #     kpi_name = kpi_level_1.get('kpi_name')
    #     kpi_fk = kpi_level_1_df[kpi_level_1_df['name'] == kpi_name].reset_index()['pk'][0]
    #     kpi_result = kpi_level_1.get('score')
    #     kpi_level_1_results = pd.DataFrame(columns=self.output.KPI_LEVEL_1_RESULTS_COLS)
    #     kpi_level_1_results = kpi_level_1_results.append({'kpi_level_1_fk': kpi_fk,
    #                                                       'result': kpi_result}, ignore_index=True)
    #     kpi_level_1_results = self.data_provider.add_session_fields_old_tables(kpi_level_1_results)
    #     return kpi_level_1_results
    #
    # def convert_kpi_level_2(self, level_2_kpi):
    #     kpi_level_2_results = pd.DataFrame(columns=self.output.KPI_LEVEL_2_RESULTS_COLS)
    #     kpi_level_2_df = self.data_provider[Data.KPI_LEVEL_2]
    #     kpi_name = level_2_kpi.get('kpi_name')
    #     kpi_fk = kpi_level_2_df[kpi_level_2_df['name'] == kpi_name].reset_index()['pk'][0]
    #     kpi_result = level_2_kpi.get('result')
    #     kpi_score = level_2_kpi.get('score')
    #     kpi_weight = level_2_kpi.get('original_weight')
    #     kpi_target = level_2_kpi.get('target')
    #     kpi_level_2_results = kpi_level_2_results.append({'kpi_level_2_fk': kpi_fk,
    #                                                       'result': kpi_result, 'score': kpi_score,
    #                                                       'weight': kpi_weight, 'target': kpi_target},
    #                                                      ignore_index=True)
    #     kpi_level_2_results = self.data_provider.add_session_fields_old_tables(kpi_level_2_results)
    #     return kpi_level_2_results
    #
    # def convert_kpi_level_3(self, level_3_kpi):
    #     kpi_level_3_results = pd.DataFrame(columns=self.output.KPI_LEVEL_3_RESULTS_COLS)
    #     kpi_level_3_df = self.data_provider[Data.KPI_LEVEL_3]
    #     kpi_name = level_3_kpi.get('KPI name')
    #     kpi_fk = kpi_level_3_df[kpi_level_3_df['name'] == kpi_name].reset_index()['pk'][0]
    #     kpi_result = level_3_kpi.get('result')
    #     kpi_score = level_3_kpi.get('score')
    #     kpi_weight = level_3_kpi.get('original_weight')
    #     kpi_target = level_3_kpi.get('target')
    #     kpi_level_3_results = kpi_level_3_results.append({'kpi_level_3_fk': kpi_fk,
    #                                                       'result': kpi_result, 'score': kpi_score,
    #                                                       'weight': kpi_weight, 'target': kpi_target},
    #                                                      ignore_index=True)
    #     kpi_level_3_results = self.data_provider.add_session_fields_old_tables(kpi_level_3_results)
    #     return kpi_level_3_results

    @kpi_runtime()
    def check_for_specific_display(self, params):
        """
        This function checks if a specific display( = scene type) exists in a store
        """
        formula_type = 'check_specific_display'
        for p in params.values()[0]:
            if p.get('Formula') != formula_type:
                continue
            result = 'TRUE'
            scene_param = p.get('Values')
            filtered_scif = self.scif.loc[self.scif['template_name'] == scene_param]
            if filtered_scif.empty:
                result = 'FALSE'
            kpi_fk = self.kpi_fetcher.get_kpi_fk(p.get('#Mars KPI NAME'))
            self.thresholds_and_results[p.get('#Mars KPI NAME')] = {'result': result}
            # Saving to old tables
            attributes_for_table2 = self.create_attributes_for_level2_df(p, 100, kpi_fk)
            self.write_to_db_result(attributes_for_table2, 'level2', kpi_fk)
            attributes_for_table3 = self.create_attributes_for_level3_df(p, 100, kpi_fk)
            self.write_to_db_result(attributes_for_table3, 'level3', kpi_fk)

    def get_product_fk(self, sku_list):
        """
        This function gets a list of SKU and returns a list of the product fk of those SKU list
        """
        product_fk_list = []
        for sku in sku_list:
            temp_df = self.products.loc[self.products['product_ean_code'] == sku]['product_fk']
            product_fk_list.append((int)(temp_df.values[0]))
        return product_fk_list

    def insert_to_level_2_and_level_3(self, p, score):
        """
        This function handles writing to DB level 2 & 3
        """
        kpi_fk = self.kpi_fetcher.get_kpi_fk(p.get('#Mars KPI NAME'))
        attributes_for_level2 = self.create_attributes_for_level2_df(p, score, kpi_fk)
        self.write_to_db_result(attributes_for_level2, 'level2', kpi_fk)
        attributes_for_level3 = self.create_attributes_for_level3_df(p, score, kpi_fk)
        self.write_to_db_result(attributes_for_level3, 'level3', kpi_fk)

    @kpi_runtime()
    def check_availability_on_golden_shelves(self, params):
        """
        This function is used to calculate availability for given SKU on golden shelves (#3,4 from bottom)
        """
        formula_type = 'availability_on_golden_shelves'
        for p in params.values()[0]:
            if p.get('Formula') != formula_type:
                continue
            result = 'TRUE'
            relevant_shelves = [3, 4]
            golden_shelves_filtered_df = self.match_product_in_scene.loc[
                (self.match_product_in_scene['shelf_number_from_bottom'].isin(relevant_shelves))]
            if golden_shelves_filtered_df.empty:
                Log.info("In the session {} there are not shelves that stands in the "
                         "criteria for availability_on_golden_shelves KPI".format(self.session_uid))
                self.insert_to_level_2_and_level_3(p, result)    # failed
                continue
            sku_list = p.get('Values').split()
            product_fk_list = self.get_product_fk(sku_list)
            bays = golden_shelves_filtered_df['bay_number'].unique().tolist()
            for bay in bays:
                current_bay_filter = golden_shelves_filtered_df.loc[golden_shelves_filtered_df['bay_number'] == bay]
                shelves = current_bay_filter['shelf_number'].unique().tolist()
                for shelf in shelves:
                    shelf_filter = current_bay_filter.loc[current_bay_filter['shelf_number'] == shelf]
                    filtered_shelf_by_products = shelf_filter.loc[(shelf_filter['product_fk'].isin(product_fk_list))]
                    if len(filtered_shelf_by_products['product_fk'].unique()) != len(product_fk_list):
                        result = 'FALSE'
                        break
                if result == 'FALSE':
                    break
            kpi_fk = self.kpi_fetcher.get_kpi_fk(p.get('#Mars KPI NAME'))
            self.thresholds_and_results[p.get('#Mars KPI NAME')] = {'result': result}
            # Saving to old tables
            attributes_for_table2 = self.create_attributes_for_level2_df(p, 100, kpi_fk)
            self.write_to_db_result(attributes_for_table2, 'level2', kpi_fk)
            attributes_for_table3 = self.create_attributes_for_level3_df(p, 100, kpi_fk)
            self.write_to_db_result(attributes_for_table3, 'level3', kpi_fk)

    def get_store_Att5(self):
        store_att5 = self.kpi_fetcher.get_store_att5(self.store_id)
        return store_att5

    def get_store_Att6(self):
        store_att6 = self.kpi_fetcher.get_store_att6(self.store_id)
        return store_att6

    def get_store_number_1_attribute(self):
        store_number_1 = self.kpi_fetcher.get_store_number_1(self.store_id)
        return store_number_1

    def get_assortment_for_attribute(self):
        assortments = self.kpi_fetcher.get_store_assortment(self.store_num_1, self.visit_date)
        return assortments

    def get_assortment_for_store_id(self):
        assortments = self.kpi_fetcher.get_store_assortment(self.store_id, self.visit_date)
        return assortments

    def get_custom_query(self, scene_fk, product_fk, assortment, oos):
        """
        This gets the query for insertion to PS custom scif
        :param scene_fk:
        :param product_fk:
        :param assortment:
        :param oos:
        :return:
        """
        attributes = pd.DataFrame([(self.session_fk, scene_fk, product_fk, assortment, oos)],
                                  columns=['session_fk', 'scene_fk', 'product_fk', IN_ASSORTMENT, IS_OOS])

        query = insert(attributes.to_dict(), PSERVICE_CUSTOM_SCIF)
        self.custom_scif_queries.append(query)

    def get_scenes_for_product(self, product_fk):
        """
        This function find all scene_fk where a product was in.
        :param product_fk:
        :return: a list of scenes_fks
        """
        product_scif = self.scif.loc[self.scif['product_fk'] == product_fk]
        scenes = product_scif['scene_fk'].unique().tolist()
        return scenes

    def commit_custom_scif(self):
        if not self.rds_conn.is_connected:
            self.rds_conn.connect_rds()
        cur = self.rds_conn.db.cursor()
        delete_query = self.kpi_fetcher.get_delete_session_custom_scif(self.session_fk)
        cur.execute(delete_query)
        self.rds_conn.db.commit()
        for query in self.custom_scif_queries:
            try:
                cur.execute(query)
            except :
                print 'could not run query: {}'.format()
        self.rds_conn.db.commit()

    def hadle_update_custom_scif(self):
        """
        This function updates the custom scif of PS with oos and assortment values for each product in each scene.
        :return:
        """
        if not self.store_num_1:
            return
        Log.info("Updating PS Custom SCIF... ")
        assortment_products = self.get_assortment_for_store_id()
        if assortment_products:
            products_in_session = self.scif.loc[self.scif['dist_sc'] == 1][PRODUCT_FK].tolist()
            for product in assortment_products:
                if product in products_in_session:
                    # This means the product in assortment and is not oos. (1,0)
                    scenes = self.get_scenes_for_product(product)
                    for scene in scenes:
                        self.get_custom_query(scene, product, 1, 0)
                else:
                    # The product is in assortment list but is oos (1,1)
                    scenes = self.scif[SCENE_FK].unique().tolist()
                    for scene in scenes:
                        self.get_custom_query(scene, product, 1, 1)

        products_not_in_assortment = self.scif[~self.scif[PRODUCT_FK].isin(assortment_products)]
        for product in products_not_in_assortment[PRODUCT_FK].unique().tolist():
            # The product is not in assortment list and not oos. (0,0)
            scenes = self.get_scenes_for_product(product)
            for scene in scenes:
                self.get_custom_query(scene, product, 0, 0)

        self.commit_custom_scif()
        Log.info("Done updating PS Custom SCIF... ")

    def get_static_list(self, type):
        object_static_list = []
        if type == 'SKUs':
            object_static_list = self.products['product_ean_code'].values.tolist()
        elif type == 'CAT' or type == 'MAN in CAT':
            object_static_list = self.products['category'].values.tolist()
        elif type == 'BRAND':
            object_static_list = self.products['brand_name'].values.tolist()
        elif type == 'MAN':
            object_static_list = self.products['manufacturer_name'].values.tolist()
        else:
            Log.warning('The type {} does not exist in the data base'.format(type))

        return object_static_list

    def insert_new_kpis_old(self, project, kpi_list=None):
        """
        This function inserts KPI metadata to static tables
        """
        # session = OrmSession(project, writable=True)
        try:
            voting_process_pk_dic = {}
            # with session.begin(subtransactions=True):
            for kpi in kpi_list.values()[0]:
                if kpi.get('first_calc?') == 2:
                    Log.info('Trying to write KPI {}'.format(kpi.get('#Mars KPI NAME')))
                    #         # kpi_level_1_hierarchy = pd.DataFrame(data=[('Canteen', None, None, 'WEIGHTED_AVERAGE',
                    #         #                                             1, '2016-11-28', None, None)],
                    #         #                                      columns=['name', 'short_name', 'eng_name', 'operator',
                    #         #                                               'version', 'valid_from', 'valid_until', 'delete_date'])
                    #         # self.output.add_kpi_hierarchy(Keys.KPI_LEVEL_1, kpi_level_1_hierarchy)
                    #         if kpi.get('level') == 2:
                    #             kpi_level_2_hierarchy = pd.DataFrame(data=[
                    #                 (1, kpi.get('KPI Name ENG'), None, None, None, None, kpi.get('weight'), 1, '2016-12-25', None, None)],
                    #                                          columns=['kpi_level_1_fk', 'name', 'short_name', 'eng_name', 'operator',
                    #                                                   'score_func', 'original_weight', 'version', 'valid_from', 'valid_until',
                    #                                                   'delete_date'])
                    #             self.output.add_kpi_hierarchy(Keys.KPI_LEVEL_2, kpi_level_2_hierarchy)
                    #         elif kpi.get('level') == 3:
                    #             kpi_level_3_hierarchy = pd.DataFrame(data=[(1, kpi.get('KPI Name ENG'), None, None, None,
                    #                                                        None, kpi.get('weight'), 1, '2016-12-25', None, None)],
                    #                                                  columns=['kpi_level_2_fk', 'name', 'short_name', 'eng_name', 'operator',
                    #                                                           'score_func', 'original_weight', 'version', 'valid_from',
                    #                                                           'valid_until', 'delete_date'])
                    #             self.output.add_kpi_hierarchy(Keys.KPI_LEVEL_3, kpi_level_3_hierarchy)
                    #     else:
                    #         Log.info('No KPIs to insert')
                    #     self.data_provider.export_kpis_hierarchy(self.output)

                    # insert_trans = """
                    #                 INSERT INTO static.kpi_level_1 (name,
                    #                operator, version, valid_from)
                    #                VALUES ('{0}', '{1}', '{2}', '{3}');""".format('test',  'WEIGHTED_AVERAGE', 1,
                    #                                                         '2016-11-28')
                    # insert_trans_level1 = """
                    #             INSERT INTO static.kpi_set (name,
                    #            missing_kpi_score, enable, normalize_weight, expose_to_api, is_in_weekly_report)
                    #            VALUES ('{0}', '{1}', '{2}', '{3}', '{4}', '{5}');""".format('MARS KPIs 2017', 'Bad',
                    #                                                          'Y', 'N', 'N', 'N')
                    # Log.get_logger().debug(insert_trans_level1)
                    # self.insert_results_data(insert_trans_level1)
                    # result = session.execute(insert_trans_level1)
                    insert_trans_level2 = """
                                    INSERT INTO static.kpi (kpi_set_fk, display_text)
                                   VALUES ('{0}', '{1}');""".format(3, kpi.get('#Mars KPI NAME'))
                    # insert_trans_level2 = """
                    #                 UPDATE static.kpi SET display_text='{0}' WHERE display_text='{1}';""".format(
                    #     kpi.get('#Mars KPI NAME'), kpi.get('OLD #Mars KPI NAME'))

                    # # #
                    # # # #     # insert_trans = """
                    # # # #     #                 UPDATE static.kpi_level_1 SET short_name=null, eng_name=null, valid_until=null, delete_date=null
                    # # # #     #                 WHERE pk=1;"""
                    # Log.get_logger().debug(insert_trans_level2)
                    kpi_fk = self.insert_results_data(insert_trans_level2)

                    # result = session.execute(insert_trans_level2)
                    # kpi_fk = result.lastrowid
                    insert_trans_level3 = """
                                    INSERT INTO static.atomic_kpi (kpi_fk,
                                   name, description, display_text, presentation_order, display)
                                   VALUES ('{0}', '{1}', '{2}', '{3}', '{4}', '{5}');""".format(kpi_fk, kpi.get(
                        '#Mars KPI NAME'),
                                                                                                kpi.get(
                                                                                                    '#Mars KPI NAME'),
                                                                                                kpi.get(
                                                                                                    'KPI Display name RU').encode(
                                                                                                    'utf-8'),
                                                                                                1, 'Y')
                    # insert_trans_level3 = """
                    #                 UPDATE static.atomic_kpi SET name='{0}',description='{1}' WHERE name='{2}';""".format(
                    #     kpi.get('#Mars KPI NAME'), kpi.get('#Mars KPI NAME'), kpi.get('OLD #Mars KPI NAME'))
                    self.insert_results_data(insert_trans_level3)
                    # Log.get_logger().debug(insert_trans_level3)
                    # result = session.execute(insert_trans_level3)
                    # voting_process_pk = result.lastrowid
                    # voting_process_pk_dic[kpi] = voting_process_pk
                    # Log.info('KPI level 1 was inserted to the DB')
                    # Log.info('Inserted voting process {} in project {} SQL DB'.format(voting_process_pk, project))
                    # voting_session_fk = self.insert_production_session(voting_process_pk, kpi, session)
                    # self.insert_production_tag(voting_process_pk, voting_session_fk, kpi, session)

            # session.close()
            # return voting_process_pk_dic
            return
        except Exception as e:
            Log.error('Caught exception while inserting new voting process to SQL: {}'.
                      format(str(e)))
            return -1

    # def insert_new_kpis(self, project, kpi_list):
    #     """
    #     This function is used to insert KPI metadata to the new tables, and currently not used
    #
    #     """
    #     for kpi in kpi_list.values()[0]:
    #         if kpi.get('To include in first calculation?') == 7:
    #             # kpi_level_1_hierarchy = pd.DataFrame(data=[('Canteen', None, None, 'WEIGHTED_AVERAGE',
    #             #                                             1, '2016-11-28', None, None)],
    #             #                                      columns=['name', 'short_name', 'eng_name', 'operator',
    #             #                                               'version', 'valid_from', 'valid_until', 'delete_date'])
    #             # self.output.add_kpi_hierarchy(Keys.KPI_LEVEL_1, kpi_level_1_hierarchy)
    #             if kpi.get('level') == 2:
    #                 kpi_level_2_hierarchy = pd.DataFrame(data=[
    #                     (3, kpi.get('KPI name Eng'), None, None, None, kpi.get('score_func'), kpi.get('KPI Weight'), 1,
    #                      '2016-12-01', None,
    #                      None)],
    #                     columns=['kpi_level_1_fk', 'name', 'short_name', 'eng_name', 'operator',
    #                              'score_func', 'original_weight', 'version', 'valid_from', 'valid_until',
    #                              'delete_date'])
    #                 self.output.add_kpi_hierarchy(Keys.KPI_LEVEL_2, kpi_level_2_hierarchy)
    #             elif kpi.get('level') == 3:
    #                 kpi_level_3_hierarchy = pd.DataFrame(
    #                     data=[(82, kpi.get('KPI Name'), None, None, 'PRODUCT AVAILABILITY',
    #                            None, kpi.get('KPI Weight'), 1, '2016-12-25', None, None)],
    #                     columns=['kpi_level_2_fk', 'name', 'short_name', 'eng_name',
    #                              'operator',
    #                              'score_func', 'original_weight', 'version',
    #                              'valid_from',
    #                              'valid_until', 'delete_date'])
    #                 self.output.add_kpi_hierarchy(Keys.KPI_LEVEL_3, kpi_level_3_hierarchy)
    #             self.data_provider.export_kpis_hierarchy(self.output)
    #         else:
    #             Log.info('No KPIs to insert')

    # def check_number_of_facings_given_answer_to_survey(self, params):
    #     set_total_res = 0
    #     for p in params.values()[0]:
    #         if p.get('Formula') != "number of facings given answer to survey" or not p.get("children"):
    #             continue
    #         kpi_fk = self.kpi_fetcher.get_kpi_fk(p.get('KPI name Eng'), self.set_name)
    #         first_atomic_score = 0
    #         children = map(int, p.get("children").split(", "))
    #         for c in params.values()[0]:
    #             if c.get("KPI ID") in children and c.get("Formula") == "atomic answer to survey":
    #                 first_atomic_score = self.check_answer_to_survey_level3(c)
    #                 # saving to DB
    #                 attributes_for_level3 = self.create_attributes_for_level3_df(c, first_atomic_score, kpi_fk)
    #                 self.write_to_db_result(attributes_for_level3, 'level3')
    #         second_atomic_res = 0
    #         for c in params.values()[0]:
    #             if c.get("KPI ID") in children and c.get("Formula") == "atomic number of facings":
    #                 second_atomic_res = self.calculate_availability(c)
    #                 second_atomic_score = self.calculate_score(second_atomic_res, c)
    #                 # write to DB
    #                 attributes_for_level3 = self.create_attributes_for_level3_df(c, second_atomic_score, kpi_fk)
    #                 self.write_to_db_result(attributes_for_level3, 'level3')
    #
    #         if first_atomic_score > 0:
    #             kpi_total_res = second_atomic_res
    #         else:
    #             kpi_total_res = 0
    #         score = self.calculate_score(kpi_total_res, p)
    #         set_total_res += score * p.get('KPI Weight')
    #         # saving to DB
    #         attributes_for_level2 = self.create_attributes_for_level2_df(p, score, kpi_fk)
    #         self.write_to_db_result(attributes_for_level2, 'level2')
    #
    #     Log.info('Calculation finished')
    #     return set_total_res

    # def check_answer_to_survey_level3(self, params):
    #     d = {'Yes': u'Да', 'No': u'Нет'}
    #     score = 0
    #     survey_data = self.survey_response.loc[self.survey_response['question_text'] == params.get('Values')]
    #     if not survey_data['selected_option_text'].empty:
    #         result = survey_data['selected_option_text'].values[0]
    #         targets = [d.get(target) if target in d.keys() else target
    #                    for target in str(params.get('Target')).split(", ")]
    #         if result in targets:
    #             score = 100
    #         else:
    #             score = 0
    #     elif not survey_data['number_value'].empty:
    #         result = survey_data['number_value'].values[0]
    #         if result == params.get('Target'):
    #             score = 100
    #         else:
    #             score = 0
    #     else:
    #         Log.warning('No survey data for this session')
    #     return score

    @kpi_runtime()
    def check_availability(self, params):
        """
        This function is used to calculate availability given a set pf parameters

        """
        set_total_res = 0
        availability_types = ['SKUs', 'BRAND', 'MAN', 'CAT', 'MAN in CAT', 'BRAND in CAT']
        formula_types = ['number of SKUs', 'number of facings']
        for p in params.values()[0]:
            if p.get('Type') not in availability_types or p.get('Formula') not in formula_types:
                continue
            if p.get('level') == 3:
                continue
            is_atomic = False
            kpi_fk = self.kpi_fetcher.get_kpi_fk(p.get('#Mars KPI NAME'))

            # if p.get('children') is not None:
            #     is_atomic = True
            #     children = [int(child) for child in str(p.get('children')).split(', ')]
            #     for child in params.values()[0]:
            #         if child.get('KPI ID') in children:
            #
            #             if child.get('children') is not None:  # atomic of atomic
            #                 atomic_score = 0
            #                 atomic_children = [int(a_child) for a_child in str(child.get('children')).split(', ')]
            #                 for atomic_child in params.values()[0]:
            #                     if atomic_child.get('KPI ID') in atomic_children:
            #                         atomic_child_res = self.calculate_availability(atomic_child)
            #                         atomic_child_score = self.calculate_score(atomic_child_res, atomic_child)
            #                         atomic_score += atomic_child.get('additional_weight',
            #                                                          1.0 / len(atomic_children)) * atomic_child_score
            #
            #             else:
            #                 atomic_res = self.calculate_availability(child)
            #                 atomic_score = self.calculate_score(atomic_res, child)
            #             kpi_total_res += child.get('additional_weight', 1.0 / len(children)) * atomic_score
            #             # write to DB
            #             attributes_for_table3 = self.create_attributes_for_level3_df(child, atomic_score, kpi_fk)
            #             self.write_to_db_result(attributes_for_table3, 'level3', kpi_fk)

            # else:
            #     kpi_total_res = self.calculate_availability(p)
            # if is_atomic:
            #     score = kpi_total_res / 100.0
            #     if score < p.get('score_min', 0):
            #         score = 0
            #     elif score > p.get('score_max', 1):
            #         score = p.get('score_max', 1)
            #     score *= 100
            # else:
            #     score = self.calculate_score(kpi_total_res, p)

            kpi_total_res = self.calculate_availability(p)
############################
            # if p.get('#Mars KPI NAME') == 2275:
            #     kpi_total_res = 15
############################
            score = self.calculate_score(kpi_total_res, p)
            # Saving to old tables
            attributes_for_table2 = self.create_attributes_for_level2_df(p, score, kpi_fk)
            self.write_to_db_result(attributes_for_table2, 'level2', kpi_fk)
            if not is_atomic:  # saving also to level3 in case this KPI has only one level
                attributes_for_table3 = self.create_attributes_for_level3_df(p, score, kpi_fk)
                self.write_to_db_result(attributes_for_table3, 'level3', kpi_fk)

                # set_total_res += score * p.get('KPI Weight')

        return set_total_res

    def calculate_availability(self, params, scenes=[], formula=None, values_list=None, object_type=None,
                               include_stacking = False):
        if not values_list:
            if '*' in str(params.get('Values')):
                values_list = str(params.get('Values')).split(', *')
            else:
                values_list = str(params.get('Values')).split(', ')
        if not formula:
            formula = params.get('Formula')
        # object_static_list = self.get_static_list(params.get('Type'))
        # include_stacking = False
        if not scenes:
            scenes = self.get_relevant_scenes(params)

        if params.get("Form Factor to include"):
            form_factors = [str(form_factor) for form_factor in params.get("Form Factor to include").split(", ")]
        else:
            form_factors = []
        if params.get("Form Factor to exclude"):
            form_factors_to_exclude = [str(form_factor) for form_factor in
                                       params.get("Form Factor to exclude").split(", ")]
        else:
            form_factors_to_exclude = []
        if params.get('Sub brand to include'):
            sub_brands = [str(sub_brand) for sub_brand in params.get('Sub brand to include').split(", ")]
        else:
            sub_brands = []
        if params.get('Sub brand to exclude'):
            sub_brands_to_exclude = [str(sub_brand) for sub_brand in params.get('Sub brand to exclude').split(", ")]
        else:
            sub_brands_to_exclude = []
        if params.get('Stacking'):
            include_stacking = True
        if params.get('Brand Category value'):
            brand_category = params.get('Brand Category value')
        else:
            brand_category = None
        if object_type:
            availability_type = object_type
        else:
            availability_type = params.get('Type')
        # if params.get("Size"):
        #     sizes = [float(size) for size in str(params.get('Size')).split(", ")]
        #     sizes = [int(size) if int(size) == size else size for size in sizes]
        # else:
        #     sizes = []
        # if params.get("Products to exclude"):
        #     products_to_exclude = [int(float(product)) for product in \
        #                            str(params.get("Products to exclude")).split(", ")]
        # else:
        #     products_to_exclude = []
        # if params.get("Form factors to exclude"):
        #     form_factors_to_exclude = str(params.get("Form factors to exclude")).split(", ")
        # else:
        #     form_factors_to_exclude = []

        object_facings = self.kpi_fetcher.get_object_facings(scenes, values_list, availability_type,
                                                             formula=formula, form_factor=form_factors,
                                                             sub_brands=sub_brands,
                                                             sub_brands_to_exclude=sub_brands_to_exclude,
                                                             include_stacking=include_stacking,
                                                             form_factor_to_exclude=form_factors_to_exclude,
                                                             brand_category=brand_category)

        return object_facings

    def get_relevant_scenes(self, params):
        all_scenes = self.scenes_info['scene_fk'].unique().tolist()
        filtered_scenes = []
        scenes_data = {}
        location_data = {}
        # sub_location_data = {}

        for scene in all_scenes:
            scene_type = list(self.scif.loc[self.scif['scene_id'] == scene]['template_name'].values)
            if scene_type:
                scene_type = scene_type[0]
                if scene_type not in scenes_data.keys():
                    scenes_data[scene_type] = []
                scenes_data[scene_type].append(scene)
                filtered_scenes.append(scene)
            else:
                Log.warning('Scene {} is not defined in reporting.scene_item_facts table'.format(scene))
                continue

            location = list(self.scif.loc[self.scif['scene_id'] == scene]['location_type'].values)
            if location:
                location = location[0]
                if location not in location_data.keys():
                    location_data[location] = []
                location_data[location].append(scene)
                #
                # sub_location = list(
                #     self.scif.loc[self.scif['template_name'] == scene_type]['additional_attribute_2'].values)
                # if sub_location:
                #     sub_location = sub_location[0]
                #     if sub_location not in sub_location_data.keys():
                #         sub_location_data[sub_location] = []
                # sub_location_data[sub_location].append(scene)

        include_list = []
        # if not params.get('Scene types'):
        #     include_list.extend(filtered_scenes)
        # else:
        #     if params.get('Scene types'):
        #         scenes_to_include = params.get('Scene types').split(', ')
        #         for scene in scenes_to_include:
        #             if scene in scenes_data.keys():
        #                 include_list.extend(scenes_data[scene])

        if not params.get('Location type'):
            include_list.extend(filtered_scenes)
        else:
            if params.get('Location type'):
                locations_to_include = params.get('Location type').split(', ')
                for location in locations_to_include:
                    if location in location_data.keys():
                        include_list.extend(location_data[location])
                        #
                        # if params.get('Sub locations to include'):
                        #     sub_locations_to_include = str(params.get('Sub locations to include')).split(', ')
                        #     for sub_location in sub_locations_to_include:
                        #         if sub_location in sub_location_data.keys():
                        #             include_list.extend(sub_location_data[sub_location])
        include_list = list(set(include_list))

        # exclude_list = []
        # if params.get('Scenes to exclude'):
        #     scenes_to_exclude = params.get('Scenes to exclude').split(', ')
        #     for scene in scenes_to_exclude:
        #         if scene in scenes_data.keys():
        #             exclude_list.extend(scenes_data[scene])
        #
        # if params.get('Locations to exclude'):
        #     locations_to_exclude = params.get('Locations to exclude').split(', ')
        #     for location in locations_to_exclude:
        #         if location in location_data.keys():
        #             exclude_list.extend(location_data[location])
        #
        # if params.get('Sub locations to exclude'):
        #     sub_locations_to_exclude = str(params.get('Sub locations to exclude')).split(', ')
        #     for sub_location in sub_locations_to_exclude:
        #         if sub_location in sub_location_data.keys():
        #             exclude_list.extend(sub_location_data[sub_location])
        # exclude_list = list(set(exclude_list))

        # relevant_scenes = []
        # for scene in include_list:
        #     if scene not in exclude_list:
        #         relevant_scenes.append(scene)
        return include_list

    @kpi_runtime()
    def check_number_of_scenes(self, params):
        """
        This function is used to calculate number of scenes

        """
        set_total_res = 0
        for p in params.values()[0]:
            if p.get('Formula') != 'number of scenes':
                continue
            kpi_total_res = 0
            scenes = self.get_relevant_scenes(p)

            if p.get('Type') == 'SCENES':
                values_list = [str(s) for s in p.get('Values').split(', ')]
                for scene in scenes:
                    try:
                        scene_type = self.scif.loc[self.scif['scene_id'] == scene]['template_name'].values[0]
                        if scene_type in values_list:
                            res = 1
                        else:
                            res = 0
                        kpi_total_res += res
                    except IndexError as e:
                        continue

            else:  # checking for number of scenes with a complex condition (only certain products/brands/etc)
                p_copy = p.copy()
                p_copy["Formula"] = "number of facings"
                for scene in scenes:
                    if self.calculate_availability(p_copy, scenes=[scene], values_list=p_copy.get('Values').split(', ')) > 0:
                        res = 1
                    else:
                        res = 0
                    kpi_total_res += res

            score = self.calculate_score(kpi_total_res, p)
            # set_total_res += score * p.get('KPI Weight')

            kpi_fk = self.kpi_fetcher.get_kpi_fk(p.get('#Mars KPI NAME'))
            attributes_for_level2 = self.create_attributes_for_level2_df(p, score, kpi_fk)
            self.write_to_db_result(attributes_for_level2, 'level2', kpi_fk)
            attributes_for_level3 = self.create_attributes_for_level3_df(p, score, kpi_fk)
            self.write_to_db_result(attributes_for_level3, 'level3', kpi_fk)

            # return set_total_res

    @kpi_runtime()
    def check_survey_answer(self, params):
        """
        This function is used to calculate survey answer according to given target

        """
        score = False
        d = {'Yes': [u'Да', u'ДА', u'да'], 'No': u'Нет'}
        for p in params.values()[0]:
            kpi_total_res = 0
            score = 0  # default score
            if p.get('Type') != 'SURVEY' or p.get('Formula') != 'answer for survey':
                continue
            survey_data = self.survey_response.loc[self.survey_response['code'] == str(int(p.get('Survey Question_ID_code')))]
            if not survey_data['selected_option_text'].empty:
                result = survey_data['selected_option_text'].values[0]
                # targets = [d.get(target) if target in d.keys() else target
                #            for target in str(p.get('Target')).split(", ")]
                if p.get('Answer type') == 'Boolean':
                    if result in d.get('Yes'):
                        self.thresholds_and_results[p.get('#Mars KPI NAME')] = {'result': 'TRUE'}
                        # score = 'TRUE'
                    else:
                        # score = 'FALSE'
                        self.thresholds_and_results[p.get('#Mars KPI NAME')] = {'result': 'FALSE'}
                elif p.get('Answer type') == 'List':
                    # result = ','.join([str(result_value.encode('utf-8')) for result_value in
                    #                    survey_data['selected_option_text'].values])
                    # result = str([result_value for result_value in survey_data['selected_option_text'].values])
                    result = self.kpi_fetcher.get_survey_answers_codes(survey_data['code'].values[0], survey_data['selected_option_text'].values[0])

                    self.thresholds_and_results[p.get('#Mars KPI NAME')] = {'result': result}
                    # score = result
                elif p.get('Answer type') == 'Int':
                    try:
                        result = int(survey_data['number_value'].values[0])
                        self.thresholds_and_results[p.get('#Mars KPI NAME')] = {'result': result}
                    except ValueError as e:
                        self.thresholds_and_results[p.get('#Mars KPI NAME')] = {'result': 'null'}
                elif p.get('Answer type') == 'Float':
                    try:
                        result = float(survey_data['number_value'].values[0])
                        self.thresholds_and_results[p.get('#Mars KPI NAME')] = {'result': result}
                    except ValueError as e:
                        self.thresholds_and_results[p.get('#Mars KPI NAME')] = {'result': 'null'}
                else:
                    Log.info('The answer type {} is not defined for surveys'.format(params.get('Answer type')))
                    continue
            else:
                self.thresholds_and_results[p.get('#Mars KPI NAME')] = {'result': 'null'}
                Log.warning('No survey data with survey response code {} for this session'
                            .format(str(int(p.get('Survey Question_ID_code')))))
            # score = self.calculate_score(kpi_total_res, p)
            # if p.get('level') == 3:  # todo should be a separate generic function
            #     # level3_output = {'result': d.get(result), 'score': score,
            #     #                  'target': p.get('Target'), 'weight': p.get('KPI Weight'),
            #     #                  'kpi_name': p.get('KPI name Eng')}
            #     # self.output.add_kpi_results(Keys.KPI_LEVEL_3_RESULTS, self.convert_kpi_level_3(level3_output))
            #     kpi_fk = self.kpi_fetcher.get_kpi_fk(p.get('KPI name Eng'), self.set_name)
            #     attributes_for_level3 = self.create_attributes_for_level3_df(p, score, kpi_fk)
            #     self.write_to_db_result(attributes_for_level3, 'level3')
            # elif p.get('level') == 2:
            #     # level2_output = {'result': d.get(result), 'score': score,
            #     #                  'target': p.get('Target'), 'weight': p.get('KPI Weight'),
            #     #                  'kpi_name': p.get('KPI name Eng')}
            #     # self.output.add_kpi_results(Keys.KPI_LEVEL_2_RESULTS, self.convert_kpi_level_2(level2_output))
            kpi_fk = self.kpi_fetcher.get_kpi_fk(p.get('#Mars KPI NAME'))
            attributes_for_level3 = self.create_attributes_for_level3_df(p, score, kpi_fk)
            self.write_to_db_result(attributes_for_level3, 'level3')
            attributes_for_level2 = self.create_attributes_for_level2_df(p, score, kpi_fk)
            self.write_to_db_result(attributes_for_level2, 'level2')
            # else:
            #     Log.warning('No level indicated for this KPI')

        return score

    @kpi_runtime()
    def check_price(self, params, scenes=[]):
        for p in params.values()[0]:
            kpi_total_res = 0
            score = 0  # default score
            if p.get('Formula') != 'price':
                continue
            values_list = str(p.get('Values')).split(', ')
            # object_static_list = self.get_static_list(params.get('Type'))
            if not scenes:
                scenes = self.get_relevant_scenes(params)
            form_factors = [str(form_factor) for form_factor in p.get('Form Factor to include').split(", ")]
            if p.get('Stacking'):
                max_price = self.kpi_fetcher.get_object_price(scenes, values_list, p.get('Type'),
                                                              self.match_product_in_scene, form_factors, include_stacking=True)
            else:
                max_price = self.kpi_fetcher.get_object_price(scenes, values_list, p.get('Type'),
                                                              self.match_product_in_scene, form_factors)
            is_atomic = False
            kpi_fk = self.kpi_fetcher.get_kpi_fk(p.get('#Mars KPI NAME'))
            if not max_price:
                max_price_for_db = 0.0
            else:
                max_price_for_db = max_price
            score = self.calculate_score(max_price_for_db, p)
            # Saving to old tables
            attributes_for_table2 = self.create_attributes_for_level2_df(p, score, kpi_fk)
            self.write_to_db_result(attributes_for_table2, 'level2', kpi_fk)
            if not is_atomic:  # saving also to level3 in case this KPI has only one level
                attributes_for_table3 = self.create_attributes_for_level3_df(p, score, kpi_fk)
                self.write_to_db_result(attributes_for_table3, 'level3', kpi_fk)

    @kpi_runtime()
    def custom_linear_sos(self, params):
        for p in params.values()[0]:
            if p.get('Formula') != 'custom_linear_sos':
                continue
            values_list = str(p.get('Values')).split(', ')
            scenes = self.get_relevant_scenes(p)
            linear_sos = self.calculate_linear_sos(scenes, p.get('Type'), values_list)
            kpi_fk = self.kpi_fetcher.get_kpi_fk(p.get('#Mars KPI NAME'))
            score = self.calculate_score(round(linear_sos, 2), p)
            # Saving to old tables
            if p.get('#Mars KPI NAME') in (2264, 2351):
                self.dict_for_2254[p.get('#Mars KPI NAME')] = float(linear_sos)
            attributes_for_table2 = self.create_attributes_for_level2_df(p, score, kpi_fk)
            self.write_to_db_result(attributes_for_table2, 'level2', kpi_fk)
            attributes_for_table3 = self.create_attributes_for_level3_df(p, score, kpi_fk)
            self.write_to_db_result(attributes_for_table3, 'level3', kpi_fk)

    # def calculate_linear_sos(self, scenes, object_type, values):  # todo change according to Evgeny's modifications
    #     object_field = self.object_type_conversion[object_type]
    #     scenes_linear_sos_dict = {}
    #     for scene in scenes:
    #         matches = self.match_product_in_scene.loc[self.match_product_in_scene['scene_fk'] == scene]
    #         bays = matches['bay_number'].unique().tolist()
    #         bays_linear_sos_dict = {}
    #         for bay in bays:
    #             shelves_linear_sos_dict = {}
    #             bay_filter = matches.loc[matches['bay_number'] == bay]
    #             shelves = bay_filter['shelf_number'].unique().tolist()
    #             filtered_products = pd.merge(bay_filter, self.products, on=['product_fk', 'product_fk'])
    #             for shelf in shelves:
    #                 #  All MARS products with object type filters
    #                 nominator = filtered_products.loc[(filtered_products[object_field].isin(values)) & (
    #                     filtered_products['manufacturer_name'] == MARS) & (
    #                                                       filtered_products['shelf_number'] == shelf)][
    #                     'width_mm_x'].sum()
    #                 #  All products with object type filters
    #                 denominator = filtered_products.loc[(filtered_products[object_field].isin(values)) & (
    #                     filtered_products['shelf_number'] == shelf)][
    #                     'width_mm_x'].sum()
    #                 if denominator:
    #                     shelf_linear_sos = nominator / float(denominator)
    #                 else:
    #                     shelf_linear_sos = 0
    #                 shelves_linear_sos_dict[shelf] = shelf_linear_sos  # Saving linear sos value per shelf
    #             bays_linear_sos_dict[bay] = sum(
    #                 shelves_linear_sos_dict.values())  # Saving sum of linear sos values per bay
    #         if bays_linear_sos_dict.values():
    #             scenes_linear_sos_dict[scene] = sum(bays_linear_sos_dict.values()) / len(
    #                 bays_linear_sos_dict.values())  # Saving average of linear sos bay values per scene
    #         else:
    #             scenes_linear_sos_dict[scene] = 0
    #     if scenes:
    #         final_linear_sos_values = sum(scenes_linear_sos_dict.values()) / len(scenes_linear_sos_dict.values())
    #         #  Returning average of scenes linear sos values
    #     else:
    #         final_linear_sos_values = 0
    #
    #     return final_linear_sos_values

    def calculate_linear_sos(self, scenes, object_type, values):  # todo change according to Evgeny's modifications
        object_field = self.object_type_conversion[object_type]
        scenes_linear_sos_dict = {}
        bay_counter = 0
        for scene in scenes:
            matches = self.match_product_in_scene.loc[self.match_product_in_scene['scene_fk'] == scene]
            bays = matches['bay_number'].unique().tolist()
            bays_linear_sos_dict = {}
            for bay in bays:
                shelves_linear_sos_dict = {}
                bay_filter = matches.loc[matches['bay_number'] == bay]
                shelves = bay_filter['shelf_number'].unique().tolist()
                filtered_products = pd.merge(bay_filter, self.products, on=['product_fk'], suffixes=['', '_1'])
                for shelf in shelves:
                    #  All MARS products with object type filters
                    shelf_data = filtered_products.loc[(filtered_products[object_field].isin(values)) & (
                        filtered_products['manufacturer_name'] == MARS) & (
                                                           filtered_products['shelf_number'] == shelf)]
                    if not shelf_data.empty:
                        shelves_linear_sos_dict[shelf] = 1
                    else:
                        shelves_linear_sos_dict[shelf] = 0
                # Saving linear sos value per shelf
                bays_linear_sos_dict[bay] = sum(
                    shelves_linear_sos_dict.values())  # Saving sum of linear sos values per bay
                if bays_linear_sos_dict[bay] > 0:
                    bay_counter += 1
            if bays_linear_sos_dict.values():
                scenes_linear_sos_dict[scene] = sum(
                    bays_linear_sos_dict.values())  # Saving average of linear sos bay values per scene
            else:
                scenes_linear_sos_dict[scene] = 0
        if scenes and bay_counter > 0:
            final_linear_sos_values = sum(scenes_linear_sos_dict.values()) / float(bay_counter)
            #  Returning average of scenes linear sos values
        else:
            final_linear_sos_values = 0

        return final_linear_sos_values

    @kpi_runtime()
    def check_layout_size(self, params):
        for p in params.values()[0]:
            if p.get('Formula') != 'layout size':
                continue
            values_list = str(p.get('Values')).split(', ')
            scenes = self.get_relevant_scenes(p)
            if p.get('Stacking'):
                if p.get('Stacking') == -1:
                    form_factor_filter = {"WET": 'gross_len_ign_stack', "DRY": 'gross_len_split_stack'}
                    linear_size, products = self.calculate_layout_size_by_form_factor(scenes, p.get('Type'),
                                                                                      values_list, form_factor_filter)
                else:
                    linear_size, products = self.calculate_layout_size(scenes, p.get('Type'), values_list,
                                                                       include_stacking=True)
            else:
                linear_size, products = self.calculate_layout_size(scenes, p.get('Type'), values_list)
            if p.get('additional_attribute_for_specials'):
                allowed_linear_size = self.calculate_allowed_products(scenes, products,
                                                                      p.get('additional_attribute_for_specials'),
                                                                      p.get('Stacking'))
                linear_size += allowed_linear_size
            is_atomic = False
            kpi_fk = self.kpi_fetcher.get_kpi_fk(p.get('#Mars KPI NAME'))

            score = self.calculate_score(linear_size, p)
            if p.get('#Mars KPI NAME') in (2261, 2265):
                self.dict_for_2254[p.get('#Mars KPI NAME')] = linear_size
            # Saving to old tables
            attributes_for_table2 = self.create_attributes_for_level2_df(p, score, kpi_fk)
            self.write_to_db_result(attributes_for_table2, 'level2', kpi_fk)
            if not is_atomic:  # saving also to level3 in case this KPI has only one level
                attributes_for_table3 = self.create_attributes_for_level3_df(p, score, kpi_fk)
                self.write_to_db_result(attributes_for_table3, 'level3', kpi_fk)

    def calculate_layout_size(self, scenes, object_type, values, include_stacking=False):
        object_field = self.object_type_conversion[object_type]
        final_linear_size = 0
        products = []
        for scene in scenes:
            if object_type == 'MAN in CAT':
                filtered_scif = self.scif.loc[
                    (self.scif['scene_id'] == scene) & (self.scif[object_field].isin(values)) & (
                        self.scif['manufacturer_name'] == MARS)]
            else:
                filtered_scif = self.scif.loc[(self.scif['scene_id'] == scene) & (self.scif[object_field].isin(values))]
            products.extend(filtered_scif['product_fk'].unique().tolist())
            if not include_stacking:
                final_linear_size += filtered_scif['gross_len_ign_stack'].sum()
            else:
                final_linear_size += filtered_scif['gross_len_split_stack'].sum()
        return float(final_linear_size / 1000), products

    def calculate_allowed_products(self, scenes, products, allowed_brands, include_stacking=False):
        allowed_brands = allowed_brands.split(', ')
        statuses = [1]
        if include_stacking:
            statuses.append(3)
        match_product_with_details = pd.merge(self.match_product_in_scene, self.products, on='product_fk')
        matches_to_include = []
        original_products_matches = match_product_with_details[match_product_with_details[
            'product_fk'].isin(products)]['scene_match_fk'].tolist()
        final_linear_size = 0
        for scene in scenes:
            match_product_current_scene = match_product_with_details[match_product_with_details['scene_fk'] == scene]
            for bay in match_product_current_scene['bay_number'].unique().tolist():
                match_product_current_bay = match_product_current_scene[
                    match_product_current_scene['bay_number'] == bay]
                for shelf in match_product_current_bay['shelf_number'].unique().tolist():
                    match_product_current_shelf = match_product_current_bay[
                        match_product_current_bay['shelf_number'] == shelf]
                    allowed_shelf_products = []
                    while True:
                        first_length = len(allowed_shelf_products)
                        for product in match_product_current_shelf[
                                    (match_product_current_shelf['brand_name'].isin(allowed_brands)) &
                                    (match_product_current_shelf['status'].isin(statuses))]['scene_match_fk'].tolist():
                            if self.neighbor(product, match_product_current_shelf, original_products_matches,
                                             allowed_shelf_products):
                                allowed_shelf_products.append(product)
                        if len(allowed_shelf_products) == first_length:
                            break
                    matches_to_include.extend(allowed_shelf_products)
            final_linear_size += match_product_current_scene[
                match_product_current_scene['scene_match_fk'].isin(matches_to_include)]['width_mm_advance'].sum()
        return float(final_linear_size) / 1000

    @staticmethod
    def neighbor(scene_match_fk, match_product_current_shelf, original_products, allowed_products):
        if scene_match_fk in allowed_products or scene_match_fk in original_products:
            return False
        filtered_good_sequences = match_product_current_shelf[
            (match_product_current_shelf['scene_match_fk'].isin(original_products + allowed_products))][
            'sku_sequence_number'].tolist()
        current_product = match_product_current_shelf[match_product_current_shelf['scene_match_fk'] == scene_match_fk]
        current_sequence = current_product['sku_sequence_number'].iloc[0]
        if (current_sequence + 1) in filtered_good_sequences or (current_sequence - 1) in filtered_good_sequences:
            return True
        return False

    def calculate_layout_size_by_filters(self, filters):
        filtered_scif = self.scif[filters]
        final_linear_size = filtered_scif['gross_len_ign_stack'].sum()
        return float(final_linear_size / 1000)

    def calculate_layout_size_by_form_factor(self, scenes, object_type, values, form_factor_filter):
        object_field = self.object_type_conversion[object_type]
        final_linear_size = 0
        products = []
        for scene in scenes:
            if object_type == 'MAN in CAT':
                filtered_scif = self.scif.loc[
                    (self.scif['scene_id'] == scene) & (self.scif[object_field].isin(values)) & (
                        self.scif['manufacturer_name'] == MARS)]
            else:
                filtered_scif = self.scif.loc[(self.scif['scene_id'] == scene) & (self.scif[object_field].isin(values))]
            for form_factor in form_factor_filter:
                final_linear_size += filtered_scif[filtered_scif['form_factor'] == form_factor][form_factor_filter[
                    form_factor]].sum()
                products.extend(filtered_scif[filtered_scif['form_factor'] == form_factor][
                                    'product_fk'].unique().tolist())
        return float(final_linear_size / 1000), products

    @kpi_runtime()
    def custom_marsru_1(self, params):
        """
        This function checks if a shelf size is more than 3 meters
        :param params:
        :return:
        """
        for p in params.values()[0]:
            if p.get('Formula') != 'custom_mars_1':
                continue
            values_list = str(p.get('Values')).split(', ')
            scenes = self.get_relevant_scenes(p)
            shelf_size_dict = self.calculate_shelf_size(scenes)
            check_product = False
            result = 'TRUE'
            for shelf in shelf_size_dict.values():
                if shelf >= 3000:
                    check_product = True
            if check_product:
                object_facings = self.kpi_fetcher.get_object_facings(scenes, values_list, p.get('Type'),
                                                                     formula='number of facings')
                if object_facings > 0:
                    result = 'TRUE'
                else:
                    result = 'FALSE'
            kpi_fk = self.kpi_fetcher.get_kpi_fk(p.get('#Mars KPI NAME'))
            self.thresholds_and_results[p.get('#Mars KPI NAME')] = {'result': result}
            # Saving to old tables
            attributes_for_table2 = self.create_attributes_for_level2_df(p, 100, kpi_fk)
            self.write_to_db_result(attributes_for_table2, 'level2', kpi_fk)
            attributes_for_table3 = self.create_attributes_for_level3_df(p, 100, kpi_fk)
            self.write_to_db_result(attributes_for_table3, 'level3', kpi_fk)

    def calculate_shelf_size(self, scenes):
        scenes_dict = {}
        for scene in scenes:
            matches = self.match_product_in_scene.loc[self.match_product_in_scene['scene_fk'] == scene]
            bays = matches['bay_number'].unique().tolist()
            bays_dict = {}
            for bay in bays:
                shelves_dict = {}
                bay_filter = matches.loc[matches['bay_number'] == bay]
                shelves = bay_filter['shelf_number'].unique().tolist()
                for shelf in shelves:
                    shelf_size = bay_filter.loc[(bay_filter['shelf_number'] == shelf)][
                        'width_mm'].sum()
                    shelves_dict[shelf] = shelf_size  # Saving shelf size value per shelf
                bays_dict[bay] = max(shelves_dict.values())  # Saving the max shelf per bay
            scenes_dict[scene] = sum(bays_dict.values())  # Summing all bays per scene

        return scenes_dict

    @kpi_runtime()
    def brand_blocked_in_rectangle(self, params):
        self.rds_conn = AwsProjectConnector('marsru-prod', DbUsers.CalculationEng)
        for p in params.values()[0]:
            if p.get('Formula') != 'custom_mars_2' and p.get('Formula') != 'custom_mars_2_2018': #todo: insert it back after testing
                continue
            brands_results_dict = {}
            values_list = str(p.get('Values')).split(', ')
            scenes = self.get_relevant_scenes(p)
            object_field = self.object_type_conversion[p.get('Type')]
            if not self.rds_conn.is_connected:
                self.rds_conn.connect_rds()
            if p.get('Stacking'):
                matches = self.kpi_fetcher.get_filtered_matches()
            else:
                matches = self.kpi_fetcher.get_filtered_matches(include_stacking=False)
            sub_brands = [str(sub_brand) for sub_brand in p.get('Sub brand to exclude').split(", ")]
            form_factors = [str(form_factor) for form_factor in p.get("Form Factor to include").split(", ")]
            for value in values_list:
                if p.get('Formula') == 'custom_mars_2' and self.visit_date.year != 2018:
                    self.initial_mapping_of_square(scenes, matches, object_field, [value], p, form_factor=form_factors,
                                                   sub_brand_to_exclude=sub_brands)
                    brand_result = self.check_brand_block(object_field, [value])
                else:
                    sub_brands_to_exclude_by_sub_cats = {'WET': [], 'DRY': sub_brands}
                    self.initial_mapping_of_square(scenes, matches, object_field, [value], p, form_factor=form_factors,
                                                   sub_brand_by_sub_cat=sub_brands_to_exclude_by_sub_cats)
                    brand_result = self.check_brand_block_2018(object_field, [value], sub_brands, form_factors,
                                                               sub_brand_by_sub_cat=sub_brands_to_exclude_by_sub_cats)
                    # if brand_result == 'FALSE':
                    #     self.initial_mapping_of_square(scenes, matches, object_field, [value], p,
                    #                                    form_factor=form_factors, sub_brand_to_exclude=[])
                    #     brand_result = self.check_brand_block_2018(object_field, [value],
                    #                                                [], form_factors)
                if brand_result == 'TRUE':
                    brands_results_dict[value] = 1
                else:
                    brands_results_dict[value] = 0
            if sum(brands_results_dict.values()) == len(values_list):
                result = 'TRUE'
            else:
                result = 'FALSE'
            kpi_fk = self.kpi_fetcher.get_kpi_fk(p.get('#Mars KPI NAME'))
            self.thresholds_and_results[p.get('#Mars KPI NAME')] = {'result': result}
            # Saving to old tables
            attributes_for_table2 = self.create_attributes_for_level2_df(p, 100, kpi_fk)
            self.write_to_db_result(attributes_for_table2, 'level2', kpi_fk)
            attributes_for_table3 = self.create_attributes_for_level3_df(p, 100, kpi_fk)
            self.write_to_db_result(attributes_for_table3, 'level3', kpi_fk)

    # def check_brand_block(self, object_field, values_list):
    #     if not self.potential_products:
    #         result = 'FALSE'
    #     else:
    #         sum_left = 0
    #         sum_right = 0
    #         potential_products_df = pd.DataFrame.from_dict(self.potential_products, orient='index')
    #         scenes_to_check = potential_products_df['scene_fk'].unique().tolist()
    #         # for scene in scenes_to_check
    #         shelves = potential_products_df['shelf'].unique().tolist()
    #         for shelf in shelves:
    #             temp = potential_products_df.loc[potential_products_df['shelf'] == shelf]
    #             # left side
    #             temp.sort_values(by=['left'], inplace=True)
    #             most_left_df = temp.loc[temp[object_field].isin([values_list])]
    #             most_left_value = most_left_df['left'].values[0]
    #             left_most_candidates = most_left_df.loc[most_left_df['left'] <= most_left_value]
    #             if not left_most_candidates.loc[left_most_candidates['product_type'].isin([EMPTY])].empty:
    #                 for i in reversed(left_most_candidates.index):
    #                     match = left_most_candidates.loc[[i]]
    #                     if match['product_type'][i] == EMPTY:
    #                         most_left_value = match['left'][i]
    #                     else:
    #                         break
    #             sum_left += most_left_value
    #             self.shelf_square_boundaries[shelf] = {'left': most_left_value}
    #             # right side
    #             temp.sort_values(by=['right'], inplace=True)
    #             most_right_df = temp.loc[temp[object_field].isin([values_list])]
    #             most_right_value = most_right_df['right'].values[-1]
    #             right_most_candidates = most_right_df.loc[most_left_df['right'] >= most_right_value]
    #             if not right_most_candidates.loc[right_most_candidates['product_type'].isin([EMPTY])].empty:
    #                 for i in right_most_candidates.index:
    #                     match = right_most_candidates.loc[[i]]
    #                     if match['product_type'][i] == EMPTY:
    #                         most_right_value = match['right'][i]
    #                     else:
    #                         break
    #             sum_right += most_right_value
    #             self.shelf_square_boundaries[shelf]['right'] = most_right_value
    #         empties_ratio = 1  # Start condition: Rectangle should not be checked
    #         average_left = 0
    #         average_right = 0
    #         if shelves:
    #             average_left = sum_left / float(len(shelves))
    #             average_right = sum_right / float(len(shelves))
    #             initial_square_df = potential_products_df.loc[(potential_products_df['left'] > average_left) &
    #                                                           (potential_products_df['right'] < average_right)]
    #             if not initial_square_df.empty:
    #                 total_products_in_square = len(initial_square_df.index)
    #                 total_empties_in_square = \
    #                     len(potential_products_df.loc[(potential_products_df['product_type'] == EMPTY)].index)
    #                 empties_ratio = total_empties_in_square / float(total_products_in_square)
    #         non_rect_conditions = (not initial_square_df.loc[
    #             ~(initial_square_df[object_field].isin([values_list]))].empty) \
    #                               or empties_ratio > ALLOWED_EMPTIES_RATIO or not shelves
    #         if non_rect_conditions:
    #             result = 'FALSE'
    #         else:
    #             average_width = initial_square_df['width'].mean()
    #             max_dev = ALLOWED_DEVIATION * average_width
    #             square_shelves_counter = 0
    #             for shelf in shelves:
    #                 if (abs(self.shelf_square_boundaries[shelf].get('left') - average_left)
    #                         + abs(self.shelf_square_boundaries[shelf].get('right') - average_right)) > max_dev:
    #                     square_shelves_counter += 1
    #             if square_shelves_counter != len(shelves):
    #                 result = 'FALSE'
    #             else:
    #                 result = 'TRUE'
    #
    #     return result

    def check_brand_block(self, object_field, values_list):
        if not self.potential_products:
            result = 'FALSE'
        else:
            scenes_results_dict = {}
            sum_left = 0
            sum_right = 0
            potential_products_df = pd.DataFrame.from_dict(self.potential_products, orient='index')
            scenes_to_check = potential_products_df['scene_fk'].unique().tolist()
            for scene in scenes_to_check:
                shelves = potential_products_df.loc[potential_products_df['scene_fk'] == scene][
                    'shelf'].unique().tolist()
                for shelf in shelves:
                    temp = potential_products_df.loc[
                        (potential_products_df['scene_fk'] == scene) & (potential_products_df['shelf'] == shelf)]
                    # left side
                    temp.sort_values(by=['left'], inplace=True)
                    most_left_df = temp.loc[temp[object_field].isin(values_list)]
                    most_left_bay = min(most_left_df['bay_number'].unique().tolist())
                    most_left_value = most_left_df.loc[most_left_df['bay_number'] == most_left_bay]['left'].values[0]
                    left_most_candidates = most_left_df.loc[
                        (most_left_df['bay_number'] == most_left_bay) & (most_left_df['left'] <= most_left_value)]
                    if not left_most_candidates.loc[left_most_candidates['product_type'].isin([EMPTY])].empty:
                        for i in reversed(left_most_candidates.index):
                            match = left_most_candidates.loc[[i]]
                            if match['product_type'][i] == EMPTY:
                                most_left_value = match['left'][i]
                            else:
                                break
                    sum_left += most_left_value
                    self.shelf_square_boundaries[shelf] = {'left': most_left_value}
                    # right side
                    temp.sort_values(by=['right'], inplace=True)
                    most_right_df = temp.loc[temp[object_field].isin(values_list)]
                    most_right_bay = max(most_right_df['bay_number'].unique().tolist())
                    most_right_value = most_right_df.loc[most_right_df['bay_number'] == most_right_bay]['right'].values[
                        -1]
                    # sum_of_px_to_add = most_right_df.loc[most_right_df['bay_number'] != most_left_bay]['shelf_px_total'].sum()
                    right_most_candidates = most_right_df.loc[
                        (most_right_df['bay_number'] == most_right_bay) & (most_right_df['right'] >= most_right_value)]
                    if not right_most_candidates.loc[right_most_candidates['product_type'].isin([EMPTY])].empty:
                        for i in right_most_candidates.index:
                            match = right_most_candidates.loc[[i]]
                            if match['product_type'][i] == EMPTY:
                                most_right_value = match['right'][i]
                            else:
                                break
                    sum_right += most_right_value
                    self.shelf_square_boundaries[shelf]['right'] = most_right_value
                empties_ratio = 1  # Start condition: Rectangle should not be checked
                average_left = 0
                average_right = 0
                if shelves:
                    average_left = sum_left / float(len(shelves))
                    average_right = sum_right / float(len(shelves))
                    initial_square_df = potential_products_df.loc[(potential_products_df['left'] > average_left) &
                                                                (potential_products_df['right'] < average_right)] # todo: why this is the calculation?
                    if not initial_square_df.empty:
                        total_products_in_square = len(initial_square_df.index)
                        total_empties_in_square = \
                            len(potential_products_df.loc[(potential_products_df['product_type'] == EMPTY)].index) # todo: remove stacking empty other
                        empties_ratio = total_empties_in_square / float(total_products_in_square)
                non_rect_conditions = (not initial_square_df.loc[
                    ~(initial_square_df[object_field].isin(values_list))].empty) \
                                      or empties_ratio > ALLOWED_EMPTIES_RATIO or not shelves
                if non_rect_conditions:
                    # scene_result = 'FALSE'
                    scenes_results_dict[scene] = 0
                else:
                    average_width = initial_square_df['width'].mean()
                    max_dev = ALLOWED_DEVIATION * average_width
                    square_shelves_counter = 0
                    for shelf in shelves:
                        if (abs(self.shelf_square_boundaries[shelf].get('left') - average_left)
                                + abs(self.shelf_square_boundaries[shelf].get('right') - average_right)) < max_dev:
                            square_shelves_counter += 1
                    if square_shelves_counter != len(shelves):
                        # scene_result = 'FALSE'
                        scenes_results_dict[scene] = 0
                    else:
                        # scene_result = 'TRUE'
                        scenes_results_dict[scene] = 1
            if sum(scenes_results_dict.values()) == len(scenes_to_check):
                result = 'TRUE'
            else:
                result = 'FALSE'

        return result

    def check_brand_block_2018(self, object_field, values_list, sub_brands_to_exclude, form_factors,
                               sub_brand_by_sub_cat={}):
        if not self.potential_products:
            result = 'FALSE'
        else:
            scenes_results_dict = {}
            self.shelf_square_boundaries = {}
            sum_left = 0
            sum_right = 0
            potential_products_df = pd.DataFrame.from_dict(self.potential_products, orient='index')
            scenes_to_check = potential_products_df['scene_fk'].unique().tolist()
            for scene in scenes_to_check:
                shelves = potential_products_df.loc[potential_products_df['scene_fk'] == scene][
                    'shelf'].unique().tolist()
                min_facings_on_shelf = 0
                relevant_shelf_counter = 0
                for shelf in shelves:
                    temp = potential_products_df.loc[
                        (potential_products_df['scene_fk'] == scene) & (potential_products_df['shelf'] == shelf)]
                    shelf_sub_category = temp['sub_category'].unique().tolist()
                    for category in shelf_sub_category:
                        if type(category) == str:
                            shelf_sub_category = category.upper()
                            break
                    if shelf_sub_category in sub_brand_by_sub_cat.keys() and not sub_brands_to_exclude:
                        sub_brands_to_exclude = sub_brand_by_sub_cat[shelf_sub_category]
                    # count facings on shelf:
                    facings_on_shelf = temp.loc[(temp['brand_name'].isin(values_list)) &
                                                (temp['stacking_layer'] == 1) &
                                                (temp['form_factor'].isin(form_factors)) &
                                                (~temp['sub_brand'].isin(sub_brands_to_exclude))]['brand_name'].count()
                    if facings_on_shelf:
                        relevant_shelf_counter += 1
                        if not min_facings_on_shelf:
                            min_facings_on_shelf = facings_on_shelf
                            min_shelf = shelf
                        else:
                            if facings_on_shelf < min_facings_on_shelf:
                                min_facings_on_shelf = facings_on_shelf
                                min_shelf = shelf
                        # left side
                        temp.sort_values(by=['left'], inplace=True)
                        most_left_df = temp.loc[(temp[object_field].isin(values_list)) &
                                                ((temp['form_factor'].isin(form_factors)) |
                                                 (temp['product_type'].isin([OTHER]))) &
                                                (~temp['sub_brand'].isin(sub_brands_to_exclude))]
                        most_left_bay = min(most_left_df['bay_number'].unique().tolist())
                        most_left_value = most_left_df.loc[most_left_df['bay_number'] == most_left_bay][
                            'left'].values[0]
                        left_most_candidates = most_left_df.loc[(most_left_df['bay_number'] == most_left_bay) &
                                                                (most_left_df['left'] <= most_left_value)]
                        if not left_most_candidates.loc[left_most_candidates['product_type'].isin([EMPTY])].empty:
                            for i in reversed(left_most_candidates.index):
                                match = left_most_candidates.loc[[i]]
                                if match['product_type'][i].isin(EMPTY):
                                    most_left_value = match['left'][i]
                                else:
                                    break
                        sum_left += most_left_value
                        self.shelf_square_boundaries[shelf] = {'left_boundary': most_left_value}
                        # right side
                        temp.sort_values(by=['right'], inplace=True)
                        most_right_df = temp.loc[(temp[object_field].isin(values_list)) &
                                                 ((temp['form_factor'].isin(form_factors)) |
                                                  (temp['product_type'].isin([OTHER]))) &
                                                 (~temp['sub_brand'].isin(sub_brands_to_exclude))]
                        most_right_bay = max(most_right_df['bay_number'].unique().tolist())
                        most_right_value = most_right_df.loc[most_right_df['bay_number'] == most_right_bay][
                            'right'].values[-1]
                        right_most_candidates = most_right_df.loc[
                            (most_right_df['bay_number'] == most_right_bay) &
                            (most_right_df['right'] >= most_right_value)]
                        if not right_most_candidates.loc[right_most_candidates['product_type'].isin([EMPTY])].empty:
                            for i in right_most_candidates.index:
                                match = right_most_candidates.loc[[i]]
                                if match['product_type'][i].isin(EMPTY):
                                    most_right_value = match['right'][i]
                                else:
                                    break
                        sum_right += most_right_value
                        self.shelf_square_boundaries[shelf]['right_boundary'] = most_right_value
                if relevant_shelf_counter == 1:
                    if min_facings_on_shelf > 4:
                        result = 'FALSE'
                        return result
                empties_ratio = 1  # Start condition: Rectangle should not be checked
                min_shelf_left = 0
                min_shelf_right = 0
                if relevant_shelf_counter == 0:
                    scenes_results_dict[scene] = 1
                    continue
                if shelves:
                    min_shelf_left = self.shelf_square_boundaries[min_shelf]['left_boundary']
                    min_shelf_right = self.shelf_square_boundaries[min_shelf]['right_boundary']
                    boundaries_list = list(self.shelf_square_boundaries.items())
                    boundaries_df = pd.DataFrame(boundaries_list, columns=['shelf','left_right_boundaries'])
                    boundaries_df['left_right_boundaries'] = boundaries_df['left_right_boundaries'].astype(str)
                    potential_products_df = pd.merge(potential_products_df, boundaries_df, on='shelf')
                    left_right_boundaries_df = pd.DataFrame(
                        [ast.literal_eval(i) for i in potential_products_df.left_right_boundaries.values])
                    potential_products_df = potential_products_df.drop('left_right_boundaries',axis=1)
                    final_potential_products_df = pd.concat([potential_products_df, left_right_boundaries_df], axis=1)
                    initial_square_df = final_potential_products_df.loc[
                        (final_potential_products_df['left'] >= final_potential_products_df['left_boundary']) &
                        (final_potential_products_df['right'] <= final_potential_products_df['right_boundary'])]
                    if not initial_square_df.empty:
                        total_products_in_square = len(initial_square_df.index)
                        total_empties_in_square = \
                            len(initial_square_df.loc[(initial_square_df['product_type'].isin([EMPTY])) |
                                                      ((initial_square_df['product_type'].isin([OTHER])) &
                                                       (~initial_square_df['brand_name'].isin(values_list))) &
                                                      (initial_square_df['stacking_layer'] == 1)].index)
                        empties_ratio = total_empties_in_square / float(total_products_in_square)
                non_rect_conditions = (not initial_square_df.loc[
                    ~((initial_square_df[object_field].isin(values_list)) &
                      ((initial_square_df['form_factor'].isin(form_factors)) |
                       (initial_square_df['product_type'].isin([OTHER]))) &
                      (~initial_square_df['sub_brand'].isin(sub_brands_to_exclude)))].empty) \
                                      or empties_ratio > ALLOWED_EMPTIES_RATIO or not shelves
                if non_rect_conditions:
                    # scene_result = 'FALSE'
                    scenes_results_dict[scene] = 0
                else:
                    average_width = initial_square_df['width'].mean()
                    max_dev = ALLOWED_DEVIATION_2018 * average_width
                    square_shelves_counter = 0
                    relevant_shelves = self.shelf_square_boundaries.keys()
                    for shelf in relevant_shelves:
                        if (abs(self.shelf_square_boundaries[shelf].get('left_boundary') - min_shelf_left)
                                    + abs(self.shelf_square_boundaries[shelf].get(
                                'right_boundary') - min_shelf_right)) < max_dev:
                            square_shelves_counter += 1
                    if square_shelves_counter != len(shelves):
                        scenes_results_dict[scene] = 0
                    else:
                        scenes_results_dict[scene] = 1
            if sum(scenes_results_dict.values()) == len(scenes_to_check):
                result = 'TRUE'
            else:
                result = 'FALSE'
        return result

    def initial_mapping_of_square(self, scenes, matches, object_field, values_list, p, form_factor=None,
                                  sub_brand_to_exclude=None, sub_brand_by_sub_cat={}):
        self.potential_products = {}
        if not scenes:
            return
        else:
            for scene in scenes:
                brand_counter = 0
                brands_presence_indicator = True
                scene_data = matches.loc[matches['scene_fk'] == scene]
                if p.get('Formula') != 'custom_mars_2_2018':
                    if form_factor:
                        scene_data = scene_data.loc[scene_data['form_factor'].isin(form_factor)]
                    scene_sub_category = scene_data['sub_category'].unique().tolist()
                    if scene_sub_category and scene_sub_category[0] is None:
                        scene_sub_category.remove(None)
                    if scene_sub_category:
                        scene_sub_category = scene_sub_category[0].upper()
                    if scene_sub_category in sub_brand_by_sub_cat.keys() and not sub_brand_to_exclude:
                        sub_brand_to_exclude = sub_brand_by_sub_cat[scene_sub_category]
                    if sub_brand_to_exclude:
                        scene_data = scene_data.loc[~scene_data['sub_brand'].isin(sub_brand_to_exclude)]
                shelves = scene_data['shelf_number'].unique().tolist()
                # unified_scene_set = set(scene_data[object_field]) & set(values_list)
                unified_scene_set = scene_data.loc[scene_data[object_field].isin(values_list)]
                if len(values_list) > 1:
                    for brand in values_list:
                        brand_df = scene_data.loc[scene_data[object_field] == brand]
                        if not brand_df.empty:
                            brand_counter += 1
                    if brand_counter != len(values_list):
                        brands_presence_indicator = False
                if unified_scene_set.empty or not brands_presence_indicator:
                    continue
                else:
                    is_sequential_shelves = False
                    for shelf_number in shelves:
                        shelf_data = scene_data.loc[scene_data['shelf_number'] == shelf_number]
                        bays = shelf_data['bay_number'].unique().tolist()
                        # for bay in bays:
                        temp_shelf_data = shelf_data.reset_index()
                        unified_shelf_set = shelf_data.loc[shelf_data[object_field].isin(values_list)]
                        # unified_shelf_set = set(shelf_data[object_field]) & set(values_list)
                        # if not unified_shelf_set:
                        if unified_shelf_set.empty:
                            if is_sequential_shelves:
                                # is_sequential_shelves = False
                                return
                            continue
                        else:
                            is_sequential_shelves = True
                            for i in temp_shelf_data.index:
                                match = temp_shelf_data.loc[[i]]
                                if match['bay_number'].values[0] == bays[0]:
                                    self.potential_products[match['scene_match_fk'].values[0]] = {
                                        'top': int(match[self.kpi_fetcher.TOP]),
                                        'bottom': int(
                                            match[self.kpi_fetcher.BOTTOM]),
                                        'left': int(
                                            match[self.kpi_fetcher.LEFT]),
                                        'right': int(
                                            match[self.kpi_fetcher.RIGHT]),
                                        object_field: match[object_field].values[0],
                                        'product_type': match['product_type'].values[0],
                                        'product_name': match['product_name'].values[0].encode('utf-8'),
                                        'shelf': shelf_number,
                                        'width': match['width_mm'].values[0],
                                        'bay_number': match['bay_number'].values[0],
                                        'scene_fk': scene,
                                        'form_factor': match['form_factor'].values[0],
                                        'sub_brand': match['sub_brand'].values[0],
                                        'stacking_layer': match['stacking_layer'].values[0],
                                        'shelf_px_total': int(match['shelf_px_total'].values[0]),
                                        'sub_category': match['sub_category'].values[0]}
                                else:
                                    sum_of_px_to_add = \
                                        temp_shelf_data.loc[
                                            temp_shelf_data['bay_number'] < match['bay_number'].values[0]][
                                            'shelf_px_total'].unique().sum()
                                    self.potential_products[match['scene_match_fk'].values[0]] = {
                                        'top': int(match[self.kpi_fetcher.TOP]),
                                        'bottom': int(
                                            match[self.kpi_fetcher.BOTTOM]),
                                        'left': int(
                                            match[self.kpi_fetcher.LEFT]) + int(sum_of_px_to_add),
                                        'right': int(
                                            match[self.kpi_fetcher.RIGHT]) + int(sum_of_px_to_add),
                                        object_field: match[object_field].values[0],
                                        'product_type': match['product_type'].values[0],
                                        'product_name': match['product_name'].values[0].encode('utf-8'),
                                        'shelf': shelf_number,
                                        'width': match['width_mm'].values[0],
                                        'bay_number': match['bay_number'].values[0],
                                        'scene_fk': scene,
                                        'form_factor': match['form_factor'].values[0],
                                        'sub_brand': match['sub_brand'].values[0],
                                        'stacking_layer': match['stacking_layer'].values[0],
                                        'shelf_px_total': int(match['shelf_px_total'].values[0])}

    @kpi_runtime()
    def multiple_brands_blocked_in_rectangle(self, params):
        for p in params.values()[0]:
            if p.get('Formula') != 'custom_mars_4' and p.get('Formula') != 'custom_mars_4_2018':
                continue
            brands_results_dict = {}
            values_list = str(p.get('Values')).split(', *')
            scenes = self.get_relevant_scenes(p)
            object_field = self.object_type_conversion[p.get('Type')]
            if p.get('Stacking'):
                matches = self.kpi_fetcher.get_filtered_matches()
            else:
                matches = self.kpi_fetcher.get_filtered_matches(include_stacking=False)
            if p.get('Sub brand to exclude'):
                sub_brands = [str(sub_brand) for sub_brand in p.get('Sub brand to exclude').split(", ")]
            else:
                sub_brands = []
            if p.get("Form Factor to include"):
                form_factors = [str(form_factor) for form_factor in p.get("Form Factor to include").split(", ")]
            else:
                form_factors = []
            # self.initial_mapping_of_square(scenes, matches, object_field, value, form_factor=form_factors,
            #                                sub_brand_to_exclude=sub_brands)
            for value in values_list:
                self.potential_products = {}
                if ',' in value:
                    sub_values_list = str(value).split(', ')
                    self.initial_mapping_of_square(scenes, matches, object_field, sub_values_list, p,
                                                   form_factor=form_factors,
                                                   sub_brand_to_exclude=sub_brands)
                    if p.get('Formula') == 'custom_mars_4':
                        brand_result = self.check_brand_block(object_field, sub_values_list)
                    else:
                        brand_result = self.check_brand_block_2018(object_field, sub_values_list, sub_brands, form_factors)
                else:
                    self.initial_mapping_of_square(scenes, matches, object_field, [value], p, form_factor=form_factors,
                                                   sub_brand_to_exclude=sub_brands)
                    if p.get('Formula') == 'custom_mars_4':
                        brand_result = self.check_brand_block(object_field, [value])
                    else:
                        brand_result = self.check_brand_block_2018(object_field, sub_values_list, sub_brands,
                                                                   form_factors)
                if brand_result == 'TRUE':
                    brands_results_dict[value] = 1
                else:
                    brands_results_dict[value] = 0

            if sum(brands_results_dict.values()) == len(values_list):
                result = 'TRUE'
            else:
                result = 'FALSE'
            kpi_fk = self.kpi_fetcher.get_kpi_fk(p.get('#Mars KPI NAME'))
            self.thresholds_and_results[p.get('#Mars KPI NAME')] = {'result': result}
            # Saving to old tables
            attributes_for_table2 = self.create_attributes_for_level2_df(p, 100, kpi_fk)
            self.write_to_db_result(attributes_for_table2, 'level2', kpi_fk)
            attributes_for_table3 = self.create_attributes_for_level3_df(p, 100, kpi_fk)
            self.write_to_db_result(attributes_for_table3, 'level3', kpi_fk)

    @kpi_runtime()
    def golden_shelves(self, params):
        """
        This function checks if a predefined product is present in golden shelves
        """
        for p in params.values()[0]:
            if p.get('Formula') != 'custom_mars_5':
                continue
            # golden_shelves_dict = {5: [3, 4], 6: [3, 4, 5], 7: [3, 4, 5], 8: [4, 5, 6]}
            values_list = str(p.get('Values')).split(', ')
            scenes = self.get_relevant_scenes(p)
            scenes_results_dict = {}
            for scene in scenes:
                matches = self.match_product_in_scene.loc[self.match_product_in_scene['scene_fk'] == scene]
                bays = matches['bay_number'].unique().tolist()
                bays_results_dict = {}
                for bay in bays:
                    shelves_result_dict = {}
                    bay_filter = matches.loc[matches['bay_number'] == bay]
                    shelves = bay_filter['shelf_number'].unique().tolist()
                    bay_golden_shelves = self.kpi_fetcher.get_golden_shelves(len(shelves))
                    for shelf in bay_golden_shelves:
                        object_facings = self.kpi_fetcher.get_object_facings(scenes, values_list, p.get('Type'),
                                                                             form_factor=[
                                                                                 p.get('Form Factor to include')],
                                                                             formula="number of SKUs",
                                                                             shelves=str(shelf))
                        if object_facings > 0:
                            shelves_result_dict[shelf] = 1
                        else:
                            shelves_result_dict[shelf] = 0
                    if 1 in shelves_result_dict.values():
                        bays_results_dict[bay] = 1
                    else:
                        bays_results_dict[bay] = 0
                if 1 in bays_results_dict.values():
                    scenes_results_dict[scene] = 1
                else:
                    scenes_results_dict[scene] = 0
            if 1 in scenes_results_dict.values():
                result = 'TRUE'
            else:
                result = 'FALSE'
            kpi_fk = self.kpi_fetcher.get_kpi_fk(p.get('#Mars KPI NAME'))
            self.thresholds_and_results[p.get('#Mars KPI NAME')] = {'result': result}
            # Saving to old tables
            attributes_for_table2 = self.create_attributes_for_level2_df(p, 100, kpi_fk)
            self.write_to_db_result(attributes_for_table2, 'level2', kpi_fk)
            attributes_for_table3 = self.create_attributes_for_level3_df(p, 100, kpi_fk)
            self.write_to_db_result(attributes_for_table3, 'level3', kpi_fk)

    @kpi_runtime()
    def facings_by_brand(self, params):
        for p in params.values()[0]:
            if p.get('Formula') != 'custom_mars_3' and p.get('Formula') != 'custom_mars_3_linear':
                continue
            brand_facings_dict = {}
            values_list = str(p.get('Values')).split(', ')
            scenes = self.get_relevant_scenes(p)
            if p.get("Form Factor to include"):
                form_factors = [str(form_factor) for form_factor in p.get("Form Factor to include").split(", ")]
            else:
                form_factors = []
            if p.get('Stacking'):
                include_stacking = True
            else:
                include_stacking = False
            linear = False
            if p.get('Formula') == 'custom_mars_3_linear':
                linear = True
            brand_category = p.get('Brand Category value')
            if brand_category and ', ' in brand_category:
                brand_category = brand_category.split(', ')
            object_facings = self.kpi_fetcher.get_object_facings(scenes, values_list, p.get('Type'),
                                                                 formula='number of facings',
                                                                 form_factor=form_factors,
                                                                 brand_category=brand_category,
                                                                 include_stacking=include_stacking,
                                                                 linear=linear)
            brands_to_check = self.scif.loc[self.scif['scene_id'].isin(scenes) &
                                            ~(self.scif['brand_name'].isin(values_list))][
                    'brand_name'].unique().tolist()
            for brand in brands_to_check:
                brand_facings = self.kpi_fetcher.get_object_facings(scenes, [brand], p.get('Type'),
                                                                    formula='number of facings',
                                                                    form_factor=form_factors,
                                                                    brand_category=brand_category,
                                                                    include_stacking=include_stacking,
                                                                    linear=linear)
                brand_facings_dict[brand] = brand_facings
            if brand_facings_dict.values():
                max_brand_facings = max(brand_facings_dict.values())
            else:
                max_brand_facings = 0
            if object_facings > max_brand_facings:
                result = 'TRUE'
            else:
                result = 'FALSE'
            kpi_fk = self.kpi_fetcher.get_kpi_fk(p.get('#Mars KPI NAME'))
            self.thresholds_and_results[p.get('#Mars KPI NAME')] = {'result': result}
            # Saving to old tables
            attributes_for_table2 = self.create_attributes_for_level2_df(p, 100, kpi_fk)
            self.write_to_db_result(attributes_for_table2, 'level2', kpi_fk)
            attributes_for_table3 = self.create_attributes_for_level3_df(p, 100, kpi_fk)
            self.write_to_db_result(attributes_for_table3, 'level3', kpi_fk)

    @kpi_runtime()
    def must_range_skus(self, params):
        for p in params.values()[0]:
            if p.get('Formula') != 'custom_mars_7':  # todo update in the file
                continue
            values_list = self.kpi_fetcher.get_must_range_skus_by_region_and_store(self.store_type, self.region,
                                                                                   p.get('#Mars KPI NAME'))
            scenes = self.get_relevant_scenes(p)
            if values_list:
                if p.get('#Mars KPI NAME') == 2317:
                    min_shelf, max_shelf = values_list.split('-')
                    min_shelf, max_shelf = int(min_shelf), int(max_shelf)
                    result = 'FALSE'
                    SKUs = p.get('Values').split('\n')
                    for scene in scenes:
                        scene_result = True
                        golden_shelf_match_products = self.match_product_in_scene[
                            (self.match_product_in_scene['scene_fk'] == scene) &
                            (self.match_product_in_scene['shelf_number_from_bottom'] >= min_shelf) &
                            (self.match_product_in_scene['shelf_number_from_bottom'] <= max_shelf)]
                        for SKU in SKUs:
                            is_sku_in_golden = True
                            filtered_scif_by_sku = self.scif[self.scif['product_ean_code'] == SKU]
                            if not filtered_scif_by_sku.empty:
                                product_fk = filtered_scif_by_sku['product_fk'].iloc[0]
                                if golden_shelf_match_products[
                                            golden_shelf_match_products['product_fk'] == product_fk].empty:
                                    is_sku_in_golden = False
                                    break
                            if not is_sku_in_golden:
                                scene_result = False
                                break
                        if scene_result:
                            result = 'TRUE'
                            break
                elif p.get('#Mars KPI NAME') == 2254:
                    type_value = str(p.get('Type'))
                    values = str(p.get('Values')).split(', ')
                    total_shelf_linear_size = self.calculate_layout_size(scenes, type_value, values)[0]
                    average_number_of_shelves_with_mars = self.calculate_linear_sos(scenes, type_value, values)
                    if average_number_of_shelves_with_mars:
                        mars_shelf_size2 = total_shelf_linear_size / average_number_of_shelves_with_mars
                        kpi_part_1 = self.dict_for_2254[2261] / self.dict_for_2254[2264] if self.dict_for_2254[
                                                                                              2264] > 0 else 0
                        kpi_part_2 = self.dict_for_2254[2265] / self.dict_for_2254[2351] if self.dict_for_2254[
                                                                                                2351] > 0 else 0
                        mars_shelf_size = kpi_part_1 + kpi_part_2
                        for row in values_list:
                            if row['shelf from'] <= mars_shelf_size < row['shelf to']:
                                result = str(row['result'])
                        if not result:
                            result = 'FALSE'
                    else:
                        result = 'FALSE'
                else:
                    sub_results = []
                    for value in values_list:
                        kpi_res = self.calculate_availability(p, scenes, formula='number of SKUs',
                                                              values_list=value.split('/'),
                                                              object_type='SKUs', include_stacking=True)
                        if kpi_res > 0:
                            sub_result = 1
                        else:
                            sub_result = 0
                        sub_results.append(sub_result)
                    sum_of_facings = sum(sub_results)
                    # if p.get('#Mars KPI NAME') == 2390:#in 2390 one missing is possible.
                    #     sum_of_facings += 1
                    if sum_of_facings >= len(values_list):
                        result = 'TRUE'
                    else:
                        result = 'FALSE'
            else:
                result = 'FALSE'
            kpi_fk = self.kpi_fetcher.get_kpi_fk(p.get('#Mars KPI NAME'))
            self.thresholds_and_results[p.get('#Mars KPI NAME')] = {'result': result}
            # Saving to old tables
            attributes_for_table2 = self.create_attributes_for_level2_df(p, 100, kpi_fk)
            self.write_to_db_result(attributes_for_table2, 'level2', kpi_fk)
            attributes_for_table3 = self.create_attributes_for_level3_df(p, 100, kpi_fk)
            self.write_to_db_result(attributes_for_table3, 'level3', kpi_fk)

    @kpi_runtime()
    def negative_neighbors(self, params):
        for p in params.values()[0]:
            if p.get('Formula') != 'custom_mars_6':
                continue
            object_field = self.object_type_conversion[p.get('Type')]
            values_list = str(p.get('Values')).split(', ')
            competitor_brands = str(p.get('competitor_brands')).split(', ')
            scenes = self.get_relevant_scenes(p)
            tested_filters = {object_field: values_list,
                              'category': p.get('Brand Category value')}
            # First check - negative adjacency to MARS products
            mars_anchor_filters = {'manufacturer_name': MARS}
            negative_mars_adjacency_result = self.calculate_non_proximity(tested_filters, mars_anchor_filters,
                                                                          scene_fk=scenes)

            # Second check - positive adjacency to competitor brands
            competitor_anchor_filters = {'brand_name': competitor_brands}
            direction_data2 = {'top': POSITIVE_ADJACENCY_RANGE,
                               'bottom': POSITIVE_ADJACENCY_RANGE,
                               'left': POSITIVE_ADJACENCY_RANGE,
                               'right': POSITIVE_ADJACENCY_RANGE}
            competitor_adjacency_result = self.calculate_relative_position(tested_filters,
                                                                           competitor_anchor_filters,
                                                                           direction_data2, scene_fk=scenes)
            if negative_mars_adjacency_result and competitor_adjacency_result:
                final_result = 'TRUE'
            else:
                final_result = 'FALSE'
            kpi_fk = self.kpi_fetcher.get_kpi_fk(p.get('#Mars KPI NAME'))
            self.thresholds_and_results[p.get('#Mars KPI NAME')] = {'result': final_result}
            # Saving to old tables
            attributes_for_table2 = self.create_attributes_for_level2_df(p, 100, kpi_fk)
            self.write_to_db_result(attributes_for_table2, 'level2', kpi_fk)
            attributes_for_table3 = self.create_attributes_for_level3_df(p, 100, kpi_fk)
            self.write_to_db_result(attributes_for_table3, 'level3', kpi_fk)

    def parse_filter_from_template(self, line):
        result_filter = {}
        for target_filter_line in line.split('. '):
            is_include = self.INCLUDE_FILTER
            filter_name = target_filter_line.split(': ')[0].lower()
            filter_values = target_filter_line.split(': ')[1].split(', ')
            if 'exclude ' in filter_name:
                is_include = self.EXCLUDE_FILTER
                filter_name = filter_name.replace('exclude ', '')
            result_filter[filter_name] = (filter_values, is_include)
        return result_filter

    @kpi_runtime()
    def get_total_linear(self, params):
        for p in params.values()[0]:
            if p.get('Formula') != 'total_linear':
                continue
            result = 'FALSE'
            targets, others, percent = p.get('Values').split('\n')
            target_filter = self.get_filter_condition(self.scif, **(self.parse_filter_from_template(targets)))
            other_filter = self.get_filter_condition(self.scif, **(self.parse_filter_from_template(others)))
            target_linear_size = self.calculate_layout_size_by_filters(target_filter)
            others_linear_size = self.calculate_layout_size_by_filters(other_filter)
            if others_linear_size > 0 and target_linear_size >= float(percent) * others_linear_size:
                result = 'TRUE'
            kpi_fk = self.kpi_fetcher.get_kpi_fk(p.get('#Mars KPI NAME'))
            self.thresholds_and_results[p.get('#Mars KPI NAME')] = {'result': result}
            # Saving to old tables
            attributes_for_table2 = self.create_attributes_for_level2_df(p, 100, kpi_fk)
            self.write_to_db_result(attributes_for_table2, 'level2', kpi_fk)
            attributes_for_table3 = self.create_attributes_for_level3_df(p, 100, kpi_fk)
            self.write_to_db_result(attributes_for_table3, 'level3', kpi_fk)

    @kpi_runtime()
    def get_placed_near(self, params):
        for p in params.values()[0]:
            if p.get('Formula') != 'placed_near':
                continue
            result = 'FALSE'
            targets, neighbors = p.get('Values').split('\n')
            target_filter = self.get_filter_condition(self.scif, **(self.parse_filter_from_template(targets)))
            neighbors_filters = self.get_filter_condition(self.scif, **(self.parse_filter_from_template(neighbors)))
            filtered_target_scif = self.scif[target_filter]
            filtered_neighbors_scif = self.scif[neighbors_filters]

            products_target = filtered_target_scif['product_fk'].tolist()
            products_neighbors = filtered_neighbors_scif['product_fk'].tolist()
            score = 0
            if products_target:
                score = self.calculate_block_together(allowed_products_filters={'product_fk': products_neighbors},
                                                  **({'product_fk': products_target}))
            if score:
                result = 'TRUE'
            kpi_fk = self.kpi_fetcher.get_kpi_fk(p.get('#Mars KPI NAME'))
            self.thresholds_and_results[p.get('#Mars KPI NAME')] = {'result': result}
            # Saving to old tables
            attributes_for_table2 = self.create_attributes_for_level2_df(p, 100, kpi_fk)
            self.write_to_db_result(attributes_for_table2, 'level2', kpi_fk)
            attributes_for_table3 = self.create_attributes_for_level3_df(p, 100, kpi_fk)
            self.write_to_db_result(attributes_for_table3, 'level3', kpi_fk)

    def separate_location_filters_from_product_filters(self, **filters):
        """
        This function gets scene-item-facts filters of all kinds, extracts the relevant scenes by the location filters,
        and returns them along with the product filters only.
        """
        relevant_scenes = self.scif[self.get_filter_condition(self.scif, **filters)]['scene_id'].unique()
        location_filters = {}
        for field in filters.keys():
            if field not in self.products.columns and field in self.scif.columns:
                location_filters[field] = filters.pop(field)
        return filters, relevant_scenes

    def get_scene_blocks(self, graph, allowed_products_filters=None, include_empty=EXCLUDE_EMPTY, **filters):
        """
        This function is a sub-function for Block Together. It receives a graph and filters and returns a list of
        clusters.
        """
        relevant_vertices = set(self.filter_vertices_from_graph(graph, **filters))
        if allowed_products_filters:
            allowed_vertices = self.filter_vertices_from_graph(graph, **allowed_products_filters)
        else:
            allowed_vertices = set()

        if include_empty == EXCLUDE_EMPTY:
            empty_vertices = {v.index for v in graph.vs.select(product_type='Empty')}
            allowed_vertices = set(allowed_vertices).union(empty_vertices)

        all_vertices = {v.index for v in graph.vs}
        vertices_to_remove = all_vertices.difference(relevant_vertices.union(allowed_vertices))
        graph.delete_vertices(vertices_to_remove)
        # removing clusters including 'allowed' SKUs only
        blocks = [block for block in graph.clusters() if set(block).difference(allowed_vertices)]
        return blocks, graph

    def calculate_block_together(self, allowed_products_filters=None, include_empty=EXCLUDE_EMPTY,
                                 minimum_block_ratio=1, result_by_scene=False, vertical=False, **filters):
        """
        :param vertical: if needed to check vertical block by average shelf
        :param allowed_products_filters: These are the parameters which are allowed to corrupt the block without failing it.
        :param include_empty: This parameter dictates whether or not to discard Empty-typed products.
        :param minimum_block_ratio: The minimum (block number of facings / total number of relevant facings) ratio
                                    in order for KPI to pass (if ratio=1, then only one block is allowed).
        :param result_by_scene: True - The result is a tuple of (number of passed scenes, total relevant scenes);
                                False - The result is True if at least one scene has a block, False - otherwise.
        :param filters: These are the parameters which the blocks are checked for.
        :return: see 'result_by_scene' above.
        """
        filters, relevant_scenes = self.separate_location_filters_from_product_filters(**filters)
        if len(relevant_scenes) == 0:
            if result_by_scene:
                return 0, 0
            else:
                Log.debug('Block Together: No relevant SKUs were found for these filters {}'.format(filters))
                return True
        number_of_blocked_scenes = 0
        cluster_ratios = []
        for scene in relevant_scenes:
            scene_graph = self.position_graphs.get(scene).copy()
            clusters, scene_graph = self.get_scene_blocks(scene_graph, allowed_products_filters=allowed_products_filters,
                                                          include_empty=include_empty, **filters)
            new_relevant_vertices = self.filter_vertices_from_graph(scene_graph, **filters)
            for cluster in clusters:
                relevant_vertices_in_cluster = set(cluster).intersection(new_relevant_vertices)
                if len(new_relevant_vertices) > 0:
                    cluster_ratio = len(relevant_vertices_in_cluster) / float(len(new_relevant_vertices))
                else:
                    cluster_ratio = 0
                cluster_ratios.append(cluster_ratio)
                if cluster_ratio >= minimum_block_ratio:
                    if result_by_scene:
                        number_of_blocked_scenes += 1
                        break
                    else:
                        if minimum_block_ratio == 1:
                            return True
                        else:
                            all_vertices = {v.index for v in scene_graph.vs}
                            non_cluster_vertices = all_vertices.difference(cluster)
                            scene_graph.delete_vertices(non_cluster_vertices)
                            if vertical:
                                return {'block': True, 'shelves': len(
                                    set(scene_graph.vs['shelf_number']))}
                            return cluster_ratio, scene_graph
        if result_by_scene:
            return number_of_blocked_scenes, len(relevant_scenes)
        else:
            if minimum_block_ratio == 1:
                return False
            elif cluster_ratios:
                return max(cluster_ratios)
            else:
                return None

    def calculate_non_proximity(self, tested_filters, anchor_filters, allowed_diagonal=False, **general_filters):
        """
        :param tested_filters: The tested SKUs' filters.
        :param anchor_filters: The anchor SKUs' filters.
        :param allowed_diagonal: True - a tested SKU can be in a direct diagonal from an anchor SKU in order
                                        for the KPI to pass;
                                 False - a diagonal proximity is NOT allowed.
        :param general_filters: These are the parameters which the general data frame is filtered by.
        :return:
        """
        direction_data = []
        if allowed_diagonal:
            direction_data.append({'top': (0, 1), 'bottom': (0, 1)})
            direction_data.append({'right': (0, 1), 'left': (0, 1)})
        else:
            direction_data.append({'top': (0, 1), 'bottom': (0, 1), 'right': (0, 1), 'left': (0, 1)})
        is_proximity = self.calculate_relative_position(tested_filters, anchor_filters, direction_data,
                                                        min_required_to_pass=1, **general_filters)
        return not is_proximity

    def calculate_relative_position(self, tested_filters, anchor_filters, direction_data, min_required_to_pass=1,
                                    **general_filters):
        """
        :param tested_filters: The tested SKUs' filters.
        :param anchor_filters: The anchor SKUs' filters.
        :param direction_data: The allowed distance between the tested and anchor SKUs.
                               In form: {'top': 4, 'bottom: 0, 'left': 100, 'right': 0}
                               Alternative form: {'top': (0, 1), 'bottom': (1, 1000), ...} - As range.
        :param min_required_to_pass: The number of appearances needed to be True for relative position in order for KPI
                                     to pass. If all appearances are required: ==a string or a big number.
        :param general_filters: These are the parameters which the general data frame is filtered by.
        :return: True if (at least) one pair of relevant SKUs fits the distance requirements; otherwise - returns False.
        """
        filtered_scif = self.scif[self.get_filter_condition(self.scif, **general_filters)]
        tested_scenes = filtered_scif[self.get_filter_condition(filtered_scif, **tested_filters)]['scene_id'].unique()
        anchor_scenes = filtered_scif[self.get_filter_condition(filtered_scif, **anchor_filters)]['scene_id'].unique()
        relevant_scenes = set(tested_scenes).intersection(anchor_scenes)

        if relevant_scenes:
            pass_counter = 0
            reject_counter = 0
            for scene in relevant_scenes:
                scene_graph = self.position_graphs.get(scene)
                tested_vertices = self.filter_vertices_from_graph(scene_graph, **tested_filters)
                anchor_vertices = self.filter_vertices_from_graph(scene_graph, **anchor_filters)
                for tested_vertex in tested_vertices:
                    for anchor_vertex in anchor_vertices:
                        moves = {'top': 0, 'bottom': 0, 'left': 0, 'right': 0}
                        path = scene_graph.get_shortest_paths(anchor_vertex, tested_vertex, output='epath')
                        if path:
                            path = path[0]
                            for edge in path:
                                moves[scene_graph.es[edge]['direction']] += 1
                            if self.validate_moves(moves, direction_data):
                                pass_counter += 1
                                if isinstance(min_required_to_pass, int) and pass_counter >= min_required_to_pass:
                                    return True
                            else:
                                reject_counter += 1
                        else:
                            Log.debug('Tested and Anchor have no direct path')
            if pass_counter > 0 and reject_counter == 0:
                return True
            else:
                return False
        else:
            Log.debug('None of the scenes contain both anchor and tested SKUs')
            return False

    def get_filter_condition(self, df, **filters):
        """
        :param df: The data frame to be filters.
        :param filters: These are the parameters which the data frame is filtered by.
                       Every parameter would be a tuple of the value and an include/exclude flag.
                       INPUT EXAMPLE (1):   manufacturer_name = ('Diageo', DIAGEOAUGENERALToolBox.INCLUDE_FILTER)
                       INPUT EXAMPLE (2):   manufacturer_name = 'Diageo'
        :return: a filtered Scene Item Facts data frame.
        """
        if not filters:
            return df['pk'].apply(bool)
        if self.facings_field in df.keys():
            filter_condition = (df[self.facings_field] > 0)
        else:
            filter_condition = None
        for field in filters.keys():
            if field in df.keys():
                if isinstance(filters[field], tuple):
                    value, exclude_or_include = filters[field]
                else:
                    value, exclude_or_include = filters[field], self.INCLUDE_FILTER
                if not value:
                    continue
                if not isinstance(value, list):
                    value = [value]
                if exclude_or_include == self.INCLUDE_FILTER:
                    condition = (df[field].isin(value))
                elif exclude_or_include == self.EXCLUDE_FILTER:
                    condition = (~df[field].isin(value))
                elif exclude_or_include == self.CONTAIN_FILTER:
                    condition = (df[field].str.contains(value[0], regex=False))
                    for v in value[1:]:
                        condition |= df[field].str.contains(v, regex=False)
                else:
                    continue
                if filter_condition is None:
                    filter_condition = condition
                else:
                    filter_condition &= condition
            else:
                Log.warning('field {} is not in the Data Frame'.format(field))

        return filter_condition

    def filter_vertices_from_graph(self, graph, **filters):
        """
        This function is given a graph and returns a set of vertices calculated by a given set of filters.
        """
        vertices_indexes = None
        for field in filters.keys():
            field_vertices = set()
            values = filters[field] if isinstance(filters[field], (list, tuple)) else [filters[field]]
            for value in values:
                vertices = [v.index for v in graph.vs.select(**{field: value})]
                field_vertices = field_vertices.union(vertices)
            if vertices_indexes is None:
                vertices_indexes = field_vertices
            else:
                vertices_indexes = vertices_indexes.intersection(field_vertices)
        vertices_indexes = vertices_indexes if vertices_indexes is not None else [v.index for v in graph.vs]
        return list(vertices_indexes)

    @staticmethod
    def validate_moves(moves, direction_data):
        """
        This function checks whether the distance between the anchor and the tested SKUs fits the requirements.
        """
        direction_data = direction_data if isinstance(direction_data, (list, tuple)) else [direction_data]
        validated = False
        for data in direction_data:
            data_validated = True
            for direction in moves.keys():
                allowed_moves = data.get(direction, (0, 0))
                min_move, max_move = allowed_moves if isinstance(allowed_moves, tuple) else (0, allowed_moves)
                if not min_move <= moves[direction] <= max_move:
                    data_validated = False
                    break
            if data_validated:
                validated = True
                break
        return validated

    def calculate_score(self, kpi_total_res, params):
        """
        This function calculates score according to predefined score functions

        """
        kpi_name = params.get('#Mars KPI NAME')
        score = 0  # default score
        if params.get('Answer type') == 'Int':
            score = int(kpi_total_res)
            self.thresholds_and_results[kpi_name] = {'result': int(kpi_total_res)}
        elif params.get('Answer type') == 'Float':
            score = kpi_total_res
            self.thresholds_and_results[kpi_name] = {'result': kpi_total_res}
        elif params.get('Answer type') == 'Boolean':
            if kpi_total_res == 0:
                self.thresholds_and_results[kpi_name] = {'result': 'FALSE'}
            else:
                self.thresholds_and_results[kpi_name] = {'result': 'TRUE'}
        else:
            score = kpi_total_res

        return score

    def write_to_db_result(self, df=None, level=None, kps_name_temp=None):
        """
        This function writes KPI results to old tables

        """
        if level == 'level3':
            df['atomic_kpi_fk'] = self.kpi_fetcher.get_atomic_kpi_fk(df['name'][0])
            df['kpi_fk'] = df['kpi_fk'][0]
            df_dict = df.to_dict()
            df_dict.pop('name', None)
            query = insert(df_dict, KPI_RESULT)
            self.kpi_results_queries.append(query)
        elif level == 'level2':
            kpi_name = df['kpk_name'][0]
            df['kpi_fk'] = self.kpi_fetcher.get_kpi_fk(kpi_name)
            df_dict = df.to_dict()
            # df_dict.pop("kpk_name", None)
            query = insert(df_dict, KPK_RESULT)
            self.kpi_results_queries.append(query)
        elif level == 'level1':
            df['kpi_set_fk'] = self.kpi_fetcher.get_kpi_set_fk()
            df_dict = df.to_dict()
            query = insert(df_dict, KPS_RESULT)
            self.kpi_results_queries.append(query)

    def insert_results_data(self, query):
        start_time = datetime.datetime.utcnow()
        cur = self.rds_conn.db.cursor()
        cur.execute(query)
        self.rds_conn.db.commit()
        self.writing_to_db_time += datetime.datetime.utcnow() - start_time
        return cur.lastrowid

    def create_attributes_for_level2_df(self, params, score, kpi_fk):
        """
        This function creates a data frame with all attributes needed for saving in level 2 tables

        """
        attributes_for_table2 = pd.DataFrame([(self.session_uid, self.store_id,
                                               self.visit_date.isoformat(), kpi_fk, params.get('#Mars KPI NAME'),
                                               score)],
                                             columns=['session_uid', 'store_fk', 'visit_date', 'kpi_fk',
                                                      'kpk_name', 'score'])

        return attributes_for_table2

    def create_attributes_for_level3_df(self, params, score, kpi_fk):
        """
        This function creates a data frame with all attributes needed for saving in level 3 tables

        """
        if self.thresholds_and_results.get(params.get('#Mars KPI NAME')):
            result = self.thresholds_and_results[params.get('#Mars KPI NAME')]['result']
            # threshold = self.thresholds_and_results[params.get('#Mars KPI NAME')]['threshold']
        else:
            result = threshold = 0
        attributes_for_table3 = pd.DataFrame([(params.get('KPI Display name RU').encode('utf-8').replace("'","''"),
                                               self.session_uid, self.set_name, self.store_id,
                                               self.visit_date.isoformat(), datetime.datetime.utcnow().isoformat(),
                                               score, kpi_fk, None, result, params.get('#Mars KPI NAME'))],
                                             columns=['display_text', 'session_uid', 'kps_name',
                                                      'store_fk', 'visit_date',
                                                      'calculation_time', 'score', 'kpi_fk',
                                                      'atomic_kpi_fk', 'result', 'name'])

        return attributes_for_table3

    def get_set(self):
        kpi_set = self.kpi_fetcher.get_session_set(self.session_uid)

        return kpi_set['additional_attribute_12'][0]

    def commit_results_data(self):
        self.rds_conn.disconnect_rds()
        self.rds_conn = AwsProjectConnector(self.project_name, DbUsers.CalculationEng)
        cur = self.rds_conn.db.cursor()
        delete_queries = self.kpi_fetcher.get_delete_session_results(self.session_uid)
        for query in delete_queries:
            cur.execute(query)
        for query in self.kpi_results_queries:
            try:
                cur.execute(query)
            except:
                Log.info('Execution failed for the following query: {}'.format(query))
        self.rds_conn.db.commit()
