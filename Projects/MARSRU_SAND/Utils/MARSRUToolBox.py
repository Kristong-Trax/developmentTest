# -*- coding: utf-8 -*-
import datetime

import pandas as pd
from Trax.Algo.Calculations.Core.Constants import Fields as Fd
from Trax.Algo.Calculations.Core.DataProvider import Data, Keys
from Trax.Algo.Calculations.Core.Shortcuts import SessionInfo, BaseCalculationsGroup
from Trax.Cloud.Services.Connector.Keys import DbUsers
from Trax.Data.Orm.OrmCore import OrmSession
from Trax.Data.Projects.ProjectConnector import AwsProjectConnector
from Trax.Data.Utils.MySQLservices import get_table_insertion_query as insert
from Trax.Utils.Logging.Logger import Log
from Projects.MARSRU_SAND.MARSRUFetcher import MARSRU_SANDMARSRUKPIFetcher
from Projects.MARSRU_SAND.Utils.MARSRUJSON import MARSRU_SANDMARSRUJsonGenerator
from Projects.MARSRU_SAND.Utils.PositionGraph import MARSRU_SANDPositionGraphs

__author__ = 'urid'

BINARY = 'BINARY'
PROPORTIONAL = 'PROPORTIONAL'
CONDITIONAL_PROPORTIONAL = 'CONDITIONAL PROPORTIONAL'
MARS = 'Mars'
KPI_RESULT = 'report.kpi_results'
KPK_RESULT = 'report.kpk_results'
KPS_RESULT = 'report.kps_results'
EMPTY = 'Empty'
ALLOWED_EMPTIES_RATIO = 0.2
ALLOWED_DEVIATION = 2
NEGATIVE_ADJACENCY_RANGE = (2, 1000)
POSITIVE_ADJACENCY_RANGE = (0, 1)


class MARSRU_SANDMARSRUKPIToolBox:
    def __init__(self, data_provider, output, set_name=None):
        self.data_provider = data_provider
        self.output = output
        self.products = self.data_provider[Data.ALL_PRODUCTS]
        self.k_engine = BaseCalculationsGroup(data_provider, output)
        self.project_name = data_provider.project_name
        self.session_uid = self.data_provider.session_uid
        self.products = self.data_provider[Data.ALL_PRODUCTS]
        self.match_product_in_scene = self.data_provider[Data.MATCHES]
        self.templates = self.data_provider[Data.ALL_TEMPLATES]
        self.visit_date = self.data_provider[Data.VISIT_DATE]
        self.scenes_info = self.data_provider[Data.SCENES_INFO]
        self.rds_conn = AwsProjectConnector(self.project_name, DbUsers.CalculationEng)
        self.session_info = SessionInfo(data_provider)
        self.store_id = self.data_provider[Data.STORE_FK]
        self.scif = self.data_provider[Data.SCENE_ITEM_FACTS]
        self.set_name = set_name
        self.kpi_fetcher = MARSRU_SANDMARSRUKPIFetcher(self.project_name, self.scif, self.match_product_in_scene,
                                                       self.set_name, self.products, self.session_uid)
        self.survey_response = self.data_provider[Data.SURVEY_RESPONSES]
        self.sales_rep_fk = self.data_provider[Data.SESSION_INFO]['s_sales_rep_fk'].iloc[0]
        self.session_fk = self.data_provider[Data.SESSION_INFO]['pk'].iloc[0]
        self.store_type = self.data_provider[Data.STORE_INFO]['store_type'].iloc[0]
        # self.region = self.data_provider[Data.STORE_INFO]['region_name'].iloc[0]
        self.region = self.get_store_Att5()
        self.thresholds_and_results = {}
        self.result_df = []
        self.writing_to_db_time = datetime.timedelta(0)
        # self.match_product_in_probe_details = self.kpi_fetcher.get_match_product_in_probe_details(self.session_uid)
        self.kpi_results_queries = []
        # self.position_graphs = MARSRU_SANDPositionGraphs(self.data_provider)
        self.potential_products = {}
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

    def get_store_Att5(self):
        store_att5 = self.kpi_fetcher.get_store_att5(self.store_id)

        return store_att5

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
            kpi_total_res = 0
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
            score = self.calculate_score(kpi_total_res, p)
            # Saving to old tables
            attributes_for_table2 = self.create_attributes_for_level2_df(p, score, kpi_fk)
            self.write_to_db_result(attributes_for_table2, 'level2', kpi_fk)
            if not is_atomic:  # saving also to level3 in case this KPI has only one level
                attributes_for_table3 = self.create_attributes_for_level3_df(p, score, kpi_fk)
                self.write_to_db_result(attributes_for_table3, 'level3', kpi_fk)

                # set_total_res += score * p.get('KPI Weight')

        return set_total_res

    def calculate_availability(self, params, scenes=[], formula=None, values_list=None):
        if not values_list:
            values_list = str(params.get('Values')).split(', ')
        if not formula:
            formula = params.get('Formula')
        # object_static_list = self.get_static_list(params.get('Type'))
        include_stacking = False
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

        object_facings = self.kpi_fetcher.get_object_facings(scenes, values_list, params.get('Type'),
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
                    if self.calculate_availability(p_copy, scenes=[scene]) > 0:
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

    def check_survey_answer(self, params):
        """
        This function is used to calculate survey answer according to given target

        """
        score = False
        d = {'Yes': u'да', 'No': u'Нет'}
        for p in params.values()[0]:
            kpi_total_res = 0
            score = 0  # default score
            if p.get('Type') != 'SURVEY' or p.get('Formula') != 'answer for survey':
                continue
            survey_data = self.survey_response.loc[self.survey_response['question_text'] == p.get('Values')]
            if not survey_data['selected_option_text'].empty:
                result = survey_data['selected_option_text'].values[0]
                # targets = [d.get(target) if target in d.keys() else target
                #            for target in str(p.get('Target')).split(", ")]
                if p.get('Answer type') == 'Boolean':
                    if result == d.get('Yes'):
                        self.thresholds_and_results[p.get('#Mars KPI NAME')] = {'result': 'TRUE'}
                        # score = 'TRUE'
                    else:
                        # score = 'FALSE'
                        self.thresholds_and_results[p.get('#Mars KPI NAME')] = {'result': 'FALSE'}
                elif p.get('Answer type') == 'List':
                    # result = ','.join([str(result_value.encode('utf-8')) for result_value in
                    #                    survey_data['selected_option_text'].values])
                    # result = str([result_value for result_value in survey_data['selected_option_text'].values])
                    result = self.kpi_fetcher.get_survey_answers_codes(survey_data['code'].values[0], survey_data['selected_option_text'].values)

                    self.thresholds_and_results[p.get('#Mars KPI NAME')] = {'result': result}
                    # score = result
                elif p.get('Answer type') == 'Int':
                    result = int(survey_data['number_value'].values[0])

                    self.thresholds_and_results[p.get('#Mars KPI NAME')] = {'result': result}
                else:
                    Log.info('The answer type {} is not defined for surveys'.format(params.get('Answer type')))
                    continue
            else:
                self.thresholds_and_results[p.get('#Mars KPI NAME')] = {'result': 'null'}
                Log.warning('No survey data for this session')
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
            max_price = self.kpi_fetcher.get_object_price(scenes, values_list, p.get('Type'),
                                                          self.match_product_in_scene)
            is_atomic = False
            kpi_fk = self.kpi_fetcher.get_kpi_fk(p.get('#Mars KPI NAME'))
            if not max_price:
                max_price_for_db = 0
            else:
                max_price_for_db = max_price
            score = self.calculate_score(max_price_for_db, p)
            # Saving to old tables
            attributes_for_table2 = self.create_attributes_for_level2_df(p, score, kpi_fk)
            self.write_to_db_result(attributes_for_table2, 'level2', kpi_fk)
            if not is_atomic:  # saving also to level3 in case this KPI has only one level
                attributes_for_table3 = self.create_attributes_for_level3_df(p, score, kpi_fk)
                self.write_to_db_result(attributes_for_table3, 'level3', kpi_fk)

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

    def check_layout_size(self, params):
        for p in params.values()[0]:
            if p.get('Formula') != 'layout size':
                continue
            values_list = str(p.get('Values')).split(', ')
            scenes = self.get_relevant_scenes(p)
            linear_size = self.calculate_layout_size(scenes, p.get('Type'), values_list)
            is_atomic = False
            kpi_fk = self.kpi_fetcher.get_kpi_fk(p.get('#Mars KPI NAME'))

            score = self.calculate_score(linear_size, p)
            # Saving to old tables
            attributes_for_table2 = self.create_attributes_for_level2_df(p, score, kpi_fk)
            self.write_to_db_result(attributes_for_table2, 'level2', kpi_fk)
            if not is_atomic:  # saving also to level3 in case this KPI has only one level
                attributes_for_table3 = self.create_attributes_for_level3_df(p, score, kpi_fk)
                self.write_to_db_result(attributes_for_table3, 'level3', kpi_fk)

    def calculate_layout_size(self, scenes, object_type, values):
        object_field = self.object_type_conversion[object_type]
        final_linear_size = 0
        for scene in scenes:
            if object_type == 'MAN in CAT':
                filtered_scif = self.scif.loc[
                    (self.scif['scene_id'] == scene) & (self.scif[object_field].isin(values)) & (
                        self.scif['manufacturer_name'] == MARS)]
            else:
                filtered_scif = self.scif.loc[(self.scif['scene_id'] == scene) & (self.scif[object_field].isin(values))]
            final_linear_size += filtered_scif['gross_len_ign_stack'].sum()

        return float(final_linear_size / 1000)

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

    def brand_blocked_in_rectangle(self, params):
        for p in params.values()[0]:
            if p.get('Formula') != 'custom_mars_2':
                continue
            brands_results_dict = {}
            values_list = str(p.get('Values')).split(', ')
            scenes = self.get_relevant_scenes(p)
            object_field = self.object_type_conversion[p.get('Type')]
            if p.get('Stacking'):
                matches = self.kpi_fetcher.get_filtered_matches()
            else:
                matches = self.kpi_fetcher.get_filtered_matches(include_stacking=False)
            sub_brands = [str(sub_brand) for sub_brand in p.get('Sub brand to exclude').split(", ")]
            form_factors = [str(form_factor) for form_factor in p.get("Form Factor to include").split(", ")]
            # self.initial_mapping_of_square(scenes, matches, object_field, value, form_factor=form_factors,
            #                                sub_brand_to_exclude=sub_brands)
            for value in values_list:
                self.potential_products = {}
                self.initial_mapping_of_square(scenes, matches, object_field, [value], form_factor=form_factors,
                                               sub_brand_to_exclude=sub_brands)
                brand_result = self.check_brand_block(object_field, [value])
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
                                                                  (potential_products_df['right'] < average_right)]
                    if not initial_square_df.empty:
                        total_products_in_square = len(initial_square_df.index)
                        total_empties_in_square = \
                            len(potential_products_df.loc[(potential_products_df['product_type'] == EMPTY)].index)
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

    def initial_mapping_of_square(self, scenes, matches, object_field, values_list, form_factor=None,
                                  sub_brand_to_exclude=None):
        self.potential_products = {}
        if not scenes:
            return
        else:
            for scene in scenes:
                brand_counter = 0
                brands_presence_indicator = True
                scene_data = matches.loc[matches['scene_fk'] == scene]
                if sub_brand_to_exclude:
                    scene_data = scene_data.loc[~scene_data['sub_brand_name'].isin(sub_brand_to_exclude)]
                if form_factor:
                    scene_data = scene_data.loc[scene_data['form_factor'].isin(form_factor)]
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
                                        'product_name': match['product_name'].values[0],
                                        'shelf': shelf_number,
                                        'width': match['width_mm'].values[0],
                                        'bay_number': match['bay_number'].values[0],
                                        'scene_fk': scene,
                                        'shelf_px_total': int(match['shelf_px_total'].values[0])}
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
                                        'product_name': match['product_name'].values[0],
                                        'shelf': shelf_number,
                                        'width': match['width_mm'].values[0],
                                        'bay_number': match['bay_number'].values[0],
                                        'scene_fk': scene,
                                        'shelf_px_total': int(match['shelf_px_total'].values[0])}

    def multiple_brands_blocked_in_rectangle(self, params):
        for p in params.values()[0]:
            if p.get('Formula') != 'custom_mars_4':
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
                    self.initial_mapping_of_square(scenes, matches, object_field, sub_values_list,
                                                   form_factor=form_factors,
                                                   sub_brand_to_exclude=sub_brands)
                    brand_result = self.check_brand_block(object_field, sub_values_list)
                else:
                    self.initial_mapping_of_square(scenes, matches, object_field, [value], form_factor=form_factors,
                                                   sub_brand_to_exclude=sub_brands)
                    brand_result = self.check_brand_block(object_field, [value])
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

    def facings_by_brand(self, params):
        for p in params.values()[0]:
            if p.get('Formula') != 'custom_mars_3':
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
            object_facings = self.kpi_fetcher.get_object_facings(scenes, values_list, p.get('Type'),
                                                                 formula='number of facings',
                                                                 form_factor=form_factors,
                                                                 brand_category=p.get('Brand Category value'),
                                                                 include_stacking=include_stacking)
            brands_to_check = \
                self.scif.loc[self.scif['scene_id'].isin(scenes) & ~(self.scif['brand_name'].isin(values_list))][
                    'brand_name'].unique().tolist()
            for brand in brands_to_check:
                brand_facings = self.kpi_fetcher.get_object_facings(scenes, [brand], p.get('Type'),
                                                                    formula='number of facings',
                                                                    form_factor=form_factors,
                                                                    brand_category=p.get('Brand Category value'),
                                                                    include_stacking=include_stacking)
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

    def must_range_skus(self, params):
        for p in params.values()[0]:
            if p.get('Formula') != 'custom_mars_7':  # todo update in the file
                continue
            values_list = self.kpi_fetcher.get_must_range_skus_by_region_and_store(self.store_type, self.region,
                                                                                   p.get('#Mars KPI NAME'))
            scenes = self.get_relevant_scenes(p)
            if values_list:
                kpi_res = self.calculate_availability(p, scenes, formula='number of SKUs', values_list=values_list)
                if kpi_res >= len(values_list):
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

    def negative_neighbors(self, params):
        for p in params.values()[0]:
            if p.get('Formula') != 'custom_mars_6':
                continue
            object_field = self.object_type_conversion[p.get('Type')]
            values_list = str(p.get('Values')).split(', ')
            competitor_brands = str(p.get('competitor_brands')).split(', ')
            scenes = self.get_relevant_scenes(p)
            tested_filters = self.scif.loc[(self.scif['scene_id'].isin(scenes)) &
                                           (self.scif[object_field].isin(values_list)) & (
                                               self.scif['category'] == p.get('Brand Category value'))]
            # First check - negative adjacency to MARS products
            mars_anchor_filters = self.scif.loc[
                (self.scif['scene_id'].isin(scenes)) & (self.scif['manufacturer_name'] == MARS)]
            direction_data1 = {'top': NEGATIVE_ADJACENCY_RANGE,
                               'bottom': NEGATIVE_ADJACENCY_RANGE,
                               'left': NEGATIVE_ADJACENCY_RANGE,
                               'right': NEGATIVE_ADJACENCY_RANGE}
            if (not tested_filters.empty) and (not mars_anchor_filters.empty):
                negative_mars_adjacency_result = self.calculate_relative_position(tested_filters, mars_anchor_filters,
                                                                                  direction_data1, scenes,
                                                                                  adjacency=False)
            else:
                negative_mars_adjacency_result = False
            # Second check - positive adjacency to competitor brands
            competitor_anchor_filters = self.scif.loc[
                (self.scif['scene_id'].isin(scenes)) & (self.scif['brand_name'].isin(competitor_brands))]
            direction_data2 = {'top': POSITIVE_ADJACENCY_RANGE,
                               'bottom': POSITIVE_ADJACENCY_RANGE,
                               'left': POSITIVE_ADJACENCY_RANGE,
                               'right': POSITIVE_ADJACENCY_RANGE}
            if (not tested_filters.empty) and (not competitor_anchor_filters.empty):
                competitor_adjacency_result = self.calculate_relative_position(tested_filters,
                                                                               competitor_anchor_filters,
                                                                               direction_data2, scenes,
                                                                               adjacency=True)
            else:
                competitor_adjacency_result = False
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

    def calculate_relative_position(self, tested_filters, anchor_filters, direction_data, scenes, adjacency=True,
                                    **general_filters):
        """
        :param tested_filters: The tested SKUs' filters.
        :param anchor_filters: The anchor SKUs' filters.
        :param direction_data: The allowed distance between the tested and anchor SKUs.
                               In form: {'top': 4, 'bottom: 0, 'left': 100, 'right': 0}
        :return: True if (at least) one pair of relevant SKUs fits the distance requirements; otherwise - returns False.
        """
        if scenes:
            for scene in scenes:
                scene_graph = self.position_graphs.position_graphs.get(scene)
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
                            if adjacency:
                                if self.validate_moves(moves, direction_data):
                                    return True
                            else:
                                if not self.validate_moves(moves, direction_data):
                                    return False
        else:
            Log.debug('None of the scenes contain both anchor and tested SKUs')
        if adjacency:
            return False
        else:
            return True

    @staticmethod
    def filter_vertices_from_graph(graph, **filters):
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
        return vertices_indexes

    @staticmethod
    def validate_moves(moves, direction_data):
        """
        This function checks whether the distance between the anchor and the tested SKUs fits the requirements.
        """
        for direction in moves.keys():
            if moves[direction] > 0:
                if direction_data[direction][0] <= moves[direction] <= direction_data[direction][1]:
                    return False
        return True

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
        attributes_for_table3 = pd.DataFrame([(params.get('KPI Display name RU').encode('utf-8'),
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
        cur = self.rds_conn.db.cursor()
        delete_queries = self.kpi_fetcher.get_delete_session_results(self.session_uid)
        for query in delete_queries:
            cur.execute(query)
        for query in self.kpi_results_queries:
            cur.execute(query)
        self.rds_conn.db.commit()
