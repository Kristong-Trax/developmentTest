# -*- coding: utf-8 -*-

import os
import datetime
# from datetime import datetime
import pandas as pd
import numpy as np

from Trax.Algo.Calculations.Core.Constants import Fields as Fd
from Trax.Algo.Calculations.Core.DataProvider import Data, Keys
from Trax.Algo.Calculations.Core.Shortcuts import SessionInfo, BaseCalculationsGroup
from Trax.Utils.Conf.Keys import DbUsers
from Trax.Data.Projects.Connector import ProjectConnector
from Trax.Data.Orm.OrmCore import OrmSession
from Trax.Data.Utils.MySQLservices import get_table_insertion_query as insert
from Trax.Utils.Logging.Logger import Log

from Projects.CCRU.Fetcher import CCRUCCHKPIFetcher
from Projects.CCRU.Utils.ExecutionContract import CCRUContract
from Projects.CCRU.Utils.TopSKU import CCRUTopSKUAssortment

__author__ = 'urid'

BINARY = 'BINARY'
PROPORTIONAL = 'PROPORTIONAL'
CONDITIONAL_PROPORTIONAL = 'CONDITIONAL PROPORTIONAL'
KPI_RESULT = 'report.kpi_results'
KPK_RESULT = 'report.kpk_results'
KPS_RESULT = 'report.kps_results'
CUSTOM_GAPS_TABLE = 'pservice.custom_gaps'
KPI_CONVERSION_PATH = os.path.join(os.path.dirname(os.path.realpath(__file__)), '..', 'Data', 'KPIConvesion2018.xlsx')
TARGET_EXECUTION = 'Target Execution 2018'
CONTRACT_SET_NAME = 'Contract Execution 2018'


# def log_runtime(description, log_start=False):
#     def decorator(func):
#         def wrapper(*args, **kwargs):
#             calc_start_time = datetime.utcnow()
#             if log_start:
#                 Log.info('{} started at {}'.format(description, calc_start_time))
#             result = func(*args, **kwargs)
#             calc_end_time = datetime.utcnow()
#             Log.info('{} took {}'.format(description, calc_end_time - calc_start_time))
#             return result
#         return wrapper
#     return decorator


class CCRUKPIToolBox:
    def __init__(self, data_provider, output, ps_data_provider, set_name=None):
        self.data_provider = data_provider
        self.ps_data_provider = ps_data_provider
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
        # self.rds_conn = AwsProjectConnector(self.project_name, DbUsers.CalculationEng)
        self.rds_conn = self.rds_connection()
        self.session_info = SessionInfo(data_provider)
        self.store_id = self.data_provider[Data.STORE_FK]
        self.scif = self.data_provider[Data.SCENE_ITEM_FACTS]
        if set_name is None:
            self.set_name = self.get_set(self.visit_date)
        else:
            self.set_name = set_name
        self.kpi_fetcher = CCRUCCHKPIFetcher(self.project_name, self.scif, self.match_product_in_scene,
                                                  self.set_name, self.products)
        self.survey_response = self.data_provider[Data.SURVEY_RESPONSES]
        self.sales_rep_fk = self.data_provider[Data.SESSION_INFO]['s_sales_rep_fk'].iloc[0]
        self.session_fk = self.data_provider[Data.SESSION_INFO]['pk'].iloc[0]
        self.thresholds_and_results = {}
        self.result_df = []
        self.kpi_results_queries = []
        self.kpk_scores = {}
        self.gaps_dict = {}
        self.gaps_queries = []
        self.top_sku_queries = []
        self.gap_groups_limit = {'Availability': 2, 'Cooler/Cold Availability': 1, 'Shelf/Displays/Activation': 3}
        self.execution_contract = CCRUContract(rds_conn=self.rds_conn)
        self.top_sku = CCRUTopSKUAssortment(rds_conn=self.rds_conn)
        self.execution_results = {}
        self.attr15 = self.kpi_fetcher.get_attr15_store(self.store_id)
        self.kpi_score_level2 = {}

    def change_set(self, set_name):
        self.set_name = set_name
        self.kpi_fetcher = CCRUCCHKPIFetcher(self.project_name, self.scif, self.match_product_in_scene,
                                                  self.set_name, self.products)

    def rds_connection(self):
        if not hasattr(self, '_rds_conn'):
            self._rds_conn = ProjectConnector(self.project_name, DbUsers.CalculationEng)
        try:
            pd.read_sql_query('select pk from probedata.session limit 1', self._rds_conn.db)
        except:
            self._rds_conn.disconnect_rds()
            self._rds_conn = ProjectConnector(self.project_name, DbUsers.CalculationEng)
        return self._rds_conn

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

    def convert_kpi_level_1(self, kpi_level_1):
        kpi_level_1_df = self.data_provider[Data.KPI_LEVEL_1]
        kpi_name = kpi_level_1.get('kpi_name')
        kpi_fk = kpi_level_1_df[kpi_level_1_df['name'] == kpi_name].reset_index()['pk'][0]
        kpi_result = kpi_level_1.get('score')
        kpi_level_1_results = pd.DataFrame(columns=self.output.KPI_LEVEL_1_RESULTS_COLS)
        kpi_level_1_results = kpi_level_1_results.append({'kpi_level_1_fk': kpi_fk,
                                                          'result': kpi_result}, ignore_index=True)
        kpi_level_1_results = self.data_provider.add_session_fields_old_tables(kpi_level_1_results)
        return kpi_level_1_results

    def convert_kpi_level_2(self, level_2_kpi):
        kpi_level_2_results = pd.DataFrame(columns=self.output.KPI_LEVEL_2_RESULTS_COLS)
        kpi_level_2_df = self.data_provider[Data.KPI_LEVEL_2]
        kpi_name = level_2_kpi.get('kpi_name')
        kpi_fk = kpi_level_2_df[kpi_level_2_df['name'] == kpi_name].reset_index()['pk'][0]
        kpi_result = level_2_kpi.get('result')
        kpi_score = level_2_kpi.get('score')
        kpi_weight = level_2_kpi.get('original_weight')
        kpi_target = level_2_kpi.get('target')
        kpi_level_2_results = kpi_level_2_results.append({'kpi_level_2_fk': kpi_fk,
                                                          'result': kpi_result, 'score': kpi_score,
                                                          'weight': kpi_weight, 'target': kpi_target},
                                                         ignore_index=True)
        kpi_level_2_results = self.data_provider.add_session_fields_old_tables(kpi_level_2_results)
        return kpi_level_2_results

    def convert_kpi_level_3(self, level_3_kpi):
        kpi_level_3_results = pd.DataFrame(columns=self.output.KPI_LEVEL_3_RESULTS_COLS)
        kpi_level_3_df = self.data_provider[Data.KPI_LEVEL_3]
        kpi_name = level_3_kpi.get('KPI name')
        kpi_fk = kpi_level_3_df[kpi_level_3_df['name'] == kpi_name].reset_index()['pk'][0]
        kpi_result = level_3_kpi.get('result')
        kpi_score = level_3_kpi.get('score')
        kpi_weight = level_3_kpi.get('original_weight')
        kpi_target = level_3_kpi.get('target')
        kpi_level_3_results = kpi_level_3_results.append({'kpi_level_3_fk': kpi_fk,
                                                          'result': kpi_result, 'score': kpi_score,
                                                          'weight': kpi_weight, 'target': kpi_target},
                                                         ignore_index=True)
        kpi_level_3_results = self.data_provider.add_session_fields_old_tables(kpi_level_3_results)
        return kpi_level_3_results

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
        session = OrmSession(project, writable=True)
        try:
            voting_process_pk_dic = {}
            with session.begin(subtransactions=True):
                for kpi in kpi_list.values()[0]:
                    if kpi.get('To include in first calculation?') == 4:
                        Log.info('Trying to write KPI {}'.format(kpi.get('KPI name Eng')))
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
                        #            VALUES ('{0}', '{1}', '{2}', '{3}', '{4}', '{5}');""".format('Hypermarket', 'Bad',
                        #                                                          'Y', 'N', 'N', 'N')
                        # Log.get_logger().debug(insert_trans_level1)
                        # result = session.execute(insert_trans_level1)
                        insert_trans_level2 = """
                                        INSERT INTO static.kpi (kpi_set_fk,
                                       logical_operator, weight, display_text)
                                       VALUES ('{0}', '{1}', '{2}', '{3}');""".format(34, kpi.get('Logical Operator'),
                                                                                      kpi.get('KPI Weight'),
                                                                                      kpi.get('KPI name Eng'))
                        # # #
                        # # # #     # insert_trans = """
                        # # # #     #                 UPDATE static.kpi_level_1 SET short_name=null, eng_name=null, valid_until=null, delete_date=null
                        # # # #     #                 WHERE pk=1;"""
                        # Log.get_logger().debug(insert_trans_level2)
                        result = session.execute(insert_trans_level2)
                        kpi_fk = result.lastrowid
                        insert_trans_level3 = """
                                        INSERT INTO static.atomic_kpi (kpi_fk,
                                       name, description, display_text, presentation_order, display)
                                       VALUES ('{0}', '{1}', '{2}', '{3}', '{4}', '{5}');""".format(kpi_fk, kpi.get(
                            'KPI name Eng'),
                                                                                                    kpi.get(
                                                                                                        'KPI name Eng'),
                                                                                                    kpi.get(
                                                                                                        'KPI name Eng'),
                                                                                                    1, 'Y')
                        Log.get_logger().debug(insert_trans_level3)
                        result = session.execute(insert_trans_level3)
                        # voting_process_pk = result.lastrowid
                        # voting_process_pk_dic[kpi] = voting_process_pk
                        # Log.info('KPI level 1 was inserted to the DB')
                        # Log.info('Inserted voting process {} in project {} SQL DB'.format(voting_process_pk, project))
                        # voting_session_fk = self.insert_production_session(voting_process_pk, kpi, session)
                        # self.insert_production_tag(voting_process_pk, voting_session_fk, kpi, session)

            session.close()
            # return voting_process_pk_dic
            return
        except Exception as e:
            Log.error('Caught exception while inserting new voting process to SQL: {}'.
                      format(str(e)))
            return -1

    def insert_new_kpis(self, project, kpi_list):
        """
        This function is used to insert KPI metadata to the new tables, and currently not used

        """
        for kpi in kpi_list.values()[0]:
            if kpi.get('To include in first calculation?') == 7:
                # kpi_level_1_hierarchy = pd.DataFrame(data=[('Canteen', None, None, 'WEIGHTED_AVERAGE',
                #                                             1, '2016-11-28', None, None)],
                #                                      columns=['name', 'short_name', 'eng_name', 'operator',
                #                                               'version', 'valid_from', 'valid_until', 'delete_date'])
                # self.output.add_kpi_hierarchy(Keys.KPI_LEVEL_1, kpi_level_1_hierarchy)
                if kpi.get('level') == 2:
                    kpi_level_2_hierarchy = pd.DataFrame(data=[
                        (3, kpi.get('KPI name Eng'), None, None, None, kpi.get('score_func'), kpi.get('KPI Weight'), 1,
                         '2016-12-01', None,
                         None)],
                        columns=['kpi_level_1_fk', 'name', 'short_name', 'eng_name', 'operator',
                                 'score_func', 'original_weight', 'version', 'valid_from', 'valid_until',
                                 'delete_date'])
                    self.output.add_kpi_hierarchy(Keys.KPI_LEVEL_2, kpi_level_2_hierarchy)
                elif kpi.get('level') == 3:
                    kpi_level_3_hierarchy = pd.DataFrame(
                        data=[(82, kpi.get('KPI Name'), None, None, 'PRODUCT AVAILABILITY',
                               None, kpi.get('KPI Weight'), 1, '2016-12-25', None, None)],
                        columns=['kpi_level_2_fk', 'name', 'short_name', 'eng_name',
                                 'operator',
                                 'score_func', 'original_weight', 'version',
                                 'valid_from',
                                 'valid_until', 'delete_date'])
                    self.output.add_kpi_hierarchy(Keys.KPI_LEVEL_3, kpi_level_3_hierarchy)
                self.data_provider.export_kpis_hierarchy(self.output)
            else:
                Log.info('No KPIs to insert')

    def get_relevant_scenes(self, params):
        all_scenes = self.scenes_info['scene_fk'].unique().tolist()
        filtered_scenes = []
        scenes_data = {}
        location_data = {}
        sub_location_data = {}
        store_area_data = {}
        relevant_scenes = []

        for scene in all_scenes:
            scene_type = list(self.scif.loc[self.scif['scene_id'] == scene]['template_name'].values)
            if scene_type:
                scene_type = scene_type[0]
                if scene_type not in scenes_data.keys():
                    scenes_data[scene_type] = []
                scenes_data[scene_type].append(scene)
                filtered_scenes.append(scene)
            else:
                # Log.warning('Scene {} is not defined in reporting.scene_item_facts table'.format(scene))
                continue

            location = list(self.scif.loc[self.scif['scene_id'] == scene]['location_type'].values)
            if location:
                location = location[0]
                if location not in location_data.keys():
                    location_data[location] = []
                location_data[location].append(scene)

            sub_location = list(
                self.scif.loc[self.scif['template_name'] == scene_type]['additional_attribute_2'].values)
            if sub_location:
                sub_location = sub_location[0]
                if sub_location not in sub_location_data.keys():
                    sub_location_data[sub_location] = []
            sub_location_data[sub_location].append(scene)

            store_area = list(
                self.ps_data_provider.loc[self.ps_data_provider['scene_fk'] == scene]['store_area_name'].values)
            if store_area:
                store_area = store_area[0]
                if store_area not in store_area_data.keys():
                    store_area_data[store_area] = []
            if len(store_area) != 0:
                store_area_data[store_area].append(scene)

        include_list = []
        include_list_candidate = []

        if not params.get('Scenes to include') and not params.get('Locations to include') and \
                not params.get('Sub locations to include') and not params.get('Zone to include'):
            include_list.extend(filtered_scenes)
        else:
            if params.get('Scenes to include'):
                scenes_to_include = params.get('Scenes to include').split(', ')
                for scene in scenes_to_include:
                    if scene in scenes_data.keys():
                        include_list_candidate.extend(scenes_data[scene])
                if not include_list_candidate:
                    return relevant_scenes
                include_list = list(set(include_list_candidate))

            if params.get('Locations to include'):
                include_list_candidate = []
                locations_to_include = params.get('Locations to include').split(', ')
                for location in locations_to_include:
                    if location in location_data.keys():
                        include_list_candidate.extend(location_data[location])
                if not include_list_candidate:
                    return relevant_scenes
                if include_list:
                    include_list = list(set(include_list) & set(include_list_candidate))
                else:
                    include_list = list(set(include_list_candidate))

            if params.get('Sub locations to include'):
                include_list_candidate = []
                if type(params.get('Sub locations to include')) == float:
                    sub_locations_to_include = str(int(params.get('Sub locations to include'))).split(', ')
                else:
                    sub_locations_to_include = str(params.get('Sub locations to include')).split(', ')
                for sub_location in sub_locations_to_include:
                    if sub_location in sub_location_data.keys():
                        include_list_candidate.extend(sub_location_data[sub_location])
                if not include_list_candidate:
                    return relevant_scenes
                if include_list:
                    include_list = list(set(include_list) & set(include_list_candidate))
                else:
                    include_list = list(set(include_list_candidate))

            if params.get('Zone to include'):
                include_list_candidate = []
                store_areas_to_include = str(params.get('Zone to include')).split(', ')
                for store_area in store_areas_to_include:
                    if store_area in store_area_data.keys():
                        include_list_candidate.extend(store_area_data[store_area])
                if not include_list_candidate:
                    return relevant_scenes
                if include_list:
                    include_list = list(set(include_list) & set(include_list_candidate))
                else:
                    include_list = list(set(include_list_candidate))

        # include_list = list(set(include_list))

        exclude_list = []
        if params.get('Scenes to exclude'):
            scenes_to_exclude = params.get('Scenes to exclude').split(', ')
            for scene in scenes_to_exclude:
                if scene in scenes_data.keys():
                    exclude_list.extend(scenes_data[scene])

        if params.get('Locations to exclude'):
            locations_to_exclude = params.get('Locations to exclude').split(', ')
            for location in locations_to_exclude:
                if location in location_data.keys():
                    exclude_list.extend(location_data[location])

        if params.get('Sub locations to exclude'):
            sub_locations_to_exclude = str(params.get('Sub locations to exclude')).split(', ')
            for sub_location in sub_locations_to_exclude:
                if sub_location in sub_location_data.keys():
                    exclude_list.extend(sub_location_data[sub_location])
        exclude_list = list(set(exclude_list))

        for scene in include_list:
            if scene not in exclude_list:
                relevant_scenes.append(scene)
        return relevant_scenes

    def check_number_of_facings_given_answer_to_survey(self, params):
        set_total_res = 0
        for p in params.values()[0]:
            if p.get('Formula') != "number of facings given answer to survey" or not p.get("Children"):
                continue
            kpi_fk = self.kpi_fetcher.get_kpi_fk(p.get('KPI name Eng'))
            first_atomic_score = 0
            children = map(int, p.get("Children").split("\n"))
            for c in params.values()[0]:
                if c.get("KPI ID") in children and c.get("Formula") == "atomic answer to survey":
                    first_atomic_score = self.check_answer_to_survey_level3(c)
                    # saving to DB
                    attributes_for_level3 = self.create_attributes_for_level3_df(c, first_atomic_score, kpi_fk)
                    self.write_to_db_result(attributes_for_level3, 'level3')
            second_atomic_res = 0
            for c in params.values()[0]:
                if c.get("KPI ID") in children and c.get("Formula") == "atomic number of facings":
                    second_atomic_res = self.calculate_availability(c)
                    second_atomic_score = self.calculate_score(second_atomic_res, c)
                    # write to DB
                    attributes_for_level3 = self.create_attributes_for_level3_df(c, second_atomic_score, kpi_fk)
                    self.write_to_db_result(attributes_for_level3, 'level3')

            if first_atomic_score > 0:
                kpi_total_res = second_atomic_res
            else:
                kpi_total_res = 0
            score = self.calculate_score(kpi_total_res, p)
            set_total_res += round(score) * p.get('KPI Weight')
            # saving to DB
            attributes_for_level2 = self.create_attributes_for_level2_df(p, score, kpi_fk)
            self.write_to_db_result(attributes_for_level2, 'level2')

        return set_total_res

    def check_answer_to_survey_level3(self, params):
        d = {'Yes': u'Да', 'No': u'Нет'}
        score = 0
        survey_data = self.survey_response.loc[self.survey_response['question_text'] == params.get('Values')]
        if not survey_data['selected_option_text'].empty:
            result = survey_data['selected_option_text'].values[0]
            targets = [d.get(target) if target in d.keys() else target
                       for target in unicode(params.get('Target')).split(", ")]
            if result in targets:
                score = 100
            else:
                score = 0
        elif not survey_data['number_value'].empty:
            result = survey_data['number_value'].values[0]
            if result == params.get('Target'):
                score = 100
            else:
                score = 0
        else:
            Log.warning('No survey data for this session')
        return score

    def check_availability(self, params):
        """
        This function is used to calculate availability given a set pf parameters

        """
        set_total_res = 0
        availability_types = ['SKUs', 'BRAND', 'MAN', 'CAT', 'MAN in CAT', 'SUB_BRAND','SUB_CATEGORY']
        formula_types = ['number of SKUs', 'number of facings']
        for p in params.values()[0]:
            if p.get('Type') not in availability_types or p.get('Formula') not in formula_types:
                continue
            if p.get('level') != 2:
                continue
            is_atomic = False
            kpi_total_res = 0
            kpi_fk = self.kpi_fetcher.get_kpi_fk(p.get('KPI name Eng'))

            if p.get('Children') is not None:
                is_atomic = True
                children = [int(child) for child in str(p.get('Children')).split(', ')]
                atomic_scores = []
                for child in params.values()[0]:
                    if child.get('KPI ID') in children:

                        if child.get('Children') is not None:  # atomic of atomic
                            atomic_score = 0
                            atomic_children = [int(a_child) for a_child in str(child.get('Children')).split(', ')]
                            for atomic_child in params.values()[0]:
                                if atomic_child.get('KPI ID') in atomic_children:
                                    atomic_child_res = self.calculate_availability(atomic_child)
                                    atomic_child_score = self.calculate_score(atomic_child_res, atomic_child)
                                    atomic_score += atomic_child.get('additional_weight',
                                                                     1.0 / len(atomic_children)) * atomic_child_score

                        else:
                            atomic_res = self.calculate_availability(child)
                            atomic_score = self.calculate_score(atomic_res, child)

                        # write to DB
                        attributes_for_table3 = self.create_attributes_for_level3_df(child, atomic_score, kpi_fk)
                        self.write_to_db_result(attributes_for_table3, 'level3', kpi_fk)

                        if p.get('Logical Operator') in ('OR', 'AND', 'MAX'):
                            atomic_scores.append(atomic_score)
                        elif p.get('Logical Operator') == 'SUM':
                            kpi_total_res += child.get('additional_weight', 1 / float(len(children))) * atomic_score

                if p.get('Logical Operator') == 'OR':
                    if len([sc for sc in atomic_scores if sc > 0]) > 0:
                        score = 100
                    else:
                        score = 0
                elif p.get('Logical Operator') == 'AND':
                    if 0 not in atomic_scores:
                        score = 100
                    else:
                        score = 0
                elif p.get('Logical Operator') == 'SUM':
                    score = round(kpi_total_res) / 100.0
                    if score < p.get('score_min', 0):
                        score = 0
                    elif score > p.get('score_max', 1):
                        score = p.get('score_max', 1)
                    score *= 100
                elif p.get('Logical Operator') == 'MAX':
                    if atomic_scores:
                        score = max(atomic_scores)
                        if not ((score > p.get('score_min', 0) * 100) and (score <= p.get('score_max', 1) * 100)):
                            score = 0
                    else:
                        score = 0
            else:
                kpi_total_res = self.calculate_availability(p)
                score = self.calculate_score(kpi_total_res, p)

            # Saving to old tables
            attributes_for_table2 = self.create_attributes_for_level2_df(p, score, kpi_fk)
            self.write_to_db_result(attributes_for_table2, 'level2', kpi_fk)
            if p.get('Target Execution 2018'):  # insert the results that needed for target execution set
                kpi_name = p.get('KPI name Eng')
                self.insert_scores_level2(kpi_total_res, score, kpi_name)
            if not is_atomic:  # saving also to level3 in case this KPI has only one level
                attributes_for_table3 = self.create_attributes_for_level3_df(p, score, kpi_fk)
                self.write_to_db_result(attributes_for_table3, 'level3', kpi_fk)

            set_total_res += round(score) * p.get('KPI Weight')

        return set_total_res

    def calculate_availability(self, params, scenes=[], all_params=None):
        values_list = str(params.get('Values')).split(', ')
        # object_static_list = self.get_static_list(params.get('Type'))
        if not scenes:
            if 'depends on' in params.keys():
                depends_on_kpi_name = params.get('depends on')
                for c in all_params.values()[0]:
                    if c.get('KPI name Eng') == depends_on_kpi_name:
                        if c.get('Formula') == 'number of coolers with facings target and fullness target':
                            scenes = self.calculate_number_of_doors_more_than_target_facings(c, 'get scenes')
                            scenes = self.calculate_number_of_doors_of_filled_coolers(c, scenes,
                                                                                      function='get scenes',
                                                                                      proportion_param=0.9)
                        else:
                            scenes = self.calculate_number_of_doors_more_than_target_facings(c, 'get scenes')

                # if params.get('KPI name Eng') == depends_on_kpi_name:
                #     scenes = self.calculate_number_of_doors_more_than_target_facings(params, 'get scenes')
                if not scenes:
                    return 0
            else:
                scenes = self.get_relevant_scenes(params)

        if params.get("Form Factor"):
            form_factors = [str(form_factor) for form_factor in params.get("Form Factor").split(", ")]
        else:
            form_factors = []
        if params.get("Size"):
            sizes = [float(size) for size in str(params.get('Size')).split(", ")]
            sizes = [int(size) if int(size) == size else size for size in sizes]
        else:
            sizes = []
        if params.get("Products to exclude"):
            products_to_exclude = [int(float(product)) for product in \
                                   str(params.get("Products to exclude")).split(", ")]
        else:
            products_to_exclude = []
        if params.get("Form factors to exclude"):
            form_factors_to_exclude = str(params.get("Form factors to exclude")).split(", ")
        else:
            form_factors_to_exclude = []
        if params.get("Product Category"):
            product_categories = str(params.get("Product Category")).split(", ")
        else:
            product_categories = []
        if params.get("Sub category"):
            product_sub_categories = str(params.get("Sub category")).split(", ")
        else:
            product_sub_categories = []
        if params.get("Brand"):
            product_brands = str(params.get("Brand")).split(", ")
        else:
            product_brands = []
        if params.get("Manufacturer"):
            product_manufacturers = str(params.get("Manufacturer")).split(", ")
        else:
            product_manufacturers = []
        if not product_manufacturers:
            product_manufacturers = ['TCCC']

        object_facings = self.kpi_fetcher.get_object_facings(scenes, values_list, params.get('Type'),
                                                             formula=params.get('Formula'),
                                                             shelves=params.get("shelf_number", None),
                                                             size=sizes, form_factor=form_factors,
                                                             products_to_exclude=products_to_exclude,
                                                             form_factors_to_exclude=form_factors_to_exclude,
                                                             product_categories=product_categories,
                                                             product_sub_categories=product_sub_categories,
                                                             product_brands = product_brands,
                                                             product_manufacturers = product_manufacturers)

        return object_facings

    def calculate_number_facings_near_food(self, params, all_params):
        total_res = 0
        if 'depends on' in params.keys():
            depends_on_kpi_name = params.get('depends on')
            for c in all_params.values()[0]:
                if c.get('KPI name Eng') == depends_on_kpi_name:
                    scenes = self.get_relevant_scenes(params)
                    for scene in scenes:
                        num_facings = self.calculate_availability(c, [scene])
                        if num_facings >= c.get('Target'):
                            num_facings_food = self.calculate_availability(params, [scene])
                            if num_facings_food >= params.get('Target'):
                                total_res += num_facings_food
                    return total_res
        total_res = self.calculate_availability(params)
        return total_res


    # def calculate_availability(self, p, object_static_list, scene, level=None, shelves=None):
    #     """
    #     This function calculates availability given one KPI
    #
    #     """
    #     atomic_total_res = 0
    #     values_list = str(p.get('Values')).split(', ')
    #     for object in values_list:  # iterating through all KPI objects
    #         res = 0
    #         if str(object) not in object_static_list:
    #             Log.warning('The object {} does not exist in the database'.format(object))
    #         else:
    #             if p.get("Products to exclude"):
    #                 products_to_exclude = [int(float(product)) for product in \
    #                                        str(p.get("Products to exclude")).split(", ")]
    #             else:
    #                 products_to_exclude = []
    #             if p.get("Form factors to exclude"):
    #                 form_factors_to_exclude = str(p.get("Form factors to exclude")).split(", ")
    #             else:
    #                 form_factors_to_exclude = []
    #             if not p.get('Form Factor') and not p.get('Size'):
    #                 # pop_filter = self.create_filter_by_type_and_single_value(self.scif, p.get('Type'), object, d)
    #                 # facings_sum = TBox.calculate_frame_column_sum(self.scif[pop_filter], Fd.FACINGS, default_value=0)
    #                 if not shelves:
    #                     object_facings = self.kpi_fetcher.get_object_facings(scene, object, p.get('Type'),
    #                                                                          products_to_exclude=products_to_exclude,
    #                                                                          form_factors_to_exclude=form_factors_to_exclude)
    #                 else:
    #                     object_facings = self.kpi_fetcher.get_object_facings(scene, object, p.get('Type'),
    #                                                                          shelves=shelves,
    #                                                                          products_to_exclude=products_to_exclude,
    #                                                                          form_factors_to_exclude=form_factors_to_exclude)
    #             elif p.get('Form Factor') and not p.get('Size'):
    #                 object_facings = self.kpi_fetcher.get_object_facings(scene, object, p.get('Type'),
    #                                                                      form_factor=p.get('Form Factor'),
    #                                                                      products_to_exclude=products_to_exclude,
    #                                                                      form_factors_to_exclude=form_factors_to_exclude)
    #             elif not p.get('Form Factor') and p.get('Size'):
    #                 sizes = [float(size) for size in str(p.get('Size')).split(", ")]
    #                 sizes = [int(size) if int(size) == size else size for size in sizes]
    #                 object_facings = self.kpi_fetcher.get_object_facings(scene, object, p.get('Type'),
    #                                                                      size=sizes,
    #                                                                      products_to_exclude=products_to_exclude,
    #                                                                      form_factors_to_exclude=form_factors_to_exclude)
    #             else:
    #                 sizes = [float(size) for size in str(p.get('Size')).split(", ")]
    #                 sizes = [int(size) if int(size) == size else size for size in sizes]
    #                 object_facings = self.kpi_fetcher.get_object_facings(scene, object, p.get('Type'),
    #                                                                      size=sizes, form_factor=p.get('Form Factor'),
    #                                                                      products_to_exclude=products_to_exclude,
    #                                                                      form_factors_to_exclude=form_factors_to_exclude)
    #             if object_facings > 0:
    #                 if 'number of facings' in p.get('Formula'):
    #                     res = round(object_facings)
    #                 else:
    #                     res = 1.0
    #             else:
    #                 res = 0
    #             atomic_total_res += res
    #     # kpi_total_res += atomic_total_res
    #
    #     if atomic_total_res >= p.get('Target') and 'number of SKUs' in p.get('Formula'):
    #         atomic_total_res_for_db = atomic_total_res / len(values_list)
    #     elif atomic_total_res >= p.get('Target') and 'number of SKUs' not in p.get('Formula'):
    #         atomic_total_res_for_db = atomic_total_res
    #     else:
    #         atomic_total_res_for_db = 0
    #     if p.get('operator') == 'AND' and atomic_total_res != p.get('weight') * len(p.get('values')):
    #         atomic_total_res_for_db = 0
    #     else:
    #         pass
    #
    #     return atomic_total_res_for_db

    # def get_relevant_scenes(self, params):
    #     relevant_scenes = []
    #     for scene in self.scenes_info['scene_fk'].unique().tolist():  # iterating thorugh each scene
    #         try:
    #             scene_type = self.scif.loc[self.scif['scene_id'] == scene]['template_name'].values[0]
    #             location = self.scif.loc[self.scif['scene_id'] == scene]['location_type'].values[0]
    #             sub_location = self.scif.loc[self.scif['template_name'] == scene_type]['additional_attribute_2'].values[0]
    #             location_and_scene_validity = self.validate_scenes_and_location(location, scene_type,
    #                                                                             sub_location, params)
    #             if location_and_scene_validity:
    #                 relevant_scenes.append(scene)
    #         except IndexError as e:
    #             Log.warning('Scene {} is not defined in reporting.scene_item_facts table'.format(scene))
    #     return relevant_scenes

    # def _check_availability(self, params):
    #     """
    #     This function is used to calculate availability given a set pf parameters
    #
    #     """
    #     set_total_res = 0
    #     availability_types = ['SKUs', 'BRAND', 'MAN', 'CAT', 'MAN in CAT']
    #     formula_types = ['number of SKUs', 'number of facings']
    #     for p in params.values()[0]:
    #         results_dict = {}
    #         is_atomic = False
    #         if p.get('Type') not in availability_types or p.get('Formula') not in formula_types:
    #             continue
    #         if p.get('level') == 3:
    #             continue
    #         kpi_fk = self.kpi_fetcher.get_kpi_fk(p.get('KPI name Eng'), self.set_name)
    #         kpi_total_res = 0
    #         object_static_list = self.get_static_list(p.get('Type'))  # generating static list of object for validation
    #         for scene in self.scenes_info['scene_fk'].unique().tolist():  # iterating thorugh each scene
    #             try:
    #                 scene_type = self.scif.loc[self.scif['scene_id'] == scene]['template_name'].values[
    #                     0]  # extracting the scene type
    #                 location = self.scif.loc[self.scif['scene_id'] == scene]['location_type'].values[
    #                     0]  # extracting the location type
    #                 sub_location = self.scif.loc[self.scif['template_name'] == scene_type][
    #                     'additional_attribute_2'].values[0]
    #                 location_and_scene_validity = self.validate_scenes_and_location(location, scene_type,
    #                                                                                 sub_location, p)
    #             except IndexError as e:
    #                 Log.warning('Scene {} is not defined in reporting.scene_item_facts table'.format(scene))
    #                 continue
    #             if location_and_scene_validity:  # scene types and locations validation
    #                 atomic_total_res = 0
    #                 if p.get('children') is not None:
    #                     is_atomic = True
    #                     score = 0
    #                     for child in params.values()[
    #                         0]:  # todo temporary solution, should be refactored to df which extracts static KPIs
    #                         if child.get('KPI ID') in [int(kpi) for kpi in p.get('children').split(', ')]:
    #                             kpi_total_res += self.calculate_availability(child, object_static_list,
    #                                                                          scene)  # todo add level as a parameter to this function
    #                             if child.get('KPI name Eng') not in results_dict.keys():
    #                                 results_dict[child.get('KPI name Eng')] = [0, child.get('KPI name Rus')]
    #                             results_dict[child.get('KPI name Eng')][0] += kpi_total_res
    #                         else:
    #                             continue
    #                 else:
    #                     if not p.get('shelf_number'):
    #                         kpi_total_res += self.calculate_availability(p, object_static_list, scene)
    #                     else:
    #                         kpi_total_res += self.calculate_availability(p, object_static_list, scene,
    #                                                                      shelves=p.get('shelf_number'))
    #             else:
    #                 Log.debug('Scene {} has no relevant location type for this KPI'.format(scene))
    #         score = self.calculate_score(kpi_total_res, p)
    #         # Saving to old tables
    #         attributes_for_table2 = self.create_attributes_for_level2_df(p, score, kpi_fk)
    #         self.write_to_db_result(attributes_for_table2, 'level2', kpi_fk)
    #         if not is_atomic:  # saving also to level3 in case this KPI has only one level
    #             attributes_for_table3 = self.create_attributes_for_level3_df(p, score, kpi_fk)
    #             self.write_to_db_result(attributes_for_table3, 'level3', kpi_fk)
    #         else:  # write all level3 results
    #             for key in results_dict.keys():
    #                 # kpi_fk = self.kpi_fetcher.get_kpi_fk(key, self.set_name)
    #                 params_for_level3 = {'KPI name Eng': key, 'KPI name Rus': results_dict[key][1]}
    #                 attributes_for_table3 = self.create_attributes_for_level3_df(params_for_level3,
    #                                                                              results_dict[key][0], kpi_fk)
    #                 self.write_to_db_result(attributes_for_table3, 'level3', kpi_fk)
    #         ##saving to new tables##
    #         # level2_output = {'result': kpi_total_res, 'score': score,
    #         #                  'target': p.get('Target'), 'weight': p.get('KPI Weight'),
    #         #                  'kpi_name': p.get('KPI name Eng')}
    #         # self.output.add_kpi_results(Keys.KPI_LEVEL_2_RESULTS, self.convert_kpi_level_2(level2_output))
    #         weight = p.get('KPI Weight')
    #         if p.get('additional_weight') is not None:
    #             weight *= kpi_total_res
    #         set_total_res += score * weight
    #     Log.info('Calculation finished')
    #
    #     return set_total_res

    # def calculate_top_k_skus(self, params):
    #     set_total_res = 0
    #     for p in params.values()[0]:
    #         if not p.get('Type') == 'TOP K' or not p.get('children'):
    #             continue
    #         results_dict = {}
    #         kpi_fk = self.kpi_fetcher.get_kpi_fk(p.get('KPI name Eng'), self.set_name)
    #         children = [int(child) for child in p.get('children').split(", ")]
    #         for child in params.values()[0]:
    #             if child.get('KPI ID') in children:
    #                 atomic_res = self.calculate_availability(child)
    #                 atomic_score = self.calculate_score(atomic_res, child)
    #                 results_dict[child.get('KPI name Eng')] = atomic_score
    #                 # writing to db
    #                 attributes_for_table3 = self.create_attributes_for_level3_df(child, atomic_score, kpi_fk)
    #                 self.write_to_db_result(attributes_for_table3, 'level3')
    #         kpi_total_res = len([result for result in results_dict.keys() if results_dict[result] > 0])
    #         kpi_score = self.calculate_score(kpi_total_res, p)
    #         set_total_res += kpi_score * p.get('KPI Weight')
    #         # writing to db
    #         attributes_for_table2 = self.create_attributes_for_level2_df(p, kpi_score, kpi_fk)
    #         self.write_to_db_result(attributes_for_table2, 'level2')
    #
    #     return set_total_res

    # def calculate_top_k_skus(self, params):
    #     """
    #     This function calculates the top K SKUs KPI
    #
    #     """
    #     set_total_res = 0
    #     for p in params.values()[0]:
    #         if not p.get('Type') == 'TOP K':
    #             continue
    #         results_dict = {}
    #         kpi_total_res = 0
    #         for scene in self.scenes_info['scene_fk'].unique().tolist():  # iterating thorugh each scene
    #             try:
    #                 scene_type = self.scif.loc[self.scif['scene_id'] == scene]['template_name'].values[
    #                     0]  # extracting the scene type
    #                 location = self.scif.loc[self.scif['scene_id'] == scene]['location_type'].values[
    #                     0]  # extracting the location type
    #                 sub_location = self.scif.loc[self.scif['template_name'] == scene_type][
    #                     'additional_attribute_2'].values[0]
    #             except IndexError as e:
    #                 Log.error('Scene {} is not defined in reporting.scene_item_facts table'.format(scene))
    #                 continue
    #             location_and_scene_validity = self.validate_scenes_and_location(location, scene_type,
    #                                                                             sub_location, p)
    #             if location_and_scene_validity:  # scene types and locations validation
    #                 if p.get('children') is not None:
    #                     for child in params.values()[0]:  # todo temporary solution, should be refactored to df which extracts static KPIs
    #                         if child.get('KPI ID') in [int(kpi) for kpi in p.get('children').split(', ')]:
    #                             object_static_list = self.get_static_list(child.get('Type'))
    #                             res = self.calculate_availability(child, object_static_list, scene)
    #                             if not results_dict.get(child.get('KPI name Eng')): # if result doesn't exist or == 0
    #                                 results_dict[child.get('KPI name Eng')] = [self.calculate_score(res, child),
    #                                                                            child.get('KPI name Rus')]
    #                                 kpi_total_res += res
    #
    #             else:
    #                 Log.debug('Scene {} has no relevant location type for this KPI'.format(scene))
    #
    #         score = self.calculate_score(kpi_total_res, p)
    #         # level2_output = {'result': kpi_total_res, 'score': score,
    #         #                  'target': p.get('Target'), 'weight': p.get('KPI Weight'),
    #         #                  'kpi_name': p.get('KPI name Eng')}
    #         # self.output.add_kpi_results(Keys.KPI_LEVEL_2_RESULTS, self.convert_kpi_level_2(level2_output))
    #         for key in results_dict.keys():
    #             kpi_fk = self.kpi_fetcher.get_kpi_fk(key, self.set_name)
    #             params_for_level3 = {'KPI name Eng': key, 'KPI name Rus': results_dict[key][1]}
    #             attributes_for_table3 = self.create_attributes_for_level3_df(params_for_level3,
    #                                                                          results_dict[key][0], kpi_fk)
    #             self.write_to_db_result(attributes_for_table3, 'level3')
    #         kpi_fk = self.kpi_fetcher.get_kpi_fk(p.get('KPI name Eng'), self.set_name)
    #         attributes_for_level2 = self.create_attributes_for_level2_df(p, score, kpi_fk)
    #         self.write_to_db_result(attributes_for_level2, 'level2')
    #
    #         set_total_res += score * p.get('KPI Weight')
    #     Log.info('Calculation finished')
    #     return set_total_res

    def check_number_of_scenes_with_target(self, params):
        scenes = None
        if 'depends on' in params.keys():
            depends_on_kpi_name = params.get('depends on')
            for c in params.values()[0]:
                if c.get('KPI name Eng') == depends_on_kpi_name:
                    scenes = self.calculate_number_of_doors_more_than_target_facings(c, 'get scenes')
            if not scenes:
                return 0
        else:
            scenes = self.get_relevant_scenes(params)
        kpi_total_res = 0
        for scene in scenes:
            res = self.calculate_availability(params, scenes=[scene])
            if res >= params.get('target_min'):
                kpi_total_res += 1
        return kpi_total_res

    def check_number_of_scenes(self, params):
        """
        This function is used to calculate number of scenes

        """
        set_total_res = 0
        for p in params.values()[0]:
            if p.get('Formula') != 'number of scenes' or p.get('level') == 3:
                continue

            kpi_total_res = 0
            if 'depends on' in p.keys():
                scenes = []
                depends_on_kpi_name = p.get('depends on')
                for c in params.values()[0]:
                    if c.get('KPI name Eng') == depends_on_kpi_name:
                        if c.get('Formula') == 'number of doors with more than Target facings':
                            depends_scenes = self.calculate_number_of_doors_more_than_target_facings(c, 'get scenes')
                        elif c.get('Formula') == 'number of doors of filled Coolers':
                            depends_scenes = self.check_number_of_doors_of_filled_coolers(c, 'get scenes')
                        elif c.get('Formula') == 'number of coolers with facings target and fullness target':
                            scenes = self.calculate_number_of_doors_more_than_target_facings(c, 'get scenes')
                            depends_scenes = self.calculate_number_of_doors_of_filled_coolers(c, scenes, function='get scenes',
                                                                                  proportion_param=0.9)
                        break
                if not depends_scenes:
                    # return 0
                    scenes = []
                else:
                    # return len(scenes)
                    relevant_scenes = self.get_relevant_scenes(p)
                    scenes = list(set(relevant_scenes) & set(depends_scenes))
            else:
                scenes = self.get_relevant_scenes(p)

            # if len(scenes) == 0:
            #     return 0
            if p.get('Type') == 'NUM_SCENES':
                kpi_total_res = len(scenes)

            elif p.get('Type') == 'SCENES':
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
            elif p.get('Type') == 'LOCATION_TYPE':
                values_list = [str(s) for s in p.get('Values').split(', ')]
                for scene in scenes:
                    try:
                        location_type = self.scif.loc[self.scif['scene_id'] == scene]['location_type'].values[0]
                        if location_type in values_list:
                            res = 1
                        else:
                            res = 0
                        kpi_total_res += res
                    except IndexError as e:
                        continue
            elif p.get('Type') == 'SUB_LOCATION_TYPE':
                values_list = [p.get('Values')]
                for scene in scenes:
                    try:
                        scene_type = self.scif.loc[self.scif['scene_id'] == scene]['template_name'].values[0]
                        sub_location_type = int(self.templates.loc[self.templates['template_name'] == scene_type][
                                                    'additional_attribute_2'].values[0])
                        if sub_location_type in values_list:
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
            if 'KPI Weight' in p.keys():
                set_total_res += round(score) * p.get('KPI Weight')
            else:
                set_total_res += round(score)
            kpi_fk = self.kpi_fetcher.get_kpi_fk(p.get('KPI name Eng'))
            if p.get('level') == 2:
                attributes_for_level2 = self.create_attributes_for_level2_df(p, score, kpi_fk)
                self.write_to_db_result(attributes_for_level2, 'level2', kpi_fk)
            if not p.get('Children'):
                atomic_kpi_fk = self.kpi_fetcher.get_atomic_kpi_fk(p.get('KPI name Eng'))
                attributes_for_level3 = self.create_attributes_for_level3_df(p, score, kpi_fk, atomic_kpi_fk)
                self.write_to_db_result(attributes_for_level3, 'level3')
        return set_total_res

    def check_number_of_doors(self, params):
        set_total_res = 0
        for p in params.values()[0]:
            if p.get('Type') not in ('DOORS', 'SUB_LOCATION_TYPE', 'LOCATION_TYPE') or p.get('Formula') != 'number of doors':
                continue
            kpi_total_res = self.calculate_number_of_doors(p)
            score = self.calculate_score(kpi_total_res, p)
            set_total_res += round(score) * p.get('KPI Weight')
            # writing to DB
            kpi_fk = self.kpi_fetcher.get_kpi_fk(p.get('KPI name Eng'))
            attributes_for_level3 = self.create_attributes_for_level3_df(p, score, kpi_fk)
            self.write_to_db_result(attributes_for_level3, 'level3', kpi_fk)
            if p.get('level') == 2:
                attributes_for_level2 = self.create_attributes_for_level2_df(p, score, kpi_fk)
                self.write_to_db_result(attributes_for_level2, 'level2', kpi_fk)
        return set_total_res

    def calculate_number_of_doors(self, params):
        total_res = 0
        relevant_scenes = self.get_relevant_scenes(params)
        for scene in relevant_scenes:
            res = 0
            # if params.get('Type') == 'SUB_LOCATION_TYPE':
            #     values_list = [str(s) for s in params.get('Values').split(', ')]
            #     scene_type = self.scif.loc[self.scif['scene_id'] == scene]['template_name'].values[0]
            #     sub_location_type = \
            #     self.templates.loc[self.templates['template_name'] == scene_type]['additional_attribute_2'].values[0]
            #     if sub_location_type in values_list:
            #         num_of_doors = \
            #             self.templates[self.templates['template_name'] == scene_type]['additional_attribute_1'].values[
            #                 0]
            #         if num_of_doors is not None:
            #             res = float(num_of_doors)
            #         total_res += res
            scene_type = self.scif.loc[self.scif['scene_id'] == scene]['template_name'].values[0]
            num_of_doors = \
                self.templates[self.templates['template_name'] == scene_type]['additional_attribute_1'].values[0]
            if num_of_doors is not None:
                res = float(num_of_doors)
            total_res += res
            # else:
            #     res = 0
            #     scene_type = self.scif.loc[self.scif['scene_id'] == scene]['template_name'].values[0]
            #     num_of_doors = \
            #     float(self.templates[self.templates['template_name'] == scene_type]['additional_attribute_1'].values[0])
            #     if num_of_doors is not None:
            #         res = float(num_of_doors)
            #     total_res += res
        return total_res

    def calculate_number_of_doors_more_than_target_facings(self, params, function=None):
        total_res = 0
        relevant_scenes = self.get_relevant_scenes(params)
        scenes_passed = []
        for scene in relevant_scenes:
            scene_type = self.scif.loc[self.scif['scene_id'] == scene]['template_name'].values[0]
            if any(self.templates[self.templates['template_name'] == scene_type]['additional_attribute_1']):
                num_of_doors = \
                    float(self.templates[self.templates['template_name'] == scene_type]['additional_attribute_1'].values[0])
                facings = self.calculate_availability(params, scenes=[scene])
                if num_of_doors is not None:
                    res = facings / float(num_of_doors)
                    if res >= int(params.get('target_min')):
                        total_res += num_of_doors
                        scenes_passed.append(scene)
        if scenes_passed:
            if params.get('depends on') == 'filled collers target':
                total_res = 0
                scenes_passed_filled = self.check_number_of_doors_of_filled_coolers(params, function='get scenes',
                                                                               proportion=0.9)
                total_scenes_passed = list(set(scenes_passed_filled) & set(scenes_passed))
                scene_types_list = self.scenes_info.loc[self.scenes_info['scene_fk'].isin(total_scenes_passed)]['template_fk'].tolist()
                for scene in total_scenes_passed:
                    scene_type = self.scif.loc[self.scif['scene_id'] == scene]['template_name'].values[0]
                    if any(self.templates[self.templates['template_name'] == scene_type]['additional_attribute_1']):
                        num_of_doors = \
                            float(self.templates[self.templates['template_name'] == scene_type][
                                      'additional_attribute_1'].values[0])
                        total_res += num_of_doors
            else:
                total_scenes_passed = list(set(scenes_passed))
        else:
            total_scenes_passed = []
        if function == 'get scenes':
            return total_scenes_passed
        return total_res

    def check_survey_answer(self, params):
        """
        This function is used to calculate survey answer according to given target

        """
        set_total_res = 0
        d = {'Yes': u'Да', 'No': u'Нет'}
        for p in params.values()[0]:
            kpi_total_res = 0
            score = 0  # default score
            if p.get('Type') != 'SURVEY' or p.get('Formula') != 'answer for survey':
                continue
            survey_data = self.survey_response.loc[self.survey_response['question_text'] == p.get('Values')]
            if not survey_data['selected_option_text'].empty:
                result = survey_data['selected_option_text'].values[0]
                targets = [d.get(target) if target in d.keys() else target
                           for target in unicode(p.get('Target')).split(", ")]
                if result in targets:
                    score = 100
                else:
                    score = 0
            elif not survey_data['number_value'].empty:
                result = survey_data['number_value'].values[0]
                if result == p.get('Target'):
                    score = 100
                else:
                    score = 0
            else:
                Log.warning('No survey data for this session')
            set_total_res += round(score) * p.get('KPI Weight')
            # score = self.calculate_score(kpi_total_res, p)
            if p.get('level') == 3:  # todo should be a separate generic function
                # level3_output = {'result': d.get(result), 'score': score,
                #                  'target': p.get('Target'), 'weight': p.get('KPI Weight'),
                #                  'kpi_name': p.get('KPI name Eng')}
                # self.output.add_kpi_results(Keys.KPI_LEVEL_3_RESULTS, self.convert_kpi_level_3(level3_output))
                kpi_fk = self.kpi_fetcher.get_kpi_fk(p.get('KPI name Eng'))
                attributes_for_level3 = self.create_attributes_for_level3_df(p, score, kpi_fk)
                self.write_to_db_result(attributes_for_level3, 'level3')
            elif p.get('level') == 2:
                # level2_output = {'result': d.get(result), 'score': score,
                #                  'target': p.get('Target'), 'weight': p.get('KPI Weight'),
                #                  'kpi_name': p.get('KPI name Eng')}
                # self.output.add_kpi_results(Keys.KPI_LEVEL_2_RESULTS, self.convert_kpi_level_2(level2_output))
                kpi_fk = self.kpi_fetcher.get_kpi_fk(p.get('KPI name Eng'))
                attributes_for_level3 = self.create_attributes_for_level3_df(p, score, kpi_fk)
                self.write_to_db_result(attributes_for_level3, 'level3')
                attributes_for_level2 = self.create_attributes_for_level2_df(p, score, kpi_fk)
                self.write_to_db_result(attributes_for_level2, 'level2')
            else:
                Log.warning('No level indicated for this KPI')

        return set_total_res

    def facings_sos(self, params):
        """
        This function is used to calculate facing share of shelf

        """
        set_total_res = 0
        score = 0
        for p in params.values()[0]:
            if p.get('Type') in ('MAN in CAT', 'MAN', 'BRAND', 'BRAND_IN_CAT', 'SUB_BRAND_IN_CAT') and \
                            p.get('Formula') in ['sos', 'SOS', 'sos with empty']:
                ratio = self.calculate_facings_sos(p)
            else:
                continue
            if p.get('depends on'):
                scenes_info = pd.merge(self.scenes_info, self.templates, on='template_fk')
                values_list = [str(s) for s in p.get('depends on').split(', ')]
                number_relevant_scenes = scenes_info['template_name'].isin(values_list).sum()
                if number_relevant_scenes < 1:
                    ratio = 0
                    score = self.calculate_score(ratio, p)
                else:
                    score = self.calculate_score(ratio, p)
            else:
                score = self.calculate_score(ratio, p)
            # saving to DB
            if np.isnan(score):
                score = 0
            kpi_fk = self.kpi_fetcher.get_kpi_fk(p.get('KPI name Eng'))
            attributes_for_level3 = self.create_attributes_for_level3_df(p, score, kpi_fk)
            self.write_to_db_result(attributes_for_level3, 'level3')
            if p.get('level') == 2:
                attributes_for_level2 = self.create_attributes_for_level2_df(p, score, kpi_fk)
                self.write_to_db_result(attributes_for_level2, 'level2')
            set_total_res += round(score) * p.get('KPI Weight')
        return set_total_res

    def calculate_facings_sos(self, params):
        relevant_scenes = self.get_relevant_scenes(params)
        if params.get('Formula') == 'sos with empty':
            if params.get('Type') == 'MAN':
                pop_filter = (self.scif['scene_id'].isin(relevant_scenes))
                subset_filter = (self.scif[Fd.M_NAME].isin(self.kpi_fetcher.TCCC))
            elif params.get('Type') == 'MAN in CAT':
                pop_filter = ((self.scif[Fd.CAT].isin(params.get('Values'))) &
                              (self.scif['scene_id'].isin(relevant_scenes)))
                subset_filter = (self.scif[Fd.M_NAME].isin(self.kpi_fetcher.TCCC))
            else:
                return 0
        else:
            try:
                values_list = params.get('Values').split(', ')
            except Exception as e:
                values_list = [params.get('Values')]
            if params.get('Type') == 'MAN':
                pop_filter = ((self.scif['scene_id'].isin(relevant_scenes)) &
                              (~self.scif['product_type'].isin(['Empty'])))
                subset_filter = ((self.scif[Fd.M_NAME].isin(self.kpi_fetcher.TCCC)) &
                                 (~self.scif['product_type'].isin(['Empty'])))
            elif params.get('Type') == 'MAN in CAT':
                pop_filter = ((self.scif[Fd.CAT].isin(values_list)) &
                              (self.scif['scene_id'].isin(relevant_scenes)) &
                              (~self.scif['product_type'].isin(['Empty'])))
                subset_filter = ((self.scif[Fd.M_NAME].isin(self.kpi_fetcher.TCCC)) &
                                 (~self.scif['product_type'].isin(['Empty'])))
            elif params.get('Type') == 'SUB_BRAND_IN_CAT':
                pop_filter = ((self.scif[Fd.CAT] == params.get('Category')) &
                              (self.scif['scene_id'].isin(relevant_scenes)) &
                              (~self.scif['product_type'].isin(['Empty'])))
                subset_filter = ((self.scif['sub_brand_name'].isin(values_list)) &
                                 (~self.scif['product_type'].isin(['Empty'])))
            elif params.get('Type') == 'BRAND_IN_CAT':
                pop_filter = ((self.scif[Fd.CAT] == params.get('Category')) &
                              (self.scif['scene_id'].isin(relevant_scenes)) &
                              (~self.scif['product_type'].isin(['Empty'])))
                subset_filter = ((self.scif['brand_name'].isin(values_list)) &
                                 (~self.scif['product_type'].isin(['Empty'])))
            else:
                return 0

        try:
            ratio = self.k_engine.calculate_sos_by_facings(pop_filter, subset_filter)
        except Exception as e:
            ratio = 0
        if ratio is None:
            ratio = 0
        return ratio

    def calculate_score(self, kpi_total_res, params):
        """
        This function calculates score according to predefined score functions

        """
        kpi_name = params.get('KPI name Eng')
        self.thresholds_and_results[kpi_name] = {'result': kpi_total_res}
        if 'Target' not in params.keys():
            # if kpi_total_res > 0:
            #     return 100
            # else:
            #     return 0
            return kpi_total_res
        if params.get('Target') == 'range of targets':
            if not (params.get('target_min', 0) < kpi_total_res <= params.get('target_max', 100)):
                score = 0
                if kpi_total_res < params.get('target_min', 0):
                    self.thresholds_and_results[kpi_name]['threshold'] = params.get('target_min')
                else:
                    self.thresholds_and_results[kpi_name]['threshold'] = params.get('target_max')
            else:
                self.thresholds_and_results[kpi_name]['threshold'] = params.get('target_min')
                numerator = kpi_total_res - params.get('target_min', 0)
                denominator = params.get('target_max', 1) - params.get('target_min', 0)
                score = (numerator / float(denominator)) * 100
            return score

        elif params.get('Target') == 'targets by guide':
            target = self.kpi_fetcher.get_category_target_by_region(params.get('Values'), self.store_id)
        else:
            target = params.get('Target')

        self.thresholds_and_results[kpi_name]['threshold'] = target
        target = float(target)
        if not target:
            score = 0
        else:
            if params.get('score_func') == PROPORTIONAL:
                score = (kpi_total_res / target) * 100
                if score > 100:
                    score = 100
            elif params.get('score_func') == CONDITIONAL_PROPORTIONAL:
                score = kpi_total_res / target
                if score > params.get('score_max', 1):
                    score = params.get('score_max', 1)
                elif score < params.get('score_min', 0):
                    score = 0
                score *= 100
            elif params.get('score_func') == 'Customer_CCRU_1':
                if kpi_total_res < target:
                    score = 0
                else:
                    score = ((kpi_total_res - target) + 1) * 100
            else:
                if kpi_total_res >= target:
                    score = 100
                else:
                    score = 0

        return score

    def check_share_of_cch(self, params):
        """
        This function calculates number of SKUs per single scene type

        """
        set_total_res = 0
        for p in params.values()[0]:
            if p.get('Formula') != 'Share of CCH doors which have 98% TCCC facings' and p.get('Formula') != 'number of pure Coolers':
                continue
            scenes = None
            if 'depends on' in p.keys():
                depends_on_kpi_name = p.get('depends on')
                for c in params.values()[0]:
                    if c.get('KPI name Eng') == depends_on_kpi_name:
                        if c.get('Formula') == 'number of doors of filled Coolers':
                            scenes = self.check_number_of_doors_of_filled_coolers(c, 'get scenes')
                        elif c.get('Formula') == 'number of doors with more than Target facings':
                            scenes = self.calculate_number_of_doors_more_than_target_facings(c, 'get scenes')
                        elif c.get('Formula') == 'number of coolers with facings target and fullness target':
                            scenes = self.calculate_number_of_doors_more_than_target_facings(c, 'get scenes')
                            scenes = self.calculate_number_of_doors_of_filled_coolers(c, scenes, function = 'get scenes',
                                                                                              proportion_param=0.9)
                        break
                if not scenes:
                    if p.get('level') == 2:
                       scenes = []
                    else:
                        return 0
            else:
                scenes = self.get_relevant_scenes(p)
            if p.get('Formula') == 'number of pure Coolers':
                score = self.calculate_share_of_cch(p, scenes, sos=False)
                # if score >= 1:
                #     score=1
            else:
                score = self.calculate_share_of_cch(p, scenes)
            atomic_kpi_fk = self.kpi_fetcher.get_atomic_kpi_fk(p.get('KPI name Eng'))
            kpi_fk = self.kpi_fetcher.get_kpi_fk(p.get('KPI name Eng'))
            if 'KPI Weight' in p.keys():
                set_total_res += round(score) * p.get('KPI Weight')
            else:
                set_total_res += round(score)
            # saving to DB
            attributes_for_level3 = self.create_attributes_for_level3_df(p, score, kpi_fk, atomic_kpi_fk)
            self.write_to_db_result(attributes_for_level3, 'level3')
            attributes_for_level2 = self.create_attributes_for_level2_df(p, score, kpi_fk)
            self.write_to_db_result(attributes_for_level2, 'level2')
        return set_total_res

    def check_number_of_doors_of_filled_coolers(self, params, function = None, proportion = None):
        """
        This function calculates number of doors of filled Coolers

        """
        set_total_res = 0
        if function == 'get scenes':
            if not proportion:
                proportion = 0.8
            relevant_scenes = self.get_relevant_scenes(params)
            scenes = self.calculate_number_of_doors_of_filled_coolers(params, relevant_scenes, function = 'get scenes',
                                                                      proportion_param = proportion)
            return scenes
        # for p in params.values()[0]:
        #     if p.get('Formula') != 'number of doors of filled Coolers':
        #         continue
        scenes = self.get_relevant_scenes(params)
        result = self.calculate_number_of_doors_of_filled_coolers(params, scenes)
        # atomic_score =
        # atomic_kpi_fk = self.kpi_fetcher.get_atomic_kpi_fk(params.get('KPI name Eng'))
        # kpi_fk = self.kpi_fetcher.get_kpi_fk(parent_kpi.get('KPI name Eng'))
        # if 'KPI Weight' in params.keys():
        #     set_total_res += round(score) * params.get('KPI Weight')
        # else:
        #     set_total_res += round(score)
        # saving to DB
        # attributes_for_level3 = self.create_attributes_for_level3_df(params, score, kpi_fk, atomic_kpi_fk)
        # self.write_to_db_result(attributes_for_level3, 'level3')
        # attributes_for_level2 = self.create_attributes_for_level2_df(params, score, kpi_fk)
        # self.write_to_db_result(attributes_for_level2, 'level2')
        return result

    def check_number_of_scenes_given_facings(self, params):
        """
        This function calculates number of doors of filled Coolers

        """
        set_total_res = 0
        for p in params.values()[0]:
            if p.get('Formula') != 'number of scenes with have at least target amount of facings':
                continue
            scenes = self.get_relevant_scenes(p)
            score = self.calculate_number_of_scenes_given_facings(p, scenes)
            atomic_kpi_fk = self.kpi_fetcher.get_atomic_kpi_fk(p.get('KPI name Eng'))
            kpi_fk = self.kpi_fetcher.get_kpi_fk(p.get('KPI name Eng'))
            if 'KPI Weight' in p.keys():
                set_total_res += round(score) * p.get('KPI Weight')
            else:
                set_total_res += round(score)
            # saving to DB
            attributes_for_level3 = self.create_attributes_for_level3_df(p, score, kpi_fk, atomic_kpi_fk)
            self.write_to_db_result(attributes_for_level3, 'level3')
            attributes_for_level2 = self.create_attributes_for_level2_df(p, score, kpi_fk)
            self.write_to_db_result(attributes_for_level2, 'level2')
        return set_total_res

    def calculate_share_of_cch(self, p, scenes, sos = True):
        sum_of_passed_doors = 0
        sum_of_passed_scenes = 0
        sum_of_all_doors = 0
        for scene in scenes:
            products_of_tccc = self.scif[(self.scif['scene_id'] == scene) &
                                         (self.scif['manufacturer_name'] == 'TCCC') &
                                         (self.scif['location_type'] == p.get('Locations to include')) &
                                         (self.scif['product_type']!='Empty')]['facings'].sum()
            all_products = self.scif[(self.scif['scene_id'] == scene) &
                                     (self.scif['location_type'] == p.get('Locations to include')) &
                                     (self.scif['product_type'] != 'Empty')]['facings'].sum()
            if products_of_tccc == 0:
                proportion = 0
            else:
                proportion = products_of_tccc / all_products
            scene_type = self.scif.loc[self.scif['scene_id'] == scene]['template_name'].values[0]
            if any(self.templates[self.templates['template_name'] == scene_type]['additional_attribute_1']):
                num_of_doors = \
                    float(self.templates[self.templates['template_name'] == scene_type]['additional_attribute_1'].values[0])
                sum_of_all_doors += num_of_doors
            else:
                num_of_doors = 1
                sum_of_all_doors += num_of_doors
            if proportion > 0.98:
                sum_of_passed_doors += num_of_doors
                sum_of_passed_scenes += 1
        if not sos:
            return sum_of_passed_scenes
        if sum_of_all_doors:
            ratio = (sum_of_passed_doors / float(sum_of_all_doors)) * 100
        else:
            ratio = 0
        # if p.get('level') == 3:
        kpi_name = p.get('KPI name Eng')
        self.thresholds_and_results[kpi_name] = {'result': sum_of_passed_doors}
        return ratio

    def calculate_number_of_doors_of_filled_coolers(self, p, scenes, function=None, proportion_param = 0.8):
        sum_of_passed_doors = 0
        scenes_passed = []
        for scene in scenes:
            products_of_tccc = self.scif[(self.scif['scene_id'] == scene) &
                                         (self.scif['manufacturer_name'] == 'TCCC') &
                                         (self.scif['location_type'] == p.get('Locations to include')) &
                                         (self.scif['product_type'] != 'Empty')]['facings'].sum()
            all_products = self.scif[(self.scif['scene_id'] == scene) &
                                     (self.scif['location_type'] == p.get('Locations to include'))]['facings'].sum()
            if all_products:
                proportion = products_of_tccc / all_products
            else:
                proportion = 0
            scene_type = self.scif.loc[self.scif['scene_id'] == scene]['template_name'].values[0]
            if any(self.templates[self.templates['template_name'] == scene_type]['additional_attribute_1']):
                num_of_doors = \
                    float(self.templates[self.templates['template_name'] == scene_type]['additional_attribute_1'].values[0])
            else:
                num_of_doors = 1
            if proportion >= proportion_param:
                sum_of_passed_doors += num_of_doors
                scenes_passed.append(scene)
        if function == 'get scenes':
            return scenes_passed
        return sum_of_passed_doors

    def calculate_number_of_scenes_given_facings(self, p, scenes):
        sum_of_passed_scenes = 0
        for scene in scenes:
            facings = self.calculate_availability(p, scenes=[scene])
            if facings >= p.get('Target'):
                sum_of_passed_scenes += 1
        return sum_of_passed_scenes

    def check_number_of_skus_per_door_range(self, params):
        """
        This function calculates number of SKUs per single scene type

        """
        set_total_res = 0
        for p in params.values()[0]:
            if p.get('Formula') != 'number of SKU per Door RANGE':
                continue
            scenes = None
            if 'depends on' in p.keys():
                depends_on_kpi_name = p.get('depends on')
                for c in params.values()[0]:
                    if c.get('KPI name Eng') == depends_on_kpi_name:
                        if c.get('Formula') == 'number of doors of filled Coolers':
                            scenes = self.check_number_of_doors_of_filled_coolers(c, 'get scenes')
                        elif c.get('Formula') == 'number of coolers with facings target and fullness target':
                            scenes = self.calculate_number_of_doors_more_than_target_facings(c, 'get scenes')
                            scenes = self.calculate_number_of_doors_of_filled_coolers(c, scenes,
                                                                                      function = 'get scenes',
                                                                                          proportion_param=0.9)
                        else:
                            scenes = self.calculate_number_of_doors_more_than_target_facings(c, 'get scenes')
                if not scenes:
                    if p.get('level') == 2:
                        scenes = []
                    else:
                        return 0
            else:
                scenes = self.get_relevant_scenes(p)
            score = self.calculate_number_of_skus_per_door_range(p, scenes)
            atomic_kpi_fk = self.kpi_fetcher.get_atomic_kpi_fk(p.get('KPI name Eng'))
            if p.get('level') == 3:
                return score
            # saving to DB
            if p.get('level') == 2:
                kpi_fk = self.kpi_fetcher.get_kpi_fk(p.get('KPI name Eng'))
                if 'KPI Weight' in p.keys():
                    set_total_res += round(score) * p.get('KPI Weight')
                else:
                    set_total_res += round(score)
                attributes_for_level3 = self.create_attributes_for_level3_df(p, score, kpi_fk, atomic_kpi_fk)
                self.write_to_db_result(attributes_for_level3, 'level3')
                attributes_for_level2 = self.create_attributes_for_level2_df(p, score, kpi_fk)
                self.write_to_db_result(attributes_for_level2, 'level2')
                # set_total_res += round(score) * p.get('KPI Weight')
        return set_total_res

    def calculate_number_of_skus_per_door_range(self, p, scenes):
        sum_of_passed_doors = 0
        sum_of_all_doors = 0
        for scene in scenes:
            eans_count = len(self.scif[(self.scif['scene_id'] == scene) &
                                       (self.scif['manufacturer_name'] == 'TCCC') &
                                       (self.scif['location_type'] == p.get('Locations to include')) &
                                       (self.scif['facings'] > 0) & (self.scif['product_type'] != 'Empty')][
                                 'product_ean_code'].unique())
            scene_type = self.scif.loc[self.scif['scene_id'] == scene]['template_name'].values[0]
            if any(self.templates[self.templates['template_name'] == scene_type]['additional_attribute_1']):
                num_of_doors = \
                    float(self.templates[self.templates['template_name'] == scene_type]['additional_attribute_1'].values[0])
            else:
                num_of_doors = 1
            sum_of_all_doors += num_of_doors
            if p.get('target_min') <= eans_count / num_of_doors <= p.get('target_max'):
                sum_of_passed_doors += num_of_doors
        kpi_name = p.get('KPI name Eng')
        self.thresholds_and_results[kpi_name] = {'result': sum_of_passed_doors}
        if sum_of_all_doors:
            return (sum_of_passed_doors / sum_of_all_doors) * 100
        return 0

    def check_number_of_skus_in_single_scene_type(self, params):
        """
        This function calculates number of SKUs per single scene type

        """
        set_total_res = 0
        for p in params.values()[0]:
            if p.get('Formula') != 'number of SKUs in one scene type' or p.get('level') == 3:
                continue
            score = self.calculate_number_of_skus_in_single_scene_type(params, p)
            set_total_res += round(score) * p.get('KPI Weight')

        return set_total_res

    def calculate_number_of_skus_in_single_scene_type(self, params, p, kpi_fk=None):
        if kpi_fk is None:
            kpi_fk = self.kpi_fetcher.get_kpi_fk(p.get('KPI name Eng'))
        results_dict = {}
        relevant_scenes = self.get_relevant_scenes(params)
        for scene in relevant_scenes:
            scene_type = self.scif.loc[self.scif['scene_id'] == scene]['template_name'].values[0]
            location = list(self.scif.loc[self.scif['scene_id'] == scene]['location_type'].values)
            if location:
                location = location[0]
            sub_location = list(
                self.scif.loc[self.scif['template_name'] == scene_type]['additional_attribute_2'].values)
            if sub_location:
                sub_location = sub_location[0]
            if p.get('Children') is not None:
                children_scores = []
                for child in params.values()[0]:
                    if child.get('KPI ID') in [int(kpi) for kpi in p.get('Children').split(', ')]:
                        res = self.calculate_number_of_skus_in_single_scene_type(params, child, kpi_fk)
                        children_scores.append(res)
                score = max(children_scores)
                # saving to level2
                attributes_for_table2 = self.create_attributes_for_level2_df(p, score, kpi_fk)
                self.write_to_db_result(attributes_for_table2, 'level2')
                return score
            else:
                res = self.calculate_availability(p, scenes=[scene])
                type_to_split_by = scene_type
                if p.get('Scenes to include'):  # by scene
                    type_to_split_by = scene_type
                elif sub_location and p.get('Sub locations to include'):
                    type_to_split_by = sub_location
                elif location and p.get('Locations to include'):
                    type_to_split_by = location
                if type_to_split_by not in results_dict:
                    results_dict[type_to_split_by] = 0
                results_dict[type_to_split_by] += res

        results_list = [self.calculate_score(res, p) for res in results_dict.values()]
        results_list = filter(bool, results_list)  # filtering the score=0
        if len(results_list) == 1:
            score = 100
        else:
            score = 0
        # Saving to old tables
        if p.get('level') == 2:  # saving also to level3 in case this KPI has only one level
            attributes_for_table2 = self.create_attributes_for_level2_df(p, score, kpi_fk)
            self.write_to_db_result(attributes_for_table2, 'level2')
        attributes_for_table3 = self.create_attributes_for_level3_df(p, score, kpi_fk)
        self.write_to_db_result(attributes_for_table3, 'level3')

        return score

    def calculate_lead_sku(self, params, scenes=[]):
        """

        :param params:
        :return:
        """
        result = 0.0
        kpi_name = params.get('KPI name Eng')
        if not scenes:
            scenes = self.get_relevant_scenes(params)
        if params.get('Product Category'):
            Category = [str(params.get('Product Category'))]
            relevant_products_and_facings = self.scif[
                (self.scif['scene_id'].isin(scenes)) & ~(self.scif['product_type'].isin(['Empty', 'Other'])) &
                (self.scif['category'].isin(Category))]
        else:
            relevant_products_and_facings = self.scif[
                (self.scif['scene_id'].isin(scenes)) & ~(self.scif['product_type'].isin(['Empty', 'Other']))]
        all_products_by_ean_code = relevant_products_and_facings.groupby(['product_ean_code'])['facings'].sum()
        # tested_sku = str(params.get('Values'))
        if ', ' not in str(params.get('Values')):
            tested_sku = [str(params.get('Values'))]
        else:
            tested_sku = [str(s) for s in params.get('Values').split(', ')]
        tested_facings = \
            relevant_products_and_facings[relevant_products_and_facings['product_ean_code'].isin(tested_sku)][
                'facings'].sum()
        self.thresholds_and_results[kpi_name] = {'result': tested_facings}
        if not all_products_by_ean_code.empty:
            if sum(tested_facings < all_products_by_ean_code) == 0:
                return tested_facings
            else:
                return 0
        else:
            return 0

    def calculate_number_of_scenes(self, p):
        """
        This function is used to calculate number of scenes

        """
        kpi_total_res = 0
        scenes = self.get_relevant_scenes(p)
        if p.get('Type') == 'SCENES':
            if p.get('Values'):
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
            else:
                return len(scenes)
        elif p.get('Type') == 'LOCATION_TYPE':
            values_list = [str(s) for s in p.get('Values').split(', ')]
            for scene in scenes:
                try:
                    location_type = self.scif.loc[self.scif['scene_id'] == scene]['location_type'].values[0]
                    if location_type in values_list:
                        res = 1
                    else:
                        res = 0
                    kpi_total_res += res
                except IndexError as e:
                    continue
        elif p.get('Type') == 'SUB_LOCATION_TYPE':
            values_list = [p.get('Values')]
            for scene in scenes:
                try:
                    scene_type = self.scif.loc[self.scif['scene_id'] == scene]['template_name'].values[0]
                    sub_location_type = int(self.templates.loc[self.templates['template_name'] == scene_type][
                                                'additional_attribute_2'].values[0])
                    if sub_location_type in values_list:
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
        # atomic_kpi_fk = self.kpi_fetcher.get_atomic_kpi_fk(p.get('KPI name Eng'))
        # kpi_fk = self.kpi_fetcher.get_kpi_fk(p.get('KPI name Eng'))
        # attributes_for_level3 = self.create_attributes_for_level3_df(p, score, kpi_fk, atomic_kpi_fk)
        # self.write_to_db_result(attributes_for_level3, 'level3', kpi_fk)
        return score

    def calculate_tccc_40(self, params):
        facings = self.calculate_availability(params)
        return float(facings) / 40

    def customer_cooler_doors(self, params):
        set_total_res = 0
        for p in params.values()[0]:
            if p.get('Formula') != "facings TCCC/40":
                continue
            kpi_total_res = self.calculate_tccc_40(p)
            score = self.calculate_score(kpi_total_res, p)
            set_total_res += round(score) * p.get('KPI Weight')
            # writing to DB
            kpi_fk = self.kpi_fetcher.get_kpi_fk(p.get('KPI name Eng'))
            attributes_for_level3 = self.create_attributes_for_level3_df(p, score, kpi_fk)
            self.write_to_db_result(attributes_for_level3, 'level3', kpi_fk)
            if p.get('level') == 2:
                attributes_for_level2 = self.create_attributes_for_level2_df(p, score, kpi_fk)
                self.write_to_db_result(attributes_for_level2, 'level2', kpi_fk)
        return set_total_res

    def check_sum_atomics(self, params):
        """

        :param params:
        :return:
        """
        set_total_res = 0
        for p in params.values()[0]:
            if p.get('Formula') != "sum of atomic KPI result" or not p.get("Children"):
                continue
            kpi_fk = self.kpi_fetcher.get_kpi_fk(p.get('KPI name Eng'))
            children = map(int, map(float, str(p.get("Children")).split("\n")))
            kpi_total = 0
            score=0
            for c in params.values()[0]:
                if c.get("KPI ID") in children:
                    if c.get("Formula") == "number of facings":
                        atomic_res = self.calculate_availability(c)
                    elif c.get("Formula") == "number of doors with more than Target facings":
                        atomic_res = self.calculate_number_of_doors_more_than_target_facings(c, 'sum of doors')
                    elif c.get("Formula") == "facings TCCC/40":
                        atomic_res = self.calculate_tccc_40(c)
                    elif c.get("Formula") == "number of doors of filled Coolers":
                        atomic_res = self.check_number_of_doors_of_filled_coolers(c)
                    elif c.get("Formula") == "check_number_of_scenes_with_facings_target":
                        atomic_res = self.check_number_of_scenes_with_target(c)
                    elif c.get("Formula") == "number of coolers with facings target and fullness target":
                        scenes = self.calculate_number_of_doors_more_than_target_facings(c, 'get scenes')
                        atomic_res = self.calculate_number_of_doors_of_filled_coolers(c, scenes,
                                                                               proportion_param=0.9)
                    else:
                        # print "sum of atomic KPI result:", c.get("Formula")
                        atomic_res = 0

                    atomic_score = self.calculate_score(atomic_res, c)

                    kpi_total += atomic_res
                    # write to DB
                    kpi_fk = self.kpi_fetcher.get_kpi_fk(p.get('KPI name Eng'))
                    atomic_kpi_fk = self.kpi_fetcher.get_atomic_kpi_fk(c.get('KPI name Eng'))
                    attributes_for_level3 = self.create_attributes_for_level3_df(c, atomic_score, kpi_fk, atomic_kpi_fk)
                    self.write_to_db_result(attributes_for_level3, 'level3')

            if p.get('Target'):
                if p.get('score_func') == 'PROPORTIONAL':
                        # score = (kpi_total/p.get('Target')) * 100
                    score = min((float(kpi_total)/p.get('Target')) * 100, 100)
                else:
                    if kpi_total >= p.get('Target'):
                        score = 100
                    else:
                        score = 0
            elif self.attr15:
                # if kpi_total >= self.attr15:
                score = (kpi_total/self.attr15) * 100
                if score > 100:
                    score = 100
            else:
                score = 0
                # print "attr 15 doesn't exist in store {}".format(self.store_id)
            if 'KPI Weight' in p.keys():
                set_total_res += round(score) * p.get('KPI Weight')
            else:
                set_total_res += score
            # saving to DB
            attributes_for_level2 = self.create_attributes_for_level2_df(p, score, kpi_fk)
            self.write_to_db_result(attributes_for_level2, 'level2')
            if p.get('Target Execution 2018'):  # insert the results that needed for target execution set
                kpi_name = p.get('KPI name Eng')
                self.insert_scores_level2(kpi_total, score, kpi_name)
        return set_total_res

    def check_atomic_passed(self, params):
        """

        :param params:
        :return:
        """
        set_total_res = 0
        for p in params.values()[0]:
            if p.get('Formula') != "number of atomic KPI Passed" or not p.get("Children"):
                continue
            kpi_fk = self.kpi_fetcher.get_kpi_fk(p.get('KPI name Eng'))
            children = map(int, p.get("Children").split("\n"))
            kpi_total = 0
            for c in params.values()[0]:
                if c.get("KPI ID") in children:
                    atomic_score = -1
                    if c.get("Formula") == "number of facings" or c.get("Formula") == "number of SKUs":
                        atomic_res = self.calculate_availability(c, all_params =params)
                    elif c.get("Formula") == "number of sub atomic KPI Passed":
                        atomic_res = self.calculate_sub_atomic_passed(c, params, parent = p)
                    elif c.get("Formula") == "Lead SKU":
                        atomic_res = self.calculate_lead_sku(c)
                        if not atomic_res:
                            atomic_score = 0
                        else:
                            atomic_score = 100
                    elif c.get("Formula") == "number of scenes":
                        atomic_res = self.calculate_number_of_scenes(c)
                    elif c.get("Formula") == "number of facings near food":
                        atomic_res = self.calculate_number_facings_near_food(c, params)
                    elif c.get("Formula") == "number of doors with more than Target facings":
                        atomic_res = self.calculate_number_of_doors_more_than_target_facings(c, 'sum of doors')
                    elif c.get("Formula") == "number of doors of filled Coolers":
                        atomic_res = self.check_number_of_doors_of_filled_coolers(c)
                    elif c.get("Formula") == "number of pure Coolers":
                        scenes = self.get_relevant_scenes(c)
                        atomic_res = self.calculate_share_of_cch(c, scenes, sos=False)
                    elif c.get("Formula") == "number of filled Coolers (scenes)":
                        scenes_list = self.check_number_of_doors_of_filled_coolers(c, function = 'get scenes')
                        atomic_res = len(scenes_list)
                    elif c.get("Formula") == "number of SKU per Door RANGE":
                        atomic_score = self.check_number_of_skus_per_door_range(params)
                    elif c.get("Formula") == "Scenes with no tagging":
                        atomic_res = self.calculate_number_of_scenes_no_tagging(c, level = 3)
                    else:
                        # print "the atomic's formula is ", c.get('Formula')
                        atomic_res = 0
                    if atomic_res == -1:
                        continue
                    if atomic_score == -1:
                        atomic_score = self.calculate_score(atomic_res, c)
                    # write to DB
                    atomic_kpi_fk = self.kpi_fetcher.get_atomic_kpi_fk(c.get('KPI name Eng'))
                    attributes_for_level3 = self.create_attributes_for_level3_df(c, atomic_score, kpi_fk, atomic_kpi_fk)
                    self.write_to_db_result(attributes_for_level3, 'level3')
                    if atomic_score > 0:
                        kpi_total += 1
            score = self.calculate_score(kpi_total, p)
            if 'KPI Weight' in p.keys():
                set_total_res += round(score) * p.get('KPI Weight')
            else:
                set_total_res += score
            # saving to DB
            attributes_for_level2 = self.create_attributes_for_level2_df(p, score, kpi_fk)
            self.write_to_db_result(attributes_for_level2, 'level2')
            if p.get('Target Execution 2018'):  # insert the results that needed for target execution set
                kpi_name = p.get('KPI name Eng')
                self.insert_scores_level2(kpi_total, score, kpi_name)
        return set_total_res

    def check_atomic_passed_on_the_same_scene(self, params):
        set_total_res = 0
        self.passed_scenes_per_kpi = {}
        # self.passed_scenes = []
        for p in params.values()[0]:
            score = 0
            if p.get('Formula') != "number of atomic KPI Passed on the same scene" or not p.get("Children"):
                continue
            kpi_fk = self.kpi_fetcher.get_kpi_fk(p.get('KPI name Eng'))
            children = map(int, p.get("Children").split("\n"))
            info_by_kpi_id = self.build_dict(params.values()[0], 'KPI ID')
            if 'depends on' in p.keys():
                depends_on_kpi_name = p.get('depends on')
                if depends_on_kpi_name in self.passed_scenes_per_kpi.keys():
                    relevant_scenes = self.passed_scenes_per_kpi[depends_on_kpi_name]
                else:
                    relevant_scenes = []
            else:
                relevant_scenes = self.get_relevant_scenes(p)
            # kpi_total = 0
            passed_scenes = []
            scenes_kpi_info = {}
            favorite_scene = None
            one_scene_passed = 0
            for scene in relevant_scenes:
                scenes_kpi_info[scene] = {'num_passed_kpi': 0, 'total_row_no_passed': 0}
                kpi_total = 0
                index = 0
                for child in children:
                    atomic_score = -1
                    index += 1
                    c = info_by_kpi_id.get(child)
                    if c.get("Formula") == "number of facings":
                        atomic_res = self.calculate_availability(c, [scene])
                    elif c.get("Formula") == "number of sub atomic KPI Passed":
                        atomic_res = self.calculate_sub_atomic_passed(c, params, [scene], parent = p, same_scene = True)
                    elif c.get("Formula") == "Lead SKU":
                        atomic_res = self.calculate_lead_sku(c, [scene])
                        if not atomic_res:
                            atomic_score = 0
                        else:
                            atomic_score = 100
                    elif c.get("Formula") == "number of scenes":
                        list_of_scenes = self.get_relevant_scenes(c)
                        if scene in list_of_scenes:
                            atomic_res = 1
                        else:
                            atomic_res = 0
                    else:
                        atomic_res = 0
                    if atomic_res == -1:
                        continue
                    if atomic_score == -1:
                        atomic_score = self.calculate_score(atomic_res, c)
                    kpi_total += atomic_score / 100
                    if atomic_score:
                        scenes_kpi_info[scene]['num_passed_kpi'] += 1
                        scenes_kpi_info[scene]['total_row_no_passed'] += 1
                score = self.calculate_score(kpi_total, p)
                if score:
                    one_scene_passed = 1
                    if p.get('KPI name Eng') in self.passed_scenes_per_kpi:
                        self.passed_scenes_per_kpi[p.get('KPI name Eng')].append(scene)
                    else:
                        self.passed_scenes_per_kpi[p.get('KPI name Eng')] = [scene]
                    # break
            if relevant_scenes:
                closest_to_pass_scenes = self.get_max_in_dict(scenes_kpi_info)
                if len(closest_to_pass_scenes) == 1:
                    favorite_scene = closest_to_pass_scenes[0]
                else:
                    favorite_scene = self.get_favorite_scene(closest_to_pass_scenes, scenes_kpi_info)
            kpi_total = 0
            for child in children:
                atomic_score = -1
                c = info_by_kpi_id.get(child)
                if favorite_scene:
                    if c.get("Formula") == "number of facings":
                        atomic_res = self.calculate_availability(c, [favorite_scene])
                    elif c.get("Formula") == "number of sub atomic KPI Passed":
                        atomic_res = self.calculate_sub_atomic_passed(c, params, [favorite_scene], parent=p)
                    elif c.get("Formula") == "Lead SKU":
                        atomic_res = self.calculate_lead_sku(c, [favorite_scene])
                        if not atomic_res:
                            atomic_score = 0
                        else:
                            atomic_score = 100
                    elif c.get("Formula") == "number of scenes":
                        list_of_scenes = self.get_relevant_scenes(c)
                        if favorite_scene in list_of_scenes:
                            atomic_res = 1
                        else:
                            atomic_res = 0
                    else:
                        # print "the atomic's formula is ", c.get('Formula')
                        atomic_res = 0
                    if atomic_res == -1:
                        continue
                    if atomic_score == -1:
                        atomic_score = self.calculate_score(atomic_res, c)
                    kpi_total += atomic_score / 100
                    # score = self.calculate_score(kpi_total, p)
                else:
                    if c.get("Formula") == "number of sub atomic KPI Passed":
                        sub_atomic_children = map(int, c.get("Children").split("\n"))
                        for sub_atomic in sub_atomic_children:
                            sub_atomic_info = info_by_kpi_id.get(sub_atomic)
                            atomic_res = 0
                            atomic_score = 0
                            self.calculate_score(atomic_res, sub_atomic_info)
                            kpi_name = sub_atomic_info.get('KPI name Eng')
                            atomic_kpi_fk = self.kpi_fetcher.get_atomic_kpi_fk(kpi_name)
                            attributes_for_level3 = self.create_attributes_for_level3_df(sub_atomic_info, atomic_score, kpi_fk,
                                                                                         atomic_kpi_fk)
                            self.write_to_db_result(attributes_for_level3, 'level4')
                    atomic_res = 0
                    atomic_score = 0
                    self.calculate_score(atomic_res, c)

                kpi_name = c.get('KPI name Eng')
                atomic_kpi_fk = self.kpi_fetcher.get_atomic_kpi_fk(kpi_name)
                attributes_for_level3 = self.create_attributes_for_level3_df(c, atomic_score, kpi_fk,
                                                                             atomic_kpi_fk)
                self.write_to_db_result(attributes_for_level3, 'level3')
            # attributes_for_level2 = self.create_attributes_for_level2_df(p, score, kpi_fk)
            # self.write_to_db_result(attributes_for_level2, 'level2')
            score = self.calculate_score(kpi_total, p)
            if 'KPI Weight' in p.keys():
                set_total_res += round(score) * p.get('KPI Weight')
            else:
                set_total_res += score
            # saving to DB
            attributes_for_level2 = self.create_attributes_for_level2_df(p, score, kpi_fk)
            self.write_to_db_result(attributes_for_level2, 'level2')
        return set_total_res



    def calculate_sub_atomic_passed(self, params, all_params, scenes=[], parent = None, same_scene = None):
        """

        :param all_params:
        :param params:
        :return:
        """
        if not scenes:
            if 'depends on' in params.keys():
                if params['depends on'] == 'scene type':
                    scenes = self.get_relevant_scenes(params)
                    if not scenes:
                        return -1
                else:
                    depends_on_kpi_name = params.get('depends on')
                    doors = 0
                    for c in all_params.values()[0]:
                        if c.get('KPI name Eng') == depends_on_kpi_name:
                            doors = self.calculate_number_of_doors_more_than_target_facings(c, 'get doors')
                            break
                    if doors < 2:
                        return -1
        children = map(int, params.get("Children").split("\n"))
        total_res = 0
        for c in all_params.values()[0]:
            if c.get("KPI ID") in children:
                sub_atomic_score = -1
                if c.get("Formula") == "number of facings":
                    sub_atomic_res = self.calculate_availability(c, scenes)
                elif c.get("Formula") == "Lead SKU":
                    sub_atomic_res = self.calculate_lead_sku(c, scenes)
                    if not sub_atomic_res:
                        sub_atomic_score = 0
                    else:
                        sub_atomic_score = 100
                else:
                    # print "the sub-atomic's formula is ", c.get('Formula')
                    sub_atomic_res = 0
                if sub_atomic_score == -1:
                    sub_atomic_score = self.calculate_score(sub_atomic_res, c)
                total_res += sub_atomic_score / 100
                if same_scene:
                    if parent.get('Formula') == 'number of atomic KPI Passed on the same scene':
                        continue
                kpi_fk = self.kpi_fetcher.get_kpi_fk(parent.get('KPI name Eng'))
                sub_atomic_kpi_fk = self.kpi_fetcher.get_atomic_kpi_fk(c.get('KPI name Eng'))
                attributes_for_level4 = self.create_attributes_for_level3_df(c, sub_atomic_score, kpi_fk,
                                                                             sub_atomic_kpi_fk)
                self.write_to_db_result(attributes_for_level4, 'level4')
        return total_res

    def check_weighted_average(self, params):
        """

        :param params:
        :return:
        """
        set_total_res = 0
        for p in params.values()[0]:
            if p.get('Formula').strip() not in ("Weighted Average", "average of atomic KPI Score") or not p.get("Children"):
                continue
            kpi_fk = self.kpi_fetcher.get_kpi_fk(p.get('KPI name Eng'))
            children = map(int, p.get("Children").split("\n"))
            kpi_total = 0
            kpi_total_weight = 0
            for c in params.values()[0]:
                if c.get("KPI ID") in children:
                    if c.get("Formula") == "number of facings":
                        atomic_res = self.calculate_availability(c)
                    elif c.get("Formula") == "number of sub atomic KPI Passed":
                        atomic_res = self.calculate_sub_atomic_passed(c, params, parent = p)
                    elif c.get("Formula") == "check_number_of_scenes_with_facings_target":
                        atomic_res = self.check_number_of_scenes_with_target(c)
                    else:
                        atomic_res = -1
                        # print "Weighted Average", c.get("Formula")
                    if atomic_res == -1:
                        continue
                    atomic_score = self.calculate_score(atomic_res, c)
                    if p.get('Formula').strip() == "Weighted Average":
                        kpi_total += atomic_score * c.get('KPI Weight')
                        kpi_total_weight += c.get('KPI Weight')
                    else:
                        kpi_total += atomic_score
                        kpi_total_weight += 1
                    # write to DB
                    atomic_kpi_fk = self.kpi_fetcher.get_atomic_kpi_fk(c.get('KPI name Eng'))
                    attributes_for_level3 = self.create_attributes_for_level3_df(c, atomic_score, kpi_fk, atomic_kpi_fk)
                    self.write_to_db_result(attributes_for_level3, 'level3')
            if kpi_total_weight:
                kpi_total /= kpi_total_weight
            else:
                kpi_total = 0
            kpi_score = self.calculate_score(kpi_total, p)
            if 'KPI Weight' in p.keys():
                set_total_res += round(kpi_score) * p.get('KPI Weight')
            else:
                set_total_res += round(kpi_score) * kpi_total_weight
            # saving to DB
            if kpi_fk:
                attributes_for_level2 = self.create_attributes_for_level2_df(p, kpi_score, kpi_fk)
                self.write_to_db_result(attributes_for_level2, 'level2')
        return set_total_res

    def calculate_number_of_scenes_no_tagging(self, params, level = None):
        scenes_info = pd.merge(self.scenes_info, self.templates, on='template_fk')
        if level == 3:
            if params.get('Scenes to include'):
                values_list = [str(s) for s in params.get('Scenes to include').split(', ')]
                number_relevant_scenes = scenes_info['template_name'].isin(values_list).sum()
                return number_relevant_scenes
            else:
                return 0
        else: # level 2
            set_total_res = 0
            number_relevant_scenes = 0
            scenes = []
            for p in params.values()[0]:
                if p.get('Formula') != "Scenes with no tagging":
                    continue
                kpi_fk = self.kpi_fetcher.get_kpi_fk(p.get('KPI name Eng'))
                if 'depends on' in p.keys():
                    depends_on_kpi_name = p.get('depends on')
                    for c in params.values()[0]:
                        if c.get('KPI name Eng') == depends_on_kpi_name:
                            if c.get('Formula') == 'number of doors with more than Target facings':
                                scenes = self.calculate_number_of_doors_more_than_target_facings(c,
                                                                                                         'get scenes')
                            elif c.get('Formula') == 'number of doors of filled Coolers':
                                scenes = self.check_number_of_doors_of_filled_coolers(c, 'get scenes')
                            break
                    if len(scenes) >= 1:
                        flag = 0
                        final_scenes = scenes_info
                        if p.get('Scenes to include'):
                            scenes_values_list = [str(s) for s in p.get('Scenes to include').split(', ')]
                            final_scenes = scenes_info['template_name'].isin(scenes_values_list)
                            flag = 1
                        if p.get('Locations to include'):
                            location_values_list = [str(s) for s in p.get('Locations to include').split(', ')]
                            if flag:
                                if sum(final_scenes):
                                    final_scenes = scenes_info[final_scenes]['location_type'].isin(location_values_list)
                            else:
                                final_scenes = final_scenes['location_type'].isin(location_values_list)
                        number_relevant_scenes = final_scenes.sum()
                else:
                    if p.get('Scenes to include'):
                        values_list = [str(s) for s in p.get('Scenes to include').split(', ')]
                        number_relevant_scenes = scenes_info['template_name'].isin(values_list).sum()

                score = self.calculate_score(number_relevant_scenes, p)
                if 'KPI Weight' in p.keys():
                    set_total_res += round(score) * p.get('KPI Weight')
                else:
                    set_total_res += round(score)
                kpi_fk = self.kpi_fetcher.get_kpi_fk(p.get('KPI name Eng'))
                attributes_for_level2 = self.create_attributes_for_level2_df(p, score, kpi_fk)
                if p.get('Target Execution 2018'): # insert the results that needed for target execution set
                    kpi_name = p.get('KPI name Eng')
                    self.insert_scores_level2(number_relevant_scenes, score, kpi_name)
                self.write_to_db_result(attributes_for_level2, 'level2', kpi_fk)
                # save to level 3
                atomic_kpi_fk = self.kpi_fetcher.get_atomic_kpi_fk(p.get('KPI name Eng'))
                attributes_for_level3 = self.create_attributes_for_level3_df(p, score, kpi_fk, atomic_kpi_fk)
                self.write_to_db_result(attributes_for_level3, 'level3')
            return set_total_res

    def build_dict(self, seq, key):
        return dict((d[key], dict(d, index=index)) for (index, d) in enumerate(seq))

    def get_max_in_dict(self, dict):
        max_value = 0
        closest_to_pass_scenes = []
        for row in dict.values():
            if row.get('num_passed_kpi') > max_value:
                max_value = row.get('num_passed_kpi')
        for scene, value in dict.items():
            if value.get('num_passed_kpi') == max_value:
                closest_to_pass_scenes.append(scene)
        return closest_to_pass_scenes

    def get_favorite_scene(self, potential_scenes, dict):
        for scene in potential_scenes:
            priority = 999
            if dict[scene]['total_row_no_passed'] < priority:
                favorite_scene = scene
        return favorite_scene


    def write_to_db_result(self, df=None, level=None, kps_name_temp=None):
        """
        This function writes KPI results to old tables

        """
        if level == 'level4':
            df['atomic_kpi_fk'] = self.kpi_fetcher.get_atomic_kpi_fk(df['name'][0])
            df['kpi_fk'] = df['kpi_fk'][0]
            df_dict = df.to_dict()
            df_dict['scope_value'] = {0: 'level 4'}
            df_dict.pop('name', None)
            query = insert(df_dict, KPI_RESULT)
            self.kpi_results_queries.append(query)
        elif level == 'level3':
            df['atomic_kpi_fk'] = self.kpi_fetcher.get_atomic_kpi_fk(df['name'][0])
            df['kpi_fk'] = df['kpi_fk'][0]
            df_dict = df.to_dict()
            df_dict.pop('name', None)
            query = insert(df_dict, KPI_RESULT)
            self.kpi_results_queries.append(query)
        elif level == 'level2':
            kpi_name = df['kpk_name'][0].encode('utf-8')
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

    def commit_results_data(self):
        self.rds_conn.disconnect_rds()
        self.rds_conn = self.rds_connection()
        cur = self.rds_conn.db.cursor()
        delete_queries = self.kpi_fetcher.get_delete_session_results(self.session_uid, self.session_fk)
        for query in delete_queries:
            cur.execute(query)
        self.rds_conn.db.commit()
        self.rds_conn.disconnect_rds()
        self.rds_conn = self.rds_connection()
        cur = self.rds_conn.db.cursor()
        for query in self.kpi_results_queries:
            cur.execute(query)
        self.rds_conn.db.commit()
        self.rds_conn.disconnect_rds()
        self.rds_conn = self.rds_connection()
        cur = self.rds_conn.db.cursor()
        for query in set(self.gaps_queries):
            cur.execute(query)
        self.rds_conn.db.commit()
        self.rds_conn.disconnect_rds()
        self.rds_conn = self.rds_connection()
        cur = self.rds_conn.db.cursor()
        top_sku_queries = self.merge_insert_queries(self.top_sku_queries)
        for query in top_sku_queries:
            cur.execute(query)
        self.rds_conn.db.commit()
        self.rds_conn.disconnect_rds()
        return

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

    def create_attributes_for_level2_df(self, params, score, kpi_fk):
        """
        This function creates a data frame with all attributes needed for saving in level 2 tables

        """
        score = round(score)
        attributes_for_table2 = pd.DataFrame([(self.session_uid, self.store_id,
                                               self.visit_date.isoformat(), kpi_fk,
                                               params.get('KPI name Eng').replace("'", "\\'"), score)],
                                             columns=['session_uid', 'store_fk', 'visit_date', 'kpi_fk',
                                                      'kpk_name', 'score'])
        self.kpk_scores[params.get('KPI name Eng')] = {'score': score, 'rus_name': params.get('KPI name Rus')}

        return attributes_for_table2

    def create_attributes_for_level3_df(self, params, score, kpi_fk, atomic_kpi_fk=None):
        """
        This function creates a data frame with all attributes needed for saving in level 3 tables

        """
        result = None
        if isinstance(score, tuple):
            score, result, threshold = score
        score = round(score)
        if result is None:
            if self.thresholds_and_results.get(params.get("KPI name Eng")):
                result = self.thresholds_and_results[params.get("KPI name Eng")]['result']
                if 'threshold' in self.thresholds_and_results[params.get("KPI name Eng")].keys():
                    threshold = self.thresholds_and_results[params.get("KPI name Eng")]['threshold']
                else:
                    threshold = 0
            else:
                result = threshold = 0
        if params.get('KPI name Rus'):
            attributes_for_table3 = pd.DataFrame([(params.get('KPI name Rus').encode('utf-8').replace("'", "\\'"),
                                                   self.session_uid, self.set_name, self.store_id,
                                                   self.visit_date.isoformat(), datetime.datetime.utcnow().isoformat(),
                                                   score, kpi_fk, atomic_kpi_fk, threshold, result,
                                                   params.get('KPI name Eng').replace("'", "\\'"))],
                                                 columns=['display_text', 'session_uid', 'kps_name',
                                                          'store_fk', 'visit_date',
                                                          'calculation_time', 'score', 'kpi_fk',
                                                          'atomic_kpi_fk', 'threshold', 'result', 'name'])
        else:
            attributes_for_table3 = pd.DataFrame([(params.get('KPI name Eng').replace("'", "\\'"),
                                                   self.session_uid, self.set_name, self.store_id,
                                                   self.visit_date.isoformat(), datetime.datetime.utcnow().isoformat(),
                                                   score, kpi_fk, atomic_kpi_fk, threshold, result,
                                                   params.get('KPI name Eng').replace("'", "\\'"))],
                                                 columns=['display_text', 'session_uid', 'kps_name',
                                                          'store_fk', 'visit_date',
                                                          'calculation_time', 'score', 'kpi_fk',
                                                          'atomic_kpi_fk', 'threshold', 'result', 'name'])

        if self.set_name == TARGET_EXECUTION:
            self.execution_results[params.get('KPI name Eng')] = {'result': result,
                                                                  'score_func': params.get('score_func')}
        return attributes_for_table3

    def check_number_of_doors_given_sos(self, params):
        set_total_res = 0
        for p in params.values()[0]:
            if p.get('Formula') != "number of doors given sos" or not p.get("Children"):
                continue
            kpi_fk = self.kpi_fetcher.get_kpi_fk(p.get('KPI name Eng'))
            children = [int(child) for child in str(p.get("Children")).split(", ")]
            first_atomic_score = second_atomic_res = 0
            for c in params.values()[0]:
                if c.get("KPI ID") in children and c.get("Formula") == "atomic sos":
                    first_atomic_res = self.calculate_facings_sos(c)
                    first_atomic_score = self.calculate_score(first_atomic_res, c)
                    # write to DB
                    attributes_for_level3 = self.create_attributes_for_level3_df(c, first_atomic_score, kpi_fk)
                    self.write_to_db_result(attributes_for_level3, 'level3')
            for c in params.values()[0]:
                if c.get("KPI ID") in children and c.get("Formula") == "atomic number of doors":
                    second_atomic_res = self.calculate_number_of_doors(c)
                    second_atomic_score = self.calculate_score(second_atomic_res, c)
                    # write to DB
                    attributes_for_level3 = self.create_attributes_for_level3_df(c, second_atomic_score, kpi_fk)
                    self.write_to_db_result(attributes_for_level3, 'level3')

            if first_atomic_score > 0:
                kpi_total_res = second_atomic_res
            else:
                kpi_total_res = 0
            score = self.calculate_score(kpi_total_res, p)
            set_total_res += round(score) * p.get('KPI Weight')
            # saving to DB
            attributes_for_level2 = self.create_attributes_for_level2_df(p, score, kpi_fk)
            self.write_to_db_result(attributes_for_level2, 'level2')

        return set_total_res

    def check_number_of_doors_given_number_of_sku(self, params):
        set_total_res = 0
        for p in params.values()[0]:
            if p.get('Formula') != "number of doors given number of SKUs" or not p.get("Children"):
                continue
            kpi_fk = self.kpi_fetcher.get_kpi_fk(p.get('KPI name Eng'))
            children = [int(child) for child in str(p.get("Children")).split(", ")]
            first_atomic_scores = []
            for c in params.values()[0]:
                if c.get("KPI ID") in children and c.get("Formula") == "atomic number of SKUs":
                    first_atomic_res = self.calculate_availability(c)
                    first_atomic_score = self.calculate_score(first_atomic_res, c)
                    first_atomic_scores.append(first_atomic_score)
                    # write to DB
                    attributes_for_level3 = self.create_attributes_for_level3_df(c, first_atomic_score, kpi_fk)
                    self.write_to_db_result(attributes_for_level3, 'level3')
            second_atomic_res = 0
            for c in params.values()[0]:
                if c.get("KPI ID") in children and c.get("Formula") == "atomic number of doors":
                    second_atomic_res = self.calculate_number_of_doors(c)
                    second_atomic_score = self.calculate_score(second_atomic_res, c)
                    # write to DB
                    attributes_for_level3 = self.create_attributes_for_level3_df(c, second_atomic_score, kpi_fk)
                    self.write_to_db_result(attributes_for_level3, 'level3')

            if 0 not in first_atomic_scores:  # if all assortment atomics have score > 0
                kpi_total_res = second_atomic_res
            else:
                kpi_total_res = 0
            score = self.calculate_score(kpi_total_res, p)
            set_total_res += round(score) * p.get('KPI Weight')
            # saving to DB
            attributes_for_level2 = self.create_attributes_for_level2_df(p, score, kpi_fk)
            self.write_to_db_result(attributes_for_level2, 'level2')

        return set_total_res

    def get_set(self, visit_date):
        if str(visit_date) < '2017-11-25':  # todo: change the date to the relevant one before deployment
            query = """
                    select ss.pk , ss.additional_attribute_12
                    from static.stores ss
                    join probedata.session ps on ps.store_fk=ss.pk
                    where ss.delete_date is null and ps.session_uid = '{}';
                    """.format(self.session_uid)
        else:
            query = """
                select ss.pk , ss.additional_attribute_11
                from static.stores ss
                join probedata.session ps on ps.store_fk=ss.pk
                where ss.delete_date is null and ps.session_uid = '{}';
                """.format(self.session_uid)

        cur = self.rds_conn.db.cursor()
        cur.execute(query)
        res = cur.fetchall()

        df = pd.DataFrame(list(res), columns=['store_fk', 'channel'])

        return df['channel'][0]

    def calculate_gaps(self, params):
        for param in params:
            kpi = param.get('KPI Name')
            score = self.kpk_scores[kpi].get('score')
            kpi_local_name = self.kpk_scores[kpi].get('rus_name')
            if score is not None:
                gap = (100 - score) * param.get('Base Weight')
                if gap < 0:
                    gap = 0
                rank = param.get('Top Gaps Rank')
                category_group = param.get('Gap Group Eng')
                group_local_name = param.get('Gap Group')
                self.gaps_dict[category_group, kpi] = {'gap': round(gap, 2), 'rank': rank, 'kpi': kpi_local_name,
                                                       'group_local_name': group_local_name}

    def write_gaps(self):
        gaps_dict_sorted_df = pd.DataFrame.from_dict(self.gaps_dict, orient='index')
        category_groups_kpis = gaps_dict_sorted_df.index.tolist()
        for index in category_groups_kpis:
            group = index[0]
            group_df = gaps_dict_sorted_df.loc[group]
            # sorted_group_df = group_df.sort_values(by=['gap', 'rank'], ascending=[False, True])
            sorted_group_df = group_df.sort_values(by=['rank'], ascending=[True])
            counter = 0
            for i in range(len(sorted_group_df)):
                if sorted_group_df['gap'].iloc[i] < 1 or counter > self.gap_groups_limit[group]:
                    continue
                counter += 1
                # attributes = pd.DataFrame([(self.session_fk, group, sorted_group_df['kpi'].iloc[i], sorted_group_df['rank'].iloc[i],
                #                             sorted_group_df['gap'].iloc[i])],
                #                           columns=['session_fk', 'gap_category', 'name', 'priority', 'impact'])
                # attributes = pd.DataFrame([(self.session_fk, group, sorted_group_df['kpi'].iloc[i], counter,
                #                             sorted_group_df['gap'].iloc[i])],
                #                           columns=['session_fk', 'gap_category', 'name', 'priority', 'impact'])
                attributes = pd.DataFrame([(self.session_fk, sorted_group_df['group_local_name'].iloc[i],
                                            sorted_group_df['kpi'].iloc[i], counter, sorted_group_df['gap'].iloc[i])],
                                          columns=['session_fk', 'gap_category', 'name', 'priority', 'impact'])
                query = insert(attributes.to_dict(), CUSTOM_GAPS_TABLE)
                self.gaps_queries.append(query)

    def calculate_contract_execution(self):
        self.change_set(CONTRACT_SET_NAME)
        log_prefix = 'Contract KPI: '
        raw_data = self.execution_contract.get_json_file_content(str(self.store_id))
        if raw_data:
            Log.info(log_prefix + 'Relevant file found')
        contract_data = None
        for data in raw_data:
            start_date = datetime.datetime.strptime(data['Start Date'], '%Y-%m-%d').date()
            end_date = datetime.datetime.now().date() if not data['End Date'] else \
                datetime.datetime.strptime(data['End Date'], '%Y-%m-%d').date()
            if start_date <= self.visit_date <= end_date:
                if contract_data is None or start_date >= contract_data[1]:
                    contract_data = (data, start_date)
        if contract_data is not None:
            contract_data = contract_data[0]
            sum_of_scores = 0
            sum_of_weights = 0
            kpi_conversion = self._get_kpi_conversion()
            for field in (self.top_sku.STORE_NUMBER, 'Start Date', 'End Date'):
                contract_data.pop(field, None)
            for kpi_id in contract_data.keys():
                target, weight = contract_data[kpi_id]
                if target == '':
                    continue
                kpi_name = kpi_conversion.get(int(kpi_id))
                if kpi_name:
                    if kpi_name in self.execution_results:
                        result = self.execution_results[kpi_name].get('result')
                        score_func = self.execution_results[kpi_name].get('score_func')
                        try:
                            if type(target) is unicode and '%' in target:
                                target = target.replace('%', '')
                                target = float(target) / 100
                            target = float(target)
                            if int(target) == target:
                                target = int(target)
                        except ValueError:
                            target = contract_data[kpi_id]
                            # score = 100 if result == target else 0
                        if score_func == PROPORTIONAL:
                            if target:
                                score = (result / float(target)) * 100
                                if score > 100:
                                    score = 100
                            else:
                                score = 0
                        else:
                            score = 100 if result >= target else 0
                        weight = float(weight)
                        sum_of_scores += score * weight
                        sum_of_weights += weight
                        params = {'KPI name Eng': kpi_name}
                        kpi_fk = self.kpi_fetcher.kpi_static_data[self.kpi_fetcher.kpi_static_data['kpi_name'] ==
                                                                  kpi_name]['kpi_fk'].values[0]
                        attributes_for_level2 = self.create_attributes_for_level2_df(params, score, kpi_fk)
                        self.write_to_db_result(attributes_for_level2, 'level2')
                        attributes_for_level3 = self.create_attributes_for_level3_df(params, (score, result, target),
                                                                                     kpi_fk)
                        self.write_to_db_result(attributes_for_level3, 'level3')
                    else:
                        Log.warning(log_prefix + "KPI '{}' was not calculated".format(kpi_name))
                else:
                    Log.warning(log_prefix + 'KPI ID {} cannot be converted'.format(kpi_id))
            # Saving results for level 1
            contract_score = 0 if not sum_of_weights else round(sum_of_scores / float(sum_of_weights), 2)
            attributes_for_table1 = pd.DataFrame([(CONTRACT_SET_NAME, self.session_uid, self.store_id,
                                                   self.visit_date.isoformat(), contract_score, None)],
                                                 columns=['kps_name', 'session_uid', 'store_fk', 'visit_date',
                                                          'score_1', 'kpi_set_fk'])
            self.write_to_db_result(attributes_for_table1, 'level1', CONTRACT_SET_NAME)

    @staticmethod
    def _get_kpi_conversion():
        data = pd.read_excel(KPI_CONVERSION_PATH)
        conversion = {}
        for x, row in data.iterrows():
            conversion[int(row['KPI ID'])] = row['KPI Name']
        return conversion

    def calculate_top_sku(self):
        top_skus = self.top_sku.get_top_skus_for_store(self.store_id, self.visit_date)
        for scene_fk in self.scif['scene_id'].unique():
            scene_data = self.scif[(self.scif['scene_id'] == scene_fk) & (self.scif['facings'] > 0)]
            facings_data = scene_data.groupby('product_fk')['facings'].sum().to_dict()
            for product_fk in top_skus:
                correlated_products = self.top_sku.get_correlated_products(top_skus[product_fk])
                product_group_facings = 0
                for correlated in correlated_products:
                    product_group_facings += facings_data.pop(correlated, 0)
                if product_fk not in facings_data:
                    facings_data[product_fk] = 0
                facings_data[product_fk] += product_group_facings
            for product_fk in facings_data.keys():
                in_assortment = True if product_fk in top_skus else False
                distributed = True if facings_data[product_fk] > 0 else False
                query = self.top_sku.get_custom_scif_query(self.session_fk, scene_fk, product_fk,
                                                           in_assortment, distributed)
                self.top_sku_queries.append(query)

    def insert_scores_level2(self, result, score, kpi_name):
        key_result = kpi_name + ' result'
        key_score =  kpi_name + ' score'
        self.kpi_score_level2[key_result] = result
        self.kpi_score_level2[key_score] = score
        return

    def check_kpi_scores(self, params):
        set_total_res = 0
        for p in params.values()[0]:
            if p.get('Formula') != "Check KPI score":
                continue
            kpi_fk = self.kpi_fetcher.get_kpi_fk(p.get('KPI name Eng'))
            kpi_name = p.get('KPI name Eng')
            key_result = kpi_name + ' result'
            key_score = kpi_name + ' score'
            if key_score in self.kpi_score_level2.keys():
                checked_kpi_score = self.kpi_score_level2[key_score]
                if checked_kpi_score == 100:
                    kpi_result = self.kpi_score_level2[key_result]
                else:
                    kpi_result = 0
                score = self.calculate_score(kpi_result, p)
                set_total_res += round(score) * p.get('KPI Weight')
                # saving to DB
                attributes_for_level2 = self.create_attributes_for_level2_df(p, score, kpi_fk)
                self.write_to_db_result(attributes_for_level2, 'level2')
                attributes_for_level3 = self.create_attributes_for_level3_df(p, score, kpi_fk)
                self.write_to_db_result(attributes_for_level3, 'level3')
            else:
                kpi_result = 0
                score = self.calculate_score(kpi_result, p)
                self.create_attributes_for_level3_df(p, score, kpi_fk)
        return set_total_res
