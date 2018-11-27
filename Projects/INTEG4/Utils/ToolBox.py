# -*- coding: utf-8 -*-
import datetime

import pandas as pd
from Trax.Algo.Calculations.Core.Constants import Fields as Fd
from Trax.Algo.Calculations.Core.DataProvider import Data, Keys
from Trax.Algo.Calculations.Core.Shortcuts import SessionInfo, BaseCalculationsGroup
from Trax.Cloud.Services.Connector.Keys import DbUsers
from Trax.Data.Orm.OrmCore import OrmSession
from KPIUtils_v2.DB.PsProjectConnector import PSProjectConnector
from Trax.Data.Utils.MySQLservices import get_table_insertion_query as insert
from Trax.Utils.Logging.Logger import Log

from Projects.INTEG4.Fetcher import INTEG4CCHKPIFetcher

__author__ = 'urid'

BINARY = 'BINARY'
PROPORTIONAL = 'PROPORTIONAL'
CONDITIONAL_PROPORTIONAL = 'CONDITIONAL PROPORTIONAL'
KPI_RESULT = 'report.kpi_results'
KPK_RESULT = 'report.kpk_results'
KPS_RESULT = 'report.kps_results'


class INTEG4KPIToolBox:
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
        self.rds_conn = PSProjectConnector(self.project_name, DbUsers.CalculationEng)
        self.session_info = SessionInfo(data_provider)
        self.store_id = self.data_provider[Data.STORE_FK]
        self.scif = self.data_provider[Data.SCENE_ITEM_FACTS]
        if set_name is None:
            self.set_name = self.get_set()
        else:
            self.set_name = set_name
        self.kpi_fetcher = INTEG4CCHKPIFetcher(self.project_name, self.scif, self.match_product_in_scene, self.set_name)
        self.survey_response = self.data_provider[Data.SURVEY_RESPONSES]
        self.sales_rep_fk = self.data_provider[Data.SESSION_INFO]['s_sales_rep_fk'].iloc[0]
        self.session_fk = self.data_provider[Data.SESSION_INFO]['pk'].iloc[0]
        self.thresholds_and_results = {}
        self.result_df = []
        self.kpi_results_queries = []

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

    def check_number_of_facings_given_answer_to_survey(self, params):
        set_total_res = 0
        for p in params.values()[0]:
            if p.get('Formula') != "number of facings given answer to survey" or not p.get("children"):
                continue
            kpi_fk = self.kpi_fetcher.get_kpi_fk(p.get('KPI name Eng'))
            first_atomic_score = 0
            children = map(int, p.get("children").split(", "))
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
            set_total_res += score * p.get('KPI Weight')
            # saving to DB
            attributes_for_level2 = self.create_attributes_for_level2_df(p, score, kpi_fk)
            self.write_to_db_result(attributes_for_level2, 'level2')

        Log.info('Calculation finished')
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
        availability_types = ['SKUs', 'BRAND', 'MAN', 'CAT', 'MAN in CAT']
        formula_types = ['number of SKUs', 'number of facings']
        for p in params.values()[0]:
            if p.get('Type') not in availability_types or p.get('Formula') not in formula_types:
                continue
            if p.get('level') != 2:
                continue
            is_atomic = False
            kpi_total_res = 0
            kpi_fk = self.kpi_fetcher.get_kpi_fk(p.get('KPI name Eng'))

            if p.get('children') is not None:
                is_atomic = True
                children = [int(child) for child in str(p.get('children')).split(', ')]
                atomic_scores = []
                for child in params.values()[0]:
                    if child.get('KPI ID') in children:

                        if child.get('children') is not None:   # atomic of atomic
                            atomic_score = 0
                            atomic_children = [int(a_child) for a_child in str(child.get('children')).split(', ')]
                            for atomic_child in params.values()[0]:
                                if atomic_child.get('KPI ID') in atomic_children:
                                    atomic_child_res = self.calculate_availability(atomic_child)
                                    atomic_child_score = self.calculate_score(atomic_child_res, atomic_child)
                                    atomic_score += atomic_child.get('additional_weight', 1.0 / len(atomic_children)) * atomic_child_score

                        else:
                            atomic_res = self.calculate_availability(child)
                            atomic_score = self.calculate_score(atomic_res, child)

                        # write to DB
                        attributes_for_table3 = self.create_attributes_for_level3_df(child, atomic_score, kpi_fk)
                        self.write_to_db_result(attributes_for_table3, 'level3', kpi_fk)

                        if p.get('Logical Operator') in ('OR', 'AND'):
                            atomic_scores.append(atomic_score)
                        elif p.get('Logical Operator') == 'SUM':
                            kpi_total_res += child.get('additional_weight', 1 / len(children)) * atomic_score

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
                    score = kpi_total_res / 100.0
                    if score < p.get('score_min', 0):
                        score = 0
                    elif score > p.get('score_max', 1):
                        score = p.get('score_max', 1)
                    score *= 100

            else:
                kpi_total_res = self.calculate_availability(p)
                score = self.calculate_score(kpi_total_res, p)

            # Saving to old tables
            attributes_for_table2 = self.create_attributes_for_level2_df(p, score, kpi_fk)
            self.write_to_db_result(attributes_for_table2, 'level2', kpi_fk)
            if not is_atomic:  # saving also to level3 in case this KPI has only one level
                attributes_for_table3 = self.create_attributes_for_level3_df(p, score, kpi_fk)
                self.write_to_db_result(attributes_for_table3, 'level3', kpi_fk)

            set_total_res += score * p.get('KPI Weight')

        return set_total_res

    def calculate_availability(self, params, scenes=[]):
        values_list = str(params.get('Values')).split(', ')
        # object_static_list = self.get_static_list(params.get('Type'))
        if not scenes:
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

        object_facings = self.kpi_fetcher.get_object_facings(scenes, values_list, params.get('Type'),
                                                             formula=params.get('Formula'),
                                                             shelves=params.get("shelf_number", None),
                                                             size=sizes, form_factor=form_factors,
                                                             products_to_exclude=products_to_exclude,
                                                             form_factors_to_exclude=form_factors_to_exclude)

        return object_facings

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

    def get_relevant_scenes(self, params):
        all_scenes = self.scenes_info['scene_fk'].unique().tolist()
        filtered_scenes = []
        scenes_data = {}
        location_data = {}
        sub_location_data = {}

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

            sub_location = list(self.scif.loc[self.scif['template_name'] == scene_type]['additional_attribute_2'].values)
            if sub_location:
                sub_location = sub_location[0]
                if sub_location not in sub_location_data.keys():
                    sub_location_data[sub_location] = []
            sub_location_data[sub_location].append(scene)

        include_list = []
        if not params.get('Scenes to include') and not params.get('Locations to include') and \
                not params.get('Sub locations to include'):
            include_list.extend(filtered_scenes)
        else:
            if params.get('Scenes to include'):
                scenes_to_include = params.get('Scenes to include').split(', ')
                for scene in scenes_to_include:
                    if scene in scenes_data.keys():
                        include_list.extend(scenes_data[scene])

            if params.get('Locations to include'):
                locations_to_include = params.get('Locations to include').split(', ')
                for location in locations_to_include:
                    if location in location_data.keys():
                        include_list.extend(location_data[location])

            if params.get('Sub locations to include'):
                sub_locations_to_include = str(params.get('Sub locations to include')).split(', ')
                for sub_location in sub_locations_to_include:
                    if sub_location in sub_location_data.keys():
                        include_list.extend(sub_location_data[sub_location])
        include_list = list(set(include_list))

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

        relevant_scenes = []
        for scene in include_list:
            if scene not in exclude_list:
                relevant_scenes.append(scene)
        return relevant_scenes

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

            else: # checking for number of scenes with a complex condition (only certain products/brands/etc)
                p_copy = p.copy()
                p_copy["Formula"] = "number of facings"
                for scene in scenes:
                    if self.calculate_availability(p_copy, scenes=[scene]) > 0:
                        res = 1
                    else:
                        res = 0
                    kpi_total_res += res

            score = self.calculate_score(kpi_total_res, p)
            set_total_res += score * p.get('KPI Weight')

            kpi_fk = self.kpi_fetcher.get_kpi_fk(p.get('KPI name Eng'))
            if p.get('level') == 2:
                attributes_for_level2 = self.create_attributes_for_level2_df(p, score, kpi_fk)
                self.write_to_db_result(attributes_for_level2, 'level2', kpi_fk)
            attributes_for_level3 = self.create_attributes_for_level3_df(p, score, kpi_fk)
            self.write_to_db_result(attributes_for_level3, 'level3', kpi_fk)

        return set_total_res

    def check_number_of_doors(self, params):
        set_total_res = 0
        for p in params.values()[0]:
            if p.get('Type') != 'DOORS' or p.get('Formula') != 'number of doors':
                continue
            kpi_total_res = self.calculate_number_of_doors(p)
            score = self.calculate_score(kpi_total_res, p)
            set_total_res += score * p.get('KPI Weight')
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
            scene_type = self.scif.loc[self.scif['scene_id'] == scene]['template_name'].values[0]
            num_of_doors = self.templates[self.templates['template_name'] == scene_type]['additional_attribute_1'].values[0]
            if num_of_doors is not None:
                res = float(num_of_doors)
            total_res += res
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
            set_total_res += score*p.get('KPI Weight')
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
        for p in params.values()[0]:
            if (p.get('Type') == 'MAN in CAT' or p.get('Type') == 'MAN') and \
                            p.get('Formula') in ['sos', 'sos with empty']:
                ratio = self.calculate_facings_sos(p)
            else:
                continue
            score = self.calculate_score(ratio, p)
            set_total_res += score*p.get('KPI Weight')
            # saving to DB
            kpi_fk = self.kpi_fetcher.get_kpi_fk(p.get('KPI name Eng'))
            attributes_for_level3 = self.create_attributes_for_level3_df(p, score, kpi_fk)
            self.write_to_db_result(attributes_for_level3, 'level3')
            if p.get('level') == 2:
                attributes_for_level2 = self.create_attributes_for_level2_df(p, score, kpi_fk)
                self.write_to_db_result(attributes_for_level2, 'level2')

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
                subset_filter = (self.scif[self.scif[Fd.M_NAME].isin(self.kpi_fetcher.TCCC)])
            else:
                return 0
        else:
            if params.get('Type') == 'MAN':
                pop_filter = ((self.scif['scene_id'].isin(relevant_scenes)) &
                              (~self.scif['product_type'].isin(['Empty'])))
                subset_filter = (self.scif[Fd.M_NAME].isin(self.kpi_fetcher.TCCC))
            elif params.get('Type') == 'MAN in CAT':
                pop_filter = ((self.scif[Fd.CAT].isin(params.get('Values'))) &
                              (self.scif['scene_id'].isin(relevant_scenes)) &
                              (~self.scif['product_type'].isin(['Empty'])))
                subset_filter = (self.scif[self.scif[Fd.M_NAME].isin(self.kpi_fetcher.TCCC)])
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
                score = (numerator / float(denominator))*100
            return round(score)

        elif params.get('Target') == 'targets by guide':
            target = self.kpi_fetcher.get_category_target_by_region(params.get('Values'), self.store_id)
        else:
            target = params.get('Target')

        self.thresholds_and_results[kpi_name]['threshold'] = target
        target = float(target)
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

        return round(score)

    def check_number_of_skus_in_single_scene_type(self, params):
        """
        This function calculates number of SKUs per single scene type

        """
        set_total_res = 0
        for p in params.values()[0]:
            if p.get('Formula') != 'number of SKUs in one scene type' or p.get('level') == 3:
                continue
            score = self.calculate_number_of_skus_in_single_scene_type(params, p)
            set_total_res += score * p.get('KPI Weight')

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
            sub_location = list(self.scif.loc[self.scif['template_name'] == scene_type]['additional_attribute_2'].values)
            if sub_location:
                sub_location = sub_location[0]
            if p.get('children') is not None:
                children_scores = []
                for child in params.values()[0]:
                    if child.get('KPI ID') in [int(kpi) for kpi in p.get('children').split(', ')]:
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
                if p.get('Scenes to include'):   # by scene
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
        cur = self.rds_conn.db.cursor()
        delete_queries = self.kpi_fetcher.get_delete_session_results(self.session_uid)
        for query in delete_queries:
            cur.execute(query)
        for query in self.kpi_results_queries:
            cur.execute(query)
        self.rds_conn.db.commit()
        return

    def create_attributes_for_level2_df(self, params, score, kpi_fk):
        """
        This function creates a data frame with all attributes needed for saving in level 2 tables

        """
        attributes_for_table2 = pd.DataFrame([(self.session_uid, self.store_id,
                                               self.visit_date.isoformat(), kpi_fk, params.get('KPI name Eng'),
                                               score)],
                                             columns=['session_uid', 'store_fk', 'visit_date', 'kpi_fk',
                                                      'kpk_name', 'score'])

        return attributes_for_table2

    def create_attributes_for_level3_df(self, params, score, kpi_fk):
        """
        This function creates a data frame with all attributes needed for saving in level 3 tables

        """
        if self.thresholds_and_results.get(params.get("KPI name Eng")):
            result = self.thresholds_and_results[params.get("KPI name Eng")]['result']
            threshold = self.thresholds_and_results[params.get("KPI name Eng")]['threshold']
        else:
            result = threshold = 0
        attributes_for_table3 = pd.DataFrame([(params.get('KPI name Rus').encode('utf-8'),
                                               self.session_uid, self.set_name, self.store_id,
                                               self.visit_date.isoformat(), datetime.datetime.utcnow().isoformat(),
                                               score, kpi_fk, None, threshold, result, params.get('KPI name Eng'))],
                                             columns=['display_text', 'session_uid', 'kps_name',
                                                      'store_fk', 'visit_date',
                                                      'calculation_time', 'score', 'kpi_fk',
                                                      'atomic_kpi_fk', 'threshold', 'result', 'name'])

        return attributes_for_table3

    def check_number_of_doors_given_sos(self, params):
        set_total_res = 0
        for p in params.values()[0]:
            if p.get('Formula') != "number of doors given sos" or not p.get("children"):
                continue
            kpi_fk = self.kpi_fetcher.get_kpi_fk(p.get('KPI name Eng'))
            children = [int(child) for child in str(p.get("children")).split(", ")]
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
            set_total_res += score * p.get('KPI Weight')
            # saving to DB
            attributes_for_level2 = self.create_attributes_for_level2_df(p, score, kpi_fk)
            self.write_to_db_result(attributes_for_level2, 'level2')

        Log.info('Calculation finished')
        return set_total_res

    def check_number_of_doors_given_number_of_sku(self, params):
        set_total_res = 0
        for p in params.values()[0]:
            if p.get('Formula') != "number of doors given number of SKUs" or not p.get("children"):
                continue
            kpi_fk = self.kpi_fetcher.get_kpi_fk(p.get('KPI name Eng'))
            children = [int(child) for child in str(p.get("children")).split(", ")]
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

            if 0 not in first_atomic_scores:    # if all assortment atomics have score > 0
                kpi_total_res = second_atomic_res
            else:
                kpi_total_res = 0
            score = self.calculate_score(kpi_total_res, p)
            set_total_res += score * p.get('KPI Weight')
            # saving to DB
            attributes_for_level2 = self.create_attributes_for_level2_df(p, score, kpi_fk)
            self.write_to_db_result(attributes_for_level2, 'level2')

        Log.info('Calculation finished')
        return set_total_res

    def get_set(self):
        query = """
                select ss.pk , ss.additional_attribute_12
                from static.stores ss
                join probedata.session ps on ps.store_fk=ss.pk
                where ss.delete_date is null and ps.session_uid = '{}';
                """.format(self.session_uid)

        cur = self.rds_conn.db.cursor()
        cur.execute(query)
        res = cur.fetchall()

        df = pd.DataFrame(list(res), columns=['store_fk', 'additional_attribute_12'])

        return df['additional_attribute_12'][0]