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

from Projects.CCRUFIFA2018.Utils.Fetcher import CCRUFIFA2018Queries

__author__ = 'urid'

BINARY = 'BINARY'
PROPORTIONAL = 'PROPORTIONAL'
CONDITIONAL_PROPORTIONAL = 'CONDITIONAL PROPORTIONAL'
KPI_RESULT = 'report.kpi_results'
KPK_RESULT = 'report.kpk_results'
KPS_RESULT = 'report.kps_results'


class CCRUFIFAKPIToolBox:
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
        self.store_type = self.data_provider.store_type
        if set_name is None:
            self.set_name = self.store_type
        else:
            self.set_name = set_name
        self.kpi_fetcher = CCRUFIFA2018Queries(self.project_name, self.scif, self.match_product_in_scene, self.set_name)
        self.survey_response = self.data_provider[Data.SURVEY_RESPONSES]
        self.sales_rep_fk = self.data_provider[Data.SESSION_INFO]['s_sales_rep_fk'].iloc[0]
        self.session_fk = self.data_provider[Data.SESSION_INFO]['pk'].iloc[0]
        self.thresholds_and_results = {}
        self.result_df = []
        self.kpi_results_queries = []

    def change_set(self, set_name):
        self.set_name = set_name
        self.kpi_fetcher = CCRUFIFA2018Queries(self.project_name, self.scif, self.match_product_in_scene, self.set_name)

    def check_number_of_facings_given_answer_to_survey(self, params):
        set_total_res = 0
        for p in params.values()[0]:
            if p.get('Formula') != "number of facings given answer to survey" or not p.get("children"):
                continue
            kpi_fk = self.kpi_fetcher.get_kpi_fk(p.get('KPI name Eng'))
            first_atomic_score = 0
            children = map(int, str(p.get("children")).strip().split(", "))
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

                        if p.get('Logical Operator') in ('OR', 'AND', 'MAX'):
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
                elif p.get('Logical Operator') == 'MAX':
                    if atomic_scores:
                        score = max(atomic_scores)
                        if not ((score > p.get('score_min', 0)*100) and (score < p.get('score_max', 1)*100)):
                            score = 0
                    else:
                        score = 0
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
            survey_data = self.survey_response.loc[self.survey_response['question_fk'] == p.get('Values')]
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
                kpi_fk = self.kpi_fetcher.get_kpi_fk(p.get('KPI name Eng'))
                attributes_for_level3 = self.create_attributes_for_level3_df(p, score, kpi_fk)
                self.write_to_db_result(attributes_for_level3, 'level3')
            elif p.get('level') == 2:
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
        if 'Target' not in params.keys():
            # if kpi_total_res > 0:
            #     return 100
            # else:
            #     return 0
            self.thresholds_and_results[kpi_name]['threshold'] = params.get('Target')
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
        score = round(score)
        attributes_for_table2 = pd.DataFrame([(self.session_uid, self.store_id,
                                               self.visit_date.isoformat(), kpi_fk,
                                               params.get('KPI name Eng').replace("'", "\\'"), score)],
                                             columns=['session_uid', 'store_fk', 'visit_date', 'kpi_fk',
                                                      'kpk_name', 'score'])

        return attributes_for_table2

    def create_attributes_for_level3_df(self, params, score, kpi_fk):
        """
        This function creates a data frame with all attributes needed for saving in level 3 tables

        """
        score = round(score)
        if self.thresholds_and_results.get(params.get("KPI name Eng")):
            result = self.thresholds_and_results[params.get("KPI name Eng")]['result']
            threshold = self.thresholds_and_results[params.get("KPI name Eng")]['threshold']
        else:
            result = threshold = 0
        attributes_for_table3 = pd.DataFrame([(params.get('KPI name Rus').encode('utf-8').replace("'", "\\'"),
                                               self.session_uid, self.set_name, self.store_id,
                                               self.visit_date.isoformat(), datetime.datetime.utcnow().isoformat(),
                                               score, kpi_fk, None, threshold, result,
                                               params.get('KPI name Eng').replace("'", "\\'"))],
                                             columns=['display_text', 'session_uid', 'kps_name',
                                                      'store_fk', 'visit_date',
                                                      'calculation_time', 'score', 'kpi_fk',
                                                      'atomic_kpi_fk', 'threshold', 'result', 'name'])

        return attributes_for_table3

    def check_weighted_average(self, params):
        """

        :param params:
        :return:
        """
        set_total_res = 0
        for p in params.values()[0]:
            if p.get('Formula') not in ('Weighted Average'):
                continue
            kpi_fk = self.kpi_fetcher.get_kpi_fk(p.get('KPI name Eng'))
            children = map(int, str(p.get("Children")).strip().split("\n"))
            info_by_kpi_id = self.build_dict(params.values()[0], 'KPI ID')
            kpi_total = 0
            kpi_total_weight = 0
            for child in children:
                c = info_by_kpi_id.get(child)
                atomic_res = self.calculate_availability(c)
                atomic_score = self.calculate_score(atomic_res, c)
                if p.get('Formula') == 'Weighted Average':
                    kpi_total += atomic_score * c.get('KPI Weight')
                    kpi_total_weight += c.get('KPI Weight')
                else:
                    kpi_total += atomic_score
                    kpi_total_weight += 1
                # write to DB
                atomic_kpi_fk = self.kpi_fetcher.get_atomic_kpi_fk(c.get('KPI name Eng'))
                attributes_for_level3 = self.create_attributes_for_level3_df(c, atomic_score, kpi_fk)
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

    def calculate_share_of_cch_collers(self, params):
        """
        This function calculates number of SKUs per single scene type

        """
        set_total_res = 0
        for p in params.values()[0]:
            if p.get('Formula') != 'Share of CCH coolers with facings target':
                continue
            scenes = self.get_relevant_scenes(p)
            score = self.calculate_share_of_cch(p, scenes)
            # atomic_kpi_fk = self.kpi_fetcher.get_atomic_kpi_fk(p.get('KPI name Eng'))
            kpi_fk = self.kpi_fetcher.get_kpi_fk(p.get('KPI name Eng'))
            if 'KPI Weight' in p.keys():
                set_total_res += round(score) * p.get('KPI Weight')
            else:
                set_total_res += round(score)
            # saving to DB
            attributes_for_level3 = self.create_attributes_for_level3_df(p, score, kpi_fk)
            self.write_to_db_result(attributes_for_level3, 'level3')
            attributes_for_level2 = self.create_attributes_for_level2_df(p, score, kpi_fk)
            self.write_to_db_result(attributes_for_level2, 'level2')
        return set_total_res

    def calculate_share_of_cch(self, p, scenes, sos=True):
        sum_of_passed_doors = 0
        sum_of_passed_scenes = 0
        sum_of_all_doors = 0
        for scene in scenes:
            products_of_tccc = self.scif[(self.scif['scene_id'] == scene) &
                                         (self.scif['manufacturer_name'] == 'TCCC') &
                                         (self.scif['product_type'] != 'Empty')]['facings'].sum()
            scene_type = self.scif.loc[self.scif['scene_id'] == scene]['template_name'].values[0]
            if any(self.templates[self.templates['template_name'] == scene_type]['additional_attribute_1']):
                num_of_doors = \
                    float(self.templates[self.templates['template_name'] == scene_type][
                              'additional_attribute_1'].values[0])
                sum_of_all_doors += num_of_doors
            else:
                num_of_doors = 1
                sum_of_all_doors += num_of_doors
            if products_of_tccc > p.get('Target'):
                sum_of_passed_doors += num_of_doors
                sum_of_passed_scenes += 1
        if not sos:
            return sum_of_passed_scenes
        if sum_of_all_doors:
            ratio = (sum_of_passed_doors / float(sum_of_all_doors)) * 100
        else:
            ratio = 0
        kpi_name = p.get('KPI name Eng')
        self.thresholds_and_results[kpi_name] = {'result': ratio, 'threshold': 0}
        return ratio

    def build_dict(self, seq, key):
        return dict((d[key], dict(d, index=index)) for (index, d) in enumerate(seq))

    def calculate_coller_standard(self, p, params, kpi_fk=None):
        set_total_res = 0
        if p.get('level') == 2:
            kpi_fk = self.kpi_fetcher.get_kpi_fk(p.get('KPI name Eng'))
        children = map(int, str(p.get("Children")).strip().split("\n"))
        info_by_kpi_id = self.build_dict(params.values()[0], 'KPI ID')
        kpi_total_weight = 0
        numerator = 0
        denominator = 0
        atomic_score = 0
        for child in children:
            c = info_by_kpi_id.get(child)
            if c.get('Formula') == 'number of facings':
                scenes = self.get_relevant_scenes(c)
                number_of_passed_scenes = 0
                number_scenes_to_check = len(scenes)
                for scene in scenes:
                    atomic_res = self.calculate_availability(c, [scene])
                    atomic_score = self.calculate_score(atomic_res, c)
                    if atomic_score == 100:
                        number_of_passed_scenes += 1
                        passed_scene = scene
                if number_of_passed_scenes > 0 :
                    atomic_res = self.calculate_availability(c, [passed_scene])
                    atomic_score = self.calculate_score(atomic_res, c)
                # saving to DB: level3/4
                sub_atomic_kpi_fk = self.kpi_fetcher.get_atomic_kpi_fk(c.get('KPI name Eng'))
                attributes_for_level3 = self.create_attributes_for_level3_df(c, atomic_score, kpi_fk)
                self.write_to_db_result(attributes_for_level3, 'level3')

                atomic_weight = c.get('KPI Weight')
                kpi_total_weight += atomic_weight
                numerator += atomic_weight * number_of_passed_scenes
                denominator += atomic_weight * number_scenes_to_check
        if denominator:
            kpi_score = (float(numerator) / denominator) * 100
        else:
            kpi_score = 0
        if p.get('level') != 2:
            attributes_for_level3 = self.create_attributes_for_level3_df(p, kpi_score, kpi_fk)
            self.write_to_db_result(attributes_for_level3, 'level3')
        set_total_res += kpi_score
        return set_total_res

    def weighted_cooler_standard(self, params):
        set_total_res = 0
        kpi_fk = None
        for p in params.values()[0]:
            if p.get('Formula') != ("Weighted coller standard"):
                continue
            if p.get('KPI ID') != '*':
                kpi_fk = self.kpi_fetcher.get_kpi_fk(p.get('KPI name Eng'))
            children = map(int, str(p.get("Children")).strip().split("\n"))
            info_by_kpi_id = self.build_dict(params.values()[0], 'KPI ID')
            kpi_total_weight = 0
            numerator = 0
            denominator = 0
            for child in children:
                c = info_by_kpi_id.get(child)
                atomic_score = self.calculate_coller_standard(c, params, kpi_fk=kpi_fk)
                num_relevant_scenes = len(self.get_relevant_scenes(c))
                atomic_weight = c.get('KPI Weight')
                kpi_total_weight += atomic_weight
                numerator += atomic_score * num_relevant_scenes
                denominator += num_relevant_scenes
            kpi_score = float(numerator)/denominator
            if p.get('KPI ID') == '*': # * means internal KPI, not for presenting, only child which is level2 KPI
                kpi_fk = self.kpi_fetcher.get_kpi_fk(c.get('KPI name Eng')) # takes
                attributes_for_level2 = self.create_attributes_for_level2_df(c, kpi_score, kpi_fk)
            else:
                attributes_for_level2 = self.create_attributes_for_level2_df(p, kpi_score, kpi_fk)
            self.write_to_db_result(attributes_for_level2, 'level2')
            set_total_res += round(kpi_score) * kpi_total_weight
        return set_total_res