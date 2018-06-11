# coding=utf-8
import os
from datetime import datetime

import pandas as pd

from Trax.Algo.Calculations.Core.DataProvider import Data
from Trax.Utils.Conf.Keys import DbUsers
# from Trax.Data.Projects.Connector import ProjectConnector
from Trax.Utils.Logging.Logger import Log
from Trax.Data.Utils.MySQLservices import get_table_insertion_query as insert
from Trax.Data.Projects.ProjectConnector import AwsProjectConnector

from Projects.CBCIL.Utils.Fetcher import CBCILCBCIL_Queries
from Projects.CBCIL.Utils.GeneralToolBox import CBCILCBCIL_GENERALToolBox
from Projects.CBCIL.Utils.ParseTemplates import parse_template
from KPIUtils.DB.Common import Common

__author__ = 'Israel'

KPI_RESULT = 'report.kpi_results'
KPK_RESULT = 'report.kpk_results'
KPS_RESULT = 'report.kps_results'

CUSTOM_GAPS_TABLE = 'pservice.custom_gaps'

TEMPLATE_PATH = os.path.join(os.path.dirname(os.path.realpath(__file__)), '..', 'Data', 'Template.xlsx')


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


class CBCILCBCIL_ToolBox(object):
    LEVEL1 = 1
    LEVEL2 = 2
    LEVEL3 = 3

    EXCLUDE_FILTER = 0
    INCLUDE_FILTER = 1
    CONTAIN_FILTER = 2

    SEPARATOR = ','

    PARAMS_TYPE_1 = 'Param Type (1)/ Numerator'
    PARAMS_VALUE_1 = 'Param (1) Values'
    PARAMS_TYPE_2 = 'Param Type (2)/ Denominator'
    PARAMS_VALUE_2 = 'Param (2) Values'
    PARAMS_TYPE_3 = 'Param Type (3)'
    PARAMS_VALUE_3 = 'Param (3) Values'
    TARGET = 'Target'
    SPLIT_SCORE = 'Split Score'
    KPI_SET = 'KPI Set'
    KPI_NAME = 'KPI Name'
    KPI_ATOMIC_NAME = 'Atomic Name'
    KPI_TYPE = 'KPI Family'
    WEIGHT = 'Weight'
    KPI_FAMILY = 'KPI Family'
    SCORE_TYPE = 'Score Type'
    BINARY = 'P/F'
    PERCENT = 'Percentage'
    GAPS = 'Gaps'

    ADDITIONAL_ATTRIBUTE_1 = 'additional_attribute_1'
    STORE_TYPE = 'store_type'

    SOS = 'SOS'
    SOS_COOLER = 'SOS Cooler'
    AVAILABILITY_FROM_TOP = 'Availability from top'
    AVAILABILITY = 'Availability'
    AVAILABILITY_FROM_MID_AND_UP = 'Availability from mid and up'
    AVAILABILITY_BY_TOP_SHELF = 'Availability by top shelf'
    AVAILABILITY_BY_SEQUENCE = 'Availability by sequence'
    AVAILABILITY_FROM_BOTTOM = 'Availability from bottom'
    MIN_2_AVAILABILITY = 'Min 2 Availability'
    BLOCK_BY_SHELF = 'Block by shelf'
    BLOCK_BY_TOP_SHELF = 'Block by top shelf'
    BLOCK_BY_BOTTOM_SHELF = 'Block by bottom shelf'
    SURVEY = 'Survey'

    def __init__(self, data_provider, output):
        self.output = output
        self.data_provider = data_provider
        self.project_name = self.data_provider.project_name
        self.session_uid = self.data_provider.session_uid
        self.products = self.data_provider[Data.PRODUCTS]
        self.all_products = self.data_provider[Data.ALL_PRODUCTS]
        self.match_product_in_scene = self.data_provider[Data.MATCHES]
        self.rds_conn = AwsProjectConnector(self.project_name, DbUsers.CalculationEng)
        self.match_display_in_scene = self.get_match_display()
        self.match_stores_by_retailer = self.get_match_stores_by_retailer()
        self.match_template_fk_by_category_fk = self.get_template_fk_by_category_fk()
        self.visit_date = self.data_provider[Data.VISIT_DATE]
        self.session_info = self.data_provider[Data.SESSION_INFO]
        self.scene_info = self.data_provider[Data.SCENES_INFO]
        self.store_id = self.data_provider[Data.STORE_FK]
        self.scif = self.data_provider[Data.SCENE_ITEM_FACTS]
        # self.rds_conn = ProjectConnector(self.project_name, DbUsers.CalculationEng)
        self.tools = CBCILCBCIL_GENERALToolBox(self.data_provider, self.output, rds_conn=self.rds_conn)
        self.session_fk = self.session_info['pk'][0]

        self.kpi_static_data = self.get_kpi_static_data()
        self.kpi_results_queries = []

        self.gaps = pd.DataFrame(columns=[self.KPI_NAME, self.KPI_ATOMIC_NAME, self.GAPS])
        self.gaps_queries = []

        self.rds_conn.disconnect_rds()
        self.rds_conn.connect_rds()
        self.kpis_data = parse_template(TEMPLATE_PATH, 'KPI', lower_headers_row_index=1)
        self.kpi_weights = parse_template(TEMPLATE_PATH, 'kpi weights', lower_headers_row_index=0)
        self.gap_data = parse_template(TEMPLATE_PATH, 'Kpi Gap', lower_headers_row_index=0)

        self.store_data = self.get_store_data_by_store_id()
        self.store_type = self.store_data[self.STORE_TYPE].str.encode('utf-8').tolist()
        self.additional_attribute_1 = self.store_data[self.ADDITIONAL_ATTRIBUTE_1].str.encode('utf-8').tolist()
        self.template_data = self.kpis_data[(self.kpis_data[self.STORE_TYPE].str.encode('utf-8').isin(self.store_type)) &
                                            (self.kpis_data[self.ADDITIONAL_ATTRIBUTE_1].str.encode('utf-8').isin(self.additional_attribute_1))]

    def get_kpi_static_data(self):
        """
        This function extracts the static KPI data and saves it into one global data frame.
        The data is taken from static.kpi / static.atomic_kpi / static.kpi_set.
        """
        query = CBCILCBCIL_Queries.get_all_kpi_data()
        kpi_static_data = pd.read_sql_query(query, self.rds_conn.db)
        return kpi_static_data

    def get_match_display(self):
        """
        This function extracts the display matches data and saves it into one global data frame.
        The data is taken from probedata.match_display_in_scene.
        """
        query = CBCILCBCIL_Queries.get_match_display(self.session_uid)
        match_display = pd.read_sql_query(query, self.rds_conn.db)
        return match_display

    def get_match_stores_by_retailer(self):
        """
        This function extracts the display matches data and saves it into one global data frame.
        The data is taken from static.stores.
        """
        query = CBCILCBCIL_Queries.get_match_stores_by_retailer()
        match_display = pd.read_sql_query(query, self.rds_conn.db)
        return match_display

    def get_store_data_by_store_id(self):
        query = CBCILCBCIL_Queries.get_store_data_by_store_id(self.store_id)
        query_result = pd.read_sql_query(query, self.rds_conn.db)
        return query_result

    def get_template_fk_by_category_fk(self):
        """
        This function extracts the display matches data and saves it into one global data frame.
        The data is taken from static.stores.
        """
        query = CBCILCBCIL_Queries.get_template_fk_by_category_fk()
        match_display = pd.read_sql_query(query, self.rds_conn.db)
        return match_display

    def get_status_session_by_display(self, session_uid):
        query = CBCILCBCIL_Queries.get_status_session_by_display(session_uid)
        match_display = pd.read_sql_query(query, self.rds_conn.db)
        return match_display

    def get_status_session_by_category(self, session_uid):
        query = CBCILCBCIL_Queries.get_status_session_by_category(session_uid)
        match_display = pd.read_sql_query(query, self.rds_conn.db)
        return match_display

    def main_calculation(self, *args, **kwargs):
        """
        This function calculates the KPI results.
        """
        if not self.template_data.empty:
            competitor_coolers, cbc_coolers, relevant_scenes = self.get_coolers('מקרר חברה מרכזית', ['מקרר מתחרה', 'מקרר קמעונאי'])
            kpi_scores = {}
            kpi_set = self.template_data[self.KPI_SET].values[0]
            self.kpi_static_data = self.kpi_static_data[self.kpi_static_data['kpi_set_name'] == kpi_set]
            kpis = self.template_data[self.template_data[self.KPI_SET] == kpi_set][self.KPI_NAME].unique()
            kpis_without_score={}
            all_kpis_in_set=[]

            for kpi in kpis:
                atomics = self.template_data[self.template_data[self.KPI_NAME] == kpi]
                scores = []

                for i in xrange(len(atomics)):
                    atomic = atomics.iloc[i]
                    kpi_type = atomic[self.KPI_TYPE]
                    score = None
                    general_filters = self.get_general_filters(atomic)
                    if self.validate_atomic_kpi(**general_filters):
                        if kpi_type == self.BLOCK_BY_TOP_SHELF:
                            shelf_number = int(general_filters.get(self.TARGET, 1))
                            general_filters['filters']['All'].update({'shelf_number': range(shelf_number + 1)[1:]})
                            score = self.calculate_block_by_shelf(**general_filters)
                        elif kpi_type == self.SOS:
                            score = self.calculate_sos(**general_filters)
                        elif kpi_type == self.SOS_COOLER:
                            score = self.calculate_sos_cooler(competitor_coolers, cbc_coolers, relevant_scenes, **general_filters)
                        elif kpi_type == self.AVAILABILITY:
                            score = self.calculate_availability(**general_filters)
                        elif kpi_type == self.AVAILABILITY_FROM_MID_AND_UP:
                            score = self.calculate_availability(**general_filters)
                        elif kpi_type == self.AVAILABILITY_BY_SEQUENCE:
                            score = self.calculate_availability_by_sequence(**general_filters)
                        elif kpi_type == self.AVAILABILITY_BY_TOP_SHELF:
                            score = self.calculate_availability_by_top_shelf(**general_filters)
                        elif kpi_type == self.AVAILABILITY_FROM_BOTTOM:
                            shelf_number = int(general_filters.get(self.TARGET, 1))
                            general_filters['filters']['All'].update({'shelf_number_from_bottom': range(shelf_number + 1)[1:]})
                            score = self.calculate_availability(**general_filters)
                        elif kpi_type == self.MIN_2_AVAILABILITY:
                            score = self.calculate_min_2_availability(**general_filters)
                        elif kpi_type == self.SURVEY:
                            score = self.calculate_survey(**general_filters)
                        else:
                            Log.warning("KPI of type '{}' is not supported".format(kpi_type))
                            continue

                    try:
                        atomic_weight = float(atomic[self.WEIGHT])
                    except:
                        atomic_weight = None

                    if score is not None:
                        atomic_fk = self.kpi_static_data[self.kpi_static_data['atomic_kpi_name'].str.encode('utf-8') == atomic[self.KPI_ATOMIC_NAME].encode('utf-8')]['atomic_kpi_fk'].values[0]
                        self.write_to_db_result(atomic_fk, self.LEVEL3, score, score)
                        if isinstance(score, tuple):
                            score = score[0]
                        if score == 0:
                            self.add_gap(atomic)

                    scores.append((score, atomic_weight))

                kpi_fk = self.kpi_static_data[self.kpi_static_data['kpi_name'] == kpi]['kpi_fk'].values[0]
                denominator_weight = self.get_kpi_weight(kpi, kpi_set)

                kpi_details = self.combine_kpi_details(kpi_fk, scores, denominator_weight)
                all_kpis_in_set.append(kpi_details)

                if all(map(lambda x: x[0] is None, scores)):
                    kpis_without_score[kpi_fk] = float(denominator_weight)

            all_kpis_in_set = self.reallocate_weights_to_kpis_with_results(kpis_without_score, all_kpis_in_set)

            for kpi in filter(lambda x: x['denominator_weight'] != 0, all_kpis_in_set):
                pass_atomics = filter(lambda x: x[0] is not None, kpi['atomic_scores_and_weights'])
                if len(pass_atomics):
                    add_weights = sum(map(lambda y: y[1], filter(lambda x: x[0] is None and x[1] is not None,
                                                                 kpi['atomic_scores_and_weights']))) / len(pass_atomics)
                else:
                    add_weights = 0

                weights = sum(map(lambda x: x[1] is not None and x[1], pass_atomics))
                denominator_weight = kpi['denominator_weight']

                if weights:
                    kpi_score = sum(map(lambda x: x[0] * (x[1] + add_weights) * 100, pass_atomics)) / 100
                else:
                    if len(pass_atomics):
                        score_weight = float(denominator_weight) / len(pass_atomics)
                    else:
                        score_weight = 0
                    kpi_score = sum(map(lambda x: x[0] * score_weight, pass_atomics))

                kpi_scores[kpi['kpi_fk']] = kpi_score
                self.write_to_db_result(kpi['kpi_fk'], self.LEVEL2, kpi_scores[kpi['kpi_fk']], float(kpi['denominator_weight']) * 100)

            final_score = sum([score for score in kpi_scores.values()])
            set_fk = self.kpi_static_data[self.kpi_static_data['kpi_set_name'] == kpi_set]['kpi_set_fk'].values[0]
            self.write_to_db_result(set_fk, self.LEVEL1, final_score)
            self.write_gaps_to_db() #Natalya - to comment out before debug
            self.commit_results_data() #Natalya - to comment out before debug

    def combine_kpi_details(self, kpi_fk, scores, denominator_weight):
        kpi_details={}
        kpi_details['kpi_fk'] = kpi_fk
        kpi_details['atomic_scores_and_weights'] = scores
        kpi_details['denominator_weight'] = float(denominator_weight)
        return kpi_details

    # @staticmethod
    # def combine_kpi_details(kpi_fk, scores, denominator_weight):
    #     kpi_details = {}
    #     kpi_details['kpi_fk'] = kpi_fk
    #     kpi_details['atomic_scores_and_weights'] = scores
    #     kpi_details['denominator_weight'] = float(denominator_weight)
    #     return kpi_details

    def reallocate_weights_to_kpis_with_results(self, kpis_without_score, all_kpis_in_set):
        if kpis_without_score:
            total_weight_to_reallocate = sum([weight for weight in kpis_without_score.values()])
            weight_to_each_kpi = total_weight_to_reallocate / (len(all_kpis_in_set) - len(kpis_without_score.items()))
            for kpi in all_kpis_in_set:
                if kpi['kpi_fk'] in kpis_without_score.keys():
                    kpi['denominator_weight'] = 0
                    kpi['atomic_scores_and_weights'] = [(score[0], 0) for score in kpi['atomic_scores_and_weights']]
                else:
                    kpi['denominator_weight'] = kpi['denominator_weight'] + weight_to_each_kpi
                    atomics_with_weights = filter(lambda x: x[1] is not None,
                                                  kpi['atomic_scores_and_weights'])
                    if atomics_with_weights:
                        kpi['atomic_scores_and_weights'] = map(
                            lambda x: (x[0], x[1] + weight_to_each_kpi / len(atomics_with_weights)),
                            atomics_with_weights)
        return all_kpis_in_set

    # @staticmethod
    # def reallocate_weights_to_kpis_with_results(kpis_without_score, all_kpis_in_set):
    #     if kpis_without_score:
    #         total_weight_to_reallocate = sum([weight for weight in kpis_without_score.values()])
    #         weight_to_each_kpi = total_weight_to_reallocate / (len(all_kpis_in_set) - len(kpis_without_score.items()))
    #         for kpi in all_kpis_in_set:
    #             if kpi['kpi_fk'] in kpis_without_score.keys():
    #                 kpi['denominator_weight'] = 0
    #                 kpi['atomic_scores_and_weights'] = [(score[0], 0) for score in kpi['atomic_scores_and_weights']]
    #             else:
    #                 kpi['denominator_weight'] = kpi['denominator_weight'] + weight_to_each_kpi
    #                 atomics_with_weights = filter(lambda x: x[1] is not None,
    #                                               kpi['atomic_scores_and_weights'])
    #                 if atomics_with_weights:
    #                     kpi['atomic_scores_and_weights'] = map(
    #                         lambda x: (x[0], x[1] + weight_to_each_kpi / len(atomics_with_weights)),
    #                         atomics_with_weights)
    #     return all_kpis_in_set

    def get_coolers(self, cbc_coller, competitor_cooler):
        cbc = self.scif[self.scif['template_name'].str.encode('utf-8') == cbc_coller]['scene_fk'].unique().tolist()
        competitor = self.scif[self.scif['template_name'].str.encode('utf-8').isin(competitor_cooler)]['scene_fk'].unique()
        return len(competitor), len(cbc), cbc

    def get_general_filters(self, params):
        template_name = params['Template Name'].split(self.SEPARATOR)
        template_group = params['Template group'].split(self.SEPARATOR)

        if template_name[0].strip() and template_group[0].strip():
            relative_scenes = self.scif[
                (self.scif['template_name'].isin(template_name)) & (self.scif['template_group'].isin(template_group))]
        elif template_group[0].strip():
            relative_scenes = self.scif[(self.scif['template_group'].isin(template_group))]
        elif template_name[0].strip():
            relative_scenes = self.scif[(self.scif['template_name'].isin(template_name))]
        else:
            relative_scenes = self.scif

        general_filters = {'scene_id': relative_scenes['scene_id'].unique().tolist()}

        try:
            params2 = map(float, params[self.PARAMS_VALUE_2].split(','))
        except:
            params2 = map(unicode.strip, params[self.PARAMS_VALUE_2].split(','))

        result = {self.TARGET: params[self.TARGET],
                  self.SPLIT_SCORE: params[self.SPLIT_SCORE],
                  'filters': {
                     '1': {params[self.PARAMS_TYPE_1]: map(unicode.strip, params[self.PARAMS_VALUE_1].split(','))},
                     '2': {params[self.PARAMS_TYPE_2]: params2},
                     '3': {params[self.PARAMS_TYPE_3]: map(unicode.strip, params[self.PARAMS_VALUE_3].split(','))},
                     'All': general_filters}
                  }
        return result

    def calculate_survey(self, **general_filters):
        params = general_filters['filters']
        filters = params['2'].copy()
        try:
            survey_question = int(filters.get('question_id')[0])
        except:
            survey_question = 0
        target_answers = general_filters[self.TARGET].split(self.SEPARATOR)
        survey_answer = self.tools.get_survey_answer(('question_fk', [survey_question]))
        if survey_answer:
            return 100 if survey_answer.strip() in target_answers else False
        else:
            return 0

    def calculate_block_by_shelf(self, **general_filters):
        params = general_filters['filters']
        if params['All']['scene_id']:
            filters = params['1'].copy()
            filters.update(params['2'])
            filters.update(params['3'])
            filters.update(params['All'])
            for scene in params['All']['scene_id']:
                filters.update({'scene_id': scene})
                block = self.tools.calculate_block_together(include_empty=False, minimum_block_ratio=0.75,
                                                             allowed_products_filters={'product_type': 'Other'},
                                                             vertical=True, **filters)
                if not isinstance(block, dict):
                    return 0
                if float(len(block['shelves'])) >= float(general_filters[self.TARGET]):
                    return 100
        return 0

    def calculate_sos(self, **general_filters):
        params = general_filters['filters']
        if params['All']['scene_id']:
            numerator_filters = params['1'].copy()
            numerator_filters.update(params['2'])
            numerator_filters.update(params['3'])
            ratio = self.tools.calculate_linear_share_of_display(numerator_filters,
                                                                   include_empty=True,
                                                                   **params['All'])

            if ratio >= float(general_filters[self.TARGET]):
                return 100
            else:
                # return round(ratio*100, 2) #Natalya - added
                return round(ratio, 2)
        return 0

    def calculate_sos_cooler(self, competitor_coolers, cbc_coolers, relevant_scenes, **general_filters):
        params = general_filters['filters']
        if params['All']['scene_id']:
            numerator_filters = params['1'].copy()
            numerator_filters.update(params['2'])

            set_scores = []
            for scene in relevant_scenes:
                filters = {'scene_fk': scene}
                ratio = self.tools.calculate_linear_share_of_display(numerator_filters, **filters)
                set_scores.append(ratio)
                set_scores.sort()

            if competitor_coolers > 0 and 0 < cbc_coolers == set_scores.count(1.0):
                return 100
            elif cbc_coolers > 1 and set_scores.count(1.0) >= (cbc_coolers - 1):
                if set_scores[0] >= 0.8:
                    return 100
            elif cbc_coolers == 1 and set_scores[0] > 0.8:
                return 100

            # #alternative - Natalya:
            # if competitor_coolers > 0:
            #     return 100 if 0 < cbc_coolers == set_scores.count(1.0) else sum(set_scores)/len(set_scores)*100
            # elif cbc_coolers > 1:
            #     set_scores.sort(reverse=True)
            #     if set_scores[0] < 0.8:
            #         return (set_scores[0]/0.8+sum(set_scores[1:]))/len(set_scores)*100
            #     elif all(score > 0.8 for score in set_scores):
            #         pass
            # elif cbc_coolers == 1:
            #     return set_scores[0]/0.8*100 if set_scores[0] < 0.8 else 100
        return 0

    def calculate_availability(self, **general_filters):
        params = general_filters['filters']
        if params['All']['scene_id']:
            filters = params['1'].copy()
            filters.update(params['All'])
            if self.tools.calculate_availability(**filters) >= 1:
                return 100
        return 0

    def calculate_availability_from_mid_and_up(self, **general_filters):
        params = general_filters['filters']
        if params['All']['scene_id']:
            filters = params['1'].copy()
            filters.update(params['2'])
            filters.update(params['3'])
            filters.update(params['All'])
            for scene in params['All']['scene_id']:
                filters.update({'scene_id': scene})
                relevant_shelf = self.match_product_in_scene[self.match_product_in_scene['scene_id'] == scene]['shelf_number'].unique().tolist()
                filters.update({'shelf_number': relevant_shelf[:len(relevant_shelf) / 2]})
                if self.tools.calculate_availability(**filters) >= 1:
                    return 100
        return 0

    def calculate_availability_by_top_shelf(self, **general_filters):
        params = general_filters['filters']
        if params['All']['scene_id']:
            shelf_number = int(general_filters.get(self.TARGET, 1))
            shelf_numbers = range(shelf_number + 1)[1:]
            if shelf_numbers:
                filters = params['1'].copy()
                filters.update(params['2'])
                filters.update(params['3'])
                filters.update(params['All'])
                filters.update({'shelf_number': shelf_numbers})
                result = self.match_product_in_scene[self.tools.get_filter_condition(self.match_product_in_scene, **filters)]
                result = result['shelf_number'].unique().tolist()
                if len(result) == len(shelf_numbers):
                    return 100
        return 0

    def calculate_availability_by_sequence(self, **general_filters):
        params = general_filters['filters']
        if params['All']['scene_id']:
            filters = params['1'].copy()
            filters.update(params['2'])
            filters.update(params['All'])
            matches = self.match_product_in_scene.merge(self.scif, on='product_fk')
            result = matches[self.tools.get_filter_condition(matches, **filters)]
            if not result.empty:
                result = result['shelf_number'].unique().tolist()
                result.sort()
            if len(result) >= 4:
                for i in range(len(result) - 3):
                    if result[i] == result[i + 1] - 1 == result[i + 2] - 2 == result[i + 3] - 3:
                        return 100
        return 0

    def calculate_min_2_availability(self, **general_filters):
        params = general_filters['filters']
        if params['All']['scene_id']:
            filters = params['1'].copy()
            filter_type = filters.keys()[0]
            pass_values = total_values = 0
            availability_filters = params['All'].copy()
            for value in filters[filter_type]:
                availability_filters.update({filter_type: value})
                availability_result = self.tools.calculate_availability(**availability_filters)
                if availability_result >= 2:
                    pass_values += 1
                    total_values += 1
                elif availability_result >= 1:
                    total_values += 1
            if pass_values:
                return (pass_values / float(total_values)) * 100

        return 0

    def get_kpi_fk_by_kpi_name(self, kpi_name):
        assert isinstance(kpi_name, unicode), "name is not a string: %r" % kpi_name
        try:
            return self.kpi_static_data[self.kpi_static_data['atomic_kpi_name'] == kpi_name]['atomic_kpi_fk'].values[0]
        except IndexError:
            Log.info('Kpi name: {}, isnt equal to any kpi name in static table'.format(kpi_name))
            return None

    def add_gap(self, params):
        """
        This function extracts a failed KPI's priority, and saves it in a general dictionary.
        """
        kpi_name = params[self.KPI_NAME]
        kpi_atomic_name = params[self.KPI_ATOMIC_NAME]
        if kpi_name in self.gap_data[self.KPI_NAME].tolist():
            gap = self.gap_data[self.gap_data[self.KPI_NAME] == kpi_name]
            if not gap.empty:
                gap = gap.iloc[0]['Order']
            else:
                Log.warning("Gaps doesn't include KPI '{}'".format(kpi_atomic_name))
                gap = None
            if not gap:
                gap = None
            elif isinstance(gap, float):
                gap = int(gap)
            elif isinstance(gap, (str, unicode)):
                gap = None if not gap.isdigit() else int(gap)
            if gap is not None:
                # if kpi_name not in self.gaps.keys():
                #     self.gaps[kpi_name] = {}
                # self.gaps[kpi_name][gap] = kpi_atomic_name
                kpi_atomic_name = int(self.get_kpi_fk_by_kpi_name(kpi_atomic_name))
                atomic_gap = {self.KPI_NAME: kpi_name, self.KPI_ATOMIC_NAME: kpi_atomic_name, self.GAPS: gap}
                self.gaps = self.gaps.append(atomic_gap, ignore_index=True)

    def write_gaps_to_db(self):
        """
        This function translates KPI gaps into SQL queries, later to be inserted into the DB.
        """
        priorities = range(1, 6)
        for gap_category in self.gap_data[self.KPI_NAME].tolist():
            for i, row in self.gaps[self.gaps[self.KPI_NAME] == gap_category].iterrows():
                if not priorities:
                    break
                kpi_atomic_name = row[self.KPI_ATOMIC_NAME]
                attributes = pd.DataFrame([(self.session_fk, gap_category, kpi_atomic_name, priorities.pop(0))],
                                          columns=['session_fk', 'gap_category', 'name', 'priority'])
                query = insert(attributes.to_dict(), CUSTOM_GAPS_TABLE)
                self.gaps_queries.append(query)

    def write_to_db_result(self, fk, level, score=None, result=None, result_2=None):
        """
        This function creates the result data frame of every KPI (atomic KPI/KPI/KPI set),
        and appends the insert SQL query into the queries' list, later to be written to the DB.
        """
        # assert isinstance(fk, int), "fk is not a int: %r" % fk
        # assert isinstance(score, float), "score is not a float: %r" % score
        attributes = self.create_attributes_dict(fk, score, result, result_2, level)
        if level == self.LEVEL1:
            table = KPS_RESULT
        elif level == self.LEVEL2:
            table = KPK_RESULT
        elif level == self.LEVEL3:
            table = KPI_RESULT
        else:
            return
        query = insert(attributes, table)
        self.kpi_results_queries.append(query)

    def create_attributes_dict(self, fk, score=None, result=None, result_2=None, level=None):
        """
        This function creates a data frame with all attributes needed for saving in KPI results tables.

        """
        if level == self.LEVEL1:
            kpi_set_name = self.kpi_static_data[self.kpi_static_data['kpi_set_fk'] == fk]['kpi_set_name'].values[0]
            attributes = pd.DataFrame([(kpi_set_name, self.session_uid, self.store_id, self.visit_date.isoformat(),
                                        format(score, '.2f'), fk)],
                                      columns=['kps_name', 'session_uid', 'store_fk', 'visit_date', 'score_1',
                                               'kpi_set_fk'])
        elif level == self.LEVEL2:
            kpi_name = self.kpi_static_data[self.kpi_static_data['kpi_fk'] == fk]['kpi_name'].values[0]
            attributes = pd.DataFrame([(self.session_uid, self.store_id, self.visit_date.isoformat(),
                                        fk, kpi_name, score, format(score, '.2f'), result)],
                                      columns=['session_uid', 'store_fk', 'visit_date', 'kpi_fk',
                                               'kpk_name', 'score', 'score_2', 'score_3'])
        elif level == self.LEVEL3:
            data = self.kpi_static_data[self.kpi_static_data['atomic_kpi_fk'] == fk]
            # atomic_kpi_name = data['atomic_kpi_name'].values[0]
            kpi_fk = data['kpi_fk'].values[0]
            kpi_set_name = self.kpi_static_data[self.kpi_static_data['atomic_kpi_fk'] == fk]['kpi_set_name'].values[0]
            attributes = pd.DataFrame([(self.session_uid, kpi_set_name, self.store_id,
                                        self.visit_date.isoformat(), datetime.utcnow().isoformat(),
                                        score, format(result, '.2f'), result_2, kpi_fk, fk)],
                                      columns=['session_uid', 'kps_name', 'store_fk', 'visit_date',
                                               'calculation_time', 'score', 'result', 'result_2', 'kpi_fk',
                                               'atomic_kpi_fk'])
        else:
            attributes = pd.DataFrame()
        return attributes.to_dict()

    @log_runtime('Saving to DB')
    def commit_results_data(self):
        """
        This function writes all KPI results to the DB, and commits the changes.
        """
        insert_queries = self.merge_insert_queries(self.kpi_results_queries)
        cur = self.rds_conn.db.cursor()
        delete_queries = CBCILCBCIL_Queries.get_delete_session_results_query(self.session_uid, self.session_fk)
        for query in delete_queries:
            cur.execute(query)
        for query in insert_queries:
            cur.execute(query)
        for query in self.gaps_queries:
            cur.execute(query)

        cur.execute('''update pservice.custom_gaps B join static.atomic_kpi A on convert(B.name, SIGNED INTEGER) = A.pk set B.name = A.display_text where B.session_fk = '{}';'''.format(self.session_fk))
        cur.execute('''update report.kpi_results B join static.atomic_kpi A on B.atomic_kpi_fk = A.pk set B.display_text = A.display_text where B.session_uid = '{}';'''.format(self.session_uid))
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

    def get_kpi_weight(self, kpi, kpi_set):
        row = self.kpi_weights[(self.kpi_weights[self.KPI_SET] == kpi_set) & (self.kpi_weights[self.KPI_NAME] == kpi)]
        weight = row.get(self.WEIGHT)
        if not weight.empty:
            return weight.values[0]
        else:
            return 0

    def validate_atomic_kpi(self, **params):
        if params.get(self.SPLIT_SCORE, 0) and not params['filters']['All'].get('scene_id'):
            return False
        return True
