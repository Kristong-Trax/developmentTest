# coding=utf-8
import os
from datetime import datetime
import pandas as pd
import time
from Trax.Algo.Calculations.Core.DataProvider import Data
from Trax.Utils.Conf.Keys import DbUsers
from Trax.Utils.Logging.Logger import Log
from Trax.Data.Utils.MySQLservices import get_table_insertion_query as insert
from KPIUtils_v2.DB.PsProjectConnector import PSProjectConnector
from Projects.CBCIL.Utils.Fetcher import CBCILCBCIL_Queries
from Projects.CBCIL.Utils.GeneralToolBox import CBCILCBCIL_GENERALToolBox
from Projects.CBCIL.Utils.ParseTemplates import parse_template
from KPIUtils_v2.DB.CommonV2 import Common
from KPIUtils_v2.Utils.Decorators.Decorators import log_runtime, kpi_runtime
from Projects.CBCIL.Utils.Consts import Consts

__author__ = 'Israel'

KPI_RESULT = 'report.kpi_results'
KPK_RESULT = 'report.kpk_results'
KPS_RESULT = 'report.kps_results'

CUSTOM_GAPS_TABLE = 'pservice.custom_gaps'

TEMPLATE_PATH = os.path.join(os.path.dirname(os.path.realpath(__file__)), '..', 'Data')
TEMPLATE_NAME_UNTIL_2019_01_15 = 'Template_until_2019-01-15.xlsx'
TEMPLATE_NAME_BETWEEN_2019_01_15_TO_2019_03_01 = 'Template_until_2019-03-01.xlsx'
CURRENT_TEMPLATE = 'Template.xlsx'


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

    TOTAL_SCORE = 'Total Score'
    TOTAL_SCORE_FOR_DASHBOARD = 'Total Score 2'

    CBCIL = 'Central Bottling Company'

    def __init__(self, data_provider, output):
        self.output = output
        self.data_provider = data_provider
        self.project_name = self.data_provider.project_name
        self.session_uid = self.data_provider.session_uid
        self.products = self.data_provider[Data.PRODUCTS]
        self.all_products = self.data_provider[Data.ALL_PRODUCTS]
        self.match_product_in_scene = self.data_provider[Data.MATCHES]
        self.rds_conn = PSProjectConnector(self.project_name, DbUsers.CalculationEng)
        self.match_display_in_scene = self.get_match_display()
        self.match_stores_by_retailer = self.get_match_stores_by_retailer()
        self.match_template_fk_by_category_fk = self.get_template_fk_by_category_fk()
        self.visit_date = self.data_provider[Data.VISIT_DATE]
        self.session_info = self.data_provider[Data.SESSION_INFO]
        self.scene_info = self.data_provider[Data.SCENES_INFO]
        self.store_id = self.data_provider[Data.STORE_FK]
        self.scif = self.data_provider[Data.SCENE_ITEM_FACTS]
        self.tools = CBCILCBCIL_GENERALToolBox(self.data_provider, self.output, rds_conn=self.rds_conn)
        self.session_fk = self.session_info['pk'][0]

        self.kpi_static_data = self.get_kpi_static_data()
        self.kpi_results_queries = []

        self.gaps = pd.DataFrame(columns=[self.KPI_NAME, self.KPI_ATOMIC_NAME, self.GAPS])
        self.gaps_queries = []

        self.rds_conn.disconnect_rds()
        self.rds_conn.connect_rds()
        self.template_path = self.get_relevant_template()
        self.kpis_data = parse_template(self.template_path, 'KPI', lower_headers_row_index=1)
        self.kpi_weights = parse_template(self.template_path, 'kpi weights', lower_headers_row_index=0)
        self.gap_data = parse_template(self.template_path, 'Kpi Gap', lower_headers_row_index=0)

        self.store_data = self.get_store_data_by_store_id()
        self.store_type = self.store_data[self.STORE_TYPE].str.encode('utf-8').tolist()
        self.additional_attribute_1 = self.store_data[self.ADDITIONAL_ATTRIBUTE_1].str.encode('utf-8').tolist()
        self.template_data = self.kpis_data[
            (self.kpis_data[self.STORE_TYPE].str.encode('utf-8').isin(self.store_type)) &
            (self.kpis_data[self.ADDITIONAL_ATTRIBUTE_1].str.encode('utf-8').isin(self.additional_attribute_1))]

        self.common = Common(self.data_provider)
        self.cbcil_id = self.get_own_manufacturer_pk()

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

    def main_calculation(self):
        """
        This function calculates the KPI results.
        """
        if not self.template_data.empty:
            competitor_coolers, cbc_coolers, relevant_scenes = self.get_coolers('מקרר חברה מרכזית',
                                                                                ['מקרר מתחרה', 'מקרר קמעונאי'])
            kpi_scores = {}
            kpi_set = self.template_data[self.KPI_SET].values[0]
            self.kpi_static_data = self.kpi_static_data[self.kpi_static_data['kpi_set_name'].str.encode('utf-8') ==
                                                        kpi_set.encode('utf-8')]
            kpis = self.template_data[self.template_data[self.KPI_SET].str.encode('utf-8') ==
                                      kpi_set.encode('utf-8')][self.KPI_NAME].unique()
            kpis_without_score = {}
            all_kpis_in_set = []

            identifier_result_set = self.get_identifier_result_set()

            for kpi in kpis:
                identifier_result_kpi = self.get_identifier_result_kpi_by_name(kpi)

                atomics = self.template_data[self.template_data[self.KPI_NAME].str.encode('utf-8') ==
                                             kpi.encode('utf-8')]
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
                            score = self.calculate_sos_cooler(competitor_coolers, cbc_coolers, relevant_scenes,
                                                              **general_filters)
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

                    atomic_weight = float(atomic[self.WEIGHT]) if atomic[self.WEIGHT] else 0
                    if score is not None:
                        atomic_fk = self.kpi_static_data[
                            self.kpi_static_data['atomic_kpi_name'].str.encode('utf-8') == atomic[
                                self.KPI_ATOMIC_NAME].encode('utf-8')]['atomic_kpi_fk'].values[0]
                        self.write_to_db_result(atomic_fk, self.LEVEL3, score, score)
                        if isinstance(score, tuple):
                            score = score[0]
                        if score == 0:
                            self.add_gap(atomic, score)
                        atomic_fk_lvl_2 = self.common.get_kpi_fk_by_kpi_type(atomic[self.KPI_ATOMIC_NAME])
                        self.common.write_to_db_result(fk=atomic_fk_lvl_2, numerator_id=self.cbcil_id,
                                                       denominator_id=self.store_id,
                                                       weight=round(atomic_weight * 100, 2),
                                                       identifier_parent=identifier_result_kpi,
                                                       result=score, score=round(score * atomic_weight, 2),
                                                       should_enter=True)

                    scores.append((score, atomic_weight))

                kpi_fk = self.kpi_static_data[self.kpi_static_data['kpi_name'].str.encode('utf-8') ==
                                              kpi.encode('utf-8')]['kpi_fk'].values[0]
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
                self.write_to_db_result(kpi['kpi_fk'], self.LEVEL2, kpi_scores[kpi['kpi_fk']],
                                        float(kpi['denominator_weight']) * 100)

                kpi_name = self.get_kpi_name_by_pk(kpi['kpi_fk'])
                kpi_lvl_2_fk = self.common.get_kpi_fk_by_kpi_type(kpi_name)
                identifier_res_kpi_2 = self.get_identifier_result_kpi_by_pk(kpi_lvl_2_fk)
                kpi_weight = float(kpi['denominator_weight']) * 100
                self.common.write_to_db_result(fk=kpi_lvl_2_fk, numerator_id=self.cbcil_id,
                                               denominator_id=self.store_id,
                                               identifier_parent=identifier_result_set,
                                               identifier_result=identifier_res_kpi_2,
                                               weight=kpi_weight,  target=kpi_weight,
                                               score=kpi_scores[kpi['kpi_fk']],
                                               should_enter=True, result=kpi_scores[kpi['kpi_fk']])

            final_score = sum([score for score in kpi_scores.values()])
            set_fk = self.kpi_static_data[self.kpi_static_data['kpi_set_name'].str.encode('utf-8') ==
                                          kpi_set.encode('utf-8')]['kpi_set_fk'].values[0]
            self.write_to_db_result(set_fk, self.LEVEL1, final_score)
            self.write_gaps_to_db()

            total_score_fk = self.common.get_kpi_fk_by_kpi_type(self.TOTAL_SCORE)
            self.common.write_to_db_result(fk=total_score_fk, numerator_id=self.cbcil_id, denominator_id=self.store_id,
                                           identifier_result=identifier_result_set, result=round(final_score, 2),
                                           weight=round(100, 2), target=round(80, 2), score=final_score,
                                           should_enter=True)

            # requested for dashboard
            total_score_fk_for_dashboard = self.common.get_kpi_fk_by_kpi_type(self.TOTAL_SCORE_FOR_DASHBOARD)

            self.common.write_to_db_result(fk=total_score_fk_for_dashboard, numerator_id=self.cbcil_id,
                                           denominator_id=self.store_id,
                                           identifier_result=identifier_result_set, result=round(final_score, 2),
                                           weight=round(100, 2), target=round(80, 2), score=final_score,
                                           should_enter=True)
            self.commit_results_data()
            self.common.commit_results_data()

    # --------new tables functionality--------#
    def get_own_manufacturer_pk(self):
        query = CBCILCBCIL_Queries.get_manufacturer_pk_by_name(self.CBCIL)
        query_result = pd.read_sql_query(query, self.rds_conn.db)
        cbcil_pk = query_result['pk'].values[0]
        return cbcil_pk

    def get_identifier_result_set(self):
        kpi_name = self.TOTAL_SCORE
        identifier_result = self.common.get_dictionary(kpi_fk=self.common.get_kpi_fk_by_kpi_type(kpi_name),
                                                       manufacturer_id=self.cbcil_id, store_id=self.store_id)
        return identifier_result

    def get_identifier_result_kpi_by_name(self, kpi_type):
        identifier_result = self.common.get_dictionary(kpi_fk=self.common.get_kpi_fk_by_kpi_type(kpi_type),
                                                       manufacturer_id=self.cbcil_id, store_id=self.store_id)
        return identifier_result

    def get_identifier_result_kpi_by_pk(self, kpi_fk):
        identifier_result = self.common.get_dictionary(kpi_fk=kpi_fk, manufacturer_id=self.cbcil_id,
                                                       store_id=self.store_id)
        return identifier_result

    def get_kpi_name_by_pk(self, kpi_pk):
        kpi_name = self.kpi_static_data[self.kpi_static_data['kpi_fk'] == kpi_pk]['kpi_name'].values[0]
        return kpi_name

    # -------- existing calculations----------#
    @staticmethod
    def combine_kpi_details(kpi_fk, scores, denominator_weight):
        kpi_details = dict()
        kpi_details['kpi_fk'] = kpi_fk
        kpi_details['atomic_scores_and_weights'] = scores
        kpi_details['denominator_weight'] = float(denominator_weight)
        return kpi_details

    @staticmethod
    def reallocate_weights_to_kpis_with_results(kpis_without_score, all_kpis_in_set):
        if kpis_without_score:
            total_weight_to_reallocate = sum([weight for weight in kpis_without_score.values()])
            weight_of_all_kpis_with_scores = sum([kpi['denominator_weight'] for kpi in
                                                 filter(lambda x: x['kpi_fk'] not in kpis_without_score.keys(),
                                                        all_kpis_in_set)])
            for kpi in all_kpis_in_set:
                if kpi['kpi_fk'] in kpis_without_score.keys():
                    kpi['denominator_weight'] = 0
                    kpi['atomic_scores_and_weights'] = [(score[0], 0) for score in kpi['atomic_scores_and_weights']]
                else:
                    weight_to_kpi = total_weight_to_reallocate * float(kpi['denominator_weight']) / weight_of_all_kpis_with_scores
                    kpi['denominator_weight'] = kpi['denominator_weight'] + weight_to_kpi
                    atomics_with_weights = filter(lambda x: x[1] is not None,
                                                  kpi['atomic_scores_and_weights'])
                    if atomics_with_weights:
                        kpi['atomic_scores_and_weights'] = map(
                            lambda x: (x[0], x[1] + weight_to_kpi / len(atomics_with_weights)),
                            atomics_with_weights)
        return all_kpis_in_set

    def get_coolers(self, cbc_coller, competitor_cooler):
        cbc = self.scif[self.scif['template_name'].str.encode('utf-8') == cbc_coller]['scene_fk'].unique().tolist()
        competitor = self.scif[
            self.scif['template_name'].str.encode('utf-8').isin(competitor_cooler)]['scene_fk'].unique()
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

        try:
            params3 = map(float, params[self.PARAMS_VALUE_3].split(','))
        except:
            params3 = map(unicode.strip, params[self.PARAMS_VALUE_3].split(','))

        result = {self.TARGET: params[self.TARGET],
                  self.SPLIT_SCORE: params[self.SPLIT_SCORE],
                  'filters': {
                     '1': {params[self.PARAMS_TYPE_1]: map(unicode.strip, params[self.PARAMS_VALUE_1].split(','))},
                     '2': {params[self.PARAMS_TYPE_2]: params2},
                     '3': {params[self.PARAMS_TYPE_3]: params3},
                     'All': general_filters}
                  }
        return result

    @kpi_runtime()
    def calculate_survey(self, **general_filters):
        params = general_filters['filters']
        filters = params['2'].copy()
        if not params['All']['scene_id']:
            return None
        try:
            survey_question = str(int(filters.get('question_id')[0]))
        except:
            survey_question = str(0)
        target_answers = general_filters[self.TARGET].split(self.SEPARATOR)
        survey_answer = self.tools.get_survey_answer(('code', [survey_question]))
        if survey_answer:
            return 100 if survey_answer.strip() in target_answers else False
        else:
            return 0

    @kpi_runtime()
    def calculate_block_by_shelf(self, **general_filters):
        params = general_filters['filters']
        if params['All']['scene_id']:
            filters = params['1'].copy()
            filters.update(params['2'])
            filters.update(params['3'])
            filters.update(params['All'])
            for scene in params['All']['scene_id']:
                filters.update({'scene_id': scene})
                try:
                    filters.pop('')
                except:
                    pass
                block = self.tools.calculate_block_together(include_empty=False, minimum_block_ratio=0.75,
                                                            allowed_products_filters={'product_type': 'Other'},
                                                            vertical=True, **filters)
                if not isinstance(block, dict):
                    return 0
                if float(len(block['shelves'])) >= float(general_filters[self.TARGET]):
                    return 100
        return 0

    @kpi_runtime()
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
                return round(ratio*100, 2)
        return 0

    @kpi_runtime()
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

            if competitor_coolers > 0 and 0 < cbc_coolers:
                return sum(set_scores)/len(set_scores)*100
            elif cbc_coolers > 1:
                if all(score < 0.8 for score in set_scores):
                    set_scores.sort(reverse=True)
                return (min(set_scores[0] / 0.8, 1) + sum(set_scores[1:])) / len(set_scores) * 100
            elif cbc_coolers == 1:
                return set_scores[0]/0.8*100 if set_scores[0] < 0.8 else 100
        return 0

    @kpi_runtime()
    def calculate_availability(self, **general_filters):
        params = general_filters['filters']
        if params['All']['scene_id']:
            filters = params['1'].copy()
            filters.update(params['All'])
            if self.tools.calculate_availability(**filters) >= 1:
                return 100
        return 0

    @kpi_runtime()
    def calculate_availability_from_mid_and_up(self, **general_filters):
        params = general_filters['filters']
        if params['All']['scene_id']:
            filters = params['1'].copy()
            filters.update(params['2'])
            filters.update(params['3'])
            filters.update(params['All'])
            for scene in params['All']['scene_id']:
                filters.update({'scene_id': scene})
                relevant_shelf = self.match_product_in_scene[
                    self.match_product_in_scene['scene_id'] == scene]['shelf_number'].unique().tolist()
                filters.update({'shelf_number': relevant_shelf[:len(relevant_shelf) / 2]})
                if self.tools.calculate_availability(**filters) >= 1:
                    return 100
        return 0

    @kpi_runtime()
    def calculate_availability_by_top_shelf(self, **general_filters):
        params = general_filters['filters']
        if params['All']['scene_id']:
            shelf_number = int(general_filters.get(self.TARGET, 1))
            shelf_numbers = range(shelf_number + 1)[1:]
            if shelf_numbers:
                session_results = []
                for scene in params['All']['scene_id']:
                    scene_result = 0
                    scif_filters = {'scene_fk': scene}
                    scif_filters.update(params['1'])
                    scif_filters.update(params['2'])
                    scif = self.scif.copy()
                    scene_skus = scif[
                        self.tools.get_filter_condition(scif, **scif_filters)]['product_fk'].unique().tolist()
                    if scene_skus:
                        matches_filters = {'scene_fk': scene}
                        matches_filters.update({'product_fk': scene_skus})
                        matches_filters.update({'shelf_number': shelf_numbers})
                        matches = self.match_product_in_scene.copy()
                        result = matches[self.tools.get_filter_condition(matches, **matches_filters)]
                        if not result.empty:
                            shelf_facings_result = result.groupby('shelf_number')['scene_fk'].count().values.tolist()
                            if len(shelf_facings_result) == len(shelf_numbers):
                                target_facings_per_shelf = params['3']['facings'][0]
                                scene_result = 100 if all([facing >= target_facings_per_shelf
                                                           for facing in shelf_facings_result]) else 0
                    session_results.append(scene_result)
                return 100 if any(session_results) else 0
        return 0

    @kpi_runtime()
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

    @kpi_runtime()
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
            return self.kpi_static_data[self.kpi_static_data['atomic_kpi_name'].str.encode('utf-8') ==
                                        kpi_name.encode('utf-8')]['atomic_kpi_fk'].values[0]
        except IndexError:
            Log.info('Kpi name: {}, isnt equal to any kpi name in static table'.format(kpi_name))
            return None

    def add_gap(self, params, score):
        """
        This function extracts a failed KPI's priority, and saves it in a general dictionary.
        """
        kpi_name = params[self.KPI_NAME]
        kpi_atomic_name = params[self.KPI_ATOMIC_NAME]
        weight = float(params[self.WEIGHT]) if params[self.WEIGHT] else 0
        rlv_kpi_level_2_fk = self.common.get_kpi_fk_by_kpi_type(kpi_atomic_name)
        if kpi_name in self.gap_data[self.KPI_NAME].tolist():
            gap = self.gap_data[self.gap_data[self.KPI_NAME].str.encode('utf-8') == kpi_name.encode('utf-8')]
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
                atomic_gap = {self.KPI_NAME: kpi_name, self.KPI_ATOMIC_NAME: kpi_atomic_name, self.GAPS: gap,
                              Consts.LEVEL_2_FK: rlv_kpi_level_2_fk, self.WEIGHT: weight, Consts.SCORE: score}
                self.gaps = self.gaps.append(atomic_gap, ignore_index=True)

    def write_gaps_to_db(self):
        """
        This function translates KPI gaps into SQL queries, later to be inserted into the DB.
        """
        priorities = range(1, 6)
        for gap_category in self.gap_data[self.KPI_NAME].tolist():
            for i, row in self.gaps[self.gaps[self.KPI_NAME].str.encode('utf-8') ==
                                    gap_category.encode('utf-8')].iterrows():
                if not priorities:
                    break
                kpi_atomic_name = row[self.KPI_ATOMIC_NAME]
                attributes = pd.DataFrame([(self.session_fk, gap_category, kpi_atomic_name, priorities.pop(0))],
                                          columns=['session_fk', 'gap_category', 'name', 'priority'])
                query = insert(attributes.to_dict(), CUSTOM_GAPS_TABLE)
                self.gaps_queries.append(query)
        self._saving_gaps_to_the_new_tables()

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
        row = self.kpi_weights[(self.kpi_weights[self.KPI_SET].str.encode('utf-8') == kpi_set.encode('utf-8')) &
                               (self.kpi_weights[self.KPI_NAME].str.encode('utf-8') == kpi.encode('utf-8'))]
        weight = row.get(self.WEIGHT)
        if not weight.empty:
            return weight.values[0]
        else:
            return 0

    def validate_atomic_kpi(self, **params):
        if params.get(self.SPLIT_SCORE, 0) and not params['filters']['All'].get('scene_id'):
            return False
        return True

    def get_relevant_template(self):
        """
        This function returns the relevant template according to it's visit date.
        Because of a change that was done in the logic there are 3 templates that match different dates.
        :return: Full template path
        """
        if self.visit_date <= datetime.date(datetime(2019, 1, 15)):
            return "{}/{}".format(TEMPLATE_PATH, TEMPLATE_NAME_UNTIL_2019_01_15)
        elif self.visit_date <= datetime.date(datetime(2019, 1, 3)):
            return "{}/{}".format(TEMPLATE_PATH, TEMPLATE_NAME_BETWEEN_2019_01_15_TO_2019_03_01)
        else:
            return "{}/{}".format(TEMPLATE_PATH, CURRENT_TEMPLATE)

    def sort_by_priority(self, gap_dict):
        """ This is a util function for the kpi's gaps sorting by priorities.
        At the moment score will be equal to 0 but we added this option in order support partial score as well. """
        return gap_dict[self.GAPS], gap_dict[Consts.SCORE]

    def _validate_gap(self, gap):
        """
        This method validate that a gap has all of the relevant attributes.
        :param gap: A dictionary with relevant attribute to save.
        :return: True in case of a valid gap, False otherwise.
        """
        required_values = [self.WEIGHT, Consts.LEVEL_2_FK]
        if not set(required_values).issubset(gap.keys()):
            return False
        for key in required_values:
            if not gap[key]:
                return False
        return True

    def _saving_gaps_to_the_new_tables(self):
        """ This function takes the top 5 gaps (by priority) and saves it to the DB """
        self.gaps = self.gaps.to_dict('records')
        self.gaps.sort(key=self.sort_by_priority)
        gaps_total_score = 0
        gaps_per_kpi_fk = self.common.get_kpi_fk_by_kpi_type(Consts.GAP_PER_ATOMIC_KPI)
        gaps_total_score_kpi_fk = self.common.get_kpi_fk_by_kpi_type(Consts.GAPS_TOTAL_SCORE_KPI)
        for gap in self.gaps[:5]:
            if not self._validate_gap(gap):
                continue
            current_gap_score = gap[self.WEIGHT] - (gap[Consts.SCORE] * gap[self.WEIGHT])
            gaps_total_score += current_gap_score
            self._insert_gap_results(gaps_per_kpi_fk, current_gap_score, gap[self.WEIGHT],
                                     numerator_id=gap[Consts.LEVEL_2_FK], parent_fk=gaps_total_score_kpi_fk)
        total_weight = sum(map(lambda res: res[self.WEIGHT], self.gaps[:5]))
        self._insert_gap_results(gaps_total_score_kpi_fk, gaps_total_score, total_weight, self.cbcil_id)

    def _insert_gap_results(self, gap_kpi_fk, score, weight, numerator_id, parent_fk=None):
        """ This is a utility function that insert results to the DB for the GAPs KPIs """
        should_enter = True if parent_fk else False
        score, weight = round(score * 100, 2), round(weight * 100, 2)
        self.common.write_to_db_result(fk=gap_kpi_fk, numerator_id=numerator_id, numerator_result=score,
                                       denominator_id=self.store_id, denominator_result=weight, weight=weight,
                                       identifier_result=gap_kpi_fk, identifier_parent=parent_fk, result=score,
                                       score=score, should_enter=should_enter)
