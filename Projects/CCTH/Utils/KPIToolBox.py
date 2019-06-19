# coding=utf-8
import os
import pandas as pd
from datetime import datetime

from Trax.Algo.Calculations.Core.DataProvider import Data
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
from Trax.Cloud.Services.Connector.Keys import DbUsers
from KPIUtils_v2.DB.PsProjectConnector import PSProjectConnector
from Trax.Utils.Logging.Logger import Log
from Trax.Data.Utils.MySQLservices import get_table_insertion_query as insert

from Projects.CCTH.Utils.Fetcher import CCTHQueries
from Projects.CCTH.Utils.GeneralToolBox import CCTHGENERALToolBox
from Projects.CCTH.Utils.ParseTemplates import CCTHParseTemplates as ParseTemplates, TEMPLATE_PATH

# from Projects.CCTH.Utils.Report import Report

__author__ = 'Nimrod'

KPI_RESULT = 'report.kpi_results'
KPK_RESULT = 'report.kpk_results'
KPS_RESULT = 'report.kps_results'

MAX_PRIORITY = 5
CUSTOM_GAPS_TABLE = 'pservice.custom_gaps'


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


class CCTHConsts(object):
    BINARY = 'BINARY'
    PROPORTIONAL = 'PROPORTIONAL'
    BY_WEIGHT = 'BY WEIGHT'
    AVERAGE = 'NORMALIZED AVERAGE'

    FLEXIBLE = 'FLEXIBLE'
    STRICT = 'STRICT'
    COMBINED = 'COMBINED'
    ONE_PER_GROUP = 'ONE_PER_GROUP'
    SINGLE_YES = 'SINGLE_YES'
    ONE_COMBINED = 'ONE_COMBINED'

    RED_SCORE = 'RED SCORE'
    REPORT = 'REPORT'
    MULTIPLE_SURVEYS = 'Multiple Surveys'
    COMPLEX_SURVEYS = 'Complex Surveys'

    SURVEY = 'Survey Question'
    SHARE_OF_SHELF = 'SOS Facings'
    SHARE_OF_SHELF_VAR_SCENE_TYPE = 'SOS Facings variant scene type'
    SHARE_OF_SHELF_BY_SCENE_AND_FACING = 'SOS Facings by scene'

    STATIC = 'Static_data'
    BPPC = 'BPPC'
    BPPC_SPARKLING = 'BPPC_Sparkling'
    BPPC_STILL = 'BPPC_Still'

    COOLER = 'COOLER'
    RACK = 'RACK'
    SOVI = 'SOVI'
    TRADER = 'Trader Cooler'
    SUMMARY = 'summary'
    OTHER_COOLERS = 'Other Coolers'
    BRAND_BLOCK = 'Brand Block'

    SURVEY_AND_AVAILABILITY = BPPC
    SOVI_SHEET = 'SOVI_PARAMS'
    SURVEY_SHEET = 'Survey_questions'
    BONUS = 'BONUS'


class CCTHToolBox(CCTHConsts):
    LEVEL1 = 1
    LEVEL2 = 2
    LEVEL3 = 3

    FLEXIBLE_MODE = 'Flexible Mode'
    STRICT_MODE = 'Strict Mode'

    YES = ['ใช่', 'ผ่าน']
    NO = ['ไม่ใช่', 'ไม่ผ่าน']
    NOT_APPLICABLE = ['ไม่มีสินค้า']

    EXCLUDE_EMPTY = False
    INCLUDE_EMPTY = True

    def __init__(self, data_provider, output):
        self.k_engine = BaseCalculationsScript(data_provider, output)
        self.output = output
        self.data_provider = data_provider
        self.project_name = self.data_provider.project_name
        self.session_uid = self.data_provider.session_uid
        self.products = self.data_provider[Data.PRODUCTS]
        self.all_products = self.data_provider[Data.ALL_PRODUCTS]
        self.match_product_in_scene = self.data_provider[Data.MATCHES]
        self.visit_date = self.data_provider[Data.VISIT_DATE]
        if self.visit_date >= datetime(2018, 01, 01).date():
            self.RED_SCORE = 'RED SCORE 2018'
        self.session_info = self.data_provider[Data.SESSION_INFO]
        self.session_fk = self.session_info['pk'][0]
        self.store_id = self.data_provider[Data.STORE_FK]
        self.store_info = self.data_provider[Data.STORE_INFO]
        self.store_type = self.store_info['store_type'].iloc[0]
        self.store_type = '' if self.store_type is None else self.store_type
        self.scif = self.data_provider[Data.SCENE_ITEM_FACTS]
        self.survey_response = self.data_provider[Data.SURVEY_RESPONSES]
        self.rds_conn = PSProjectConnector(self.project_name, DbUsers.CalculationEng)
        self.segmentation, self.region = self.get_segmentation_and_region()
        self.kpi_static_data = self.get_kpi_static_data()
        self.tools = CCTHGENERALToolBox(self.data_provider, self.output)
        if '7-11' in self.store_type:
            if self.visit_date >= datetime(2018, 02, 01).date():
                template_name = ParseTemplates.TEMPLATE_7_11_AFTER_FEB2018
            elif self.visit_date >= datetime(2018, 01, 01).date():
                template_name = ParseTemplates.TEMPLATE_7_11
            else:
                template_name = ParseTemplates.TEMPLATE_7_11_AFTER_JULY2017
            self.template = ParseTemplates(template=template_name)
            self.calculation_type = template_name
            self.availability_id = self.gap_id = self.store_type
        else:
            if self.visit_date >= datetime(2018, 01, 01).date():
                if self.visit_date >= datetime(2018, 02, 01).date():
                    template_name = ParseTemplates.TEMPLATE_TT_AFTER_FEB2018
                else:
                    template_name = ParseTemplates.TEMPLATE_TT
                self.template = ParseTemplates(template=template_name)
                self.calculation_type = self.template.TEMPLATE_TT
                self.survey_id = '{};{};{}'.format('All Regions', self.store_type, self.segmentation)
                self.availability_id = self.gap_id = '{};{}'.format('All Regions', self.store_type)
                self.survey_questions = self.template.parse_sheet(self.SURVEY_SHEET)
            else:
                template_name = ParseTemplates.TEMPLATE_TT_AFTER_NOV2017
                self.template = ParseTemplates(template=template_name)
                self.calculation_type = self.template.TEMPLATE_TT_AFTER_NOV2017
                self.survey_id = '{};{};{}'.format(self.region, self.store_type, self.segmentation)
                self.availability_id = self.gap_id = '{};{}'.format(self.region, self.store_type)
        self.templates_data = self.template.parse_kpi()
        self.availability_data = self.template.parse_availability()
        self.survey_data = self.template.parse_survey()
        self.gap_data = self.template.parse_gap()
        self.gap_translations = pd.read_excel(os.path.join(TEMPLATE_PATH, 'Gaps.xlsx'))
        self.kpi_results_queries = []
        self.gaps = {}
        self.gaps_queries = []
        self.bppc_scores = {}
        self.sovi_scores = {}
        self.summary = {}

    def get_segmentation_and_region(self):
        """
        This function extracts missing attributes of the visit's store.
        """
        query = CCTHQueries.get_segmentation_and_region_data(self.store_id)
        data = pd.read_sql_query(query, self.rds_conn.db).iloc[0]
        return data.segmentation, data.region

    def get_store_data(self):
        query = CCTHQueries.get_store_data(self.store_id)
        kpi_static_data = pd.read_sql_query(query, self.rds_conn.db).iloc[0]
        return kpi_static_data

    def get_kpi_static_data(self):
        """
        This function extracts the static KPI data and saves it into one global data frame.
        The data is taken from static.kpi / static.atomic_kpi / static.kpi_set.
        """
        query = CCTHQueries.get_all_kpi_data(self.RED_SCORE)
        kpi_static_data = pd.read_sql_query(query, self.rds_conn.db)
        return kpi_static_data

    def get_kpi_set_fk(self, kpi_name):
        """
        This function extracts the static KPI data and saves it into one global data frame.
        The data is taken from static.kpi / static.atomic_kpi / static.kpi_set.
        """
        query = CCTHQueries.get_kpi_fk(kpi_name)
        kpi_static_data = pd.read_sql_query(query, self.rds_conn.db)
        return kpi_static_data

    def add_gap(self, params):
        """
        This function extracts a failed KPI's priority, and saves it in a general dictionary.
        """
        kpi_set = params[self.template.KPI_GROUP]
        kpi_name = params[self.template.KPI_NAME]
        if kpi_set in [self.BPPC, self.BPPC_SPARKLING, self.BPPC_STILL]:
            gap = self.gap_data[self.gap_data[self.template.gap_consts.KPI_NAME] == kpi_name]
            kpi_set = self.BPPC
            if not gap.empty:
                gap = gap.iloc[0][self.gap_id]
            else:
                Log.warning("BCCP gaps doesn't include KPI '{}'".format(kpi_name))
                gap = None
        else:
            gap = params[self.template.GAP_PRIORITY]
        if not gap:
            gap = None
        elif isinstance(gap, float):
            gap = int(gap)
        elif isinstance(gap, (str, unicode)):
            gap = None if not gap.isdigit() else int(gap)
        if gap is not None:
            if kpi_set not in self.gaps.keys():
                self.gaps[kpi_set] = {}
            self.gaps[kpi_set][gap] = kpi_name

    def calculate_trader(self):
        survey_q = self.survey_questions
        trader = ['Thainamthip product at least 1 door', 'Minimum 2 Still Shelf',
                  'Price Tags CSD', 'Price Tags Still']
        trader_results = pd.DataFrame(columns=trader)
        trader_questions_ids = \
            {trader[0]:
                 survey_q[survey_q['KPI'] == 'Thainamthip product 1 door']['Survey Q ID'],
             trader[1]:
                 survey_q[survey_q['KPI'] == 'Minimum 2 Still Shelf']['Survey Q ID'],
             trader[2]:
                 survey_q[(survey_q['KPI'] == 'Price Tag') &
                          (survey_q['KPI Group'] == 'Trader Cooler') &
                          (survey_q['Question Type'] == 'csd')]['Survey Q ID'],
             trader[3]:
                 survey_q[(survey_q['KPI'] == 'Price Tag') &
                          (survey_q['KPI Group'] == 'Trader Cooler') &
                          (survey_q['Question Type'] == 'still')]['Survey Q ID']}
        survey_a = self.survey_response
        result = {}
        for column in trader_results.columns.tolist():
            result[column] = self.build_result_from_answers(trader_questions_ids[column], survey_a)
        trader_results = trader_results.append(pd.Series(result), ignore_index=True)
        return trader_results

    def calculate_cooler_rack(self, types, kpi_name):
        survey_q = self.survey_questions
        cooler_rack = ['name', 'count', 'Purity', 'SOVI Cola>=50%', 'Minimum 8 Still Facings', 'Price Tags CSD',
                       'Price Tags Still', 'First Position', 'Hotspots', 'Twinning']
        cooler_rack_results = pd.DataFrame(columns=cooler_rack)
        cooler_rack_questions_ids = \
            {cooler_rack[2]:
                 survey_q[survey_q['KPI'] == 'Purity']['Survey Q ID'],
             cooler_rack[5]:
                 survey_q[(survey_q['KPI'] == 'Price Tag') &
                          (survey_q['KPI Group'].isin(['COOLER', 'RACK'])) &
                          (survey_q['Question Type'] == 'csd')]['Survey Q ID'],
             cooler_rack[6]:
                 survey_q[(survey_q['KPI'] == 'Price Tag') &
                          (survey_q['KPI Group'].isin(['COOLER', 'RACK'])) &
                          (survey_q['Question Type'] == 'still')]['Survey Q ID'],
             cooler_rack[7]:
                 survey_q[(survey_q['KPI'] == '1st Position/Hot Spot') &
                          (survey_q['Question'].str.contains('pass the 1st position?'))]['Survey Q ID'],
             cooler_rack[8]:
                 survey_q[(survey_q['KPI'] == '1st Position/Hot Spot') &
                          (survey_q['Question'].str.contains('pass the hot spot?'))]['Survey Q ID'],
             cooler_rack[9]:
                 survey_q[survey_q['KPI'] == 'Twinning']['Survey Q ID']
             }

        survey_a = self.survey_response
        result = {}
        total = {'name': 'Total'}
        for type in types.split(','):
            sovi = self.sovi_scores.get(type)
            type_to_filter = 'PM' if type == 'KO PM Rack' else type
            groups = survey_a[(survey_a['selected_option_text'] is not None) & (
                     survey_a['selected_option_text'].str.upper().str.contains(type_to_filter.upper()))]['group_num'].unique()
            answers = survey_a[survey_a['group_num'].isin(groups)][['question_fk', 'selected_option_text']]
            for column in cooler_rack_results.columns.tolist():
                if column == 'name':
                    result[column] = type
                elif column == 'count':
                    result[column] = len(groups) if len(groups) else None
                    if result[column]: total[column] = (total[column] if total.get(column) else 0) + int(result[column])
                elif column == 'SOVI Cola>=50%':
                    result[column] = sovi[0] if sovi else None
                    if result[column]: total[column] = (str(total[column]) if total.get(column) else '') + ' ' + str(
                        result[column])
                elif column == 'Minimum 8 Still Facings':
                    result[column] = sovi[1] if sovi else None
                    if result[column]: total[column] = (str(total[column]) if total.get(column) else '') + ' ' + str(
                        result[column])
                else:
                    result[column] = self.build_result_from_answers(cooler_rack_questions_ids[column], answers)
                    if result[column]: total[column] = (str(total[column]) if total.get(column) else '') + ' ' + str(
                        result[column])
            cooler_rack_results = cooler_rack_results.append(pd.Series(result), ignore_index=True)
        cooler_rack_results = cooler_rack_results.append(pd.Series(total), ignore_index=True)

        if kpi_name == 'RACK':
            del cooler_rack_results['Minimum 8 Still Facings']
        return cooler_rack_results

    def calculate_bppc(self, types):
        bppc = ['name', 'score']
        bppc_results = pd.DataFrame(columns=bppc)
        for type in types.split(','):
            score = self.bppc_scores.get(type)
            if score:
                score /= 100
            result = {'name': type, 'score': score}
            bppc_results = bppc_results.append(pd.Series(result), ignore_index=True)
        return bppc_results

    def build_cooler_rack_and_save(self, order, kpi_name, scene_type, kpi_set_fk):
        results = self.calculate_cooler_rack(scene_type, kpi_name)
        results.loc[results['name'] == 'Total', 'name'] = 'Total KO Cooler' if kpi_name == 'COOLER' else 'Total KO Rack'
        for i, result in results.iterrows():
            for column in results:
                if column != 'name':
                    score = result[column]
                    if column == 'count':
                        atomic = ['# of ' + result[0], score]
                    else:
                        atomic = [result[0] + ' - ' + str(column), score]
                    self.write_to_db_result_report(kpi_set_fk, atomic[1], atomic[0], order)
                    order += 1

    def build_summary_and_save(self, order, kpi_name, result, kpi_set_fk):
        score = self.summary[result][1] if self.summary[result][1] else None
        self.write_to_db_result_report(kpi_set_fk, score, kpi_name + ' Targeted', order)
        if score:
            score = self.summary[result][0] if self.summary[result] else None
        self.write_to_db_result_report(kpi_set_fk, score, kpi_name + ' Passed', order + 1)
        if score is not None:
            score = str("%.2f" % self.summary[result][2]) if self.summary[result] else None
        self.write_to_db_result_report(kpi_set_fk, score, '% ' + kpi_name, order + 100)

    def build_static_data_and_save(self, order, kpi_set_fk, kpi_names):
        store_data = self.get_store_data()
        match = {'Store Id': store_data['store_number_1'], 'Visit Date': str(self.visit_date),
                 'Store Name': store_data['name'], 'Channel': store_data['additional_attribute_15'],
                 'Store Type': store_data['store_type'], 'Segment': store_data['additional_attribute_2'],
                 'SD': store_data['additional_attribute_13'], 'Sales Manager': store_data['additional_attribute_12'],
                 'Sale office': store_data['additional_attribute_7'], 'Route Manager': store_data['manager_name'],
                 'Presell': store_data['sales_rep_name'], 'AD': store_data['additional_attribute_11'],
                 'Hybrid': store_data['additional_attribute_10'], 'VRP': store_data['additional_attribute_9'],
                 'Operation Manager': store_data['additional_attribute_12'],
                 'Customer Developer Supervisor': store_data['manager_name'],
                 'Customer Developer': store_data['sales_rep_name']}
        for kpi_name in kpi_names:
            try:
                value = match[kpi_name].replace("'","") if match[kpi_name] else ''
                self.write_to_db_result_report(kpi_set_fk, value, kpi_name, order)
            except:
                pass
            order += 1

    def calculate_report(self):
        childrens = self.templates_data[self.templates_data[self.template.KPI_GROUP] == self.REPORT]
        kpi_set_fk = self.get_kpi_set_fk(self.REPORT)
        for c in xrange(len(childrens)):
            child = childrens.iloc[c]
            kpi = child[self.template.KPI_NAME]
            if kpi == self.STATIC:
                self.build_static_data_and_save(1, kpi_set_fk, child[self.template.SCENE_TYPE].split(','))
            if kpi == self.BPPC:
                results = self.calculate_bppc(child['Scene Type'])
                for i, result in results.iterrows():
                    self.write_to_db_result_report(kpi_set_fk, result[1], result[0], i + 100)
            elif kpi == self.COOLER:
                self.build_cooler_rack_and_save(200, kpi, child['Scene Type'], kpi_set_fk)
            elif kpi == self.OTHER_COOLERS:
                order = 300
                params = child['Scene Type'].split(',')
                for param in params:
                    kpi_name, scene_type = param.split(':')
                    cooler_count = len(self.scif[self.scif['template_name'] == scene_type]['scene_id'].unique())
                    self.write_to_db_result_report(kpi_set_fk, cooler_count, kpi_name, order + 1)
                    order += 1
            elif kpi == self.RACK:
                self.build_cooler_rack_and_save(400, kpi, child['Scene Type'], kpi_set_fk)
            elif kpi == self.SOVI:
                sovi = self.sovi_scores
                if sovi['SOVI Cola'][0] is not None:
                    cola_result = "%.2f" % sovi['SOVI Cola'][0]
                else:
                    cola_result = None
                if sovi['SOVI Sparkling'][0] is not None:
                    sparkling_result = "%.2f" % sovi['SOVI Sparkling'][0]
                else:
                    sparkling_result = None
                self.write_to_db_result_report(kpi_set_fk, cola_result, 'SOVI Cola', 501)
                self.write_to_db_result_report(kpi_set_fk, sparkling_result, 'SOVI Sparkling', 502)
            elif kpi == self.TRADER:
                order = 600
                results = self.calculate_trader()
                for column in results.columns:
                    self.write_to_db_result_report(kpi_set_fk, results.loc[0, column], column, order + 1)
                    order += 1
            elif kpi == self.BRAND_BLOCK:
                result = self.build_result_from_answers([child['Scene Type']], self.survey_response)
                self.write_to_db_result_report(kpi_set_fk, result, kpi, 600)
            elif kpi == self.SUMMARY:
                for result in self.summary:
                    # TT
                    if result == 'BPPC':
                        self.build_summary_and_save(700, 'BPPC', result, kpi_set_fk)
                    if result == 'COOLER':
                        self.build_summary_and_save(702, 'Cooler', result, kpi_set_fk)
                    if result == 'RACK':
                        self.build_summary_and_save(704, 'Rack', result, kpi_set_fk)
                    if result == 'SOVI':
                        self.build_summary_and_save(706, 'SOVI', result, kpi_set_fk)
                    if result == 'Trader Cooler':
                        self.build_summary_and_save(708, 'Trader Cooler', result, kpi_set_fk)

                    # 7-11
                    if result == 'BPPC_Sparkling':
                        self.build_summary_and_save(700, 'BPPC Sparkling', result, kpi_set_fk)
                    if result == 'BPPC_Still':
                        self.build_summary_and_save(702, 'BPPC Still', result, kpi_set_fk)
                    if result == 'SOVI Cola':
                        self.build_summary_and_save(704, 'SOVI Cola', result, kpi_set_fk)
                    if result == 'SOVI Sparkling':
                        self.build_summary_and_save(706, 'SOVI Sparkling', result, kpi_set_fk)
                    if result == 'BRAND BLOCK':
                        self.build_summary_and_save(708, 'BRAND BLOCK', result, kpi_set_fk)

                    if result == 'RED SCORE':
                        self.write_to_db_result_report(kpi_set_fk, "%.2f" % self.summary[result][0], 'RED Score Achieved', 900)
                        self.write_to_db_result_report(kpi_set_fk, "%.2f" % self.summary[result][1], 'RED Score Targeted', 901)
                        self.write_to_db_result_report(kpi_set_fk, "%.2f" % self.summary[result][2], 'RED Score (Outlet)', 902)
                        if '7-11' not in self.store_type:
                            self.write_to_db_result_report(kpi_set_fk, self.summary[result][3],
                                                           'RED 80 - 100 = A 60 - 79.99 = B 0 - 59.99 = C', 903)
            else:
                Log.warning("KPI of type '{}' is not supported".format(kpi))
                continue

    def build_atomic_and_save(self, result, presentation_order, kpi_set_fk):
        self.write_to_db_result_report(kpi_set_fk, result[1], result[0], presentation_order)

    def build_result_from_answers(self, question_ids, answers):
        if answers.empty:
            return None
        result = ''
        thi_answers = answers[answers['question_fk'].isin(question_ids)]['selected_option_text'].tolist()
        for thi_answer in thi_answers:
            if thi_answer.encode('utf-8') in self.YES:
                result += ' Yes'
            elif thi_answer.encode('utf-8') in self.NO:
                result += ' No'
            elif thi_answer.encode('utf-8') in self.NOT_APPLICABLE:
                result += ' Not Applicable'
        result = result.strip()
        if result:
            return result
        else:
            return None

    def calculate_red_score(self):
        """
        This function calculates the KPI results.
        """
        set_scores = {}
        main_children = self.templates_data[self.templates_data[self.template.KPI_GROUP] == self.RED_SCORE]
        for c in xrange(len(main_children)):
            main_child = main_children.iloc[c]
            children = self.templates_data[self.templates_data[self.template.KPI_GROUP] ==
                                           main_child[self.template.KPI_NAME]]
            scores = []
            scores_weights = []
            validate_main_child = self.validate_kpi_run(main_child)
            for i in xrange(len(children)):
                child = children.iloc[i]
                if self.validate_kpi_run(child):
                    kpi_type = child[self.template.KPI_TYPE]
                    if not validate_main_child:
                        score = 0
                    elif kpi_type == self.MULTIPLE_SURVEYS:
                        if child[self.template.SURVEY_MODE] == self.FLEXIBLE:
                            mode = self.FLEXIBLE_MODE
                        else:
                            mode = self.STRICT_MODE
                        score = self.calculate_multiple_surveys(child, mode=mode)
                    elif kpi_type == self.COMPLEX_SURVEYS:
                        mode = child[self.template.SURVEY_MODE]
                        score = self.calculate_complex_surveys(child, mode=mode)
                    elif kpi_type == self.SURVEY:
                        score = self.calculate_survey(child)
                    elif kpi_type == self.SHARE_OF_SHELF:
                        score = self.calculate_share_of_shelf(child)
                    elif kpi_type == self.SHARE_OF_SHELF_BY_SCENE_AND_FACING:
                        score = self.calculate_share_of_shelf_by_scene(child)
                    elif kpi_type == self.SHARE_OF_SHELF_VAR_SCENE_TYPE:
                        score = self.calculate_share_of_shelf_var_scene_type(child)
                    elif kpi_type == self.SURVEY_AND_AVAILABILITY:
                        score = self.calculate_survey_and_availability(child)
                    else:
                        Log.warning("KPI of type '{}' is not supported".format(kpi_type))
                        continue
                    if score is not None:
                        atomic_fk = self.get_atomic_fk(child)
                        self.write_to_db_result(atomic_fk, score, level=self.LEVEL3)
                        if isinstance(score, tuple):
                            score = score[0]
                        scores.append(score)
                        try:
                            score_percent = float(child[self.template.WEIGHT])
                        except:
                            score_percent = 1
                        score_percent = score * score_percent
                        scores_weights.append(score_percent)
                        if score == 0:
                            self.add_gap(child)
            if not scores:
                scores = [0]
                self.summary[main_child[self.template.KPI_NAME]] = [None, None, None]
                if self.template.EXECUTION_CONDITION in main_child.keys() and main_child[
                    self.template.EXECUTION_CONDITION]:
                    scores = None
            if scores:
                targeted_atomic_kpi = 0
                score_type = main_child[self.template.SCORE]
                score_weight = main_child[self.template.WEIGHT]
                pass_percentage = (scores.count(100) / float(len(scores))) * 100
                if score_type == self.BINARY:
                    target = main_child[self.template.TARGET] * 100
                    score = 100 if pass_percentage >= target else 0
                elif score_type == self.BY_WEIGHT:
                    targeted_atomic_kpi = main_child[self.template.TARGET]
                    score = sum(scores_weights)
                else:
                    score = pass_percentage
                targeted = targeted_atomic_kpi if targeted_atomic_kpi else len(scores)
                self.summary[main_child[self.template.KPI_NAME]] = [scores.count(100), targeted, score]
                kpi_name = main_child[self.template.KPI_NAME]
                print(kpi_name)
                kpi_fk = self.kpi_static_data[self.kpi_static_data['kpi_name'] == kpi_name]['kpi_fk'].values[0]
                set_scores[kpi_fk] = (score_weight, score)
        total_weight = sum([score[0] for score in set_scores.values()])
        for kpi_fk in set_scores.keys():
            normalized_weight = round((set_scores[kpi_fk][0] / total_weight) * 100, 2)
            self.write_to_db_result(kpi_fk, (set_scores[kpi_fk][1], normalized_weight), level=self.LEVEL2)
        self.calculate_bonus(set_scores)
        final_score = sum([score[0] * score[1] for score in set_scores.values()])
        red_score = final_score / total_weight

        red_score_result = 100 if red_score > 100 else red_score
        red_letter = 'A' if 100 >= red_score_result >= 80 else ('B' if 79.99 >= red_score_result >= 60 else 'C')
        self.summary['RED SCORE'] = [final_score, total_weight * 100, red_score_result, red_letter]
        set_fk = self.kpi_static_data[self.kpi_static_data['kpi_set_name'] == self.RED_SCORE]['kpi_set_fk'].values[0]
        self.write_to_db_result(set_fk, red_score_result, level=self.LEVEL1)

    def calculate_bonus(self, set_scores):
        main_children = self.templates_data[self.templates_data[self.template.KPI_GROUP] == self.BONUS]
        for c in xrange(len(main_children)):
            main_child = main_children.iloc[c]
            children = self.templates_data[self.templates_data[self.template.KPI_GROUP] ==
                                           main_child[self.template.KPI_NAME]]
            self.summary[main_child[self.template.KPI_NAME]] = [0, len(children) + 1, None]
            scores = 0
            for i in xrange(len(children)):
                child = children.iloc[i]
                if self.validate_kpi_run(child):
                    kpi_type = child[self.template.KPI_TYPE]
                    if kpi_type == self.COMPLEX_SURVEYS:
                        mode = child[self.template.SURVEY_MODE]
                        score = self.calculate_complex_surveys(child, mode=mode)
                    else:
                        Log.warning("KPI of type '{}' is not supported".format(kpi_type))
                        continue
                    if score is not None:
                        atomic_fk = self.get_atomic_fk(child)
                        self.write_to_db_result(atomic_fk, score, level=self.LEVEL3)
                        score_weight = child[self.template.WEIGHT]
                        if isinstance(score, tuple):
                            score = score[0]
                        scores += (score * score_weight)
                        if score == 100:
                            # for weights that are more than 1 point
                            points = int(score_weight * (len(children) + 1))

                            self.summary[main_child[self.template.KPI_NAME]][0] += points
            score_weight = main_child[self.template.WEIGHT]
            result = scores
            kpi_name = main_child[self.template.KPI_NAME]
            kpi_fk = self.kpi_static_data[self.kpi_static_data['kpi_name'] == kpi_name]['kpi_fk'].values[0]
            self.write_to_db_result(kpi_fk, (result, score_weight * 100), level=self.LEVEL2)
            self.summary[main_child[self.template.KPI_NAME]][2] = result
            set_scores[kpi_fk] = (score_weight, result)

    def validate_kpi_run(self, params):
        """
        This function checks if a KPI needs to be run, based on the visit's static data and the template's.
        """
        store_type_validation = segmentation_validation = execution_validation = True

        store_types = params[self.template.STORE_TYPE]
        if store_types:
            store_types = store_types.split(self.template.SEPARATOR)
            if self.store_type not in store_types:
                store_type_validation = False

        if self.template.SEGMENTATION in params.keys():
            segmentations = params[self.template.SEGMENTATION]
            if segmentations:
                segmentations = segmentations.split(self.template.SEPARATOR)
                if self.segmentation not in segmentations:
                    segmentation_validation = False

        if self.template.EXECUTION_CONDITION in params.keys():
            execution_conditions = params[self.template.EXECUTION_CONDITION]
            if execution_conditions:
                execution_conditions = execution_conditions.split(';')
                for condition in execution_conditions:
                    condition_type, condition_values = condition.split('=')
                    if condition_type == 'scene':
                        result = self.tools.calculate_number_of_scenes(template_name=(condition_values.split(','),
                                                                                      self.tools.CONTAIN_FILTER))
                        if result == 0:
                            execution_validation = False
                            break
                    elif condition_type == 'survey':
                        survey_ids = map(int, condition_values.split(','))
                        if self.survey_response[self.survey_response['question_fk'].isin(survey_ids)].empty:
                            execution_validation = False
                            break

        if store_type_validation and segmentation_validation and execution_validation:
            return True
        else:
            return False

    def calculate_survey_and_availability(self, params):
        """
        This function calculates BCCP Atomics (including both surveys and availability), and saves the result to the DB.
        """
        kpi_name = params[self.template.KPI_NAME]
        scene_types = params[self.template.SCENE_TYPE].split(self.template.SEPARATOR)

        # Check survey
        if self.calculation_type == self.template.TEMPLATE_TT_AFTER_NOV2017 or self.calculation_type == self.template.TEMPLATE_TT:
            survey_data = self.survey_data[(self.survey_data[self.template.survey_consts.KPI_NAME] == kpi_name) &
                                           (self.survey_data[self.survey_id] == 'Y')]
            if survey_data.empty:
                return None
            survey_data = survey_data.iloc[0]
            survey_id = int(survey_data[self.template.survey_consts.SURVEY_ID])
            target_answers = survey_data[self.template.survey_consts.SURVEY_ANSWER].split(self.template.SEPARATOR)
            survey_answer = self.tools.get_survey_answer(('question_fk', survey_id))
            if survey_answer:
                survey_result = True if survey_answer.strip() in target_answers else False
            else:
                survey_result = False
        else:
            survey_result = True

        # Check availability
        availability_data = self.availability_data[
            self.availability_data[self.template.availability_consts.KPI_NAME] == kpi_name].iloc[0]
        availability_target = availability_data[self.availability_id]
        if not isinstance(availability_target, (int, float)) and not availability_target.isdigit():
            return None
        availability_target = int(availability_target)
        products = str(availability_data[self.template.availability_consts.PRODUCT_EAN_CODES]).split(
            self.template.SEPARATOR)
        products = map(lambda x: x.strip(), products)
        availability = self.tools.calculate_availability(product_ean_code=products, additional_attribute_1=scene_types)
        availability_result = 100 if availability >= availability_target else 0

        if survey_result and availability_result:
            score = 100
        else:
            score = 0
        self.bppc_scores[kpi_name] = score
        return score, availability, availability_target

    def calculate_multiple_surveys(self, params, mode=STRICT_MODE):
        """
        This function calculates Multiple Surveys typed Atomics, and writes the result to the DB.
        - For both modes, at least one survey answer must be equal to the target's
        - STRICT_MODE - all existing answers must be equal to the target's
        - FLEXIBLE_MODE - Only one answer must be equal to the target's
        """
        mandatory_scenes = params[self.template.MANDATORY_SCENES]
        if mandatory_scenes:
            survey_groups = params[self.template.SURVEY_ID].split(self.template.SEPARATOR2)
            mandatory_scenes = mandatory_scenes.split(self.template.SEPARATOR2)
            survey_ids = []
            for index, survey_group in enumerate(survey_groups):
                scenes_group = mandatory_scenes[index].split(self.template.SEPARATOR)
                relevant_scenes = self.scif[
                    self.tools.get_filter_condition(self.scif, template_name=(scenes_group, self.tools.CONTAIN_FILTER))]
                relevant_scenes = relevant_scenes[~relevant_scenes['template_name'].isin(['KO Cooler Other'])]
                number_of_scenes = len(relevant_scenes['scene_id'].unique())
                if number_of_scenes > 0:
                    survey_ids.extend(survey_group.split(self.template.SEPARATOR))
            if not survey_ids:
                return 0
        else:
            survey_ids = params[self.template.SURVEY_ID].split(self.template.SEPARATOR)
        survey_ids = map(int, survey_ids)
        target_answers = params[self.template.SURVEY_ANSWER].split(self.template.SEPARATOR)
        score = 0
        desired_survey_answers = []
        undesired_survey_answers = []
        for survey_id in survey_ids:
            survey_answer = self.tools.get_survey_answer(('question_fk', survey_id))
            if survey_answer in target_answers:
                score = 100
                desired_survey_answers.append(survey_answer)
                if mode == self.FLEXIBLE_MODE:
                    return score, survey_answer
            elif survey_answer is not None:
                undesired_survey_answers.append(survey_answer)
                if mode == self.STRICT_MODE:
                    return 0, survey_answer
        if score == 0 and undesired_survey_answers:
            score = score, undesired_survey_answers[0]
        elif score == 100 and desired_survey_answers:
            score = score, desired_survey_answers[0]
        return score

    def calculate_complex_surveys(self, params, mode=STRICT_MODE):
        """
        This function calculates Multiple Surveys typed Atomics, and writes the result to the DB.
        - For both m196530EF-5321-41A2-8238-1D2F160BFAAFodes, at least one survey answer must be equal to the target's
        - STRICT_MODE - all existing answers must be equal to the target's
        - FLEXIBLE_MODE - Only one answer must be equal to the target's
        """
        score = 0
        survey_params = self.survey_questions[(self.survey_questions['KPI Group'] == params['KPI Group']) & (
            self.survey_questions['KPI'] == params['KPI name Eng'])]
        mandatory_scenes = params[self.template.MANDATORY_SCENES]
        if mandatory_scenes:
            scenes_group = mandatory_scenes.split(self.template.SEPARATOR)
            relevant_scenes = self.scif[
                self.tools.get_filter_condition(self.scif, template_name=(scenes_group, self.tools.CONTAIN_FILTER))]
            relevant_scenes = relevant_scenes[~relevant_scenes['template_name'].isin(['KO Cooler Other'])]
            number_of_scenes = len(relevant_scenes['scene_id'].unique())
            if number_of_scenes == 0:
                return 0
        if mode == self.STRICT:
            score = self.calculate_strict(survey_params)
        elif mode == self.COMBINED:
            score = self.calculate_combined(survey_params)
        elif mode == self.ONE_PER_GROUP:
            score = self.calculate_one_per_group(survey_params)
        elif mode == self.SINGLE_YES:
            score = self.calculate_single_yes(survey_params)
        elif mode == self.ONE_COMBINED:
            score = self.calculate_one_combined(survey_params)
        return score

    def calculate_strict(self, survey_params):
        # Purity
        score = 0
        for survey_group in survey_params['Question Group'].unique():
            for survey_id in survey_params[survey_params['Question Group'] == survey_group]['Survey Q ID']:
                survey_answer = self.tools.get_survey_answer(('question_fk', survey_id))
                if survey_answer and survey_answer.encode('utf-8') in self.YES:
                    score = 100
                elif survey_answer is not None:
                    return 0, survey_answer
        return score

    def calculate_combined(self, survey_params):
        # Price Tag
        score = 0
        for survey_group in survey_params['Question Group'].unique():
            desired_survey_answers = []
            for survey_id in survey_params[survey_params['Question Group'] == survey_group]['Survey Q ID']:
                survey_answer = self.tools.get_survey_answer(('question_fk', survey_id))
                if survey_answer and survey_answer.encode('utf-8') in self.NO:
                    return 0
                elif survey_answer is not None:
                    desired_survey_answers.append(survey_answer)
            if desired_survey_answers:
                one_yes_least = 0
                for answer in desired_survey_answers:
                    if answer.encode('utf-8') in self.YES:
                        score = 100
                        one_yes_least += 1
                if one_yes_least == 0:
                    return 0
        return score

    def calculate_one_per_group(self, survey_params):
        # 1st Position/Hot Spot
        score = 0
        for survey_group in survey_params['Question Group'].unique():
            desired_survey_answers = []
            none_answer_number = 0
            survey_q_ids = survey_params[survey_params['Question Group'] == survey_group]['Survey Q ID']
            for survey_id in survey_q_ids:
                survey_answer = self.tools.get_survey_answer(('question_fk', survey_id))
                if survey_answer and survey_answer.encode('utf-8') in self.YES:
                    desired_survey_answers.append(survey_answer)
                if not survey_answer:
                    none_answer_number += 1
            if none_answer_number < len(survey_q_ids):
                if not desired_survey_answers:
                    return 0
                else:
                    score = 100
        return score

    def calculate_single_yes(self, survey_params):
        # 1 door / minimum 2
        for survey_group in survey_params['Question Group'].unique():
            for survey_id in survey_params[survey_params['Question Group'] == survey_group]['Survey Q ID']:
                survey_answer = self.tools.get_survey_answer(('question_fk', survey_id))
                if survey_answer and survey_answer.encode('utf-8') in self.YES:
                    return 100
        return 0

    def calculate_one_combined(self, survey_params):
        # Price Tag
        for survey_group in survey_params['Question Group'].unique():
            desired_survey_answers = 0
            for survey_id in survey_params[survey_params['Question Group'] == survey_group]['Survey Q ID']:
                survey_answer = self.tools.get_survey_answer(('question_fk', survey_id))
                if survey_answer and survey_answer.encode('utf-8') in self.YES:
                    desired_survey_answers += 2
                elif survey_answer and survey_answer.encode('utf-8') in self.NOT_APPLICABLE:
                    desired_survey_answers += 1
            if desired_survey_answers >= 3:
                return 100
        return 0

    def calculate_survey(self, params):
        """
        This function calculates Survey-Question typed Atomics, and writes the result to the DB.
        """
        survey_id = int(params[self.template.SURVEY_ID])
        target_answers = params[self.template.SURVEY_ANSWER].split(self.template.SEPARATOR)
        survey_answer = self.tools.get_survey_answer(('question_fk', survey_id))
        score = 100 if survey_answer in target_answers else 0
        return score, survey_answer

    def calculate_share_of_shelf(self, params):
        """
        This function calculates Facings Share-of-Shelf typed Atomics, and writes the result to the DB.
        """
        sos_brands = params[self.template.SOS_NUMERATOR].split(self.template.SEPARATOR)
        all_brands = params[self.template.SOS_DENOMINATOR].split(self.template.SEPARATOR)
        result = self.tools.calculate_share_of_shelf({'brand_name': sos_brands}, brand_name=all_brands)
        target = params[self.template.TARGET]
        score = 100 if result >= target else 0
        return score, result * 100, target * 100

    def calculate_share_of_shelf_by_scene(self, params):
        """
        This function calculates Facings Share-of-Shelf typed Atomics, and writes the result to the DB.
        """
        sovi_params = self.template.parse_sheet(self.SOVI_SHEET)
        sovi_params = sovi_params[sovi_params['KPI'] == params[self.template.KPI_NAME]]
        target = params[self.template.TARGET]
        score = 0, None, target * 100
        facing_target = int(sovi_params['Facing Target'])
        scene_types = sovi_params['Scene Type'].iloc[0].split(',')
        if scene_types[0] != 'all':
            scene_types = self.scif[self.scif['template_name'].isin(scene_types)]['template_name'].unique().tolist()
        params_row = sovi_params[:]
        params_row.pop(self.template.SCENE_TYPE)
        check_failed_type = True
        for scene_type in scene_types:
            if scene_type == 'all':
                result_numerator, result_denominator = self.calculate_share_of_shelf_single(params=params_row.iloc[0])
                try:
                    result = float(result_numerator) / float(result_denominator)
                except:
                    result = 0
                scene_type = params[self.template.KPI_NAME]
                self.sovi_scores[scene_type] = [result * 100, 0]
                if result < target:
                    return 0, result * 100, target * 100
                else:
                    return 100, result * 100, target * 100
            else:
                number_failed_scenes = 0
                scenes = self.scif[self.scif['template_name'] == scene_type]['scene_id'].unique()
                for scene in scenes:
                    result_numerator, result_denominator = self.calculate_share_of_shelf_single(
                        params=params_row.iloc[0],
                        scene_type=scene_type,
                        scene_type_source='template_name',
                        include_empty=self.INCLUDE_EMPTY,
                        **{'scene_id': scene})
                    try:
                        result = float(result_numerator) / float(result_denominator)
                    except:
                        result = 0
                    facings = self.scif[(self.scif['manufacturer_name'] == 'Thainamthip') &
                                        (self.scif['category'] == 'Still') &
                                        (self.scif['template_name'] == scene_type) &
                                        (self.scif['scene_id'] == scene)].facings.sum()

                    scene_type = scene_type
                    sovi_scores_result = str(int(result * 100)) + '%'
                    sovi_scores_facings = str(int(facings))
                    if self.sovi_scores.get(scene_type):
                        old = self.sovi_scores[scene_type]
                        self.sovi_scores[scene_type] = [str(old[0]) + ' ' + sovi_scores_result,
                                                        str(old[1]) + ' ' + sovi_scores_facings]
                    else:
                        self.sovi_scores[scene_type] = [sovi_scores_result, sovi_scores_facings]
                    if (result < target) or (facings < facing_target):
                        score = 0, None, target * 100
                        number_failed_scenes += 1
                        check_failed_type = False
                if number_failed_scenes == 0:
                    score = 100, None, target * 100
        return score if check_failed_type else 0

    def calculate_share_of_shelf_var_scene_type(self, params):
        """
        This function calculates Facings Share-of-Shelf typed Atomics, and writes the result to the DB.
        """
        sovi_params = self.template.parse_sheet(self.SOVI_SHEET)
        sovi_params = sovi_params[sovi_params['KPI'] == params[self.template.KPI_GROUP]]

        result_numerator = result_denominator = 0
        for scene_type in xrange(len(sovi_params)):
            params_row = sovi_params.iloc[scene_type]
            result_num, result_den = self.calculate_share_of_shelf_single(params=params_row,
                                                                          scene_type=params_row[
                                                                              self.template.SCENE_TYPE])
            result_numerator += result_num
            result_denominator += result_den
        try:
            result = float(result_numerator) / float(result_denominator)
        except:
            result = 0
        self.sovi_scores[params[self.template.KPI_GROUP]] = [result * 100, 0]
        target = params[self.template.TARGET]
        score = 100 if result >= target else 0
        return score, result * 100, target * 100

    def calculate_share_of_shelf_single(self, params, scene_type_source='additional_attribute_1', scene_type=None,
                                        include_empty=EXCLUDE_EMPTY, **kargs):
        numerator = {params['Type_Numerator']: params['Numerator'].split(self.template.SEPARATOR)}
        denominator = {params['Type_Denominator']: params['Denominator'].split(self.template.SEPARATOR)}
        if scene_type is not None:
            numerator.update({scene_type_source: scene_type})
            denominator.update({scene_type_source: scene_type})
        if include_empty == self.EXCLUDE_EMPTY:
            denominator['product_type'] = (self.tools.EMPTY, self.tools.EXCLUDE_FILTER)
        result_numerator = self.scif[
            self.tools.get_filter_condition(self.scif, **dict(numerator, **kargs))].facings.sum()
        result_denominator = self.scif[
            self.tools.get_filter_condition(self.scif, **dict(denominator, **kargs))].facings.sum()
        return result_numerator, result_denominator

    def get_atomic_fk(self, params):
        """
        This function gets an Atomic KPI's FK out of the template data.
        """
        atomic_name = params[self.template.KPI_NAME]
        print('atomic_name={}'.format(atomic_name))
        kpi_name = params[self.template.KPI_GROUP]
        atomic_fk = self.kpi_static_data[(self.kpi_static_data['kpi_name'] == kpi_name) &
                                         (self.kpi_static_data['atomic_kpi_name'] == atomic_name)]['atomic_kpi_fk']
        if atomic_fk.empty:
            return None
        return atomic_fk.values[0]

    def write_to_db_result_report(self, kpi_fk, score, atomic_kpi_name, kpi_order=1):
        print ("KPI Name={}".format(atomic_kpi_name))
        attributes = self.create_attributes_dict_report(kpi_fk, score, atomic_kpi_name, kpi_order)
        table = KPI_RESULT
        query = insert(attributes, table)
        self.kpi_results_queries.append(query)

    def write_to_db_result(self, fk, score, level):
        """
        This function creates the result data frame of every KPI (atomic KPI/KPI/KPI set),
        and appends the insert SQL query into the queries' list, later to be written to the DB.
        """
        attributes = self.create_attributes_dict(fk, score, level)
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

    def create_attributes_dict_report(self, kpi_fk, score, atomic_kpi_name, order):
        result = threshold = None
        if isinstance(score, tuple):
            if len(score) == 2:
                score, result = score
            else:
                score, result, threshold = score
        if isinstance(result, (int, float)):
            result = round(result, 2)
        attributes = pd.DataFrame([(atomic_kpi_name, self.session_uid, self.REPORT, self.store_id,
                                    self.visit_date.isoformat(), datetime.utcnow().isoformat(),
                                    kpi_fk.values[0][0], threshold, score, order)],
                                  columns=['display_text', 'session_uid', 'kps_name', 'store_fk', 'visit_date',
                                           'calculation_time', 'kpi_fk', 'threshold',
                                           'result', 'atomic_kpi_presentation_order'])
        return attributes.to_dict()

    def create_attributes_dict(self, fk, score, level):
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
            score, weight = score
            kpi_name = self.kpi_static_data[self.kpi_static_data['kpi_fk'] == fk]['kpi_name'].values[0]
            attributes = pd.DataFrame([(self.session_uid, self.store_id, self.visit_date.isoformat(),
                                        fk, kpi_name, score, weight)],
                                      columns=['session_uid', 'store_fk', 'visit_date', 'kpi_fk', 'kpk_name',
                                               'score', 'score_2'])
        elif level == self.LEVEL3:
            result = threshold = None
            if isinstance(score, tuple):
                if len(score) == 2:
                    score, result = score
                else:
                    score, result, threshold = score
            if isinstance(result, (int, float)):
                result = round(result, 2)
            data = self.kpi_static_data[self.kpi_static_data['atomic_kpi_fk'] == fk]

            if data.empty:
                print ("FK value is {}".format(fk))

            atomic_kpi_name = data['atomic_kpi_name'].values[0]
            kpi_fk = data['kpi_fk'].values[0]
            kpi_set_name = self.kpi_static_data[self.kpi_static_data['atomic_kpi_fk'] == fk]['kpi_set_name'].values[0]
            attributes = pd.DataFrame([(atomic_kpi_name, self.session_uid, kpi_set_name, self.store_id,
                                        self.visit_date.isoformat(), datetime.utcnow().isoformat(),
                                        score, kpi_fk, fk, threshold, result)],
                                      columns=['display_text', 'session_uid', 'kps_name', 'store_fk', 'visit_date',
                                               'calculation_time', 'score', 'kpi_fk', 'atomic_kpi_fk', 'threshold',
                                               'result'])
        else:
            attributes = pd.DataFrame()
        return attributes.to_dict()

    def write_gaps_to_db(self):
        """
        This function translates KPI gaps into SQL queries, later to be inserted into the DB.
        """
        for gap_category in self.gaps:
            priorities = range(1, 6)
            for gap in sorted(self.gaps[gap_category].keys()):
                if not priorities:
                    break
                kpi_name = self.gaps[gap_category][gap]
                translation_data = self.gap_translations[self.gap_translations['KPI Name'] == kpi_name]
                if not translation_data.empty:
                    kpi_name = translation_data['Gap Text'].iloc[0]
                attributes = pd.DataFrame([(self.session_fk, gap_category, kpi_name, priorities.pop(0))],
                                          columns=['session_fk', 'gap_category', 'name', 'priority'])
                query = insert(attributes.to_dict(), CUSTOM_GAPS_TABLE)
                self.gaps_queries.append(query)

    @log_runtime('Saving to DB')
    def commit_results_data(self):
        """
        This function writes all KPI results to the DB, and commits the changes.
        """
        insert_queries = self.merge_insert_queries(self.kpi_results_queries)
        cur = self.rds_conn.db.cursor()
        delete_queries = CCTHQueries.get_delete_session_results_query(self.session_uid, self.session_fk)
        for query in delete_queries:
            cur.execute(query)
        for query in insert_queries:
            cur.execute(query)
        for query in self.gaps_queries:
            cur.execute(query)
        self.rds_conn.db.commit()

    def merge_insert_queries(self, insert_queries):
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
