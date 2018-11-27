
import os
import pandas as pd
from datetime import datetime

from Trax.Algo.Calculations.Core.DataProvider import Data
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
from Trax.Cloud.Services.Connector.Keys import DbUsers
from KPIUtils_v2.DB.PsProjectConnector import PSProjectConnector
from Trax.Utils.Logging.Logger import Log
from Trax.Data.Utils.MySQLservices import get_table_insertion_query as insert

from Projects.CCTH_UAT.Utils.Fetcher import CCTH_UATQueries
from Projects.CCTH_UAT.Utils.GeneralToolBox import CCTH_UATGENERALToolBox
from Projects.CCTH_UAT.Utils.ParseTemplates import CCTH_UATParseTemplates, TEMPLATE_PATH

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


class CCTH_UATConsts(object):

    BINARY = 'BINARY'
    PROPORTIONAL = 'PROPORTIONAL'
    AVERAGE = 'NORMALIZED AVERAGE'

    FLEXIBLE = 'FLEXIBLE'
    STRICT = 'STRICT'

    RED_SCORE = 'RED SCORE'
    MULTIPLE_SURVEYS = 'Multiple Surveys'
    SURVEY = 'Survey Question'
    SHARE_OF_SHELF = 'SOS Facings'
    BPPC = 'BPPC'
    SURVEY_AND_AVAILABILITY = BPPC


class CCTH_UATToolBox(CCTH_UATConsts):

    LEVEL1 = 1
    LEVEL2 = 2
    LEVEL3 = 3

    FLEXIBLE_MODE = 'Flexible Mode'
    STRICT_MODE = 'Strict Mode'

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
        self.tools = CCTH_UATGENERALToolBox(self.data_provider, self.output)
        if '7-11' in self.store_type:
            if self.visit_date >= datetime(2017, 7, 1).date():
                template_name = CCTH_UATParseTemplates.TEMPLATE_7_11_AFTER_JULY2017
            else:
                template_name = CCTH_UATParseTemplates.TEMPLATE_7_11_UNTIL_JULY2017
            self.template = CCTH_UATParseTemplates(template=template_name)
            self.calculation_type = template_name
            self.availability_id = self.gap_id = self.store_type
        else:
            if self.visit_date >= datetime(2017, 10, 25).date():
                template_name = CCTH_UATParseTemplates.TEMPLATE_TT_AFTER_NOV2017
                self.template = CCTH_UATParseTemplates(template=template_name)
                self.calculation_type = self.template.TEMPLATE_TT_AFTER_NOV2017

            else:
                template_name = CCTH_UATParseTemplates.TEMPLATE_TT_UNTIL_NOV2017
                self.template = CCTH_UATParseTemplates(template=template_name)
                self.calculation_type = self.template.TEMPLATE_TT_UNTIL_NOV2017
            self.availability_id = self.gap_id = '{};{}'.format(self.region, self.store_type)
            self.survey_id = '{};{};{}'.format(self.region, self.store_type, self.segmentation)
        self.templates_data = self.template.parse_kpi()
        self.availability_data = self.template.parse_availability()
        self.survey_data = self.template.parse_survey()
        self.gap_data = self.template.parse_gap()
        self.gap_translations = pd.read_excel(os.path.join(TEMPLATE_PATH, 'Gaps.xlsx'))
        self.kpi_results_queries = []
        self.gaps = {}
        self.gaps_queries = []

    def get_segmentation_and_region(self):
        """
        This function extracts missing attributes of the visit's store.
        """
        query = CCTH_UATQueries.get_segmentation_and_region_data(self.store_id)
        data = pd.read_sql_query(query, self.rds_conn.db).iloc[0]
        return data.segmentation, data.region

    def get_kpi_static_data(self):
        """
        This function extracts the static KPI data and saves it into one global data frame.
        The data is taken from static.kpi / static.atomic_kpi / static.kpi_set.
        """
        query = CCTH_UATQueries.get_all_kpi_data(self.RED_SCORE)
        kpi_static_data = pd.read_sql_query(query, self.rds_conn.db)
        return kpi_static_data

    def add_gap(self, params):
        """
        This function extracts a failed KPI's priority, and saves it in a general dictionary.
        """
        kpi_set = params[self.template.KPI_GROUP]
        kpi_name = params[self.template.KPI_NAME]
        if kpi_set == self.BPPC:
            gap = self.gap_data[self.gap_data[self.template.gap_consts.KPI_NAME] == kpi_name]
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
            for i in xrange(len(children)):
                child = children.iloc[i]
                if self.validate_kpi_run(child):
                    kpi_type = child[self.template.KPI_TYPE]
                    if kpi_type == self.MULTIPLE_SURVEYS:
                        if child[self.template.SURVEY_MODE] == self.FLEXIBLE:
                            mode = self.FLEXIBLE_MODE
                        else:
                            mode = self.STRICT_MODE
                        score = self.calculate_multiple_surveys(child, mode=mode)
                    elif kpi_type == self.SURVEY:
                        score = self.calculate_survey(child)
                    elif kpi_type == self.SHARE_OF_SHELF:
                        score = self.calculate_share_of_shelf(child)
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
                        if score == 0:
                            self.add_gap(child)
            if not scores:
                scores = [0]
                if self.template.EXECUTION_CONDITION in main_child.keys() and main_child[self.template.EXECUTION_CONDITION]:
                    scores = None
            if scores:
                score_type = main_child[self.template.SCORE]
                score_weight = main_child[self.template.WEIGHT]
                pass_percentage = (scores.count(100) / float(len(scores))) * 100
                if score_type == self.BINARY:
                    target = main_child[self.template.TARGET] * 100
                    score = 100 if pass_percentage >= target else 0
                else:
                    score = pass_percentage
                kpi_name = main_child[self.template.KPI_NAME]
                kpi_fk = self.kpi_static_data[self.kpi_static_data['kpi_name'] == kpi_name]['kpi_fk'].values[0]
                # self.write_to_db_result(kpi_fk, score, level=self.LEVEL2)
                set_scores[kpi_fk] = (score_weight, score)
        total_weight = sum([score[0] for score in set_scores.values()])
        for kpi_fk in set_scores.keys():
            normalized_weight = round((set_scores[kpi_fk][0] / total_weight) * 100, 2)
            self.write_to_db_result(kpi_fk, (set_scores[kpi_fk][1], normalized_weight), level=self.LEVEL2)
        red_score = sum([score[0] * score[1] for score in set_scores.values()]) / total_weight
        set_fk = self.kpi_static_data[self.kpi_static_data['kpi_set_name'] == self.RED_SCORE]['kpi_set_fk'].values[0]
        self.write_to_db_result(set_fk, red_score, level=self.LEVEL1)

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
        if self.calculation_type == self.template.TEMPLATE_TT_UNTIL_NOV2017:
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
        availability = self.tools.calculate_availability(product_ean_code=products, additional_attribute_1=scene_types)
        availability_result = 100 if availability >= availability_target else 0

        if survey_result and availability_result:
            score = 100
        else:
            score = 0
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
                number_of_scenes = self.tools.calculate_number_of_scenes(template_name=(scenes_group,
                                                                                        self.tools.CONTAIN_FILTER))
                number_of_scenes_to_exclude = self.tools.calculate_number_of_scenes(template_name='KO Cooler Other')
                if (number_of_scenes - number_of_scenes_to_exclude) > 0:
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

    def get_atomic_fk(self, params):
        """
        This function gets an Atomic KPI's FK out of the template data.
        """
        atomic_name = params[self.template.KPI_NAME]
        kpi_name = params[self.template.KPI_GROUP]
        atomic_fk = self.kpi_static_data[(self.kpi_static_data['kpi_name'] == kpi_name) &
                                         (self.kpi_static_data['atomic_kpi_name'] == atomic_name)]['atomic_kpi_fk']
        if atomic_fk.empty:
            return None
        return atomic_fk.values[0]

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
        cur = self.rds_conn.db.cursor()
        delete_queries = CCTH_UATQueries.get_delete_session_results_query(self.session_uid, self.session_fk)
        for query in delete_queries:
            cur.execute(query)
        for query in self.kpi_results_queries:
            cur.execute(query)
        for query in self.gaps_queries:
            cur.execute(query)
        self.rds_conn.db.commit()
