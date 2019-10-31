import os
import pandas as pd
from datetime import datetime

from Trax.Algo.Calculations.Core.DataProvider import Data
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
from Trax.Cloud.Services.Connector.Keys import DbUsers
from KPIUtils_v2.DB.PsProjectConnector import PSProjectConnector
from Trax.Utils.Logging.Logger import Log
from Trax.Data.Utils.MySQLservices import get_table_insertion_query as insert

from Projects.CCKH_SAND.Utils.Fetcher import CCKH_SANDQueries
from Projects.CCKH_SAND.Utils.GeneralToolBox import CCKH_SANDGENERALToolBox
from KPIUtils_v2.Utils.Consts.DB import SessionResultsConsts
from KPIUtils_v2.Utils.Consts.GlobalConsts import HelperConsts
from KPIUtils_v2.DB.CommonV2 import Common as CommonV2
from KPIUtils_v2.GlobalDataProvider.PsDataProvider import PsDataProvider

__author__ = 'Nimrod'

KPI_RESULT = 'report.kpi_results'
KPK_RESULT = 'report.kpk_results'
KPS_RESULT = 'report.kps_results'

TEMPLATE_PATH = os.path.join(os.path.dirname(os.path.realpath(__file__)),
                             '..', 'Data', 'Template.xlsx')
SCENE_TYPE_FIELD = 'additional_attribute_1'


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


class CCKH_SANDConsts(object):
    BINARY = 'BINARY'
    PROPORTIONAL = 'PROPORTIONAL'

    RED_SCORE = 'RED SCORE'
    WEB_HIERARCHY = 'hierarchy'
    AVAILABILITY = 'Availability'
    SURVEY = 'Survey Question'
    SHARE_OF_SHELF = 'SOS Facings'
    NUMBER_OF_SCENES = 'Scene Count'


class CCKH_SANDTemplateConsts(object):
    SEPARATOR = ','
    ALL = 'ALL'

    STORE_TYPE = 'store_type'
    SEGMENTATION = 'Segmentation (Store.Additional_attribute_2)'
    EXECUTION_CONDITION = 'KPI Execution Condition (When no condition then Mandatory)'
    KPI_NAME = 'KPI Name'
    TRANSLATION = 'Translation'
    KPI_GROUP = 'KPI Group'
    SCENE_TYPE = 'Scene Type'
    KPI_TYPE = 'KPI Type'
    SOS_ENTITY = 'SOVI Etitiy Type'
    SOS_NUMERATOR = 'SOVI Numerator'
    SOS_DENOMINATOR = 'SOVI Denominator'
    PRODUCT_TYPES_TO_EXCLUDE = 'Exclude Product_Type'
    SURVEY_ID = 'Survey Q ID'
    SURVEY_ANSWER = 'Accepted Survey Answer'
    TESTED_KPI_GRUOP = 'Tested KPI Group'
    TARGET = 'Target'
    SCORE = 'SCORE'
    WEIGHT = 'WEIGHT'
    GAP_PRIORITY = 'GAP PRIORITY'
    TEMPLATE_OPERATION = 'operation_type'
    PRODUCTS = 'Products EANs'
    BASIC_SHEET = 'KPIs'
    AVAILABILITY_SHEET = 'AVAILABILITY'
    VISIBILITY_SHEET = 'VISIBILITY'
    COOLER_SHEET = 'COOLER'


class CCKH_SANDToolBox(CCKH_SANDConsts):
    LEVEL1 = 1
    LEVEL2 = 2
    LEVEL3 = 3

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
        self.scene_info = self.data_provider[Data.SCENES_INFO]
        self.store_id = self.data_provider[Data.STORE_FK]
        self.store_info = self.data_provider[Data.STORE_INFO]
        self.store_type = self.store_info['store_type'].iloc[0]
        self.scif = self.data_provider[Data.SCENE_ITEM_FACTS]
        self.rds_conn = PSProjectConnector(self.project_name, DbUsers.CalculationEng)
        self.general_tools = CCKH_SANDGENERALToolBox(self.data_provider, self.output)
        self.template = CCKH_SANDTemplateConsts()
        self.kpi_static_data = self.get_kpi_static_data()
        self.kpi_results_queries = []
        self.commonV2 = CommonV2(self.data_provider)
        self.kpi_new_static_data = self.commonV2.get_new_kpi_static_data()
        self.manufacturer = int(self.data_provider.own_manufacturer.param_value.values[0])
        self.ps_data_provider = PsDataProvider(self.data_provider, self.output)
        self.external_targets = self.ps_data_provider.get_kpi_external_targets()
        self.templates_info = self.external_targets[self.external_targets[CCKH_SANDTemplateConsts.TEMPLATE_OPERATION] ==
                                                    CCKH_SANDTemplateConsts.BASIC_SHEET]
        self.availability_info = self.external_targets[self.external_targets[CCKH_SANDTemplateConsts.TEMPLATE_OPERATION]
                                                       == CCKH_SANDTemplateConsts.AVAILABILITY_SHEET]
        self.visibility_info = self.external_targets[self.external_targets[CCKH_SANDTemplateConsts.TEMPLATE_OPERATION]
                                                     == CCKH_SANDTemplateConsts.VISIBILITY_SHEET]
        self.cooler_info = self.external_targets[self.external_targets[CCKH_SANDTemplateConsts.TEMPLATE_OPERATION]
                                                 == CCKH_SANDTemplateConsts.COOLER_SHEET]

    def get_kpi_static_data(self):
        """
        This function extracts the static KPI data and saves it into one global data frame.
        The data is taken from static.kpi / static.atomic_kpi / static.kpi_set.
        """
        query = CCKH_SANDQueries.get_all_kpi_data()
        kpi_static_data = pd.read_sql_query(query, self.rds_conn.db)
        return kpi_static_data

    def calculate_red_score(self):
        """
        This function calculates the KPI results.
        """
        set_scores = {}
        results_list_new_db = []
        if self.templates_info.empty:
            Log.info("This sessions doesnt have relevant external targets")
        main_children = self.templates_info[self.templates_info[self.template.KPI_GROUP] == self.RED_SCORE]
        final_main_child = main_children[main_children['Tested KPI Group'] == self.RED_SCORE].iloc[0]
        for c in xrange(0, len(main_children)):
            main_child = main_children.iloc[c]
            if main_child['Tested KPI Group'] == self.RED_SCORE:
                continue
            main_child_kpi_fk = self.get_new_kpi_fk(main_child)  # kpi fk from new tables
            main_kpi_identifier = self.commonV2.get_dictionary(kpi_fk=main_child_kpi_fk)
            if self.validate_store_type(main_child):
                children = self.templates_info[self.templates_info
                                               [self.template.KPI_GROUP].str.encode(HelperConsts.UTF8) ==
                                               main_child[self.template.KPI_NAME].encode(HelperConsts.UTF8)]
                scores = []
                for i in xrange(len(children)):
                    child = children.iloc[i]
                    numerator, denominator, result_new_db, numerator_id = 0, 0, 0, None
                    kpi_weight = self.validate_kpi_run(child)
                    if kpi_weight is not False:
                        kpi_type = child[self.template.KPI_TYPE]
                        result = threshold = None
                        if kpi_type == self.AVAILABILITY:
                            score = self.calculate_availability(child)
                            if score is not False:
                                score, result, threshold = score
                                numerator, denominator, result_new_db = result, threshold, result
                        elif kpi_type == self.SURVEY:
                            score, result, threshold, survey_answer_fk = self.check_survey(child)
                            threshold = None
                            numerator, denominator, result_new_db = 1, 1, score
                            numerator_id = survey_answer_fk
                        elif kpi_type == self.SHARE_OF_SHELF:
                            score, result, threshold, result_new_db, numerator, denominator = \
                                self.calculate_share_of_shelf(child)
                        elif kpi_type == self.NUMBER_OF_SCENES:
                            scene_types = self.get_scene_types(child)
                            result = self.general_tools.calculate_number_of_scenes(
                                **{SCENE_TYPE_FIELD: scene_types})
                            numerator, denominator, result_new_db = result, 1, result
                            score = 1 if result >= 1 else 0
                        else:
                            Log.warning("KPI of type '{}' is not supported".format(kpi_type))
                            continue
                        if score is not False:
                            if score is None:
                                points = 0
                            else:
                                points = float(child[self.template.WEIGHT]
                                               ) if kpi_weight is True else kpi_weight
                                scores.append((points, score))
                            atomic_fk = self.get_atomic_fk(main_child, child)
                            self.write_to_db_result(
                                atomic_fk, (score, result, threshold, points), level=self.LEVEL3)
                            identifier_parent = main_kpi_identifier
                            child_kpi_fk = self.get_new_kpi_fk(child)  # kpi fk from new tables
                            results_list_new_db.append(self.write_to_db_new_results(child_kpi_fk, result_new_db, score,
                                                                                    numerator, denominator,
                                                                                    weight=points, target=denominator,
                                                                                    identifier_parent=identifier_parent
                                                                                    , numerator_id=numerator_id))
                max_points = sum([score[0] for score in scores])
                actual_points = sum([score[0] * score[1] for score in scores])
                percentage = 0 if max_points == 0 else round(
                    (actual_points / float(max_points)) * 100, 2)

                kpi_name = main_child[self.template.TRANSLATION]
                kpi_fk = self.kpi_static_data[self.kpi_static_data['kpi_name'].str.encode(HelperConsts.UTF8) ==
                                              kpi_name.encode(HelperConsts.UTF8)]['kpi_fk'].values[0]
                self.write_to_db_result(kpi_fk, (actual_points, max_points,
                                                 percentage), level=self.LEVEL2)
                set_scores[kpi_name] = (max_points, actual_points)
                results_list_new_db.append(self.write_to_db_new_results(main_child_kpi_fk, percentage, percentage,
                                                                        actual_points, max_points,
                                                                        target=max_points,
                                                                        weight=actual_points,
                                                                        identifier_result=main_kpi_identifier,
                                                                        identifier_parent=self.RED_SCORE))

        max_points = sum([score[0] for score in set_scores.values()])
        actual_points = sum([score[1] for score in set_scores.values()])
        red_score = 0 if max_points == 0 else round((actual_points / float(max_points)) * 100, 2)
        set_fk = self.kpi_static_data['kpi_set_fk'].values[0]
        self.write_to_db_result(set_fk, (actual_points, max_points, red_score), level=self.LEVEL1)
        results_list_new_db.append(self.write_to_db_new_results(self.get_new_kpi_fk(final_main_child), red_score,
                                                                red_score, actual_points, max_points,
                                                                target=max_points,
                                                                weight=actual_points,
                                                                identifier_result=self.RED_SCORE,
                                                                identifier_parent=CCKH_SANDConsts.WEB_HIERARCHY))
        results_list_new_db.append(self.write_to_db_new_results(self.get_new_kpi_by_name(self.RED_SCORE), red_score,
                                                                red_score, actual_points, max_points,
                                                                target=max_points, weight=actual_points,
                                                                identifier_result=CCKH_SANDConsts.WEB_HIERARCHY))
        self.commonV2.save_json_to_new_tables(results_list_new_db)
        self.commonV2.commit_results_data()

    def validate_store_type(self, params):
        """
        This function checks whether or not a KPI is relevant for calculation, by the session's store type.
        """
        validation = False
        stores = params[self.template.STORE_TYPE]
        if not stores:
            validation = True
        elif isinstance(stores, (str, unicode)):
            if stores.upper() == self.template.ALL:
                validation = True
        elif isinstance(stores, list):
            if self.store_type in stores:
                validation = True
        return validation

    def validate_kpi_run(self, params):
        """
        This function checks whether or not a KPI Atomic needs to be calculated, based on a customized template.
        """
        weight = params[self.template.WEIGHT]
        if str(weight).isdigit():
            validation = True
        else:
            kpi_group = params[self.template.KPI_GROUP]
            if kpi_group == 'Visibility':
                custom_template = self.visibility_info
            elif kpi_group in ('Ambient Space', 'Cooler Space'):
                custom_template = self.cooler_info
            else:
                return False
            condition = (custom_template[self.template.KPI_NAME] == params[self.template.KPI_NAME])
            if self.template.KPI_GROUP in custom_template.keys() and kpi_group != 'Visibility':
                condition &= (custom_template[self.template.KPI_GROUP]
                              == params[self.template.KPI_GROUP])
            kpi_data = custom_template[condition]
            if kpi_data.empty:
                return False
            weight = kpi_data[kpi_data['store_type'] == self.store_type]['Target'].values[0]
            try:
                validation = float(weight)
            except ValueError:
                validation = False
        return validation

    def get_atomic_fk(self, pillar, params):
        """
        This function gets an Atomic KPI's FK out of the template data.
        """
        atomic_name = params[self.template.TRANSLATION]
        kpi_name = pillar[self.template.TRANSLATION]
        atomic_fk = self.kpi_static_data[(self.kpi_static_data['kpi_name'].str.encode(HelperConsts.UTF8) ==
                                          kpi_name.encode(HelperConsts.UTF8)) & (
                                                 self.kpi_static_data['atomic_kpi_name'].str.encode(
                                                     HelperConsts.UTF8) == atomic_name.encode(HelperConsts.UTF8))][
            'atomic_kpi_fk']
        if atomic_fk.empty:
            return None
        return atomic_fk.values[0]

    def get_new_kpi_fk(self, params):
        """
        This function gets an KPI's FK from new kpi table 'static.kpi_level_2' out of the template data .
        """
        kpi_name = params[self.template.TRANSLATION]
        return self.get_new_kpi_by_name(kpi_name)

    def get_new_kpi_by_name(self, kpi_name):
        kpi_fk = self.kpi_new_static_data[self.kpi_new_static_data['type'].str.encode(HelperConsts.UTF8) ==
                                          kpi_name.encode(HelperConsts.UTF8)]['pk']
        if kpi_fk.empty:
            return None
        return kpi_fk.values[0]

    def get_scene_types(self, params):
        """
        This function extracts the relevant scene types (==additional_attribute_1) from the template.
        """
        scene_types = params[self.template.SCENE_TYPE]
        if not scene_types or (isinstance(scene_types, (str, unicode)) and scene_types.upper() == self.template.ALL):
            return None
        return scene_types

    def calculate_availability(self, params):
        """
        This function calculates Availability typed Atomics from a customized template, and saves the results to the DB.
        """
        kpi_data = self.availability_info[(self.availability_info[self.template.KPI_NAME] ==
                                           params[self.template.KPI_NAME]) & (self.availability_info[
                                                                                  self.template.STORE_TYPE] == self.store_type)]
        if kpi_data.empty:
            return False
        kpi_data = kpi_data.iloc[0]
        target = kpi_data[CCKH_SANDTemplateConsts.TARGET]
        if not target:
            return False
        target = float(target)
        filters = {'product_ean_code': kpi_data[self.template.PRODUCTS].split(
            self.template.SEPARATOR)}
        scene_types = self.get_scene_types(params)
        if scene_types:
            filters[SCENE_TYPE_FIELD] = scene_types
        result = self.general_tools.calculate_availability(**filters)
        score = 1 if result >= target else 0
        return score, result, target

    def check_survey(self, params):
        """
        This function calculates Survey typed Atomics, and saves the results to the DB.
        """
        survey_id = int(float(params[self.template.SURVEY_ID]))
        target_answer = params[self.template.SURVEY_ANSWER]
        survey_answer, survey_answer_fk = self.general_tools.get_survey_answer(survey_data=('question_fk', survey_id))
        score = 1 if survey_answer == target_answer else 0
        return score, survey_answer, target_answer, survey_answer_fk

    def calculate_share_of_shelf(self, params):
        """
        This function calculates Facings Share of Shelf typed Atomics, and saves the results to the DB.
        """
        if params[self.template.SOS_NUMERATOR].startswith('~'):
            sos_filters = {params[self.template.SOS_ENTITY]: (params[self.template.SOS_NUMERATOR][1:],
                                                              self.general_tools.EXCLUDE_FILTER)}
        else:
            sos_filters = {params[self.template.SOS_ENTITY]: params[self.template.SOS_NUMERATOR]}
        general_filters = {}
        scene_types = self.get_scene_types(params)
        if scene_types:
            general_filters[SCENE_TYPE_FIELD] = scene_types
        products_to_exclude = params[self.template.PRODUCT_TYPES_TO_EXCLUDE]
        if products_to_exclude:
            general_filters['product_type'] = (products_to_exclude.split(self.template.SEPARATOR),
                                               self.general_tools.EXCLUDE_FILTER)
        numerator_result = self.general_tools.calculate_availability(
            **dict(sos_filters, **general_filters))
        denominator_result = self.general_tools.calculate_availability(**general_filters)
        if denominator_result == 0:
            result = 0
        else:
            result = round((numerator_result / float(denominator_result)) * 100, 2)
        if params[self.template.TARGET]:
            target = float(params[self.template.TARGET]) * 100
            score = 1 if result >= target else 0
        else:
            score = target = None
        result_string = '{0}% ({1}/{2})'.format(result, int(numerator_result), int(denominator_result))
        return score, result_string, target, result, numerator_result, denominator_result

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
            score_2, score_3, score_1 = score
            kpi_set_name = self.kpi_static_data[self.kpi_static_data['kpi_set_fk']
                                                == fk]['kpi_set_name'].values[0]
            attributes = pd.DataFrame([(kpi_set_name, self.session_uid, self.store_id, self.visit_date.isoformat(),
                                        format(score_1, '.2f'), score_2, score_3, fk)],
                                      columns=['kps_name', 'session_uid', 'store_fk', 'visit_date', 'score_1',
                                               'score_2', 'score_3', 'kpi_set_fk'])
        elif level == self.LEVEL2:
            score_2, score_3, score = score
            kpi_name = self.kpi_static_data[self.kpi_static_data['kpi_fk']
                                            == fk]['kpi_name'].values[0]
            attributes = pd.DataFrame([(self.session_uid, self.store_id, self.visit_date.isoformat(),
                                        fk, kpi_name, score, score_2, score_3)],
                                      columns=['session_uid', 'store_fk', 'visit_date', 'kpi_fk', 'kpk_name',
                                               'score', 'score_2', 'score_3'])
        elif level == self.LEVEL3:
            score, result, threshold, weight = score
            data = self.kpi_static_data[self.kpi_static_data['atomic_kpi_fk'] == fk]
            atomic_kpi_name = data['atomic_kpi_name'].values[0]
            kpi_fk = data['kpi_fk'].values[0]
            kpi_set_name = self.kpi_static_data[self.kpi_static_data['atomic_kpi_fk']
                                                == fk]['kpi_set_name'].values[0]
            attributes = pd.DataFrame([(atomic_kpi_name, self.session_uid, kpi_set_name, self.store_id,
                                        self.visit_date.isoformat(), datetime.utcnow().isoformat(),
                                        score, kpi_fk, fk, threshold, result, weight)],
                                      columns=['display_text', 'session_uid', 'kps_name', 'store_fk', 'visit_date',
                                               'calculation_time', 'score', 'kpi_fk', 'atomic_kpi_fk', 'threshold',
                                               'result', 'kpi_weight'])
        else:
            attributes = pd.DataFrame()
        return attributes.to_dict()

    @log_runtime('Saving to DB')
    def commit_results_data(self):
        """
        This function writes all KPI results to the DB, and commits the changes.
        """
        cur = self.rds_conn.db.cursor()
        delete_queries = CCKH_SANDQueries.get_delete_session_results_query(self.session_uid)
        for query in delete_queries:
            cur.execute(query)
        for query in self.kpi_results_queries:
            cur.execute(query)
        self.rds_conn.db.commit()

    def write_to_db_new_results(self, kpi_fk, result, score, numerator_result, denominator_result,
                                score_after_action=0, weight=None, target=None, identifier_parent=None,
                                identifier_result=None, numerator_id=None):

        """
        This function gets all kpi info  and add the relevant numerator_id and denominator_id and than create the db
        result
              :param kpi_fk: pk of kpi
             :param result
             :param score
             :param numerator_result
             :param denominator_result
             :param weight
             :param target
             :param identifier_parent
             :param identifier_result
             :param numerator_id
             :param score_after_action
             :returns dict in format of db result
        """
        numerator_id = self.manufacturer if numerator_id is None else numerator_id
        denominator_id = self.store_id
        results = [denominator_result, numerator_result, result, score]
        result_dict = self.create_db_result(kpi_fk, numerator_id, denominator_id, results, weight, target,
                                            identifier_parent, identifier_result, score_after_action)
        return result_dict

    @staticmethod
    def create_db_result(kpi_fk, numerator_id, denominator_id, results, weight, target, identifier_parent=None,
                         identifier_result=None, score_after_action=0):
        """
       The function build db result for sos result
             :param kpi_fk: pk of kpi
             :param numerator_id
             :param denominator_id
             :param results: array of 4 parameters [denominator , numerator ,result,score]
             :param weight : weight of kpi result for kpi parent
             :param target : this case we will save denominator result (decinal)
             :param identifier_parent : dictionary of filters of level above  in calculation
             :param identifier_result : dictionary of filters of first level in calculation
             :param score_after_action :  in this case we will save numerator result(decimal)
             :returns dict in format of db result
         """
        return {'fk': kpi_fk, SessionResultsConsts.NUMERATOR_ID: numerator_id,
                SessionResultsConsts.DENOMINATOR_ID: denominator_id,
                SessionResultsConsts.DENOMINATOR_RESULT: results[0], SessionResultsConsts.NUMERATOR_RESULT: results[1],
                SessionResultsConsts.RESULT: results[2], SessionResultsConsts.SCORE: results[3],
                'identifier_parent': identifier_parent, 'identifier_result': identifier_result, 'should_enter': True,
                SessionResultsConsts.TARGET: target, SessionResultsConsts.WEIGHT: weight,
                'score_after_actions':score_after_action}
