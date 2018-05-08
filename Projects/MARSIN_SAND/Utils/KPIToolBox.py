
import os
import pandas as pd
from datetime import datetime

from Trax.Algo.Calculations.Core.DataProvider import Data
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
from Trax.Cloud.Services.Connector.Keys import DbUsers
from Trax.Data.Projects.ProjectConnector import AwsProjectConnector
from Trax.Utils.Logging.Logger import Log
from Trax.Data.Utils.MySQLservices import get_table_insertion_query as insert

from Projects.MARSIN_SAND.Utils.Fetcher import MARSIN_SANDQueries
from Projects.MARSIN_SAND.Utils.GeneralToolBox import MARSIN_SANDGENERALToolBox
from Projects.MARSIN_SAND.Utils.ParseComplexTemplates import parse_template

__author__ = 'Nimrod'

KPI_RESULT = 'report.kpi_results'
KPK_RESULT = 'report.kpk_results'
KPS_RESULT = 'report.kps_results'
THRESHOLD = 0.5
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


class MARSIN_SANDKPIConsts(object):

    PICOS = 'PicOS'
    SURVEY = 'Survey'
    AVAILABILITY = 'Availability'
    AVAILABILITY_WITH_SHELVES = 'Specific shelf availability'
    SHARE_OF_SHELF = 'SOS Facings'
    SEQUENCE_WITHIN_BLOCK = 'Blocked Together in Sequence'
    BLOCKS_IN_SEQUENCE = 'Product Group Adjacency'
    AVAILABILITY_AND_SURVEY = 'Availability & Survey'



class MARSIN_SANDTemplateConsts(object):

    # HEADERS #
    STORE_TYPE = 'Store Type'
    OUTLET_CLASS = 'Outlet Class'
    KPI_NAME = 'KPI Name'
    KPI_GROUP = 'KPI Group'
    KPI_TYPE = 'KPI Type'
    SCENE_TYPE = 'Scene Type'

    PARAM1 = 'Param1'
    PARAM2 = 'Param2'
    VALUE1 = 'Value1'
    VALUE2 = 'Value2'
    PAIR1 = (PARAM1, VALUE1)
    PAIR2 = (PARAM2, VALUE2)

    SCORE_TYPE = 'Score'
    TARGET = 'Target'

    # Customized template #

    ENTITY = 'Entity'
    # Availability
    VALUES = 'Values'
    MIN_FACINGS = 'Min Facings'
    # Product Group Adjacency
    GROUP1 = 'Group 1'
    GROUP2 = 'Group 2'
    # Blocked Together in Sequence
    BLOCK_ENTITY = 'Block Entity'
    BLOCK_VALUES = 'Block'
    SEQUENCE_ENTITY = 'Sequence Entity'
    SEQUENCE_VALUES = 'Sequence'
    # Survey
    SURVEY_ID = 'Survey ID'
    SURVEY_TEXT = 'Survey Text'
    SURVEY_ANSWER = 'Accepted Answer'
    # SOS Facings
    NUMERATOR = 'Numerator'
    DENOMINATOR = 'Denominator'

    # OTHER #
    SEPARATOR = ', '      # Default
    SEPARATOR2 = ' : '    # Between 2 different entities' value
    SEPARATOR3 = ' / '    # Or


class MARSIN_SANDToolBox(MARSIN_SANDTemplateConsts, MARSIN_SANDKPIConsts):

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
        self.store_info = self.data_provider[Data.STORE_INFO]
        store_type = self.store_info['store_type'].values[0]
        if store_type is None:
            self.store_type = self.outlet_class = ''
        elif store_type.count(' - '):
            self.store_type, self.outlet_class = store_type.split(' - ')
        else:
            self.store_type = store_type
            self.outlet_class = None
        self.rds_conn = AwsProjectConnector(self.project_name, DbUsers.CalculationEng)
        self.store_id = self.data_provider[Data.STORE_FK]
        self.scif = self.data_provider[Data.SCENE_ITEM_FACTS]
        self.scif = self.scif.merge(self.get_missing_attributes(), on='product_fk', how='left', suffixes=['', '_1'])
        self.kpi_static_data = self.get_kpi_static_data()
        self.match_display_in_scene = self.get_match_display()
        self.tools = MARSIN_SANDGENERALToolBox(self.data_provider, self.output, rds_conn=self.rds_conn, scif=self.scif)
        self.weights = parse_template(TEMPLATE_PATH, 'Weight')
        self.kpi_data = parse_template(TEMPLATE_PATH, 'KPIs')
        self.template_id = self.store_type + (';{}'.format(self.outlet_class) if self.outlet_class else '')
        self.kpi_results_queries = []
        self.results = {}

    def get_template_data(self, name):
        if not hasattr(self, '_templates_data'):
            self._templates_data = {}
        if name not in self._templates_data.keys():
            self._templates_data[name] = parse_template(TEMPLATE_PATH, name, 1, 0,
                                                        5 if name != self.SEQUENCE_WITHIN_BLOCK else 6)
        return self._templates_data[name]

    def get_missing_attributes(self):
        query = MARSIN_SANDQueries.get_missing_attributes_data()
        data = pd.read_sql_query(query, self.rds_conn.db)
        return data

    def get_match_display(self):
        """
        This function extracts the display matches data and saves it into one global data frame.
        The data is taken from probedata.match_display_in_scene.
        """
        query = MARSIN_SANDQueries.get_match_display(self.session_uid)
        match_display = pd.read_sql_query(query, self.rds_conn.db)
        return match_display

    def get_kpi_static_data(self):
        """
        This function extracts the static KPI data and saves it into one global data frame.
        The data is taken from static.kpi / static.atomic_kpi / static.kpi_set.
        """
        query = MARSIN_SANDQueries.get_all_kpi_data()
        kpi_static_data = pd.read_sql_query(query, self.rds_conn.db)
        return kpi_static_data

    def main_calculation(self):
        """
        This function calculates the KPI results.
        """
        group_scores = {}
        for p in xrange(len(self.kpi_data)):
            params = self.kpi_data.iloc[p]
            kpi_group = params[self.KPI_GROUP]
            kpi_name = params[self.KPI_NAME]
            if self.validate_kpi_run(kpi_group, kpi_name):
                kpi_type = params[self.KPI_TYPE]
                kpi_fk, atomics = self.get_fk_for_kpi_and_its_atomics(params)
                if kpi_type == self.SURVEY:
                    kpi_score = self.check_survey_answer(params, atomics[0])
                elif kpi_type == self.AVAILABILITY:
                    kpi_score = self.calculate_availability(params, atomics)
                elif kpi_type == self.AVAILABILITY_WITH_SHELVES:
                    kpi_score = self.calculate_availability_for_shelves(params, atomics[0])
                elif kpi_type == self.SHARE_OF_SHELF:
                    kpi_score = self.calculate_facings_sos(params, atomics[0])
                elif kpi_type == self.BLOCKS_IN_SEQUENCE:
                    kpi_score = self.calculate_blocks_in_sequence(params, atomics)
                elif kpi_type == self.SEQUENCE_WITHIN_BLOCK:
                    kpi_score = self.calculate_sequence_within_block(params, atomics)
                elif kpi_type == self.AVAILABILITY_AND_SURVEY:
                    survey = self.check_survey_answer(params, atomics[0])
                    availability = self.calculate_availability(params, atomics[1:])
                    kpi_score = 0 if availability == 0 or survey == 0 else 1
                else:
                    Log.warning("KPI type '{}' is not supported".format(kpi_type))
                    continue

                if kpi_score is not None:
                    number_of_atomics = len(self.results.get(kpi_fk, []))
                    number_of_passed_atomics = self.results.get(kpi_fk, []).count(1)
                    self.write_to_db_result(kpi_fk, (kpi_score, number_of_passed_atomics, number_of_atomics),
                                            level=self.LEVEL2)
                    if kpi_group not in group_scores.keys():
                        group_scores[kpi_group] = [0, 0]
                    group_scores[kpi_group][0] += kpi_score
                    group_scores[kpi_group][1] += 1
        for group_name in group_scores:
            set_fk = self.kpi_static_data[self.kpi_static_data['kpi_set_name'] == group_name]['kpi_set_fk'].values[0]
            actual_points, max_points = group_scores[group_name]
            set_score = round((actual_points / float(max_points)) * 100, 2)
            group_scores[group_name].insert(0, set_score)
            self.write_to_db_result(set_fk, (set_score, actual_points, max_points), level=self.LEVEL1)
        self.save_picos_hierarchy(group_scores)

    def save_picos_hierarchy(self, pillar_scores):
        picos_static_data = self.kpi_static_data[self.kpi_static_data['kpi_set_name'] == self.PICOS]
        actual_points = sum(map(lambda x: x[1], pillar_scores.values()))
        total_points = sum(map(lambda x: x[2], pillar_scores.values()))
        picos_score = round((actual_points / float(total_points)) * 100, 2), actual_points, total_points
        self.write_to_db_result(picos_static_data['kpi_set_fk'].iloc[0], picos_score, level=self.LEVEL1)
        for pillar in pillar_scores.keys():
            pillar_static_data = picos_static_data[picos_static_data['kpi_name'] == pillar]
            self.write_to_db_result(pillar_static_data['kpi_fk'].iloc[0], pillar_scores[pillar], level=self.LEVEL2)
            self.write_to_db_result(pillar_static_data['atomic_kpi_fk'].iloc[0], pillar_scores[pillar], level=self.LEVEL3)

    def validate_kpi_run(self, set_name, kpi_name):
        kpi_run = False
        data = self.weights[(self.weights[self.KPI_GROUP] == set_name) &
                            (self.weights[self.KPI_NAME] == kpi_name)]
        if not data.empty:
            if data[self.store_type].values[0] == 'Y':
                kpi_run = True
        return kpi_run

    def get_fk_for_kpi_and_its_atomics(self, params):
        """
        This function extracts a KPI's fk and its Atomics' fk-s ordered by presentation_order.
        """
        kpi_data = self.kpi_static_data[(self.kpi_static_data['kpi_set_name'] == params[self.KPI_GROUP]) &
                                        (self.kpi_static_data['kpi_name'] == params[self.KPI_NAME])]
        kpi_fk = kpi_data['kpi_fk'].values[0]
        atomics = kpi_data['atomic_kpi_fk'].tolist()
        return kpi_fk, atomics

    def calculate_sequence_within_block(self, params, atomics):
        """
        This function calculates Atomics of type 'Sequence within block' - It checks whether a set of parameters are
        positioned in a block, and then checks for a certain sequence within it. Later it saves the results to the DB.
        """
        template_data = self.get_template_data(self.SEQUENCE_WITHIN_BLOCK)
        kpi_data = template_data[(template_data[self.KPI_GROUP] == params[self.KPI_GROUP]) &
                                 (template_data[self.KPI_NAME] == params[self.KPI_NAME]) &
                                 (~template_data[self.template_id].isin(['']))]
        if kpi_data.empty:
            return None
        kpi_data = kpi_data.iloc[0]
        scene_types = self.get_scene_types(params)
        if scene_types:
            relevant_scenes = self.scif[self.scif['template_name'].isin(scene_types)]['scene_id'].unique().tolist()
        else:
            relevant_scenes = self.scif['scene_id'].unique().tolist()
        block_filters = self.get_filters(kpi_data, (self.BLOCK_ENTITY, self.BLOCK_VALUES))
        relevant_scenes = list(set(relevant_scenes).intersection(
            self.scif[self.tools.get_filter_condition(self.scif, **block_filters)]['scene_id'].unique().tolist()))

        if len(relevant_scenes) == 1:
            block_target = float(kpi_data[self.template_id])
            block_result = self.tools.calculate_block_together(allowed_products_filters={'front_facing': 'N'},
                                                               minimum_block_ratio=block_target, front_facing='Y',
                                                               scene_fk=relevant_scenes, **block_filters)
            if isinstance(block_result, tuple):
                block_result, graph = block_result
            else:
                graph = None
            block_score = 1 if block_result >= block_target else 0
            if isinstance(block_result, (int, tuple)):
                block_result = round(block_result*100, 1)
            self.write_to_db_result(atomics[0], (block_score, block_result, block_target*100), level=self.LEVEL3)

            if kpi_data[self.SEQUENCE_ENTITY]:
                sequence_filters = (kpi_data[self.SEQUENCE_ENTITY], kpi_data[self.SEQUENCE_VALUES].split(self.SEPARATOR))
                if graph is not None:
                    sequence_result = self.tools.calculate_product_sequence(sequence_filters, direction='right',
                                                                            empties_allowed=True,
                                                                            irrelevant_allowed=False,
                                                                            min_required_to_pass=self.tools.STRICT_MODE,
                                                                            custom_graph=graph)
                else:
                    sequence_result = self.tools.calculate_product_sequence(sequence_filters, direction='right',
                                                                            empties_allowed=True,
                                                                            irrelevant_allowed=False,
                                                                            min_required_to_pass=self.tools.STRICT_MODE)
                sequence_score = 1 if sequence_result else 0
                self.write_to_db_result(atomics[1], (sequence_score, sequence_score, 1), level=self.LEVEL3)
                score = 1 if block_score and sequence_result else 0
            else:
                score = block_score

        else:
            for atomic_fk in atomics:
                self.write_to_db_result(atomic_fk, (0, 0, 1), level=self.LEVEL3)
            score = 0
        return score

    def calculate_blocks_in_sequence(self, params, atomics):
        """
        This function calculates Atomics of type 'Blocks in sequence' - It checks whether 2 sets of parameters are
        positioned in a block, and then checks whether the two blocks are in a sequence.
        Later it saves the results to the DB.
        """
        template_data = self.get_template_data(self.BLOCKS_IN_SEQUENCE)
        kpi_data = template_data[(template_data[self.KPI_GROUP] == params[self.KPI_GROUP]) &
                                 (template_data[self.KPI_NAME] == params[self.KPI_NAME]) &
                                 (~template_data[self.template_id].isin(['']))]
        if kpi_data.empty:
            return None
        kpi_data = kpi_data.iloc[0]
        scene_types = self.get_scene_types(params)
        if scene_types:
            relevant_scenes = self.scif[self.scif['template_name'].isin(scene_types)]['scene_id'].unique().tolist()
        else:
            relevant_scenes = self.scif['scene_id'].unique().tolist()
        group1_filters = self.get_filters(kpi_data, (self.ENTITY, self.GROUP1))
        group2_filters = self.get_filters(kpi_data, (self.ENTITY, self.GROUP2))

        relevant_scenes = set(relevant_scenes).intersection(
            self.scif[self.tools.get_filter_condition(self.scif, **group1_filters)]['scene_id'].unique().tolist())
        relevant_scenes = set(relevant_scenes).intersection(
            self.scif[self.tools.get_filter_condition(self.scif, **group2_filters)]['scene_id'].unique().tolist())

        if len(relevant_scenes) == 1:
            relevant_scenes = list(relevant_scenes)
            block1_result = self.tools.calculate_block_together(allowed_products_filters={'front_facing': 'N'},
                                                                front_facing='Y', scene_fk=relevant_scenes,
                                                                **group1_filters)
            block1_score = 1 if block1_result else 0
            self.write_to_db_result(atomics[0], (block1_score, block1_score, 1), level=self.LEVEL3)

            block2_result = self.tools.calculate_block_together(allowed_products_filters={'front_facing': 'N'},
                                                                front_facing='Y', scene_fk=relevant_scenes,
                                                                **group2_filters)
            block2_score = 1 if block2_result else 0
            self.write_to_db_result(atomics[1], (block2_score, block2_score, 1), level=self.LEVEL3)

            if block1_result and block2_result:
                merged_filters = group1_filters.copy()
                for field in group2_filters:
                    if field in merged_filters.keys():
                        merged_filters[field].extend(group2_filters[field])
                    else:
                        merged_filters[field] = group2_filters[field]
                merged_block_result = self.tools.calculate_block_together(allowed_products_filters={'front_facing': 'N'},
                                                                          front_facing='Y', scene_fk=relevant_scenes,
                                                                          **merged_filters)
                score = 1 if merged_block_result else 0
                self.write_to_db_result(atomics[2], (score, score, 1), level=self.LEVEL3)
            else:
                score = 0

        else:
            for atomic_fk in atomics:
                self.write_to_db_result(atomic_fk, (0, 0, 1), level=self.LEVEL3)
            score = 0
        return score

    def calculate_facings_sos(self, params, atomic_fk):
        """
        This function calculates simple facing Share of Shelf typed Atomic KPI, and writes it to the DB.
        """
        template_data = self.get_template_data(self.SHARE_OF_SHELF)
        kpi_data = template_data[(template_data[self.KPI_GROUP] == params[self.KPI_GROUP]) &
                                 (template_data[self.KPI_NAME] == params[self.KPI_NAME]) &
                                 (~template_data[self.template_id].isin(['']))]
        if kpi_data.empty:
            return None
        kpi_data = kpi_data.iloc[0]
        scene_types = self.get_scene_types(params)
        numerator_filters = self.get_filters(kpi_data, (self.ENTITY, self.NUMERATOR))
        numerator_result = self.tools.calculate_availability(front_facing='Y', template_name=scene_types,
                                                             **numerator_filters)
        denominator_filters = self.get_filters(kpi_data, (self.ENTITY, self.DENOMINATOR))
        denominator_result = self.tools.calculate_availability(front_facing='Y', template_name=scene_types,
                                                               **denominator_filters)
        result = 0 if denominator_result == 0 else round(numerator_result / float(denominator_result),2)
        threshold = float(kpi_data[self.template_id])
        score = result if result >= (threshold * THRESHOLD) else 0
        self.write_to_db_result(atomic_fk, (score, round(result * 100, 1), threshold * 100), level=self.LEVEL3)
        return score

    def calculate_availability(self, params, atomics):
        """
        This function calculates simple Availability-typed Atomic KPI, and writes it to the DB.
        """
        template_data = self.get_template_data(self.AVAILABILITY)
        kpi_data = template_data[(template_data[self.KPI_GROUP] == params[self.KPI_GROUP]) &
                                 (template_data[self.KPI_NAME] == params[self.KPI_NAME]) &
                                 (~template_data[self.template_id].isin(['']))]
        if kpi_data.empty:
            return None
        scene_types = self.get_scene_types(params)

        if params[self.KPI_GROUP] == self.AVAILABILITY:
            kpi_data = kpi_data.iloc[0]
            products = kpi_data[self.VALUES].split(self.SEPARATOR)
            target = len(products) if not str(kpi_data[self.template_id]).isdigit() else int(kpi_data[self.template_id])
            result = 0
            for product in products:
                product_result = 0
                for sub_product in product.split(self.SEPARATOR3):
                    sub_product_result = self.tools.calculate_availability(front_facing='Y', template_name=scene_types,
                                                                           product_ean_code=sub_product)
                    #sub_product_score = 1 if sub_product_result >= 1 else 0
                    if sub_product_result >= 1:
                        product_result = 1
                    value = None
                    s = self.save_result_for_product(params, sub_product, (value, sub_product_result,1))
                    if s is None and len(product.split(self.SEPARATOR3)) == 1:
                        product_result += 1
                result += product_result
            result = round(float(result)/float(target), 2)

            score = 0 if result < THRESHOLD else 1 if result >=1 else result


        else:
            scores = []
            for g in xrange(len(kpi_data)):
                group = kpi_data.iloc[g]
                filters = self.get_filters(group, (self.ENTITY, self.VALUES))
                target = int(group[self.template_id])
                if group[self.ENTITY] == 'display_name':
                    atomic_fk = atomics.pop(-1)
                    condition = (self.match_display_in_scene['display_name'].isin(filters['display_name']))
                    if scene_types:
                        scenes = self.scif[self.scif['template_name'].isin(scene_types)]['scene_id'].unique().tolist()
                        condition &= (self.match_display_in_scene['scene_fk'].isin(scenes))
                    result = len(self.match_display_in_scene[condition])
                else:
                    atomic_fk = atomics.pop(0)
                    try:
                        min_facings = int(float(group[self.MIN_FACINGS]))
                    except ValueError:
                        min_facings = 1
                    result = self.tools.calculate_assortment(minimum_assortment_for_entity=min_facings,
                                                             front_facing='Y', template_name=scene_types, **filters)
                score = 1 if result >= target else 0
                self.write_to_db_result(atomic_fk, (score, result, target), level=self.LEVEL3)
                scores.append(score)
            score = 1 if scores and all(scores) else 0
        return score

    def save_result_for_product(self, params, product, score):
        """
        This function writes an availability typed Atomic's result to the DB.
        """
        product_name = self.all_products[self.all_products['product_ean_code'] == product]['product_name']
        if not product_name.empty:
            product_name = product_name.values[0]
            atomic_data = self.kpi_static_data[(self.kpi_static_data['kpi_set_name'] == params[self.KPI_GROUP]) &
                                               (self.kpi_static_data['kpi_name'] == params[self.KPI_NAME]) &
                                               (self.kpi_static_data['atomic_kpi_name'] == product_name)]
            if not atomic_data.empty:
                atomic_fk = atomic_data['atomic_kpi_fk'].values[0]
                self.write_to_db_result(atomic_fk, score, level=self.LEVEL3)
                return True
            else:
                Log.warning("Atomic '{}' for KPI '{}' doesn't exist".format(product_name, params[self.KPI_NAME]))
                return False
        else:
            Log.warning("Product with EAN '{}' doesn't exist".format(product))
            return None

    def calculate_availability_for_shelves(self, params, atomic_fk):
        """
        This function calculates shelf-level typed Availability Atomic KPI, and writes it to the DB.
        """
        scene_types = self.get_scene_types(params)
        filters = self.get_filters(params, self.PAIR2)
        shelves = map(int, params[self.VALUE1].split(self.SEPARATOR))
        result = self.tools.calculate_shelf_level_assortment(shelves, from_top_or_bottom=self.tools.BOTTOM,
                                                             front_facing='Y', template_name=scene_types, **filters)
        threshold = 1 if not str(params[self.TARGET]).isdigit() else int(params[self.TARGET])
        score = 1 if result >= threshold else 0
        self.write_to_db_result(atomic_fk, (score, result, threshold), level=self.LEVEL3)
        return score

    def check_survey_answer(self, params, atomic_fk):
        """
        This function checks whether a survey has a required answer, and writes the result to the DB (as Atomic).
        """
        template_data = self.get_template_data(self.SURVEY)
        kpi_data = template_data[(template_data[self.KPI_GROUP] == params[self.KPI_GROUP]) &
                                 (template_data[self.KPI_NAME] == params[self.KPI_NAME]) &
                                 (~template_data[self.template_id].isin(['']))]
        if kpi_data.empty:
            return None
        kpi_data = kpi_data.iloc[0]
        target_answer = kpi_data[self.SURVEY_ANSWER]
        survey_answer = self.tools.get_survey_answer(survey_data=('question_fk', int(kpi_data[self.SURVEY_ID])))
        score = 1 if survey_answer == target_answer else 0
        survey_answer = '-' if survey_answer is None else survey_answer
        self.write_to_db_result(atomic_fk, (score, survey_answer, target_answer), level=self.LEVEL3)
        return score

    def get_filters(self, params, pairs_of_fields):
        filters = {}
        pairs_of_fields = pairs_of_fields if isinstance(pairs_of_fields, list) else [pairs_of_fields]
        for pair in pairs_of_fields:
            param_field, value_field = pair
            fields = params[param_field].split(self.SEPARATOR2)
            values = params[value_field].split(self.SEPARATOR2)
            pair_filters = zip(fields, values)
            for pair_filter in pair_filters:
                field, value = pair_filter
                if field not in filters.keys():
                    filters[field] = []
                filters[field].extend(value.split(self.SEPARATOR))
        return filters

    def get_scene_types(self, params):
        scene_types = params[self.SCENE_TYPE]
        scene_types = scene_types.split(self.SEPARATOR) if scene_types else []
        return scene_types

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
            score_1, score_2, score_3 = score
            kpi_set_name = self.kpi_static_data[self.kpi_static_data['kpi_set_fk'] == fk]['kpi_set_name'].values[0]
            attributes = pd.DataFrame([(kpi_set_name, self.session_uid, self.store_id, self.visit_date.isoformat(),
                                        format(score_1, '.2f'), score_2, score_3, fk)],
                                      columns=['kps_name', 'session_uid', 'store_fk', 'visit_date', 'score_1',
                                               'score_2', 'score_3', 'kpi_set_fk'])
        elif level == self.LEVEL2:
            score, score_2, score_3 = score
            kpi_name = self.kpi_static_data[self.kpi_static_data['kpi_fk'] == fk]['kpi_name'].values[0]
            attributes = pd.DataFrame([(self.session_uid, self.store_id, self.visit_date.isoformat(),
                                        fk, kpi_name, score, score_2, score_3)],
                                      columns=['session_uid', 'store_fk', 'visit_date', 'kpi_fk', 'kpk_name',
                                               'score', 'score_2', 'score_3'])
        elif level == self.LEVEL3:
            if isinstance(score, (tuple, list)):
                score, result, threshold = score
            else:
                result = threshold = None
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
            if kpi_fk not in self.results.keys():
                self.results[kpi_fk] = []
            self.results[kpi_fk].append(score)
        else:
            attributes = pd.DataFrame()
        return attributes.to_dict()

    @log_runtime('Saving to DB')
    def commit_results_data(self):
        """
        This function writes all KPI results to the DB, and commits the changes.
        """
        cur = self.rds_conn.db.cursor()
        delete_queries = MARSIN_SANDQueries.get_delete_session_results_query(self.session_uid)
        for query in delete_queries:
            cur.execute(query)
        for query in self.kpi_results_queries:
            cur.execute(query)
        self.rds_conn.db.commit()
