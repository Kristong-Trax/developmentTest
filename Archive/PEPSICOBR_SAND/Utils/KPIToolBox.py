
import os
import numpy as np
import pandas as pd
from datetime import datetime

from Trax.Algo.Calculations.Core.DataProvider import Data
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
from Trax.Cloud.Services.Connector.Keys import DbUsers
from Trax.Data.Projects.ProjectConnector import AwsProjectConnector
from Trax.Utils.Logging.Logger import Log
from Trax.Data.Utils.MySQLservices import get_table_insertion_query as insert

from Projects.PEPSICOBR_SAND.Utils.Fetcher import PEPSICOBR_SANDQueries
from Projects.PEPSICOBR_SAND.Utils.GeneralToolBox import PEPSICOBR_SANDGENERALToolBox
from Projects.PEPSICOBR_SAND.Utils.ParseTemplates import parse_template

__author__ = 'Nimrod'

KPI_RESULT = 'report.kpi_results'
KPK_RESULT = 'report.kpk_results'
KPS_RESULT = 'report.kps_results'

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


class PEPSICOBR_SANDConsts(object):

    ALL = 'All'
    SEPARATOR = ','

    STORE_TYPE = 'Store Type'
    REGION = 'Region'
    SEGMENTATION = 'Segmentation'
    ATOMIC_KPI_NAME = 'Atomic KPI Name'
    KPI_NAME = 'KPI Name'
    SCENE_TYPE = 'Scene Type'
    ADDITIONAL_ATTRIBUTE_1 = 'Additional_attribute_1'
    KPI_TYPE = 'KPI Type'
    MIN_FACINGS = 'Minimum Number of Manufacturer Facings'
    SOS_NUMERATOR_ENTITY = 'SOS Numerator Entity'
    SOS_NUMERATOR = 'SOS Numerator'
    SOS_DENOMINATOR_ENTITY = 'SOS Denominator Entity'
    SOS_DENOMINATOR = 'SOS Denominator'
    SOS_IRRELEVANT_ENTITY = 'SOS Irrelevant Entity'
    SOS_IRRELEVANT = 'SOS Irrelevant'
    AVAILABILITY_ENTITY = 'Availability Entity'
    AVAILABILITY = 'Availability'
    SURVEY_QUESTION = 'Survey Question'
    TARGET = 'Target'
    WEIGHT = 'Weight'

    PRODUCT_EAN = 'Product EAN'

    PLANOGRAM_NAME = 'Planograma'
    COUNT_OF_SHELVES = 'Count of Shelves'
    SHELF_NUMBER = 'Shelf'
    POSITION = 'Item'

    PEPSICO = 'PEPSICO'


class PEPSICOBR_SANDToolBox(PEPSICOBR_SANDConsts):

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
        self.scenes_info = self.data_provider[Data.SCENES_INFO].merge(self.data_provider[Data.ALL_TEMPLATES],
                                                                      how='left', on='template_fk', suffixes=['', '_y'])
        self.store_id = self.data_provider[Data.STORE_FK]
        self.scif = self.data_provider[Data.SCENE_ITEM_FACTS]
        self.store_info = self.data_provider[Data.STORE_INFO]
        self.store_type = self.store_info['store_type'].values[0]
        self.number_of_checkouts = self.store_info['additional_attribute_1'].values[0]
        self.segmentation = self.get_segmentation()
        self.region = self.store_info['region_name'].values[0]
        self.rds_conn = AwsProjectConnector(self.project_name, DbUsers.CalculationEng)
        self.match_display_in_scene = self.get_match_display()
        self.tools = PEPSICOBR_SANDGENERALToolBox(self.data_provider, self.output, rds_conn=self.rds_conn)
        self.price_data = self.get_price_data()
        self.kpi_static_data = self.get_kpi_static_data()
        self.template_data = parse_template(TEMPLATE_PATH, 'KPIs')
        self.availability_template = parse_template(TEMPLATE_PATH, 'Assortment', 3, 2, 3)
        self.price_template = parse_template(TEMPLATE_PATH, 'Price', 1, 0, 2)
        self.planogram_template = parse_template(TEMPLATE_PATH, 'Planogram', 0, 0, 5,
                                                 columns_for_vertical_padding=[self.PLANOGRAM_NAME,
                                                                               self.COUNT_OF_SHELVES,
                                                                               self.SCENE_TYPE])
        self.planogram_template[self.COUNT_OF_SHELVES] = self.planogram_template[self.COUNT_OF_SHELVES].astype(float)
        self.availability_id = '{};{}'.format(self.region, self.segmentation)
        self.kpi_results_queries = []

    def get_kpi_static_data(self):
        """
        This function extracts the static KPI data and saves it into one global data frame.
        The data is taken from static.kpi / static.atomic_kpi / static.kpi_set.
        """
        query = PEPSICOBR_SANDQueries.get_all_kpi_data()
        kpi_static_data = pd.read_sql_query(query, self.rds_conn.db)
        return kpi_static_data

    def get_match_display(self):
        """
        This function extracts the display matches data and saves it into one global data frame.
        The data is taken from probedata.match_display_in_scene.
        """
        query = PEPSICOBR_SANDQueries.get_match_display(self.session_uid)
        match_display = pd.read_sql_query(query, self.rds_conn.db)
        return match_display

    def get_price_data(self):
        """
        This function extracts the products' prices data from probedata.match_product_in_probe_details.
        """
        query = PEPSICOBR_SANDQueries.get_prices(self.session_uid)
        prices = pd.read_sql_query(query, self.rds_conn.db)
        prices = prices.merge(self.all_products[['product_fk', 'product_ean_code']], on='product_fk', how='left')
        return prices

    def get_segmentation(self):
        """
        This function converts the store's segmentation (=additional_attriubte_1) into one of the segmentation groups.
        """
        segmentation = self.store_info['additional_attribute_1'].values[0]
        if segmentation is None or (not isinstance(segmentation, (float, int)) and not segmentation.isdigit()):
            segmentation = ''
        else:
            segmentation = int(segmentation)
            if 1 <= segmentation <= 5:
                segmentation = '1-5'
            elif 6 <= segmentation <= 10:
                segmentation = '6-10'
            else:
                segmentation = '11+'
        return segmentation

    def main_calculation(self):
        """
        This function calculates the KPI results.
        """
        set_score = 0
        for p in xrange(len(self.template_data)):
            params = self.template_data.iloc[p]
            if self.validate_kpi_run(params):
                kpi_type = params[self.KPI_TYPE]
                try:
                    weight = float(params[self.WEIGHT])
                except ValueError:
                    weight = 0
                kpi_data = self.kpi_static_data[(self.kpi_static_data['kpi_name'] == params[self.KPI_NAME])]
                atomic_fk = kpi_data[kpi_data['atomic_kpi_name'] == params[self.ATOMIC_KPI_NAME]]['atomic_kpi_fk']
                if kpi_type == 'Availability':
                    score, result, threshold = self.calculate_availability(params, kpi_data)
                elif kpi_type == 'Availability %':
                    score, result, threshold = self.calculate_percentage_of_availability(params, kpi_data)
                elif kpi_type == 'SOS':
                    score, result, threshold = self.calculate_share_of_shelf(params, atomic_fk.values[0])
                elif kpi_type == 'Count of Scenes':
                    score, result, threshold = self.calculate_number_of_scenes(params, atomic_fk.values[0])
                elif kpi_type == 'Share of Scenes %':
                    score, result, threshold = self.calculate_share_of_scenes(params, atomic_fk.values[0])
                elif kpi_type == 'Survey Question':
                    score, result, threshold = self.check_survey_answer(params, atomic_fk.values[0])
                elif kpi_type == 'Scene Recognition':
                    score, result, threshold = self.calculate_scene_recognition_assortment(params, atomic_fk.values[0])
                elif kpi_type == '% Price Compliance':
                    score, result, threshold = self.calculate_price_range(params, kpi_data)
                elif kpi_type == 'Planogram':
                    score, result, threshold = self.calculate_planogram(params, kpi_data)
                elif kpi_type == 'Count of Scenes & SOS':
                    score, result, threshold = self.calculate_number_of_scenes_and_sos(params, kpi_data)
                else:
                    Log.warning("KPI type '{}' is not supported".format(kpi_type))
                    continue
                if score is not None:
                    score *= weight
                    set_score += score
                    kpi_fk = kpi_data['kpi_fk'].values[0]
                    self.write_to_db_result(kpi_fk, (score, result, threshold), level=self.LEVEL2)

        set_fk = self.kpi_static_data['kpi_set_fk'].values[0]
        self.write_to_db_result(set_fk, set_score, level=self.LEVEL1)

    def validate_kpi_run(self, params):
        """
        This function checks whether or not a KPI should run, given the store's region, type and segmentation.
        """
        stores = params[self.STORE_TYPE]
        if stores and stores != self.ALL and self.store_type not in stores.split(self.SEPARATOR):
            return False
        regions = params[self.REGION]
        if regions and regions != self.ALL and self.region not in regions.split(self.SEPARATOR):
            return False
        segmentation = params[self.SEGMENTATION]
        if segmentation and segmentation != self.ALL and self.segmentation != segmentation:
            return False
        return True

    def calculate_number_of_scenes_and_sos(self, params, atomics_data):
        """
        This function calculates the result of a 'Number of scenes & Facings SOS' typed KPI,
        and saves the results to the DB.
        """
        atomics = atomics_data['atomic_kpi_fk'].tolist()
        scenes_target, sos_target = map(float, params[self.TARGET].split(self.SEPARATOR))
        scene_filters = self.get_scene_filters(params)
        # Number of scenes
        scenes_result = self.tools.calculate_number_of_scenes(
            additional_attribute_1=scene_filters.get('additional_attribute_1'))
        scenes_score = 100 if scenes_result >= scenes_target else 0
        scenes_facings = self.scif[(self.scif['manufacturer_name'] == self.PEPSICO) &
            (self.scif['additional_attribute_1'].isin(scene_filters.get('additional_attribute_1')))]['facings'].sum()
        # Facing SOS
        sos_params = params.to_dict()
        sos_params[self.ADDITIONAL_ATTRIBUTE_1] = ''

        sos_filters, general_filters = self.get_share_of_shelf_filters(sos_params)
        sos_result = self.tools.calculate_share_of_shelf(sos_filters, include_empty=self.tools.EXCLUDE_EMPTY, **general_filters)
        sos_score = 100 if sos_result >= sos_target else 0
        sos_facings = self.scif[(self.scif['manufacturer_name'] == self.PEPSICO) &
                                (self.scif['template_name'].isin(scene_filters.get('template_name')))]['facings'].sum()
        sos_result *= 100
        sos_target *= 100

        # Aggregation
        if scenes_score and sos_score:
            if scenes_facings > sos_facings:
                self.write_to_db_result(atomics[0], (scenes_score, scenes_result, scenes_target), level=self.LEVEL3)
                return scenes_score, scenes_result, scenes_target
            else:
                self.write_to_db_result(atomics[1], (sos_score, sos_result, sos_target), level=self.LEVEL3)
                return sos_score, sos_result, sos_target
        elif scenes_score and not sos_score:
            self.write_to_db_result(atomics[0], (scenes_score, scenes_result, scenes_target), level=self.LEVEL3)
            return scenes_score, scenes_result, scenes_target
        elif sos_score and not scenes_score:
            self.write_to_db_result(atomics[1], (sos_score, sos_result, sos_target), level=self.LEVEL3)
            return sos_score, sos_result, sos_target
        else:
            self.write_to_db_result(atomics[0], (scenes_score, scenes_result, scenes_target), level=self.LEVEL3)
            self.write_to_db_result(atomics[1], (sos_score, sos_result, sos_target), level=self.LEVEL3)
            if scenes_facings > sos_facings:
                return scenes_score, scenes_result, scenes_target
            else:
                return sos_score, sos_result, sos_target

    def check_survey_answer(self, params, atomic_fk):
        """
        This function calculates 'Survey Question' typed KPI, and saves the results to the DB.
        """
        target_answer = params[self.TARGET]
        survey_answer = self.tools.get_survey_answer(params[self.SURVEY_QUESTION])
        score = 100 if survey_answer == target_answer else 0
        survey_answer = '-' if survey_answer is None else survey_answer
        self.write_to_db_result(atomic_fk, (score, survey_answer, target_answer), level=self.LEVEL3)
        return score, survey_answer, target_answer

    def calculate_share_of_shelf(self, params, atomic_fk):
        """
        This function calculates 'Facing SOS' typed KPI, and saves the results to the DB.
        """
        sos_filters, general_filters = self.get_share_of_shelf_filters(params)
        result = self.tools.calculate_share_of_shelf(sos_filters, include_empty=self.tools.EXCLUDE_EMPTY, **general_filters)
        target = float(params[self.TARGET])
        score = 100 if result >= target else 0
        self.write_to_db_result(atomic_fk, (score, result * 100, target * 100), level=self.LEVEL3)
        return score, result * 100, target * 100

    def get_share_of_shelf_filters(self, params):
        scene_filters = self.get_scene_filters(params)
        general_filters = dict(scene_filters, **self.get_filters(params, [self.SOS_DENOMINATOR_ENTITY, self.SOS_DENOMINATOR]))
        irrelevant_filters = self.get_filters(params, [self.SOS_IRRELEVANT_ENTITY, self.SOS_IRRELEVANT], exclude=True)
        general_filters = dict(general_filters, **irrelevant_filters)
        sos_filters = self.get_filters(params, [self.SOS_NUMERATOR_ENTITY, self.SOS_NUMERATOR])
        return sos_filters, general_filters

    def calculate_number_of_scenes(self, params, atomic_fk):
        """
        This function calculates 'Number of scenes' typed KPI, and saves the results to the DB.
        """
        scene_filters = self.get_scene_filters(params)
        result = self.tools.calculate_number_of_scenes(**scene_filters)
        target = int(params[self.TARGET])
        score = 100 if result >= target else 0
        self.write_to_db_result(atomic_fk, (score, result, target), level=self.LEVEL3)
        return score, result, target

    def calculate_availability(self, params, atomics_data):
        """
        This function calculates 'Assorment' typed KPI, and saves the results to the DB.
        """
        scene_filters = self.get_scene_filters(params)
        products = params[self.AVAILABILITY].split(self.SEPARATOR)
        final_result = 0
        for product in products:
            result = self.tools.calculate_availability(product_ean_code=product, **scene_filters)
            score = 1 if result >= 1 else 0
            atomic_fk = atomics_data[atomics_data['description'] == product]['atomic_kpi_fk'].values[0]
            self.write_to_db_result(atomic_fk, (score * 100, result, 1), level=self.LEVEL3)
            final_result += score
        score = 100 if final_result >= len(products) else 0
        return score, final_result, len(products)

    def calculate_percentage_of_availability(self, params, atomics_data):
        """
        This function calculates percentage of passed 'Assortment' typed KPIs (from a custom template),
        and saves the results to the DB.
        """
        scene_filters = self.get_scene_filters(params)
        if self.availability_id not in self.availability_template.keys():
            return None, None, None
        products = self.availability_template[self.availability_template[self.availability_id] == 'Y'][self.PRODUCT_EAN].tolist()
        results = []
        for product in products:
            result = self.tools.calculate_availability(product_ean_code=product, **scene_filters)
            score = 1 if result >= 1 else 0
            atomic_fk = atomics_data[atomics_data['description'] == product]['atomic_kpi_fk'].iloc[0]
            self.write_to_db_result(atomic_fk, (score * 100, result, 1), level=self.LEVEL3)
            results.append(score)
        if not results:
            return None, None, None
        target = float(params[self.TARGET])
        final_result = results.count(1) / float(len(results))
        score = 100 if final_result >= target else 0
        return score, final_result * 100, target * 100

    def calculate_share_of_scenes(self, params, atomic_fk):
        """
        This function calculates the percentage of a specific templates out of all the visit's scenes,
        and saves the results to the DB.
        """
        scene_filters = self.get_scene_filters(params)
        try:
            total_number_of_scenes = int(float(self.number_of_checkouts))
        except ValueError:
            total_number_of_scenes = 0
        if total_number_of_scenes == 0:
            return None, None, None
        number_of_scenes = self.tools.calculate_assortment(assortment_entity='scene_id',
                                                           minimum_assortment_for_entity=float(params[self.MIN_FACINGS]),
                                                           manufacturer_name=self.PEPSICO, **scene_filters)
        result = number_of_scenes / float(total_number_of_scenes)
        target = float(params[self.TARGET])
        score = 100 if result >= target else 0
        self.write_to_db_result(atomic_fk, (score, result * 100, target * 100), level=self.LEVEL3)
        return score, result * 100, target * 100

    def calculate_scene_recognition_assortment(self, params, atomic_fk):
        """
        This function calculates the assortment of certain displays (recognized by scene recognition),
        and saves the results to the DB.
        """
        scene_filters = self.get_scene_filters(params)
        scenes = self.scenes_info[self.tools.get_filter_condition(self.scenes_info, **scene_filters)]['scene_fk'].unique().tolist()
        relevant_scenes = []
        for scene in scenes:
            scene_facings = self.scif[(self.scif['scene_id'] == scene) &
                                      (self.scif['manufacturer_name'] == self.PEPSICO)]
            if scene_facings.empty or scene_facings['facings'].sum() == 0:
                relevant_scenes.append(scene)
        filters = self.get_filters(params, [self.AVAILABILITY_ENTITY, self.AVAILABILITY])
        filters['scene_id'] = relevant_scenes
        result = len(self.match_display_in_scene[self.tools.get_filter_condition(self.match_display_in_scene, **filters)])
        target = int(params[self.TARGET])
        score = 100 if result >= target else 0
        self.write_to_db_result(atomic_fk, (score, result, target), level=self.LEVEL3)
        return score, result, target

    def calculate_price_range(self, params, atomics_data):
        """
        This function calculates the percentage of products whose prices are within a given range,
        and saves the results to the DB.
        """
        if '{};MIN'.format(self.region) not in self.price_template.keys():
            return None, None, None
        products_in_price_range = 0
        total_numbers_of_products = 0
        for p in xrange(len(self.price_template)):
            product = self.price_template.iloc[p]
            product_ean_code = product[self.PRODUCT_EAN]
            min_range = product['{};MIN'.format(self.region)]
            max_range = product['{};MAX'.format(self.region)]
            if min_range and max_range:
                min_range = float(min_range)
                max_range = float(max_range)
                pricing_data = self.price_data[self.price_data['product_ean_code'] == product_ean_code]
                if not pricing_data.empty:
                    pricing_data = pricing_data.iloc[0]
                    total_numbers_of_products += 1
                    score = 1 if min_range <= float(pricing_data['price']) <= max_range else 0
                    products_in_price_range += score
                    atomic_fk = atomics_data[atomics_data['description'] == product_ean_code]['atomic_kpi_fk'].iloc[0]
                    self.write_to_db_result(atomic_fk, (score * 100, pricing_data['price'], None), level=self.LEVEL3)
        if total_numbers_of_products == 0:
            return None, None, None
        else:
            result = products_in_price_range / float(total_numbers_of_products)
        target = float(params[self.TARGET])
        score = 100 if result >= target else 0
        return score, result * 100, target * 100

    def calculate_planogram(self, params, atomics_data):
        """
        This function calculates the percentage of products whose position on a given shelf is within a given range,
        and saves the results to the DB.
        """
        if self.region not in self.planogram_template.keys():
            return None, None, None
        scene_types = []
        for types in self.planogram_template[self.SCENE_TYPE].unique().tolist():
            scene_types.extend(types.split(self.SEPARATOR))
        relevant_scenes = self.scif[self.scif['template_name'].isin(scene_types)]
        target = float(params[self.TARGET]) * 100

        results = {}
        for scene in relevant_scenes['scene_id'].unique().tolist():
            template_name = relevant_scenes[relevant_scenes['scene_id'] == scene]['template_name'].values[0]
            number_of_shelves = self.match_product_in_scene[self.match_product_in_scene['scene_fk'] ==
                                                            scene]['shelf_number'].max()
            plangoram_data = self.planogram_template[self.planogram_template[self.COUNT_OF_SHELVES] == number_of_shelves]
            plangoram_data = plangoram_data[plangoram_data[self.SCENE_TYPE].apply(
                lambda x: template_name in x.split(self.SEPARATOR))]
            if plangoram_data.empty:
                continue
            planogram_scores = {}
            for planogram in plangoram_data[self.PLANOGRAM_NAME].unique():
                data = plangoram_data[plangoram_data[self.PLANOGRAM_NAME] == planogram]
                number_of_all_products = 0
                number_of_aligned_products = 0
                for p in xrange(len(data)):
                    position_data = data.iloc[p]
                    if not position_data[self.region]:
                        continue
                    products = position_data[self.region].split(self.SEPARATOR)
                    position = int(float(position_data[self.POSITION]))
                    shelf_number = int(float(position_data[self.SHELF_NUMBER]))
                    positions = self.tools.get_product_unique_position_on_shelf(scene, shelf_number, include_empty=True)
                    if positions:
                        number_of_all_products += 1
                        for product in products:
                            if product in positions:
                                min_position = position
                                max_position = position + positions[:positions.index(product)].count('Empty')
                                if min_position <= positions.index(product) + 1 <= max_position:
                                    number_of_aligned_products += 1
                                    break
                atomic_name = u'{} - {}'.format(planogram, template_name)
                score = (number_of_aligned_products / float(number_of_all_products)) * 100
                planogram_scores[atomic_name] = (score, number_of_aligned_products, number_of_all_products)
            planogram_name = sorted(planogram_scores.items(), key=lambda x: x[1][0], reverse=True)[0][0]
            if planogram_name not in results or planogram_scores[planogram_name][1] > results[planogram_name][0]:
                results[planogram_name] = [planogram_scores[planogram_name][1], planogram_scores[planogram_name][2]]
        if not results:
            return None, None, None
        else:
            scores = []
            for planogram in results.keys():
                number_of_aligned_products, number_of_all_products = results[planogram]
                atomic_fk = atomics_data[atomics_data['atomic_kpi_name'] == planogram]['atomic_kpi_fk'].values[0]
                result = (number_of_aligned_products / float(number_of_all_products)) * 100
                score = 100 if result >= target else 0
                scores.append(score)
                self.write_to_db_result(atomic_fk, (score, result, target), level=self.LEVEL3)
            return round(np.mean(scores), 2), scores.count(100), len(scores)

    def get_filters(self, params, pair_of_headers, exclude=False):
        """
        This function receives a pair of column headers (entity, value) and returns a dictionary of filters.
        """
        entity_field, value_field = pair_of_headers
        flag = self.tools.EXCLUDE_FILTER if exclude else self.tools.INCLUDE_FILTER
        if params[entity_field]:
            filters = {params[entity_field]: (params[value_field].split(self.SEPARATOR), flag)}
        else:
            filters = {}
        return filters

    def get_scene_filters(self, params):
        """
        This function extracts the template filters from the template (template_name & additional_attribute_1),
        and returns it as dictionary.
        """
        filters = {}
        scene_types = params[self.SCENE_TYPE]
        if scene_types and scene_types != self.ALL:
            filters['template_name'] = scene_types.split(self.SEPARATOR)
        additional_attribute = params[self.ADDITIONAL_ATTRIBUTE_1]
        if additional_attribute and additional_attribute != self.ALL:
            filters['additional_attribute_1'] = additional_attribute.split(self.SEPARATOR)
        return filters

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
            score, result, threshold = score
            result = self._modify_param(result)
            threshold = self._modify_param(threshold)
            kpi_name = self.kpi_static_data[self.kpi_static_data['kpi_fk'] == fk]['kpi_name'].values[0].replace("'", "\\'")
            attributes = pd.DataFrame([(self.session_uid, self.store_id, self.visit_date.isoformat(),
                                        fk, kpi_name, score, result, threshold)],
                                      columns=['session_uid', 'store_fk', 'visit_date', 'kpi_fk', 'kpk_name',
                                               'score', 'score_2', 'score_3'])
        elif level == self.LEVEL3:
            if isinstance(score, tuple):
                score, result, threshold = score
            else:
                result = threshold = None
            score = self._modify_param(score)
            result = self._modify_param(result)
            threshold = self._modify_param(threshold)
            data = self.kpi_static_data[self.kpi_static_data['atomic_kpi_fk'] == fk]
            atomic_kpi_name = data['atomic_kpi_name'].values[0].replace("'", "\\'")
            kpi_fk = data['kpi_fk'].values[0]
            kpi_set_name = self.kpi_static_data[self.kpi_static_data['atomic_kpi_fk'] == fk]['kpi_set_name'].values[0]
            attributes = pd.DataFrame([(atomic_kpi_name, self.session_uid, kpi_set_name, self.store_id,
                                        self.visit_date.isoformat(), datetime.utcnow().isoformat(),
                                        score, result, threshold, kpi_fk, fk)],
                                      columns=['display_text', 'session_uid', 'kps_name', 'store_fk', 'visit_date',
                                               'calculation_time', 'score', 'result', 'threshold', 'kpi_fk',
                                               'atomic_kpi_fk'])
        else:
            attributes = pd.DataFrame()
        return attributes.to_dict()

    @staticmethod
    def _modify_param(param):
        if isinstance(param, float):
            param = round(param, 2)
            if param == int(param):
                param = int(param)
        return param

    @log_runtime('Saving to DB')
    def commit_results_data(self):
        """
        This function writes all KPI results to the DB, and commits the changes.
        """
        cur = self.rds_conn.db.cursor()
        delete_queries = PEPSICOBR_SANDQueries.get_delete_session_results_query(self.session_uid)
        for query in delete_queries:
            cur.execute(query)
        for query in self.kpi_results_queries:
            cur.execute(query)
        self.rds_conn.db.commit()
