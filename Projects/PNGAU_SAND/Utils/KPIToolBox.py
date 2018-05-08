
import os
import pandas as pd
from datetime import datetime

from Trax.Algo.Calculations.Core.DataProvider import Data
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
from Trax.Utils.Conf.Keys import DbUsers
from Trax.Data.Projects.Connector import ProjectConnector
from Trax.Utils.Logging.Logger import Log
from Trax.Data.Utils.MySQLservices import get_table_insertion_query as insert

from Projects.PNGAU_SAND.Utils.Fetcher import PNGAU_SANDQueries
from Projects.PNGAU_SAND.Utils.GeneralToolBox import PNGAU_SANDGENERALToolBox
from Projects.PNGAU_SAND.Utils.EmptySpaces import PNGAU_SANDEmptySpaces
from Projects.PNGAU_SAND.Utils.ParseTemplates import parse_template

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


class PNGAU_SANDConsts(object):

    OWN_MANUFACTURER = 'Procter & Gamble'

    FACING_SOS = 'Facing SOS'
    EMPTY_FACING_SOS = 'Empty Facing SOS'
    LINEAR_SOS = 'Linear SOS'
    SHELF_SPACE_LENGTH = 'Shelf Space Length'
    FACING_COUNT = 'Facing Count'
    DISTRIBUTION = 'Distribution'
    ASSORTMENT = 'Assortment'
    ASSORTMENT_SHARE = 'Assortment Share'
    SHARE_OF_DISPLAY = 'Share of Display'
    COUNT_OF_SCENES = 'Count of Scenes'
    COUNT_OF_POSM = 'Count of POSM'
    COUNT_OF_POSM_BY_TYPE = 'Count of POSM by Type'
    POSM_ASSORTMENT = 'POSM Assortment'
    SURVEY_QUESTION = 'Survey Question'
    EMPTY_SPACES = 'Empty Spaces'

    MANUFACTURERS = 'Manufacturer'
    AGGREGATED_SCORE = 'Aggregated Score'
    REFERENCE_KPI = 'Reference KPI'

    CATEGORY_PRIMARY_SHELF = 'Category Primary Shelf'
    DISPLAY = 'Display'
    PRIMARY_SHELF = 'Primary Shelf'

    KPI_TYPE = 'KPI Type'
    SCENE_TYPES = 'Scene Types'
    TEMPLATE_GROUP = 'Template Group'
    KPI_NAME = 'KPI Name'
    CUSTOM_SHEET = 'Custom Sheet'
    PER_CATEGORY = 'Per Category'
    SUB_CALCULATION = 'Sub Calculation'
    WEIGHT = 'Weight'
    SET_NAME = 'Set Name'
    STORE_TYPE = 'Store Type'
    UNICODE_DASH = u' \u2013 '

    CATEGORY = 'Category'
    CLAIM_NAME = 'Claim Name'
    BRAND = 'Brand'
    PRODUCT_NAME = 'Product Name'
    PRODUCT_EAN = 'Product EAN'

    SURVEY_ID = 'Survey Question ID'
    SURVEY_TEXT = 'Survey Question Text'

    SEPARATOR = ','


class PNGAU_SANDToolBox(PNGAU_SANDConsts):
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
        self.all_templates = self.data_provider[Data.ALL_TEMPLATES]
        self.match_product_in_scene = self.data_provider[Data.MATCHES]
        self.visit_date = self.data_provider[Data.VISIT_DATE]
        self.session_info = self.data_provider[Data.SESSION_INFO]
        self.scene_info = self.data_provider[Data.SCENES_INFO]
        self.store_id = self.data_provider[Data.STORE_FK]
        self.store_type = self.data_provider[Data.STORE_INFO]['store_type'].values[0]
        self.scif = self.data_provider[Data.SCENE_ITEM_FACTS]
        self.brand_to_manufacturer = {x['brand_name']: x['manufacturer_name'] for i, x in
                                      self.scif[['brand_name', 'manufacturer_name']].drop_duplicates().iterrows()}
        self.match_display_in_scene = self.get_match_display()
        self.store_retailer = self.get_retailer()
        self.tools = PNGAU_SANDGENERALToolBox(self.data_provider, self.output, rds_conn=self.rds_conn)
        self.template_data = parse_template(TEMPLATE_PATH, 'KPIs')
        self.scoring_data = parse_template(TEMPLATE_PATH, 'Scores')
        self.category_scene_types, self.template_groups = self.get_category_scene_types()
        self._custom_templates = {}
        self.scenes_types_for_categories = {}
        self.kpi_static_data = self.get_kpi_static_data()
        self.kpi_results_queries = []
        self.kpi_results = {}
        self.atomic_results = {}
        self.categories = self.all_products['category_fk'].unique().tolist()

    @property
    def rds_conn(self):
        if not hasattr(self, '_rds_conn'):
            self._rds_conn = ProjectConnector(self.project_name, DbUsers.CalculationEng)
        try:
            pd.read_sql_query('select pk from probedata.session limit 1', self._rds_conn.db)
        except:
            Log.info('Disconnecting and connecting to DB')
            self._rds_conn.disconnect_rds()
            self._rds_conn = ProjectConnector(self.project_name, DbUsers.CalculationEng)
        return self._rds_conn

    @property
    def empty_spaces(self):
        if not hasattr(self, '_empty_spaces'):
            matches = self.match_product_in_scene.merge(self.products, on='product_fk', how='left', suffixes=['', '_1'])
            self._empty_spaces = PNGAU_SANDEmptySpaces(matches)
            self._empty_spaces.calculate_empty_spaces()
        return self._empty_spaces.empty_spaces

    def get_template(self, name):
        """
        This function extracts the template's data, given a sheet name.
        """
        if name not in self._custom_templates.keys():
            self._custom_templates[name] = parse_template(TEMPLATE_PATH, name)
        return self._custom_templates[name]

    def get_category_scene_types(self):
        """
        This function converts the category-scene template into a dictionary.
        """
        scene_types = parse_template(TEMPLATE_PATH, 'Category-Scene_Type')
        category_scene_types = {self.PRIMARY_SHELF: []}
        for category in scene_types[self.CATEGORY].unique():
            data = scene_types[scene_types[self.CATEGORY] == category]
            types = data[self.SCENE_TYPES].unique().tolist()
            category_scene_types[category] = types
            if category != self.DISPLAY:
                category_scene_types[self.PRIMARY_SHELF].extend(types)
        template_groups = {}
        for template_group in scene_types[self.TEMPLATE_GROUP].unique():
            if template_group:
                template_groups[template_group] = scene_types[scene_types[self.TEMPLATE_GROUP] ==
                                                              template_group][self.SCENE_TYPES].unique().tolist()
        return category_scene_types, template_groups

    def get_kpi_static_data(self):
        """
        This function extracts the static KPI data and saves it into one global data frame.
        The data is taken from static.kpi / static.atomic_kpi / static.kpi_set.
        """
        query = PNGAU_SANDQueries.get_all_kpi_data()
        kpi_static_data = pd.read_sql_query(query, self.rds_conn.db)
        return kpi_static_data

    def get_match_display(self):
        """
        This function extracts the display matches data and saves it into one global data frame.
        The data is taken from probedata.match_display_in_scene.
        """
        query = PNGAU_SANDQueries.get_match_display(self.session_uid)
        match_display = pd.read_sql_query(query, self.rds_conn.db)
        match_display = match_display.merge(self.scene_info[['scene_fk', 'template_fk']], on='scene_fk', how='left')
        match_display = match_display.merge(self.all_templates, on='template_fk', how='left', suffixes=['', '_y'])
        return match_display

    def get_retailer(self):
        query = PNGAU_SANDQueries.get_retailer(self.store_id)
        data = pd.read_sql_query(query, self.rds_conn.db)
        retailer = '' if data.empty else data['retailer'].values[0]
        if 'woolworth' in retailer.lower():
            retailer = 'Woolworths'
        return retailer

    @log_runtime('Main Calculation')
    def main_calculation(self):
        """
        This function calculates the KPI results.
        """
        self.calculation_per_product()
        self.calculation_per_brand()
        self.calculation_per_manufacturer()
        self.calculation_per_category()

        set_names = self.template_data[self.SET_NAME].tolist()
        scoring_sets = self.scoring_data[self.SET_NAME].tolist()
        for set_name in set(set_names):
            self.write_to_db_result(level=self.LEVEL1, kpi_set_name=set_name)
        for set_name in set(scoring_sets):
            set_weight = float(self.scoring_data[self.scoring_data[self.SET_NAME] == set_name][self.WEIGHT].values[0])
            set_weight = int(set_weight * 100)
            self.write_to_db_result(score=set_weight, level=self.LEVEL1, kpi_set_name=set_name)

    def get_target(self, kpi_name, category):
        """
        This function extracts a given KPI's target from the 'Targets' template.
        """
        target = None
        targets_data = self.get_template('Targets')
        targets_data = targets_data[(targets_data[self.KPI_NAME] == kpi_name) &
                                    (targets_data[self.CATEGORY] == category)]
        if not targets_data.empty:
            targets_data = targets_data.iloc[0]
            if self.store_retailer in targets_data.keys():
                target = targets_data[self.store_retailer]
                if target:
                    try:
                        if isinstance(target, (str, unicode)) and '%' in target:
                            target = float(target.replace('%', '')) / 100
                        target = int(float(target) * 10000)
                    except ValueError:
                        pass
        return target

    def calculate_distribution(self, category, params):
        """
        This function calculates 'Distribution' typed KPIs (and its sub-KPIs - the KPIs which inherit its results).
        """
        set_name = params[self.SET_NAME]
        scenes_filters = self.get_scenes_filters(params, category)
        if params[self.CUSTOM_SHEET]:
            custom_template = self.get_template(params[self.CUSTOM_SHEET])
            if self.store_type not in custom_template.columns:
                return
            distribution_products = custom_template[(custom_template[self.CATEGORY] == category) &
                                                    (custom_template[self.store_type].apply(bool))]
            distribution_products = distribution_products[self.PRODUCT_EAN].unique().tolist()
        else:
            distribution_products = self.scif[(self.scif['category'] == category) &
                                              (self.scif['in_assort_sc'] == 1)]['product_ean_code'].unique().tolist()
        number_of_distributed_products = 0
        for product_ean_code in distribution_products:
            product_brand = self.all_products[self.all_products['product_ean_code'] == product_ean_code]
            if product_brand.empty:
                # Log.warning('Product EAN {} does not match any active product'.format(product_ean_code))
                continue
            product_brand = product_brand['brand_name'].values[0]
            kpi_name = params[self.KPI_NAME].format(category=category, brand=product_brand, ean=product_ean_code)
            result = int(self.tools.calculate_assortment(product_ean_code=product_ean_code, **scenes_filters))
            self.save_result(set_name, kpi_name, result=result)
            number_of_distributed_products += result
        self.calculation_by_reference(category, set_name, number_of_distributed_products, len(distribution_products))

    def calculation_by_reference(self, category, reference_name, numerator, denominator):
        """
        Given a parent-KPI's results, this function calculates a reference KPI.
        """
        referenced_kpis = self.template_data[self.template_data[self.REFERENCE_KPI] == reference_name]
        for x, params in referenced_kpis.iterrows():
            score = 0
            aggregation_type = params[self.AGGREGATED_SCORE]
            if aggregation_type.startswith('Percentage'):
                if aggregation_type.endswith('Passed'):
                    numerator = numerator
                else:
                    numerator = denominator - numerator
                result = 0 if not denominator else round((numerator / float(denominator)) * 100, 2)
            elif aggregation_type.startswith('Count'):
                if aggregation_type.endswith('All'):
                    result = denominator
                elif aggregation_type.endswith('Passed'):
                    result = numerator
                else:
                    result = denominator - numerator
                score = 0 if not denominator else (result / float(denominator)) * 10000
            else:
                continue
            kpi_name = params[self.KPI_NAME].format(category=category)
            self.save_result(params[self.SET_NAME], kpi_name, score=score, result=result, threshold=denominator)

    def get_scenes_filters(self, params, category):
        """
        This function extracts the scene-type data (==filter) of a given KPI.
        """
        filters = {}
        if params[self.SCENE_TYPES] and params[self.SCENE_TYPES] != 'ALL':
            scene_types = params[self.SCENE_TYPES].split(self.SEPARATOR)
            template_names = []
            for scene_type in scene_types:
                if scene_type == self.CATEGORY_PRIMARY_SHELF:
                    # if category in self.category_scene_types.keys():
                    #     template_names.extend(self.category_scene_types[category])
                    template_names.extend(self.category_scene_types[self.PRIMARY_SHELF])
                elif scene_type == self.DISPLAY:
                    template_names.extend(self.category_scene_types[self.DISPLAY])
                else:
                    template_names.append(scene_type)
            if template_names:
                filters['template_name'] = template_names
        return filters

    def calculation_per_product(self):
        """
        This function calculates all KPIs which are saved per product_ean_code.
        """
        kpis = self.template_data[self.template_data[self.SUB_CALCULATION] == 'Product']
        for x, entity in self.scif.drop_duplicates(subset=['product_ean_code']).iterrows():
            for y, params in kpis.iterrows():
                score = result = threshold = None
                set_name = params[self.SET_NAME]
                kpi_name = params[self.KPI_NAME].format(category=entity['category'], brand=entity['brand_name'],
                                                        ean=entity['product_ean_code'])
                kpi_type = params[self.KPI_TYPE]
                scene_filters = self.get_scenes_filters(params, entity['category'])
                if kpi_type == self.FACING_COUNT:
                    result = int(self.tools.calculate_availability(product_ean_code=entity['product_ean_code'],
                                                                   **scene_filters))
                else:
                    continue
                self.save_result(set_name, kpi_name, score=score, result=result, threshold=threshold)

    def calculation_per_brand(self):
        """
        This function calculates all KPIs which are saved per brand_name.
        """
        kpis = self.template_data[self.template_data[self.SUB_CALCULATION] == 'Brand']
        for x, entity in self.scif.drop_duplicates(subset=['brand_fk']).iterrows():
            for y, params in kpis.iterrows():
                score = result = threshold = None
                set_name = params[self.SET_NAME]
                kpi_name = None
                kpi_type = params[self.KPI_TYPE]
                scene_filters = self.get_scenes_filters(params, entity['category'])
                if kpi_type == self.FACING_COUNT:
                    result = int(self.tools.calculate_availability(brand_fk=entity['brand_fk'], **scene_filters))
                    threshold = int(self.tools.calculate_availability(category_fk=entity['category_fk'], **scene_filters))
                    if set_name == 'Size of Display Raw Data':
                        score = 0 if not threshold else (result / float(threshold)) * 10000
                elif kpi_type == self.FACING_SOS:
                    score = self.tools.calculate_share_of_shelf({'brand_fk': entity['brand_fk']},
                                                                category_fk=entity['category_fk'], **scene_filters)
                    score *= 10000
                elif kpi_type == self.LINEAR_SOS:
                    score, dummy = self.tools.calculate_linear_share_of_shelf({'brand_fk': entity['brand_fk']},
                                                                              category_fk=entity['category_fk'],
                                                                              **scene_filters)
                    score *= 10000
                elif kpi_type == self.SHELF_SPACE_LENGTH:
                    result = int(self.tools.calculate_share_space_length(brand_fk=entity['brand_fk'],
                                                                         category_fk=entity['category_fk'], **scene_filters))
                    threshold = int(self.tools.calculate_share_space_length(category_fk=entity['category_fk'],
                                                                            **scene_filters))
                elif kpi_type == self.ASSORTMENT:
                    if params[self.MANUFACTURERS]:
                        manufacturers = params[self.MANUFACTURERS].split(self.SEPARATOR)
                        if entity['manufacturer_name'] not in manufacturers:
                            continue
                    result = int(self.tools.calculate_assortment(brand_fk=entity['brand_fk']))
                elif kpi_type == self.COUNT_OF_SCENES:
                    brand_aggregation = 0
                    for group in self.template_groups.keys():
                        result = int(self.tools.calculate_number_of_scenes(brand_fk=entity['brand_fk'],
                                                                           template_name=self.template_groups[group]))
                        brand_aggregation += result
                        kpi_name = params[self.KPI_NAME].format(category=entity['category'],
                                                                brand=entity['brand_name'],
                                                                display_type=group)
                        self.save_result(set_name, kpi_name, result=result)
                    reference_kpi = self.template_data[self.template_data[self.REFERENCE_KPI] ==
                                                       params[self.SET_NAME]].iloc[0]
                    reference_name = reference_kpi[self.KPI_NAME].format(category=entity['category'],
                                                                         brand=entity['brand_name'])
                    self.save_result(reference_kpi[self.SET_NAME], reference_name, result=brand_aggregation)
                    continue
                else:
                    continue
                if kpi_name is None:
                    kpi_name = params[self.KPI_NAME].format(category=entity['category'], brand=entity['brand_name'])
                self.save_result(set_name, kpi_name, score=score, result=result, threshold=threshold)

    def calculation_per_manufacturer(self):
        """
        This function calculates all KPIs which are saved per manufacturer_name.
        """
        kpis = self.template_data[self.template_data[self.SUB_CALCULATION] == 'Manufacturer']
        for x, entity in self.scif.drop_duplicates(subset=['manufacturer_fk', 'category']).iterrows():
            for y, params in kpis.iterrows():
                score = result = threshold = None
                set_name = params[self.SET_NAME]
                kpi_name = params[self.KPI_NAME].format(category=entity['category'],
                                                        manufacturer=entity['manufacturer_name'])
                kpi_type = params[self.KPI_TYPE]
                scene_filters = self.get_scenes_filters(params, entity['category'])
                if kpi_type == self.FACING_COUNT:
                    result = int(self.tools.calculate_availability(manufacturer_fk=entity['manufacturer_fk'],
                                                                   category_fk=entity['category_fk'],
                                                                   **scene_filters))
                elif kpi_type == self.FACING_SOS:
                    score = self.tools.calculate_share_of_shelf({'manufacturer_fk': entity['manufacturer_fk']},
                                                                category_fk=entity['category_fk'], **scene_filters)
                    score *= 10000
                elif kpi_type == self.LINEAR_SOS:
                    score, dummy = self.tools.calculate_linear_share_of_shelf({'manufacturer_fk': entity['manufacturer_fk']},
                                                                              category_fk=entity['category_fk'],
                                                                              **scene_filters)
                    score *= 10000
                elif kpi_type == self.SHELF_SPACE_LENGTH:
                    result = int(self.tools.calculate_share_space_length(manufacturer_fk=entity['manufacturer_fk'],
                                                                         category_fk=entity['category_fk'],
                                                                         **scene_filters))
                elif kpi_type == self.ASSORTMENT_SHARE:
                    category_assortment = self.tools.calculate_availability(category_fk=entity['category_fk'])
                    manufacturer_assortment = self.tools.calculate_availability(manufacturer_fk=entity['manufacturer_fk'],
                                                                                category_fk=entity['category_fk'])
                    score = 0 if not category_assortment else manufacturer_assortment / float(category_assortment)
                    score *= 10000
                else:
                    continue
                self.save_result(set_name, kpi_name, score=score, result=result, threshold=threshold)

    def calculation_per_category(self):
        """
        This function calculates all KPIs which are saved per category.
        """
        kpis = self.template_data[self.template_data[self.SUB_CALCULATION] == 'Category']
        for x, entity in self.scif.drop_duplicates(subset=['category_fk']).iterrows():
            for y, params in kpis.iterrows():
                score = result = threshold = None
                set_name = params[self.SET_NAME]
                kpi_name = None
                kpi_type = params[self.KPI_TYPE]
                scene_filters = self.get_scenes_filters(params, entity['category'])
                if kpi_type == self.EMPTY_FACING_SOS:
                    result = self.tools.calculate_share_of_shelf({'product_type': 'Empty'},
                                                                 include_empty=self.tools.INCLUDE_EMPTY,
                                                                 category_fk=entity['category_fk'], **scene_filters)
                    result *= 10000
                elif kpi_type == self.DISTRIBUTION:
                    self.calculate_distribution(entity['category'], params)
                elif kpi_type == self.COUNT_OF_POSM_BY_TYPE:
                    if scene_filters:
                        match_display = self.match_display_in_scene[self.tools.get_filter_condition(
                            self.match_display_in_scene, **scene_filters)]
                    else:
                        match_display = self.match_display_in_scene.copy()
                    custom_sheet = self.get_template(params[self.CUSTOM_SHEET])
                    if self.store_retailer not in custom_sheet.columns:
                        continue
                    filtered_sheet = custom_sheet[(custom_sheet[self.CATEGORY] == entity['category']) &
                                                  (custom_sheet[self.store_retailer].apply(bool))]
                    for display_type in filtered_sheet[self.BRAND].unique():
                        displays = filtered_sheet[filtered_sheet[self.BRAND] == display_type]
                        for display_name in displays[self.CLAIM_NAME].unique():
                            kpi_name = params[self.KPI_NAME].format(category=entity['category'],
                                                                    display_type=display_type,
                                                                    display=display_name)
                            result = len(match_display[match_display['display_name'] == display_name])
                            self.save_result(set_name, kpi_name, result=result)
                    continue
                elif kpi_type == self.COUNT_OF_POSM:
                    custom_sheet = self.get_template(params[self.CUSTOM_SHEET])
                    if self.store_retailer not in custom_sheet.columns:
                        continue
                    filtered_sheet = custom_sheet[(custom_sheet[self.CATEGORY] == entity['category']) &
                                                  (custom_sheet[self.store_retailer].apply(bool))]
                    displays = filtered_sheet[self.CLAIM_NAME].unique().tolist()
                    result = len(match_display[match_display['display_name'].isin(displays)])
                elif kpi_type == self.ASSORTMENT_SHARE:
                    reference_results = self.convert_results_to_df(params[self.REFERENCE_KPI])
                    reference_results = reference_results[reference_results['category'] == entity['category']]
                    category_assortment = self.tools.calculate_assortment(category_fk=entity['category_fk'])
                    for brand in reference_results['brand'].unique():
                        brand_assortment = reference_results[reference_results['brand'] == brand]['result'].sum()
                        score = 0 if not category_assortment else int((brand_assortment / float(category_assortment)) * 10000)
                        kpi_name = params[self.KPI_NAME].format(category=entity['category'], brand=brand)
                        self.save_result(set_name, kpi_name, score=score, result=brand_assortment,
                                         threshold=category_assortment)
                    continue
                elif kpi_type == self.SHARE_OF_DISPLAY:
                    reference_kpi = self.template_data[(~self.template_data[self.SET_NAME].isin([params[self.SET_NAME]])) &
                                                       (self.template_data[self.REFERENCE_KPI] ==
                                                        params[self.REFERENCE_KPI])].iloc[0]
                    reference_results = self.convert_results_to_df(params[self.REFERENCE_KPI])
                    reference_results = reference_results[reference_results['category'] == entity['category']]
                    manufacturers = params[self.MANUFACTURERS].split(self.SEPARATOR)
                    total_share = reference_results['result'].sum()
                    manufacturer_share = 0
                    for brand in reference_results['brand'].unique():
                        brand_share = reference_results[reference_results['brand'] == brand]['result'].sum()
                        # P&G Share
                        brand_manufacturer = self.brand_to_manufacturer.get(brand, '')
                        if brand_manufacturer in manufacturers:
                            manufacturer_share += brand_share
                        # Share for brand
                        score = 0 if not total_share else int((brand_share / float(total_share)) * 10000)
                        reference_name = reference_kpi[self.KPI_NAME].format(category=entity['category'], brand=brand)
                        self.save_result(reference_kpi[self.SET_NAME], reference_name, score=score)
                    score = 0 if not total_share else int((manufacturer_share / float(total_share)) * 10000)
                elif kpi_type == self.EMPTY_SPACES:
                    category_empty = self.empty_spaces.get(entity['category'], {}).get('png', 0)
                    category_facings = int(self.tools.calculate_availability(category_fk=entity['category_fk'], manufacturer_fk=entity['manufacturer_fk']))
                    category_facings_with_empty = category_facings + category_empty
                    if not category_facings_with_empty:
                        score = 0
                    else:
                        score = int((category_empty / float(category_facings_with_empty)) * 10000)
                    result = category_empty
                    threshold = category_facings_with_empty
                else:
                    continue
                if result is not None or score is not None or threshold is not None:
                    if kpi_name is None:
                        kpi_name = params[self.KPI_NAME].format(category=entity['category'])
                    self.save_result(set_name, kpi_name, score=score, result=result, threshold=threshold)

            self.scoring_calculation(category=entity['category'])

    def convert_results_to_df(self, set_name):
        """
        This function extracts the atomic KPI results of a given KPI, and turns it into a dataframe.
        """
        output = []
        if set_name in self.atomic_results.keys():
            for atomic in self.atomic_results[set_name].keys():
                results = {'result': self.atomic_results[set_name][atomic]}
                for data in atomic.split(self.UNICODE_DASH):
                    name, value = data.split(':')
                    results[name.lower()] = value
                output.append(results)
        return pd.DataFrame(output)

    def save_result(self, set_name, kpi_name, score=None, result=None, threshold=None):
        """
        Given a score and/or result and/or threshold, this function writes an atomic KPI to the DB.
        """
        if isinstance(score, float):
            score = int(round(score))
        if isinstance(result, float):
            result = int(round(result))
        if isinstance(threshold, float):
            threshold = int(round(threshold))
        self.write_to_db_result(level=self.LEVEL3, score=score, result=result, threshold=threshold,
                                kpi_name=set_name, atomic_kpi_name=kpi_name.replace("'", ""))

    def scoring_calculation(self, category):
        """
        This function calculates the results of the scoring KPIs, for a given category.
        """
        kpi_name = 'Category:{}'.format(category)
        for x, params in self.scoring_data.iterrows():
            set_name = params[self.SET_NAME]

            target = self.get_target(set_name, category)
            if target is None:
                continue

            kpi_type = params[self.KPI_TYPE]
            reference_kpi = params[self.REFERENCE_KPI]
            scenes_filters = self.get_scenes_filters(params, category)

            if reference_kpi:
                reference_results = self.convert_results_to_df(reference_kpi)
                if reference_results.empty:
                    continue
                reference_results = reference_results[reference_results['category'] == category]
                if kpi_type == 'Percentage Passed':
                    total_calculated = len(reference_results)
                    total_passed = reference_results['result'].sum()
                    result = 0 if not total_calculated else int((total_passed / float(total_calculated)) * 10000)
                else:
                    continue
            else:
                if kpi_type == self.LINEAR_SOS:
                    result, dummy = self.tools.calculate_linear_share_of_shelf(sos_filters={'manufacturer_name': self.OWN_MANUFACTURER},
                                                                               category=category, **scenes_filters)
                    result *= 10000
                elif kpi_type == self.FACING_SOS:
                    result = self.tools.calculate_share_of_shelf(sos_filters={'manufacturer_name': self.OWN_MANUFACTURER},
                                                                 category=category, **scenes_filters)
                    result *= 10000
                elif kpi_type == self.POSM_ASSORTMENT:
                    result = self.calculate_posm_assortment_share(category, params, scenes_filters)
                elif kpi_type == self.SURVEY_QUESTION:
                    survey_template = self.get_template(params[self.CUSTOM_SHEET])
                    survey_template = survey_template[(survey_template[self.KPI_NAME] == set_name) &
                                                      (survey_template[self.CATEGORY] == category)]
                    if survey_template.empty:
                        continue
                    survey_id = None
                    for y, survey in survey_template.iterrows():
                        if self.store_type in survey[self.STORE_TYPE].split(self.SEPARATOR):
                            survey_id = int(float(survey[self.SURVEY_ID]))
                    if survey_id is None:
                        continue
                    result = self.tools.get_survey_answer(('question_fk', survey_id))
                else:
                    continue
            weight = int(float(params[self.WEIGHT]) * 100)
            if isinstance(target, (int, float)):
                score = weight if result >= target else 0
            else:
                score = weight if result == target else 0
            self.save_result(set_name, kpi_name, result=result, score=score, threshold=target)

    def calculate_posm_assortment_share(self, category, params, filters):
        """
        This function calculates the share of POSM assortment (number of unique POSMs out of all POSMs in the template).
        """
        template_data = self.get_template(params[self.CUSTOM_SHEET])
        template_data = template_data[template_data[self.CATEGORY] == category]
        if template_data.empty or self.store_retailer not in template_data.keys():
            return None
        posm_items = template_data[template_data[self.store_retailer].apply(bool)][self.CLAIM_NAME].tolist()
        match_display = self.match_display_in_scene[
            self.tools.get_filter_condition(self.match_display_in_scene, **filters)]
        assortment = len(match_display[match_display['display_name'].isin(posm_items)]['display_name'].unique())
        result = 0 if len(posm_items) == 0 else int((assortment / float(len(posm_items))) * 10000)
        return result

    def write_to_db_result(self, level, score=None, result=None, threshold=None, **kwargs):
        """
        This function creates the result data frame of every KPI (atomic KPI/KPI/KPI set),
        and appends the insert SQL query into the queries' list, later to be written to the DB.
        """
        attributes = self.create_attributes_dict(level, score, result, threshold, **kwargs)
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

    def create_attributes_dict(self, level, score=None, result=None, threshold=None, **kwargs):
        """
        This function creates a data frame with all attributes needed for saving in KPI results tables.

        """
        if level == self.LEVEL1:
            set_name = kwargs['kpi_set_name']
            set_fk = self.kpi_static_data[self.kpi_static_data['kpi_set_name'] == set_name]['kpi_set_fk'].values[0]
            attributes = pd.DataFrame([(set_name, self.session_uid, self.store_id, self.visit_date.isoformat(),
                                        score, set_fk)],
                                      columns=['kps_name', 'session_uid', 'store_fk', 'visit_date', 'score_1',
                                               'kpi_set_fk'])
        elif level == self.LEVEL2:
            kpi_name = kwargs['kpi_name']
            kpi_fk = self.kpi_static_data[self.kpi_static_data['kpi_name'] == kpi_name]['kpi_fk'].values[0]
            attributes = pd.DataFrame([(self.session_uid, self.store_id, self.visit_date.isoformat(),
                                        kpi_fk, kpi_name, score)],
                                      columns=['session_uid', 'store_fk', 'visit_date', 'kpi_fk', 'kpk_name', 'score'])
            self.kpi_results[kpi_name] = score
        elif level == self.LEVEL3:
            kpi_name = kwargs['kpi_name']
            kpi_fk = self.kpi_static_data[self.kpi_static_data['kpi_name'] == kpi_name]['kpi_fk'].values[0]
            kpi_set_name = self.kpi_static_data[self.kpi_static_data['kpi_fk'] == kpi_fk]['kpi_set_name'].values[0]
            atomic_kpi_name = kwargs['atomic_kpi_name']
            attributes = pd.DataFrame([(atomic_kpi_name, self.session_uid, kpi_set_name, self.store_id,
                                        self.visit_date.isoformat(), datetime.utcnow().isoformat(),
                                        score, result, threshold, kpi_fk)],
                                      columns=['display_text', 'session_uid', 'kps_name', 'store_fk', 'visit_date',
                                               'calculation_time', 'score', 'result', 'threshold', 'kpi_fk'])
            if kpi_set_name not in self.atomic_results.keys():
                self.atomic_results[kpi_set_name] = {}
            self.atomic_results[kpi_set_name][atomic_kpi_name] = result if result is not None else score
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
        delete_queries = PNGAU_SANDQueries.get_delete_session_results_query(self.session_uid)
        for query in delete_queries:
            cur.execute(query)
        for query in insert_queries:
            cur.execute(query)
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
