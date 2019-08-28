import os
import pandas as pd
from datetime import datetime
# from timeit import default_timer as timer
from KPIUtils_v2.Utils.Decorators.Decorators import kpi_runtime
from Trax.Algo.Calculations.Core.DataProvider import Data
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
from Trax.Utils.Conf.Keys import DbUsers
from KPIUtils_v2.DB.PsProjectConnector import PSProjectConnector
from Trax.Utils.Logging.Logger import Log
from Trax.Data.Utils.MySQLservices import get_table_insertion_query as insert

from Projects.PNGJP.Utils.Fetcher import PNGJPQueries
from Projects.PNGJP.Utils.GeneralToolBox import PNGJPGENERALToolBox
from Projects.PNGJP.Utils.ParseTemplates import parse_template
from Projects.PNGJP.Data.LocalConsts import Consts


__author__ = 'Nimrod'


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


class PNGJPConsts(object):
    FACING_SOS = 'Facing SOS'
    FACING_SOS_BY_SCENE = 'Facing SOS by Scene'
    LINEAR_SOS = 'Linear SOS'
    SHELF_SPACE_LENGTH = 'Shelf Space Length'
    SHELF_SPACE_LENGTH_BY_SCENE = 'Shelf Space Length by Scene'
    FACING_COUNT = 'Facing Count'
    FACING_COUNT_BY_SCENE = 'Facing Count By Scene'
    DISTRIBUTION = 'Distribution'
    DISTRIBUTION_BY_SCENE = 'Distribution By Scene'
    SHARE_OF_DISPLAY = 'Share of Display'
    COUNT_OF_SCENES = 'Count of Scenes'
    COUNT_OF_SCENES_BY_SCENE_TYPE = 'Count of Scenes by scene type'
    COUNT_OF_POSM = 'Count of POSM'
    POSM_COUNT = 'POSM Count'
    POSM_ASSORTMENT = 'POSM Assortment'
    SURVEY_QUESTION = 'Survey Question'

    SHELF_POSITION = 'Shelf Position'
    BRANDS = 'Brand'
    MANUFACTURERS = 'Manufacturer'
    AGGREGATED_SCORE = 'Aggregated Score'
    REFERENCE_KPI = 'Reference KPI'

    CATEGORY_PRIMARY_SHELF = 'Category Primary Shelf'
    DISPLAY = 'Display'
    PRIMARY_SHELF = 'Primary Shelf'

    KPI_TYPE = 'KPI Type'
    SCENE_TYPES = 'Scene Types'
    KPI_NAME = 'KPI Name'
    CUSTOM_SHEET = 'Custom Sheet'
    PER_CATEGORY = 'Per Category'
    SUB_CALCULATION = 'Sub Calculation'
    VALUES_TO_INCLUDE = 'Values to Include'
    SHELF_LEVEL = 'Shelf Level'
    WEIGHT = 'Weight'
    SET_NAME = 'Set Name'
    UNICODE_DASH = u' \u2013 '

    CATEGORY_LOCAL_NAME = 'category_local_name'
    BRAND_LOCAL_NAME = 'brand_local_name'
    MANUFACTURER_NAME = 'manufacturer_name'
    CATEGORY = 'Category'
    POSM_NAME = 'POSM Name'
    POSM_TYPE = 'POSM Type'
    PRODUCT_NAME = 'Product Name'
    PRODUCT_EAN = 'Product EAN'

    SURVEY_ID = 'Survey Question ID'
    SURVEY_TEXT = 'Survey Question Text'

    SEPARATOR = ','


class PNGJPToolBox(PNGJPConsts):
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
        self.match_display_in_scene = self.get_match_display()
        self.tools = PNGJPGENERALToolBox(self.data_provider, self.output, rds_conn=self.rds_conn)
        if self.visit_date >= datetime(2018, 8, 01).date():
            template_name = 'Template.xlsx'
        elif self.visit_date >= datetime(2018, 01, 01).date():
            template_name = 'Template_1-07.xlsx'
        elif self.visit_date <= datetime(2017, 10, 31).date():
            template_name = 'Template_7-10.xlsx'
        else:
            template_name = 'Template_11-12.xlsx'
        # self.TEMPLATE_PATH = os.path.join(os.path.dirname(os.path.realpath(__file__)), '..', 'Data', template_name)
        self.TEMPLATE_PATH = self.get_template_path(template_name)
        self.template_data = parse_template(self.TEMPLATE_PATH, 'KPIs')
        self.innovation_assortment = parse_template(self.TEMPLATE_PATH, 'Innovation Assortment')
        self.psku_assortment = parse_template(self.TEMPLATE_PATH, 'PSKU Assortment')
        self.scene_types = parse_template(self.TEMPLATE_PATH, 'Category-Scene_Type')
        self.GOLDEN_ZONE_PATH = self.get_template_path('TemplateQualitative.xlsx')
        self.golden_zone_data_criteria = parse_template(self.GOLDEN_ZONE_PATH, 'Golden Zone Criteria')
        self.category_scene_types = self.get_category_scene_types()
        self._custom_templates = {}
        self.scenes_types_for_categories = {}
        self.kpi_static_data = self.get_kpi_static_data()
        self.kpi_results_queries = []
        self.kpi_results = {}
        self.atomic_results = {}
        self.categories = self.all_products['category_fk'].unique().tolist()
        self.display_types = ['Aisle', 'Casher', 'End-shelf', 'Entrance', 'Island', 'Side-End', 'Side-net']
        self.custom_scif_queries = []
        self.session_fk = self.data_provider[Data.SESSION_INFO]['pk'].iloc[0]

    def get_template_path(self, template_name):
        return os.path.join(os.path.dirname(os.path.realpath(__file__)), '..', 'Data', template_name)

    @property
    def rds_conn(self):
        if not hasattr(self, '_rds_conn'):
            self._rds_conn = PSProjectConnector(self.project_name, DbUsers.CalculationEng)
        try:
            pd.read_sql_query('select pk from probedata.session limit 1', self._rds_conn.db)
        except:
            self._rds_conn.disconnect_rds()
            self._rds_conn = PSProjectConnector(self.project_name, DbUsers.CalculationEng)
        return self._rds_conn

    def get_template(self, name):
        if name not in self._custom_templates.keys():
            self._custom_templates[name] = parse_template(self.TEMPLATE_PATH, name)
        return self._custom_templates[name]

    def get_category_scene_types(self):
        scene_types = self.scene_types.copy()
        category_scene_types = {self.PRIMARY_SHELF: []}
        for category in scene_types[self.CATEGORY].unique():
            data = scene_types[scene_types[self.CATEGORY].str.encode("utf8") == category.encode("utf8")]
            types = data[self.SCENE_TYPES].unique().tolist()
            category_scene_types[category] = types
            if category != self.DISPLAY:
                category_scene_types[self.PRIMARY_SHELF].extend(types)
        return category_scene_types

    def get_display_scene_types(self):
        scene_types = self.scene_types.copy()
        relevant_scene_types = scene_types[(scene_types[self.CATEGORY] == self.DISPLAY) &
                                           (scene_types['Store Type'] == self.store_type)]
        if relevant_scene_types.empty:
            relevant_scene_types = scene_types[(scene_types[self.CATEGORY] == self.DISPLAY) &
                                               (scene_types['Store Type'] == 'All')]
        category_scene_types = {}
        for category in relevant_scene_types[self.CATEGORY].unique():
            data = relevant_scene_types[
                relevant_scene_types[self.CATEGORY].str.encode('utf-8') == category.encode('utf-8')]
            category_scene_types[category] = data[self.SCENE_TYPES].unique().tolist()
        return category_scene_types

    def get_kpi_static_data(self):
        """
        This function extracts the static KPI data and saves it into one global data frame.
        The data is taken from static.kpi / static.atomic_kpi / static.kpi_set.
        """
        query = PNGJPQueries.get_all_kpi_data()
        kpi_static_data = pd.read_sql_query(query, self.rds_conn.db)
        return kpi_static_data

    def get_match_display(self):
        """
        This function extracts the display matches data and saves it into one global data frame.
        The data is taken from probedata.match_display_in_scene.
        """
        query = PNGJPQueries.get_match_display(self.session_uid)
        match_display = pd.read_sql_query(query, self.rds_conn.db)
        match_display = match_display.merge(self.scene_info[['scene_fk', 'template_fk']], on='scene_fk', how='left')
        match_display = match_display.merge(self.all_templates, on='template_fk', how='left', suffixes=['', '_y'])
        return match_display

    @log_runtime('Main Calculation')
    def main_calculation(self):
        """
        This function calculates the KPI results.
        """
        for category_fk in self.categories:
            category = \
                self.all_products[self.all_products['category_fk'] == category_fk][self.CATEGORY_LOCAL_NAME].values[0]
            self.category_calculation(category)
        for kpi_set in self.template_data[self.SET_NAME].unique().tolist():
            self.write_to_db_result(score=None, level=self.LEVEL1, kpi_set_name=kpi_set)

    def get_target(self, kpi_name, category):
        target = None
        targets_data = self.get_template('Targets')
        targets_data = targets_data[
            (targets_data[self.KPI_NAME] == kpi_name) & (
                    targets_data[self.CATEGORY].str.encode('utf-8') == category.encode('utf-8'))]
        if not targets_data.empty:
            targets_data = targets_data.iloc[0]
            if self.store_type in targets_data.keys():
                try:
                    target = float(targets_data[self.store_type])
                except ValueError:
                    target = targets_data[self.store_type]
        return target

    @kpi_runtime(kpi_desc='category_calculation')
    def category_calculation(self, category):

        self.calculation_per_entity(category)

        one_score_kpis = self.template_data[~self.template_data[self.SUB_CALCULATION].apply(bool)]
        for p in xrange(len(one_score_kpis)):
            params = one_score_kpis.iloc[p]
            kpi_type = params[self.KPI_TYPE]
            if kpi_type == self.SHARE_OF_DISPLAY:
                self.calculate_share_of_display(category, params)
            elif kpi_type == self.DISTRIBUTION:
                self.calculate_distribution(category, params)
            elif kpi_type == self.DISTRIBUTION_BY_SCENE:
                self.calculate_distribution_by_scene(category, params)
        self.category_aggregation_calculation(category)

    def calculate_share_of_display(self, category, params):
        set_name = params[self.SET_NAME]
        reference_kpi_name = params[self.REFERENCE_KPI]
        try:
            denominator = sum(self.atomic_results[reference_kpi_name].values())
        except KeyError as e:
            # In case there is no display data
            if 'brand' in params[self.KPI_NAME]:
                brands_to_write = \
                self.scif.loc[self.scif[self.CATEGORY_LOCAL_NAME].str.encode('utf-8') == category.encode('utf-8')][
                    self.BRAND_LOCAL_NAME].unique().tolist()
                for brand in brands_to_write:
                    kpi_name = params[self.KPI_NAME].format(category=category, brand=brand)
                    self.write_to_db_result(score=None, level=self.LEVEL3, kpi_set_name=set_name, kpi_name=set_name,
                                            atomic_kpi_name=kpi_name, level3_score=0)
            else:
                kpi_name = params[self.KPI_NAME].format(category=category)
                self.write_to_db_result(score=None, level=self.LEVEL3, kpi_set_name=set_name, kpi_name=set_name,
                                        atomic_kpi_name=kpi_name, level3_score=0)
            return
        if params[self.MANUFACTURERS]:
            numerator = 0
            for item in self.atomic_results[reference_kpi_name].keys():
                if category not in item:
                    continue
                item.encode('utf-8')
                parsed_item = item.split(self.UNICODE_DASH)
                brand = parsed_item[1][6:]
                if ((self.all_products[self.MANUFACTURER_NAME].isin(params[self.MANUFACTURERS].split(self.SEPARATOR))) &
                        (self.all_products[self.BRAND_LOCAL_NAME].str.encode('utf-8') == brand.encode('utf-8'))).any():
                    numerator += self.atomic_results[reference_kpi_name][item]
            result = 0 if denominator == 0 else 100 * (round((numerator / float(denominator)) * 100, 2))
            kpi_name = params[self.KPI_NAME].format(category=category)
            if result > 0:
                self.write_to_db_result(score=None, level=self.LEVEL3, kpi_set_name=set_name, kpi_name=set_name,
                                        atomic_kpi_name=kpi_name, level3_score=result)
            else:
                self.create_attributes_dict(score=None, level=self.LEVEL3, kpi_set_name=set_name, kpi_name=set_name,
                                            atomic_kpi_name=kpi_name, level3_score=result)
        else:
            for item in self.atomic_results[reference_kpi_name].keys():
                if category not in item:
                    continue
                numerator = self.atomic_results[reference_kpi_name][item]
                result = 0 if denominator == 0 else 100 * (round((numerator / float(denominator)) * 100, 2))
                if result > 0:
                    self.write_to_db_result(score=None, level=self.LEVEL3, kpi_set_name=set_name, kpi_name=set_name,
                                            atomic_kpi_name=item, level3_score=result)
                else:
                    self.create_attributes_dict(score=None, level=self.LEVEL3, kpi_set_name=set_name, kpi_name=set_name,
                                                atomic_kpi_name=item, level3_score=result)

    def calculate_posm_assortment_share(self, category, params, filters):
        template_data = self.get_template(params[self.CUSTOM_SHEET])
        if template_data.empty:  # while not all 'Assortment' sheets are filled by the client
            return None
        template_data = template_data[template_data[self.CATEGORY].str.encode('utf-8') == category.encode('utf-8')]
        if template_data.empty or self.store_type not in template_data.keys():
            return None
        posm_items = template_data[template_data[self.store_type].apply(bool)][self.POSM_NAME].tolist()
        match_display = self.match_display_in_scene[
            self.tools.get_filter_condition(self.match_display_in_scene, **filters)]
        result = len(match_display[match_display['display_name'].isin(posm_items)]['display_name'].unique())
        share = result / float(len(posm_items))
        return share

    def calculate_distribution(self, category, params):
        kpi_set_name = params[self.SET_NAME]
        atomic_name = params[self.KPI_NAME]
        if kpi_set_name not in self.atomic_results.keys():
            self.atomic_results[kpi_set_name] = {}

        if params[self.CUSTOM_SHEET]:
            custom_template = self.get_template(params[self.CUSTOM_SHEET])
            if custom_template.empty:  # while not all 'Assortment' sheets are filled by the client
                return
            try:
                distribution_products = custom_template[
                    (custom_template[self.CATEGORY].str.encode('utf-8') == category.encode('utf-8')) &
                    (custom_template[self.store_type].apply(bool))]
                distribution_products = distribution_products.rename(columns={self.PRODUCT_EAN: 'product_ean_code',
                                                                              self.PRODUCT_NAME: 'product_name'})
                distribution_products = distribution_products['product_ean_code'].dropna().unique().tolist()
            except KeyError as e:
                distribution_products = []
        else:
            distribution_products = \
            self.scif[(self.scif[self.CATEGORY_LOCAL_NAME].str.encode('utf-8') == category.encode('utf-8')) &
                      (self.scif['dist_sc'] == 1)]['product_ean_code'].dropna().unique().tolist()

        scenes_filters = self.get_scenes_filters(params, category)
        for product_ean_code in distribution_products:
            try:
                product_brand = self.all_products.loc[self.all_products['product_ean_code'] ==
                                                      product_ean_code][self.BRAND_LOCAL_NAME].values[0]
                atomic_kpi_name = atomic_name.format(category=category, brand=product_brand,
                                                     ean=product_ean_code)
                result = int(self.tools.calculate_assortment(product_ean_code=product_ean_code, **scenes_filters))
                if result > 0:
                    self.write_to_db_result(score=result, level=self.LEVEL3, kpi_set_name=kpi_set_name,
                                            kpi_name=kpi_set_name, atomic_kpi_name=atomic_kpi_name)
                else:
                    self.atomic_results[kpi_set_name][atomic_kpi_name] = result
            except Exception as e:
                Log.warning("product_ean_code:'{}' or atomic_kpi_name does not exist in DB".format(product_ean_code))

    def calculate_distribution_by_scene(self, category, params):
        kpi_set_name = params[self.SET_NAME]
        atomic_name = params[self.KPI_NAME]
        if kpi_set_name not in self.atomic_results.keys():
            self.atomic_results[kpi_set_name] = {}

        if params[self.CUSTOM_SHEET]:
            custom_template = self.get_template(params[self.CUSTOM_SHEET])
            if custom_template.empty:  # while not all 'Assortment' sheets are filled by the client
                return
            try:
                distribution_products = custom_template[
                    (custom_template[self.CATEGORY].str.encode('utf-8') == category.encode('utf-8')) &
                    (custom_template[self.store_type].apply(bool))]
                distribution_products = distribution_products.rename(columns={self.PRODUCT_EAN: 'product_ean_code',
                                                                              self.PRODUCT_NAME: 'product_name'})
            except KeyError as e:
                distribution_products = []
        else:
            distribution_products = self.scif[
                (self.scif[self.CATEGORY_LOCAL_NAME].str.encode('utf-8') == category.encode('utf-8')) & (
                        self.scif['dist_sc'] == 1)]
        scenes_filters = self.get_scenes_filters(params, category)
        for product_ean_code in distribution_products['product_ean_code'].dropna().unique().tolist():
            product_brand, manufacturer = self.all_products.loc[self.all_products['product_ean_code'] ==
                                                                product_ean_code][
                [self.BRAND_LOCAL_NAME, self.MANUFACTURER_NAME]].values[0]
            template_kpi_filters = scenes_filters['template_name']
            for scene_type in template_kpi_filters:
                filters = {'template_name': scene_type, 'product_ean_code': product_ean_code}
                result = int(distribution_products[self.tools.get_filter_condition(distribution_products, **filters)][
                                 'facings'].sum())
                atomic_kpi_name = atomic_name.format(category=category, brand=product_brand,
                                                     ean=product_ean_code, manufacturer=manufacturer,
                                                     scene_type=scene_type)
                if result > 0:
                    self.write_to_db_result(score=result, level=self.LEVEL3, kpi_set_name=kpi_set_name,
                                            kpi_name=kpi_set_name, atomic_kpi_name=atomic_kpi_name)
                else:
                    self.atomic_results[kpi_set_name][atomic_kpi_name] = result

    def category_aggregation_calculation(self, category):
        for p in xrange(len(self.template_data)):
            params = self.template_data.iloc[p]
            kpi_type = params[self.KPI_TYPE]
            if not params[self.REFERENCE_KPI] or kpi_type == self.SHARE_OF_DISPLAY:
                continue
            set_name = params[self.SET_NAME]
            kpi_name = params[self.KPI_NAME]
            kpi_name = kpi_name.format(category=category)
            aggregation_type = params[self.AGGREGATED_SCORE]
            total_calculated = 0
            if aggregation_type and self.atomic_results.get(params[self.REFERENCE_KPI]):
                reference_results = self.convert_results_to_df(params[self.REFERENCE_KPI])
                reference_results = reference_results[
                    reference_results['category'].str.encode('utf-8') == category.encode('utf-8')]
                total_calculated = len(reference_results)

                number_of_failed = len(reference_results[reference_results['result'] == 0])
                number_of_passed = total_calculated - number_of_failed

                if aggregation_type.startswith('Percentage'):
                    if aggregation_type.endswith('Passed'):
                        numerator = number_of_passed
                    else:
                        numerator = number_of_failed
                    result = 0 if not total_calculated else round((numerator / float(total_calculated)) * 100, 2)
                elif aggregation_type.startswith('Count'):
                    if aggregation_type.endswith('All'):
                        result = total_calculated
                    elif aggregation_type.endswith('Passed'):
                        result = number_of_passed
                    else:
                        result = number_of_failed
                elif aggregation_type.startswith('Sum'):
                    result = sum(reference_results['result'])
                else:
                    try:
                        result = int(float(aggregation_type))
                    except ValueError:
                        Log.warning("Aggregation type '{}' is not valid".format(aggregation_type))
                        continue
            else:
                result = 0

            if kpi_name not in self.atomic_results.keys():
                if total_calculated:
                    score = 0 if not total_calculated else 10000 * (result / float(total_calculated))
                else:
                    score = 0
                if set_name == 'POSM Primary':
                    score = None
                    total_calculated = None
                if result > 0 or score > 0:
                    self.write_to_db_result(score=result, level=self.LEVEL3, kpi_set_name=set_name, kpi_name=set_name,
                                            atomic_kpi_name=kpi_name, level3_score=score, threshold=total_calculated)
                else:
                    self.create_attributes_dict(score=result, level=self.LEVEL3, kpi_set_name=set_name,
                                                kpi_name=set_name,
                                                atomic_kpi_name=kpi_name, level3_score=score,
                                                threshold=total_calculated)

    def calculation_per_entity(self, category):
        template_data = self.template_data[self.template_data[self.SUB_CALCULATION].apply(bool)]
        filters = {self.CATEGORY_LOCAL_NAME: category}

        for sub_entity in template_data[self.SUB_CALCULATION].unique():
            entity_kpis = template_data[template_data[self.SUB_CALCULATION] == sub_entity]
            custom_sheet = None
            if sub_entity in self.scif.keys():
                entity_items = self.scif[self.tools.get_filter_condition(self.scif, **filters)][
                    sub_entity].dropna().unique().tolist()

                # entity_items = self.scif[self.scif[self.CATEGORY_LOCAL_NAME] == category][sub_entity].unique().tolist()
            else:
                custom_sheet = self.get_template(entity_kpis[self.CUSTOM_SHEET].values[0])
                if custom_sheet.empty:  # while not all 'Assortment' sheets are filled by the client
                    continue
                entity_items = \
                    custom_sheet[custom_sheet[self.CATEGORY].str.encode('utf-8') == category.encode('utf-8')][
                        sub_entity].unique().tolist()

            for item in entity_items:
                score = None
                threshold = None
                entity_filters = dict({sub_entity: item}, **filters)

                for p in xrange(len(entity_kpis)):
                    score = threshold = result = None
                    params = entity_kpis.iloc[p]
                    set_name = params[self.SET_NAME]
                    kpi_type = params[self.KPI_TYPE]
                    scenes_filters = self.get_scenes_filters(params, category)
                    kpi_filters = dict(scenes_filters, **entity_filters)
                    values_to_include = params[self.VALUES_TO_INCLUDE]
                    if values_to_include and item not in values_to_include.split(self.SEPARATOR):
                        for value in values_to_include.split(self.SEPARATOR):
                            if value not in entity_items:
                                entity_items.append(value)
                        continue
                    result_dict = {}

                    if kpi_type == self.FACING_COUNT:
                        result = int(self.tools.calculate_availability(**kpi_filters))
                        if sub_entity == self.MANUFACTURER_NAME:
                            kpi_name = params[self.KPI_NAME].format(category=category, manufacturer=item)
                            score = threshold = None
                        elif sub_entity == self.BRAND_LOCAL_NAME:
                            kpi_name = params[self.KPI_NAME].format(category=category, brand=item)
                            cat_filters = dict(filters, **scenes_filters)
                            threshold = int(self.tools.calculate_availability(**cat_filters))
                            score = None
                        elif sub_entity == 'product_ean_code':
                            brand = self.all_products.loc[self.all_products['product_ean_code'] == item][
                                self.BRAND_LOCAL_NAME].values[0]
                            kpi_name = params[self.KPI_NAME].format(category=category, brand=brand, ean=item)
                        else:
                            continue

                    elif kpi_type == self.FACING_COUNT_BY_SCENE:
                        template_kpi_filters = scenes_filters['template_name']
                        if sub_entity == self.BRAND_LOCAL_NAME:
                            brand = item
                            score = None
                            manufacturer = self.all_products.loc[
                                self.all_products[self.BRAND_LOCAL_NAME].str.encode('utf-8') == brand.encode('utf-8')][
                                self.MANUFACTURER_NAME].values[0]
                        elif sub_entity == 'product_ean_code':
                            brand, manufacturer = \
                            self.all_products.loc[self.all_products['product_ean_code'] == item][
                                [self.BRAND_LOCAL_NAME, self.MANUFACTURER_NAME]].values[0]

                        updated_scif = self.scif[self.tools.get_filter_condition(self.scif, **kpi_filters)]

                        if set_name == 'Number of Facings (Primary)':
                            denominator_filter = dict(kpi_filters)
                            denominator_filter.pop(sub_entity)
                            updated_scif_denominator = int(
                                self.scif[self.tools.get_filter_condition(self.scif, **denominator_filter)][
                                    'facings'].sum())

                        kpi_filter = {}

                        for scene_type in template_kpi_filters:
                            # dict(kpi_filters)
                            if set_name == 'Number of Facings (Primary)':
                                kpi_filter['template_name'] = scene_type
                                result = int(updated_scif[self.tools.get_filter_condition(updated_scif, **kpi_filter)][
                                                 'facings'].sum())
                                threshold = updated_scif_denominator

                                kpi_name = params[self.KPI_NAME].format(category=category, brand=brand,
                                                                        manufacturer=manufacturer,
                                                                        scene_type=scene_type)
                                scores = {'result': result, 'threshold': threshold}
                                result_dict[kpi_name] = scores
                            else:
                                kpi_filter['template_name'] = scene_type

                                result = int(updated_scif[self.tools.get_filter_condition(updated_scif, **kpi_filter)][
                                                 'facings'].sum())
                                score = int(updated_scif[self.tools.get_filter_condition(updated_scif, **kpi_filter)][
                                                'gross_len_ign_stack'].sum())

                                scores = {'result': result, 'score': score}
                                kpi_name = params[self.KPI_NAME].format(category=category, brand=brand, ean=item,
                                                                        manufacturer=manufacturer,
                                                                        scene_type=scene_type)
                                result_dict[kpi_name] = scores

                    elif kpi_type == self.FACING_SOS:
                        result = self.tools.calculate_share_of_shelf(
                            sos_filters={sub_entity: kpi_filters.pop(sub_entity)}, **kpi_filters)
                        score = int(result * 10000)
                        result = threshold = None
                        if sub_entity == self.MANUFACTURER_NAME:
                            kpi_name = params[self.KPI_NAME].format(category=category, manufacturer=item)
                        elif sub_entity == self.BRAND_LOCAL_NAME:
                            kpi_name = params[self.KPI_NAME].format(category=category, brand=item)
                        elif sub_entity == 'product_type':
                            result = score
                            score = None
                            kpi_name = params[self.KPI_NAME].format(category=category)
                        else:
                            continue

                    elif kpi_type == self.FACING_SOS_BY_SCENE:
                        brand = item
                        template_kpi_filters = scenes_filters['template_name']
                        update_scif = self.scif[self.tools.get_filter_condition(self.scif, **kpi_filters)]
                        for scene_type in template_kpi_filters:
                            kpi_filter = dict(kpi_filters)
                            denominator_filter = dict(kpi_filter)
                            denominator_filter.pop(sub_entity)
                            kpi_filter['template_name'] = scene_type
                            result = update_scif[self.tools.get_filter_condition(update_scif, **{
                                'template_name': scene_type})].facings.sum()
                            threshold = self.scif[
                                self.tools.get_filter_condition(self.scif, **denominator_filter)].facings.sum()
                            sos_filters = {sub_entity: kpi_filter.pop(sub_entity), 'template_name': scene_type}
                            score = self.tools.calculate_share_of_shelf(
                                sos_filters=sos_filters, **denominator_filter)
                            kpi_name = params[self.KPI_NAME].format(category=category, brand=brand,
                                                                    manufacturer=manufacturer, scene_type=scene_type)
                            score = int(score * 10000)
                            scores = {'result': result, 'threshold': threshold, 'score': score}
                            result_dict[kpi_name] = scores

                    elif kpi_type == self.LINEAR_SOS:
                        result, threshold = self.tools.calculate_linear_share_of_shelf(
                            sos_filters={sub_entity: kpi_filters.pop(sub_entity)}, **kpi_filters)
                        score = result * 10000
                        result = None
                        threshold = None
                        if sub_entity == self.MANUFACTURER_NAME:
                            kpi_name = params[self.KPI_NAME].format(category=category, manufacturer=item)
                        elif sub_entity == self.BRAND_LOCAL_NAME:
                            kpi_name = params[self.KPI_NAME].format(category=category, brand=item)
                        else:
                            continue

                    elif kpi_type == self.SHELF_SPACE_LENGTH:
                        result = int(self.tools.calculate_share_space_length(**kpi_filters))
                        score = None
                        if sub_entity == self.MANUFACTURER_NAME:
                            kpi_name = params[self.KPI_NAME].format(category=category, manufacturer=item)
                            threshold = None
                        elif sub_entity == self.BRAND_LOCAL_NAME:
                            kpi_name = params[self.KPI_NAME].format(category=category, brand=item)
                            cat_filters = dict(filters, **scenes_filters)
                            threshold = int(self.tools.calculate_share_space_length(**cat_filters))
                        else:
                            continue

                    elif kpi_type == self.SHELF_SPACE_LENGTH_BY_SCENE:
                        brand = item
                        template_kpi_filters = scenes_filters['template_name']
                        manufacturer = self.all_products.loc[
                            self.all_products[self.BRAND_LOCAL_NAME].str.encode('utf-8') == brand.encode('utf-8')][
                            self.MANUFACTURER_NAME].values[0]

                        update_scif = self.scif[self.tools.get_filter_condition(self.scif, **kpi_filters)]

                        for scene_type in template_kpi_filters:
                            result = int(update_scif[self.tools.get_filter_condition(update_scif,
                                                                                     **{'template_name': scene_type})][
                                             'gross_len_ign_stack'].sum())
                            score = None
                            cat_filters = dict(filters, **{'template_name': kpi_filters['template_name']})
                            threshold = int(self.tools.calculate_share_space_length(**cat_filters))
                            kpi_name = params[self.KPI_NAME].format(category=category, brand=brand,
                                                                    manufacturer=manufacturer, scene_type=scene_type)
                            scores = {'result': result, 'threshold': threshold}
                            result_dict[kpi_name] = scores

                    elif kpi_type == self.SHELF_POSITION:
                        shelves = map(int, params[self.SHELF_LEVEL].split(self.SEPARATOR))
                        result = int(
                            self.tools.calculate_assortment(assortment_entity=self.BRAND_LOCAL_NAME,
                                                            shelf_number_from_bottom=shelves,
                                                            **kpi_filters))
                        score = None
                        threshold = None
                        if sub_entity == self.BRAND_LOCAL_NAME:
                            kpi_name = params[self.KPI_NAME].format(category=category, brand=item)
                        else:
                            continue

                    elif kpi_type == self.COUNT_OF_SCENES:
                        kpi_filter = kpi_filters
                        if params[self.MANUFACTURERS].strip():
                            kpi_filter = dict({self.MANUFACTURER_NAME: params[self.MANUFACTURERS]}, **kpi_filter)
                        result = self.tools.calculate_number_of_scenes(**kpi_filter)
                        score = None
                        threshold = None
                        if set_name == 'Number of Display (by Brand)':
                            kpi_name = params[self.KPI_NAME].format(category=category, brand=item)
                        elif set_name == 'Number of Display (by Category)':
                            kpi_name = params[self.KPI_NAME].format(category=category, brand=item)
                        elif set_name == 'Display Raw Data':
                            old_kpi_filters = scenes_filters['template_name']
                            scenes_to_check = self.scif.loc[
                                (self.scif[self.CATEGORY_LOCAL_NAME].str.encode('utf-8') == category.encode('utf-8')) &
                                (self.scif[sub_entity].str.encode('utf-8') == item.encode('utf-8'))][
                                'scene_fk'].unique().tolist()
                            display_types = \
                                self.match_display_in_scene.loc[self.match_display_in_scene['scene_fk'].isin(
                                    scenes_to_check)]['display_type'].unique().tolist()
                            if not display_types:
                                display_types = self.display_types
                            for display_type in display_types:
                                for scene_type in old_kpi_filters:
                                    if display_type in scene_type:
                                        kpi_filters['template_name'] = scene_type
                                product_kpi_filters = dict({'scene_fk': scenes_to_check}, **kpi_filters)
                                result = self.tools.calculate_number_of_scenes(**product_kpi_filters)
                                kpi_name = params[self.KPI_NAME].format(category=category, brand=item,
                                                                        display_type=display_type)
                                result_dict[kpi_name] = result

                    elif kpi_type == self.COUNT_OF_SCENES_BY_SCENE_TYPE:
                        kpi_filter = kpi_filters
                        if params[self.MANUFACTURERS].strip():
                            kpi_filter = dict({self.MANUFACTURER_NAME: params[self.MANUFACTURERS]}, **kpi_filter)
                        template_kpi_filters = scenes_filters['template_name']
                        score = None
                        threshold = None
                        brand = item
                        manufacturer = self.all_products.loc[
                            self.all_products[self.BRAND_LOCAL_NAME].str.encode('utf-8') == brand.encode('utf-8')][
                            self.MANUFACTURER_NAME].values[0]

                        for scene_type in template_kpi_filters:
                            kpi_filter['template_name'] = scene_type
                            result = self.tools.calculate_number_of_scenes(**kpi_filter)
                            kpi_name = params[self.KPI_NAME].format(category=category, brand=brand, ean=item,
                                                                    manufacturer=manufacturer, scene_type=scene_type)
                            result_dict[kpi_name] = result

                    elif kpi_type == self.COUNT_OF_POSM:
                        match_display = self.match_display_in_scene[self.tools.get_filter_condition(
                            self.match_display_in_scene, **scenes_filters)]
                        score = None
                        threshold = None
                        if sub_entity == 'POSM Type':
                            if params[self.CUSTOM_SHEET] == 'Solution Center POSM':
                                custom_sheet = self.get_template('Solution Center POSM')
                            posm_items = custom_sheet[(custom_sheet[sub_entity] == item) &
                                                      (custom_sheet[self.store_type].apply(bool)) &
                                                      (custom_sheet['Category'].str.encode('utf-8') == category.encode(
                                                          'utf-8'))]['POSM Name'].values
                            for posm in posm_items:
                                kpi_name = params[self.KPI_NAME].format(category=category, display_type=item,
                                                                        display=posm)
                                result = len(match_display[match_display['display_name'].isin([posm])])
                                result_dict[kpi_name] = result
                        elif sub_entity in ('category', 'template_name'):
                            if params[self.CUSTOM_SHEET] == 'Solution Center POSM':
                                custom_sheet = self.get_template('Solution Center POSM')
                            else:
                                custom_sheet = self.get_template('POSM Assortment')
                            posm_items = custom_sheet[(custom_sheet[self.store_type].apply(bool)) & (
                                    custom_sheet['Category'].str.encode('utf-8') == category.encode('utf-8'))][
                                'POSM Name'].values

                            for posm in posm_items:
                                kpi_name = params[self.KPI_NAME].format(category=category, display_type=item,
                                                                        display=posm)
                                result = len(match_display[match_display['display_name'].isin([posm])])
                                result_dict[kpi_name] = result
                        else:
                            continue
                    elif kpi_type == self.POSM_COUNT:
                        match_display = self.match_display_in_scene[self.tools.get_filter_condition(
                            self.match_display_in_scene, **scenes_filters)]
                        score = None
                        threshold = None
                        if sub_entity == 'POSM Type':
                            if params[self.CUSTOM_SHEET] == 'Solution Center POSM':
                                custom_sheet = self.get_template('Solution Center POSM')
                            posm_items = custom_sheet[(custom_sheet[sub_entity] == item) &
                                                      (custom_sheet[self.store_type].apply(bool)) &
                                                      (custom_sheet['Category'].str.encode('utf-8') == category.encode(
                                                          'utf-8'))]['POSM Name'].values

                            kpi_name = params[self.KPI_NAME].format(category=category)
                            result = len(match_display[match_display['display_name'].isin(posm_items)])
                            result_dict[kpi_name] = result
                        elif sub_entity in ('category', 'template_name'):
                            if params[self.CUSTOM_SHEET] == 'Solution Center POSM':
                                custom_sheet = self.get_template('Solution Center POSM')
                            else:
                                custom_sheet = self.get_template('POSM Assortment')

                            posm_items = custom_sheet[(custom_sheet[self.store_type].apply(bool)) & (
                                    custom_sheet['Category'].str.encode('utf-8') == category.encode('utf-8'))][
                                'POSM Name'].values

                            kpi_name = params[self.KPI_NAME].format(category=category)
                            result = len(match_display[match_display['display_name'].isin(posm_items)])
                            result_dict[kpi_name] = result
                        else:
                            continue
                    else:
                        # Log.warning("KPI type '{}' is not supported".format(kpi_type))
                        continue

                    if result is not None or score is not None:
                        if not kpi_name:
                            kpi_name = params[self.KPI_NAME].format(category=category)
                        if score is None and threshold is None:
                            if not result_dict:
                                if result > 0:
                                    self.write_to_db_result(score=result, level=self.LEVEL3, kpi_set_name=set_name,
                                                            kpi_name=set_name, atomic_kpi_name=kpi_name)
                                else:
                                    self.create_attributes_dict(score=result, level=self.LEVEL3, kpi_set_name=set_name,
                                                                kpi_name=set_name, atomic_kpi_name=kpi_name)
                            else:
                                for kpi_name, result in result_dict.items():
                                    if type(result) == dict:
                                        if result['result'] > 0:
                                            self.write_to_db_result(score=result['result'], level=self.LEVEL3,
                                                                    level3_score=result['score'], kpi_set_name=set_name,
                                                                    kpi_name=set_name, atomic_kpi_name=kpi_name)
                                        else:
                                            self.create_attributes_dict(score=result['result'], level=self.LEVEL3,
                                                                        level3_score=result['score'],
                                                                        kpi_set_name=set_name,
                                                                        kpi_name=set_name, atomic_kpi_name=kpi_name)
                                    else:
                                        if result > 0:
                                            self.write_to_db_result(score=result, level=self.LEVEL3,
                                                                    kpi_set_name=set_name, kpi_name=set_name,
                                                                    atomic_kpi_name=kpi_name)
                                        else:
                                            self.create_attributes_dict(score=result, level=self.LEVEL3,
                                                                        kpi_set_name=set_name, kpi_name=set_name,
                                                                        atomic_kpi_name=kpi_name)
                                result_dict = {}

                        elif score is not None and threshold is None:
                            if not result_dict:
                                if result > 0 or score > 0:
                                    self.write_to_db_result(score=result, level=self.LEVEL3, level3_score=score,
                                                            kpi_set_name=set_name, kpi_name=set_name,
                                                            atomic_kpi_name=kpi_name)
                                    # else:
                                    #     self.create_attributes_dict(score=result, level=self.LEVEL3, level3_score=score,
                                    #                             kpi_set_name=set_name, kpi_name=set_name,
                                    #                             atomic_kpi_name=kpi_name)
                            else:
                                for kpi_name, result in result_dict.items():
                                    if type(result) == dict:
                                        if result['result'] > 0 and result['score'] > 0:
                                            self.write_to_db_result(score=result['result'], level=self.LEVEL3,
                                                                    level3_score=result['score'],
                                                                    kpi_set_name=set_name, kpi_name=set_name,
                                                                    atomic_kpi_name=kpi_name)
                                            # else:
                                            #     self.create_attributes_dict(score=result['result'], level=self.LEVEL3,
                                            #                             level3_score=result['score'],
                                            #                             kpi_set_name=set_name, kpi_name=set_name,
                                            #                             atomic_kpi_name=kpi_name)
                                    else:

                                        if result > 0 or score > 0:
                                            self.write_to_db_result(score=result, level=self.LEVEL3, level3_score=score,
                                                                    kpi_set_name=set_name, kpi_name=set_name,
                                                                    atomic_kpi_name=kpi_name)
                                            # else:
                                            #     self.create_attributes_dict(score=result, level=self.LEVEL3,
                                            #                                 level3_score=score,
                                            #                                 kpi_set_name=set_name, kpi_name=set_name,
                                            #                                 atomic_kpi_name=kpi_name)
                                result_dict = {}
                        elif score is None and threshold is not None:
                            if not result_dict:
                                if result > 0 and threshold > 0:
                                    self.write_to_db_result(score=result, level=self.LEVEL3, threshold=threshold,
                                                            kpi_set_name=set_name,
                                                            kpi_name=set_name, atomic_kpi_name=kpi_name)
                                    # else:
                                    #     self.create_attributes_dict(score=result, level=self.LEVEL3, threshold=threshold,
                                    #                                 kpi_set_name=set_name,
                                    #                                 kpi_name=set_name, atomic_kpi_name=kpi_name)
                            else:
                                for kpi_name, result in result_dict.items():
                                    if type(result) == dict:
                                        if result['result'] > 0 and result['threshold'] > 0:
                                            self.write_to_db_result(score=result['result'], level=self.LEVEL3,
                                                                    threshold=result['threshold'],
                                                                    kpi_set_name=set_name, kpi_name=set_name,
                                                                    atomic_kpi_name=kpi_name)
                                            # else:
                                            #     self.create_attributes_dict(score=result['result'], level=self.LEVEL3,
                                            #                                 threshold=result['threshold'],
                                            #                                 kpi_set_name=set_name, kpi_name=set_name,
                                            #                                 atomic_kpi_name=kpi_name)
                                    else:
                                        for kpi_name, result in result_dict.items():
                                            if result > 0 or threshold > 0:
                                                self.write_to_db_result(score=result, level=self.LEVEL3,
                                                                        threshold=threshold, kpi_set_name=set_name,
                                                                        kpi_name=set_name, atomic_kpi_name=kpi_name)
                                                # else:
                                                #     self.create_attributes_dict(score=result, level=self.LEVEL3,
                                                #                                 threshold=threshold, kpi_set_name=set_name,
                                                #                                 kpi_name=set_name, atomic_kpi_name=kpi_name)
                                result_dict = {}
                        else:
                            if not result_dict:
                                if result > 0 or threshold > 0 or score > 0:
                                    self.write_to_db_result(score=result, level=self.LEVEL3, level3_score=score,
                                                            threshold=threshold, kpi_set_name=set_name,
                                                            kpi_name=set_name, atomic_kpi_name=kpi_name)
                                    # else:
                                    #     self.create_attributes_dict(score=result, level=self.LEVEL3, level3_score=score,
                                    #                                 threshold=threshold, kpi_set_name=set_name,
                                    #                                 kpi_name=set_name, atomic_kpi_name=kpi_name)
                            else:
                                for kpi_name, result in result_dict.items():
                                    if type(result) == dict:
                                        if result['result'] > 0 and result['threshold'] > 0 and result['score'] > 0:
                                            self.write_to_db_result(score=result['result'], level=self.LEVEL3,
                                                                    level3_score=result['score'],
                                                                    threshold=result['threshold'],
                                                                    kpi_set_name=set_name,
                                                                    kpi_name=set_name, atomic_kpi_name=kpi_name)
                                            # else:
                                            #     self.create_attributes_dict(score=result['result'], level=self.LEVEL3,
                                            #                                 level3_score=result['score'],
                                            #                                 threshold=result['threshold'],
                                            #                                 kpi_set_name=set_name,
                                            #                                 kpi_name=set_name, atomic_kpi_name=kpi_name)
                                    else:
                                        if result > 0 or threshold > 0 or score > 0:
                                            self.write_to_db_result(score=result, level=self.LEVEL3, level3_score=score,
                                                                    threshold=threshold, kpi_set_name=set_name,
                                                                    kpi_name=set_name, atomic_kpi_name=kpi_name)
                                            # else:
                                            #     self.create_attributes_dict(score=result, level=self.LEVEL3,
                                            #                                 level3_score=score,
                                            #                                 threshold=threshold, kpi_set_name=set_name,
                                            #                                 kpi_name=set_name, atomic_kpi_name=kpi_name)
                                result_dict = {}
                            score = None
                            threshold = None

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

    def get_scenes_filters(self, params, category):
        filters = {}
        if params[self.SCENE_TYPES]:
            scene_types = params[self.SCENE_TYPES].split(self.SEPARATOR)
            template_names = []
            for scene_type in scene_types:
                if scene_type == self.CATEGORY_PRIMARY_SHELF:
                    # if category in self.category_scene_types.keys():
                    #     template_names.extend(self.category_scene_types[category])
                    template_names.extend(self.category_scene_types[self.PRIMARY_SHELF])
                elif scene_type == self.DISPLAY:
                    relevant_scene_types = self.get_display_scene_types()
                    template_names.extend(relevant_scene_types[self.DISPLAY])
                else:
                    template_names.append(scene_type)
            if template_names:
                filters['template_name'] = template_names
        return filters

    @kpi_runtime(kpi_desc='update_custom_scene_item_facts')
    def hadle_update_custom_scif(self):
        """
        This function updates the custom scif of PS with oos and assortment values for each product in each scene.
        :return:
        """
        store_type = self.store_type
        try:
            psku_assortment_products = self.psku_assortment['Product EAN'][self.psku_assortment[store_type] == '1']
            psku_assortment_products = self.all_products['product_fk'][
                self.all_products['product_ean_code'].isin(psku_assortment_products)]

            innovation_assortment_products = self.innovation_assortment['Product EAN'][
                self.psku_assortment[store_type] == '1']
            innovation_assortment_products = self.all_products['product_fk'][
                self.all_products['product_ean_code'].isin(innovation_assortment_products)]
        except Exception as e:
            Log.warning("store_type '{}' is not valid : {}".format(store_type, e.message))
            innovation_assortment_products = psku_assortment_products = []

        assortment_products = list(innovation_assortment_products.append(psku_assortment_products).unique())

        psku_assortment_products = psku_assortment_products.tolist()
        innovation_assortment_products = innovation_assortment_products.tolist()
        # shelves = [4, 5]
        all_scenes_in_scif = self.scif[Consts.SCENE_FK].unique().tolist()

        if all_scenes_in_scif:
            products_in_session = self.scif.loc[self.scif['dist_sc'] == 1][Consts.PRODUCT_FK].unique().tolist()
            for product in assortment_products:
                if product in products_in_session:
                    # This means the product in assortment and is not oos. (1,0)
                    scenes = self.get_scenes_for_product(product)
                    in_assortment_OSA = oos_osa = mha_in_assortment = mha_oos = 0

                    if product in psku_assortment_products:
                        in_assortment_OSA = 1
                    if product in innovation_assortment_products:
                        mha_in_assortment = 1

                    for scene in scenes:
                        result = int(self.tools.calculate_linear_facings_on_golden_zone(self.golden_zone_data_criteria, linear = True
                                                                                 ,product_fk=product, scene_fk=scene))
                        length_mm_custom = result if result else 0
                        self.get_custom_query(scene, product, in_assortment_OSA, oos_osa, mha_in_assortment, mha_oos,
                                              length_mm_custom)
                else:
                    # The product is in assortment list but is oos (1,1)
                    in_assortment_OSA = oos_osa = mha_in_assortment = mha_oos = length_mm_custom = 0

                    if product in psku_assortment_products:
                        in_assortment_OSA = oos_osa = 1

                    if product in innovation_assortment_products:
                        mha_in_assortment = mha_oos = 1

                    scenes = self.get_scenes_for_product(product)
                    if scenes:
                        for scene in scenes:
                            self.get_custom_query(scene, product, in_assortment_OSA, oos_osa, mha_in_assortment,
                                                  mha_oos,
                                                  length_mm_custom)
                    else:
                        self.get_custom_query(all_scenes_in_scif[0], product, in_assortment_OSA, oos_osa,
                                              mha_in_assortment,
                                              mha_oos, length_mm_custom)

            products_not_in_assortment = self.scif[~self.scif[Consts.PRODUCT_FK].isin(assortment_products)]
            for product in products_not_in_assortment[Consts.PRODUCT_FK].unique().tolist():
                # The product is not in assortment list and not oos. (0,0)
                scenes = self.get_scenes_for_product(product)
                for scene in scenes:
                    result = int(self.tools.calculate_linear_facings_on_golden_zone(self.golden_zone_data_criteria, linear=True
                                                                                 ,product_fk=product, scene_fk=scene))
                    length_mm_custom = result if result else 0
                    self.get_custom_query(scene, product, 0, 0, 0, 0, length_mm_custom)

            self.commit_custom_scif()

    def get_scenes_for_product(self, product_fk=None):
        """
        This function find all scene_fk where a product was in.
        :param product_fk:
        :return: a list of scenes_fks
        """
        if product_fk:
            product_scif = self.scif.loc[self.scif['product_fk'] == product_fk]
        else:
            product_scif = self.scif

        scenes = product_scif['scene_fk'].unique().tolist()
        return scenes

    def get_custom_query(self, scene_fk, product_fk, in_assortment_OSA, oos_osa, mha_in_assortment, mha_oos,
                         length_mm_custom):
        """
        This gets the query for insertion to PS custom scif
        :param length_mm_custom:
        :param mha_oos:
        :param mha_in_assortment:
        :param oos_osa:
        :param in_assortment_OSA:
        :param scene_fk:
        :param product_fk:
        :return:
        """
        attributes = pd.DataFrame([(
            self.session_fk, scene_fk, product_fk, in_assortment_OSA, oos_osa, mha_in_assortment,
            mha_oos, length_mm_custom)],
            columns=['session_fk', 'scene_fk', 'product_fk', 'in_assortment_OSA', 'oos_osa',
                     'mha_in_assortment', 'mha_oos', 'length_mm_custom'])

        query = insert(attributes.to_dict(), Consts.PSERVICE_CUSTOM_SCIF)
        self.custom_scif_queries.append(query)

    def write_to_db_result(self, score, level, threshold=None, level3_score=None, **kwargs):
        """
        This function creates the result data frame of every KPI (atomic KPI/KPI/KPI set),
        and appends the insert SQL query into the queries' list, later to be written to the DB.
        """
        attributes = self.create_attributes_dict(score, level, threshold, level3_score, **kwargs)
        if level == self.LEVEL1:
            table = Consts.KPS_RESULT
        elif level == self.LEVEL2:
            table = Consts.KPK_RESULT
        elif level == self.LEVEL3:
            table = Consts.KPI_RESULT
        else:
            return
        query = insert(attributes, table)
        self.kpi_results_queries.append(query)

    def create_attributes_dict(self, score, level, threshold=None, level3_score=None, **kwargs):
        """
        This function creates a data frame with all attributes needed for saving in KPI results tables.

        """
        if level == self.LEVEL1:
            set_name = kwargs['kpi_set_name']
            set_fk = self.kpi_static_data[self.kpi_static_data['kpi_set_name'] == set_name]['kpi_set_fk'].values[0]
            if score is not None:
                attributes = pd.DataFrame([(set_name, self.session_uid, self.store_id, self.visit_date.isoformat(),
                                            format(score, '.2f'), set_fk)],
                                          columns=['kps_name', 'session_uid', 'store_fk', 'visit_date', 'score_1',
                                                   'kpi_set_fk'])
            else:
                attributes = pd.DataFrame([(set_name, self.session_uid, self.store_id, self.visit_date.isoformat(),
                                            None, set_fk)],
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
            atomic_kpi_name = kwargs['atomic_kpi_name']
            kpi_set_name = kwargs['kpi_set_name']
            if level3_score is None and threshold is None:
                attributes = pd.DataFrame([(atomic_kpi_name, self.session_uid, kpi_set_name, self.store_id,
                                            self.visit_date.isoformat(), datetime.utcnow().isoformat(), score, kpi_fk)],
                                          columns=['display_text', 'session_uid', 'kps_name', 'store_fk', 'visit_date',
                                                   'calculation_time', 'result', 'kpi_fk'])
            elif level3_score is not None and threshold is None:
                attributes = pd.DataFrame([(atomic_kpi_name, self.session_uid, kpi_set_name, self.store_id,
                                            self.visit_date.isoformat(), datetime.utcnow().isoformat(), score, kpi_fk,
                                            level3_score, None)],
                                          columns=['display_text', 'session_uid', 'kps_name', 'store_fk', 'visit_date',
                                                   'calculation_time', 'result', 'kpi_fk', 'score', 'threshold'])
            elif level3_score is None and threshold is not None:
                attributes = pd.DataFrame([(atomic_kpi_name, self.session_uid, kpi_set_name, self.store_id,
                                            self.visit_date.isoformat(), datetime.utcnow().isoformat(), score, kpi_fk,
                                            threshold, None)],
                                          columns=['display_text', 'session_uid', 'kps_name', 'store_fk', 'visit_date',
                                                   'calculation_time', 'result', 'kpi_fk', 'threshold', 'score'])
            else:
                attributes = pd.DataFrame([(atomic_kpi_name, self.session_uid, kpi_set_name, self.store_id,
                                            self.visit_date.isoformat(), datetime.utcnow().isoformat(), score, kpi_fk,
                                            threshold, level3_score)],
                                          columns=['display_text', 'session_uid', 'kps_name', 'store_fk', 'visit_date',
                                                   'calculation_time', 'result', 'kpi_fk', 'threshold', 'score'])
            if kpi_set_name not in self.atomic_results.keys():
                self.atomic_results[kpi_set_name] = {}
            self.atomic_results[kpi_set_name][atomic_kpi_name] = score
        else:
            attributes = pd.DataFrame()
        return attributes.to_dict()

    def commit_custom_scif(self):
        if not self.rds_conn.is_connected:
            self.rds_conn.connect_rds()
        cur = self.rds_conn.db.cursor()
        delete_query = PNGJPQueries.get_delete_session_custom_scif(self.session_fk)
        cur.execute(delete_query)
        self.rds_conn.db.commit()
        queries = self.merge_insert_queries(self.custom_scif_queries)
        for query in queries:
            try:
                cur.execute(query)
            except:
                print 'could not run query: {}'.format(query)
        self.rds_conn.db.commit()

    @log_runtime('Saving to DB')
    def commit_results_data(self):
        """
        This function writes all KPI results to the DB, and commits the changes.
        """
        cur = self.rds_conn.db.cursor()
        delete_queries = PNGJPQueries.get_delete_session_results_query(self.session_uid)
        for query in delete_queries:
            cur.execute(query)
        queries = self.merge_insert_queries(self.kpi_results_queries)
        for query in queries:
            cur.execute(query)
        self.rds_conn.db.commit()

    def merge_insert_queries(self, insert_queries):
        # other_queries = []
        query_groups = {}
        for query in insert_queries:
            if 'update' in query:
                self.update_queries.append(query)
            else:
                static_data, inserted_data = query.split('VALUES ')
                if static_data not in query_groups:
                    query_groups[static_data] = []
                query_groups[static_data].append(inserted_data)
        merged_queries = []
        for group in query_groups:
            for group_index in xrange(0, len(query_groups[group]), 10 ** 4):
                merged_queries.append('{0} VALUES {1}'.format(group, ',\n'.join(query_groups[group]
                                                                                [group_index:group_index + 10 ** 4])))
        # merged_queries.extend(other_queries)
        return merged_queries
