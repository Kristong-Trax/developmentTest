import os
import pandas as pd
import numpy as np
from Trax.Algo.Calculations.Core.DataProvider import Data
from Trax.Cloud.Services.Connector.Keys import DbUsers
from KPIUtils_v2.DB.PsProjectConnector import PSProjectConnector
from Trax.Utils.Logging.Logger import Log

from Projects.CCBZA_SAND.Utils.Fetcher import CCBZA_SAND_Queries
from Projects.CCBZA_SAND.Utils.ParseTemplates import parse_template
from Projects.CCBZA_SAND.Utils.GeneralToolBox import CCBZA_SAND_GENERALToolBox

__author__ = 'natalyak'

KPI_RESULT = 'report.kpi_results'
KPK_RESULT = 'report.kpk_results'
KPS_RESULT = 'report.kps_results'

# Template tabs
KPI_TAB = 'KPI'
# SET_TAB = 'Set'
PLANOGRAM_TAB = 'Planogram'
PRICE_TAB = 'Price'
SURVEY_TAB = 'Survey'
AVAILABILITY_TAB = 'Availability'
SOS_TAB = 'SOS'
COUNT_TAB = 'Count'

# Template columns
SET_NAME = 'Set Name'
KPI_NAME = 'KPI Name'
KPI_TYPE = 'KPI Type'
SPLIT_SCORE = 'Split Score'
DEPENDENCY = 'Dependancy'
ATOMIC_KPI_NAME = 'Atomic KPI Name'
TARGET = 'Target'
SCORE = 'Score'
STORE_TYPE = 'Store Type'
ATTRIBUTE_1 = 'Attribute_1'
ATTRIBUTE_2 = 'Attribute_2'
EXPECTED_RESULT = 'Expected Result'
SURVEY_QUESTION_CODE = 'Survey Q CODE'
TEMPLATE_NAME = 'Template Name'
TEMPLATE_GROUP = 'Template Group'
CONDITION_1_NUMERATOR = 'Condition 1 - Numerator'
CONDITION_1_NUMERATOR_TYPE = 'Condition 1 - Numerator Type'
CONDITION_1_DENOMINATOR = 'Condition 1 - Denominator'
CONDITION_1_DENOMINATOR_TYPE = 'Condition 1 - Denominator Type'
CONDITION_1_TARGET = 'Condition 1 - Target'
CONDITION_2_NUMERATOR = 'Condition 2 - Numerator'
CONDITION_2_NUMERATOR_TYPE = 'Condition 2 - Numerator Type'
CONDITION_2_DENOMINATOR = 'Condition 2 - Denominator'
CONDITION_2_DENOMINATOR_TYPE = 'Condition 2 - Denominator Type'
CONDITION_2_TARGET = 'Condition 2 - Target'
AVAILABILITY_TYPE = 'type avia'
TYPE1 = 'type1'
TYPE2 = 'type2'
TYPE3 = 'type3'
VALUE1 = 'value1'
VALUE2 = 'value2'
VALUE3 = 'value3'
TEMPLATE_DISPLAY_NAME = 'Template Display Name'
KO_ONLY = 'KO Only'
BY_SCENE = 'By Scene'

#Other constants
CONDITION_1 = 'Condition 1'
CONDITION_2 = 'Condition 2'
NUMER = '- Numerator'
DENOM = '- Denominator'
TYPE = 'Type'
C_TARGET = '- Target'
CONDITION_FIELDS = ['- Numerator', '- Numerator Type', '- Denominator', '- Denominator Type', '- Target']
AVAILABILITY_POS = 'Availability POS'
AVAILABILITY_SKU_FACING_AND = 'Availability SKU facing And'
AVAILABILITY_SKU_FACING_OR = 'Availability SKU facing Or'
AVAILABILITY_POSM = 'Availability POSM'
AVAILABLITY_IF_THEN = 'Availability If Then'
KO_PRODUCTS = 'KO PRODUCTS'
GENERAL_FILTERS = 'general_filters'
KPI_SPECIFIC_FILTERS = 'kpi_specific_filters'

# scif fields
EAN_CODE = 'product_ean_code'
BRAND_NAME = 'brand_name'
POS_BRAND_FIELD = 'brand' # in the labels will need to be changed
PRODUCT_TYPE = 'product_type'
NUMERIC_FIELDS = ['size']
MANUFACTURER_NAME = 'manufacturer_name'

#scif values
SKU = 'SKU'
POS = 'POS'
OTHER = 'Other'
MAX_SCORE = 'Max_Score'
IRRELEVANT = 'Irrelevant'
EMPTY = 'Empty'
KO_ID = 1 # to checkout
CORRECT = 1

#
RESULT = 'Result'

class CCBZA_SANDSceneToolBox:
    LEVEL1 = 1
    LEVEL2 = 2
    LEVEL3 = 3

    def __init__(self, data_provider, output, common):
        self.output = output
        self.data_provider = data_provider
        self.common = common
        self.project_name = self.data_provider.project_name
        self.session_uid = self.data_provider.session_uid
        self.products = self.data_provider[Data.PRODUCTS]
        self.templates = self.data_provider[Data.TEMPLATES]
        self.all_products = self.data_provider[Data.ALL_PRODUCTS]
        self.match_product_in_scene = self.data_provider[Data.MATCHES]
        self.visit_date = self.data_provider[Data.VISIT_DATE]
        self.session_info = self.data_provider[Data.SESSION_INFO]
        self.scene_info = self.data_provider[Data.SCENES_INFO]
        self.store_id = self.data_provider[Data.STORE_FK]
        self.store_type = self.data_provider.store_type
        self.rds_conn = PSProjectConnector(self.project_name, DbUsers.CalculationEng)
        self.kpi_static_data = self.common.get_kpi_static_data()
        self.kpi_results_queries = []

        self.tools = CCBZA_SAND_GENERALToolBox(self.data_provider, self.output)
        self.scif = self.data_provider[Data.SCENE_ITEM_FACTS] # do we have it for scene??? debug CCIT
        self.store_data = self.get_store_data_by_store_id()
        self.template_path = self.get_template_path()
        self.template_data = self.get_template_data()
        self.session_kpis = self.template_data[KPI_TAB][KPI_NAME].unique().tolist()
        self.kpi_score_values = self.get_kpi_score_values_df()
        self.kpi_result_values = self.get_kpi_result_values_df()  # maybe for ps data provider, create mock

    def get_template_path(self):
        store_type = self.store_data['store_type'].values[0]      # to check
        # print store_type
        template_name = 'Template_{}.xlsx'.format(store_type)
        return os.path.join(os.path.dirname(os.path.realpath(__file__)), '..', 'Data', template_name)

    def get_template_data(self):
        template_data = {}
        try:
            sheet_names = pd.ExcelFile(self.template_path).sheet_names
            for sheet in sheet_names:
                template_data[sheet] = parse_template(self.template_path, sheet, lower_headers_row_index=0)
        except IOError as e:
            # raise DataError('Template for store type {} does not exist'.format(self.store_data['store_type'].values[0])) # ask Israel
            Log.error('Template for store type {} does not exist. {}'.format(self.store_data['store_type'].values[0], repr(e)))
        return template_data

    def get_store_data_by_store_id(self):
        query = CCBZA_SAND_Queries.get_store_data_by_store_id(self.store_id)
        query_result = pd.read_sql_query(query, self.rds_conn.db)
        return query_result

    def scene_calculation(self, *args, **kwargs):
        if not self.template_data:
            # raise DataError('Template data is empty does not exist')
            Log.error('Template data is empty does not exist') # it should interrupt the code!
            return
        for kpi_name in self.session_kpis:
            kpi = self.template_data[KPI_TAB][self.template_data[KPI_TAB][KPI_NAME] == kpi_name]
            kpi_types = self.get_kpi_types_by_kpi(kpi)
            for kpi_type in kpi_types:
                atomic_kpis_data = self.get_atomic_kpis_data(kpi_type, kpi)
                if not atomic_kpis_data.empty:
                    if kpi_type == 'Availability':
                        self.calculate_availability_scene(atomic_kpis_data, identifier_result)
                    elif kpi_type == 'Price':
                        self.calculate_price_scene(atomic_kpis_data, identifier_result)
                    else:
                        Log.warning("KPI of type '{}' is not supported by scene calculations".format(kpi_type))
                        continue

    def get_kpi_types_by_kpi(self, kpi):
        kpi_types_list = []
        if kpi[KPI_TYPE]:
            kpi_types = self.template_data[KPI_TAB][self.template_data[KPI_TAB][KPI_NAME] == kpi[KPI_NAME]][KPI_TYPE].values[0]
            kpi_types_list = self.split_and_strip(kpi_types)
        return kpi_types_list

    @staticmethod
    def split_and_strip(string):
        return map(lambda x: x.strip(' '), str(string).split(',')) if string else []

    def get_atomic_kpis_data(self, kpi_type, kpi):
        atomic_kpis_data = self.template_data[kpi_type][(self.template_data[kpi_type][KPI_NAME] == kpi[KPI_NAME]) &
                                                        (self.template_data[kpi_type][STORE_TYPE].isin(self.store_data['store_type'])) &
                                                        ((self.template_data[kpi_type][ATTRIBUTE_1].isin(self.store_data['additional_attribute_1']))
                                                        |(self.template_data[kpi_type][ATTRIBUTE_1]=='')) &
                                                        ((self.template_data[kpi_type][ATTRIBUTE_2].isin(self.store_data['additional_attribute_2']))
                                                        |(self.template_data[kpi_type][ATTRIBUTE_2]==''))]
        return atomic_kpis_data

    def calculate_availability_scene(self, atomic_kpis_data, identifier_parent):
        for i in xrange(len(atomic_kpis_data)):
            atomic_kpi = atomic_kpis_data.iloc[i]
            is_by_scene = self.is_by_scene(atomic_kpi)
            score = 0
            if is_by_scene:
                if atomic_kpi[AVAILABILITY_TYPE] == AVAILABILITY_POS:
                    score = self.calculate_availability_brand_strips(atomic_kpi)
                elif atomic_kpi[AVAILABILITY_TYPE] == AVAILABILITY_SKU_FACING_AND:
                    score = self.calculate_availability_sku_facing_and(atomic_kpi)
                elif atomic_kpi[AVAILABILITY_TYPE] == AVAILABILITY_SKU_FACING_OR:
                    score = self.calculate_availability_sku_facing_or(atomic_kpi)
                elif atomic_kpi[AVAILABILITY_TYPE] == AVAILABLITY_IF_THEN:
                    score = self.calculate_avialability_against_competitors(atomic_kpi)
                else:
                    Log.warning('Availablity of type {} is not supported by calculation'.format(atomic_kpi[AVAILABILITY_TYPE]))
                    continue
            # write atomic score (maybe also result) to DB

    def calculate_price_scene(self, atomic_kpis_data, identifier_parent):
        for i in xrange(len(atomic_kpis_data)):
            atomic_kpi = atomic_kpis_data.iloc[i]
            is_by_scene = self.is_by_scene(atomic_kpi)
            if is_by_scene:
                target = atomic_kpi[TARGET]
                max_score = atomic_kpi[SCORE]
                atomic_result = 0
                calculation_filters = self.get_general_calculation_parameters(atomic_kpi, product_types=[SKU, OTHER])
                calculation_filters.update(self.get_availability_and_price_calculation_parameters(atomic_kpi))
                scene_result = 0
                scif = self.scif.copy()
                matches = self.match_product_in_scene.copy()
                merged_df = matches.merge(scif, left_on='product_fk', right_on='item_id', how='left')
                scene_df = merged_df[self.tools.get_filter_condition(merged_df, **calculation_filters)]
                if not scene_df.empty:
                    scene_result = self.get_price_result(scene_df, target, atomic_kpi)
                    # write scene result to DB

            atomic_score = self.calculate_atomic_score(atomic_result, max_score)
            # write session result to DB

    def get_price_result(self, matches_scif_df, target, atomic_kpi):
        unique_skus_list = matches_scif_df['product_fk'].unique().tolist()
        if target:
            result = self.calculate_price_vs_target(matches_scif_df, unique_skus_list, target, atomic_kpi)
        else:
            result = self.calculate_price_presence(matches_scif_df, unique_skus_list, atomic_kpi)
        return result

    def calculate_price_presence(self, matches, skus_list, atomic_kpi):
        price_presence = []
        for sku in skus_list:
            sku_prices = matches[matches['product_fk'] == sku]['price'].values.tolist()
            is_price = True if any([price is not None for price in sku_prices]) else False
            price_presence.append(is_price)
        result = 100 if all(price_presence) else 0
        self.add_scene_price_kpi_to_db(result, atomic_kpi, identifier_result)
        return result

    def add_scene_price_kpi_to_db(self, result, atomic_kpi, identifier_parent):
        custom_score = self.get_pass_fail(result)
        atomic_name = '{}_scene'.format(atomic_kpi[ATOMIC_KPI_NAME])
        kpi_fk = self.common.get_kpi_fk_by_kpi_name(atomic_name)
        self.common.write_to_db_result(fk=kpi_fk, numerator_id=KO_ID, score=custom_score,
                                       denominator_id=self.store_id, result=result,
                                       identifier_parent=identifier_parent, should_enter=True)

    def calculate_price_vs_target(self, matches, skus_list, target, atomic_kpi):
        identifier_result = self.get_identfier_result_atomic(atomic_kpi)
        target = float(target)
        price_reaching_target = []
        for sku in skus_list:
            sku_prices = matches[matches['product_fk'] == sku]['price'].values.tolist()
            price_meets_target = True if any([(price is not None and price <= target)
                                              for price in sku_prices]) else False
            price_reaching_target.append(price_meets_target)
            self.add_sku_price_kpi_to_db(sku, sku_prices, atomic_kpi, target, identifier_result)
        result = 100 if all(price_reaching_target) else 0
        return result

    def is_by_scene(self, atomic_kpi):
        return True if atomic_kpi[BY_SCENE] == 'Y' else False

    def get_general_calculation_parameters(self, atomic_kpi, product_types=None):
        calculation_parameters = {}
        try:
            template_names = self.split_and_strip(atomic_kpi[TEMPLATE_NAME])
        except KeyError:
            template_names = None

        try:
            template_display_names = self.split_and_strip(atomic_kpi[TEMPLATE_DISPLAY_NAME])
        except KeyError:
            template_display_names = None

        if template_names and template_display_names:
            relevant_scenes = self.scif[(self.scif['template_name'].isin(template_names)) &
                                        (self.scif['template_display_name'].isin(template_display_names))]
        elif template_names:
            relevant_scenes = self.scif[(self.scif['template_name'].isin(template_names))]
        elif template_display_names:
            relevant_scenes = self.scif[(self.scif['template_display_name'].isin(template_display_names))]
        else:
            relevant_scenes = self.scif
        scene_id_filter = {'scene_fk': relevant_scenes['scene_fk'].unique().tolist()[0]}
        calculation_parameters.update(scene_id_filter)

        try:
            is_KO_Only = True if atomic_kpi[KO_ONLY] == 'Y' else False
        except:
            is_KO_Only = None
        if is_KO_Only:
            manufacturer_filter = {MANUFACTURER_NAME: KO_PRODUCTS}
            calculation_parameters.update(manufacturer_filter)
        if product_types is not None:
            calculation_parameters.update({'product_type': product_types})
        return calculation_parameters

    def get_availability_and_price_calculation_parameters(self, atomic_kpi):
        condition_filters = {}
        relevant_columns = filter(lambda x: x.startswith('type') or x.startswith('value'), atomic_kpi.index.values)
        for column in relevant_columns:
            if atomic_kpi[column]:
                if column.startswith('type'):
                    condition_number = str(column.strip('type'))
                    matching_value_col = filter(lambda x: x.startswith('value') and str(x[len(x) - 1]) == condition_number,
                                                relevant_columns)
                    value_col = matching_value_col[0] if len(matching_value_col) > 0 else None
                    if value_col:
                        value_list = map(lambda x: self.get_string_or_number(atomic_kpi[column], x),
                                         self.split_and_strip(atomic_kpi[value_col]))
                        condition_filters.update({atomic_kpi[column]: value_list[0] if len(value_list) == 1 else value_list})
                    else:
                        Log.error('condition {} does not have corresponding value column'.format(column))
        return condition_filters

    @staticmethod
    def get_string_or_number(field, value):
        if field in NUMERIC_FIELDS:
            try:
                value = float(value)
            except:
                value = value
        return value

    def get_pass_fail(self, score):
        value = 'Failed' if not score else 'Passed'
        custom_score = self.get_kpi_score_value_pk_by_value(value)
        return custom_score

    # def get_pass_fail(self, score, value_type):
    #     value = 'Failed' if not score else 'Passed'
    #     custom_value_pk = 0
    #     if value_type == SCORE:
    #         custom_value_pk = self.get_kpi_score_value_pk_by_value(value)
    #     elif value_type == RESULT:
    #         custom_value_pk = self.get_kpi_score_value_pk_by_value(value)
    #     return custom_value_pk

    def get_kpi_score_value_pk_by_value(self, value):
        pk = None  # I want to stop code - maybe w/o try/except?
        try:
            pk = self.kpi_score_values[self.kpi_score_values['value'] == value]['pk'].values[0]
        except:
            Log.error('Value {} does not exist'.format(value))
        return pk

    def get_kpi_score_values_df(self):
        query = CCBZA_SAND_Queries.get_kpi_score_values()
        query_result = pd.read_sql_query(query, self.rds_conn.db)
        return query_result

    def get_kpi_result_values_df(self):
        query = CCBZA_SAND_Queries.get_kpi_result_values()
        query_result = pd.read_sql_query(query, self.rds_conn.db)
        return query_result