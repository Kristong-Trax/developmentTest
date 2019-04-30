import pandas as pd
import os
import re
from fractions import gcd

from Trax.Algo.Calculations.Core.DataProvider import Data
from Trax.Cloud.Services.Connector.Keys import DbUsers
from KPIUtils_v2.DB.PsProjectConnector import PSProjectConnector
from Trax.Utils.Logging.Logger import Log

# from KPIUtils_v2.DB.Common import Common
# from KPIUtils_v2.DB.CommonV2 import Common
from Projects.CCBZA_SAND.Utils.Common_TEMP import Common
# from KPIUtils_v2.Calculations.AssortmentCalculations import Assortment
# from KPIUtils_v2.Calculations.AvailabilityCalculations import Availability
# from KPIUtils_v2.Calculations.NumberOfScenesCalculations import NumberOfScenes
# from KPIUtils_v2.Calculations.PositionGraphsCalculations import PositionGraphs
# from KPIUtils_v2.Calculations.SOSCalculations import SOS
# from KPIUtils_v2.Calculations.SequenceCalculations import Sequence
# from KPIUtils_v2.Calculations.SurveyCalculations import Survey

# from KPIUtils_v2.Calculations.CalculationsUtils.GENERALToolBoxCalculations import GENERALToolBox

from Projects.CCBZA_SAND.Utils.Fetcher import CCBZA_SAND_Queries
from Projects.CCBZA_SAND.Utils.ParseTemplates import parse_template
from Projects.CCBZA_SAND.Utils.GeneralToolBox import CCBZASAND_GENERALToolBox
from Projects.CCBZA_SAND.Utils.Errors import DataError
from KPIUtils_v2.GlobalDataProvider.PsDataProvider import PsDataProvider

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
BONUS = 'Bonus'
QUESTION_TYPE = 'Question type'
MIN_SKU_TARGET = 'Min sku target'

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
AVAILABILITY_SKU_FACING_OR_MIN = 'Availability SKU facing Or min'
KO_PRODUCTS = 'KO PRODUCTS'
GENERAL_FILTERS = 'general_filters'
KPI_SPECIFIC_FILTERS = 'kpi_specific_filters'

# scif fields
EAN_CODE = 'product_ean_code'
BRAND_NAME = 'brand_name'
POS_BRAND_FIELD = 'Brand' # in the labels will need to be changed
PRODUCT_TYPE = 'product_type'
NUMERIC_FIELDS = ['size']
MANUFACTURER_NAME = 'manufacturer_name'
ITEM_ID = 'item_id'

#scif values
SKU = 'SKU'
POS = 'POS'
OTHER = 'Other'
MAX_SCORE = 'Max_Score'
IRRELEVANT = 'Irrelevant'
EMPTY = 'Empty'
KO_ID = 11 # to check
CORRECT = 1
RED_SCORE = 'Red_score_ccbza' # to check
GROCERY = 'GROCERY'
LnT = 'L&T'
QSR = 'QSR'
PRODUCT_FK='product_fk'
NON_KPI = 999999

class CCBZA_SANDToolBox:
    LEVEL1 = 1
    LEVEL2 = 2
    LEVEL3 = 3

    def __init__(self, data_provider, output):
        self.output = output
        self.data_provider = data_provider

        self.common = Common(self.data_provider)
        self.project_name = self.data_provider.project_name
        self.session_uid = self.data_provider.session_uid
        self.products = self.data_provider[Data.PRODUCTS]
        self.all_products = self.data_provider[Data.ALL_PRODUCTS]
        self.match_product_in_scene = self.data_provider[Data.MATCHES]
        self.visit_date = self.data_provider[Data.VISIT_DATE]
        self.session_info = self.data_provider[Data.SESSION_INFO]
        self.scene_info = self.data_provider[Data.SCENES_INFO]
        self.store_id = self.data_provider[Data.STORE_FK]
        self.scif = self.data_provider[Data.SCENE_ITEM_FACTS]
        self.merged_matches_scif = self.get_merged_matches_scif()
        self.rds_conn = PSProjectConnector(self.project_name, DbUsers.CalculationEng)

        self.kpi_results_queries = []
        self.kpi_results_data = self.create_kpi_results_container()
        self.store_data = self.get_store_data_by_store_id()
        self.template_path = self.get_template_path()
        self.template_data = self.get_template_data()
        self.kpi_sets = self.template_data[KPI_TAB][SET_NAME].unique().tolist()
        self.current_kpi_set_name = ''
        self.tools = CCBZA_SAND_GENERALToolBox(self.data_provider, self.output)
        self.kpi_result_values = self.get_kpi_result_values_df()
        self.kpi_score_values = self.get_kpi_score_values_df()
        self.full_store_type = self.get_full_store_type()
        self.ps_data_provider = PsDataProvider(self.data_provider, self.output)
        self.scene_kpi_results = self.ps_data_provider.get_scene_results(
            self.scene_info['scene_fk'].drop_duplicates().values)

        self.availability_for_scene_calc = {AVAILABILITY_POS: self.availability_brand_strips_scene,
                                            AVAILABILITY_SKU_FACING_AND: self.availability_and_or_scene,
                                            AVAILABILITY_SKU_FACING_OR: self.availability_and_or_scene,
                                            AVAILABLITY_IF_THEN: self.availability_against_competitors_scene}
        self.availability_by_scene_router = {AVAILABILITY_SKU_FACING_OR: self.get_availability_results_scene_table,
                                             AVAILABILITY_SKU_FACING_AND: self.get_availability_results_scene_table,
                                             AVAILABLITY_IF_THEN: self.get_availability_results_scene_table,
                                             AVAILABILITY_POS: self.get_availability_results_scene_table}
        self.availability_router = {AVAILABILITY_SKU_FACING_AND: self.calculate_availability_sku_and_or,
                                    AVAILABILITY_SKU_FACING_OR: self.calculate_availability_sku_and_or,
                                    AVAILABILITY_POSM: self.calculate_availability_posm,
                                    AVAILABILITY_SKU_FACING_OR_MIN: self.calculate_availability_min_facings_unique_list}
        # if not self.data_provider.scene_id:
        #     self.ps_data_provider = PsDataProvider(self.data_provider, self.output)
        #     self.scene_kpi_results = self.ps_data_provider.get_scene_results(
        #         self.scene_info['scene_fk'].drop_duplicates().values)
        # self.scif_KO_only = self.get_manufacturer_related_scif()
        # self.kpi_static_data = self.get_kpi_static_data()
        # self.new_kpi_static_data = self.get_new_kpi_static_data()
        # self.planogram_results = self.get_planogram_results()
        # self.set_results = {}
        # self.kpi_static_data = self.common.get_kpi_static_data()


#------------------scene calculations-----------------------------

    def scene_main_calculation(self):
        if not self.template_data:
            Log.error('Template data is empty')
            return
        session_kpis = self.template_data[KPI_TAB][KPI_NAME].unique().tolist()
        for kpi_name in session_kpis:
            kpi = self.template_data[KPI_TAB][self.template_data[KPI_TAB][KPI_NAME] == kpi_name].iloc[0]
            kpi_types = self.get_kpi_types_by_kpi(kpi)
            for kpi_type in kpi_types:
                atomic_kpis_data = self.get_atomic_kpis_data(kpi_type, kpi)
                if not atomic_kpis_data.empty:
                    if kpi_type == 'Availability':
                        self.calculate_availability_scene(atomic_kpis_data)
                    elif kpi_type == 'Price':
                        self.calculate_price_scene(atomic_kpis_data)
                    else:
                        Log.warning("KPI of type '{}' is not supported by scene calculations".format(kpi_type))
                        continue
        if not self.common.kpi_results.empty:
            self.common.commit_results_data(result_entity='scene')

    def calculate_price_scene(self, atomic_kpis_data):
        for i in xrange(len(atomic_kpis_data)):
            atomic_kpi = atomic_kpis_data.iloc[i]
            is_by_scene = self.is_by_scene(atomic_kpi)
            if is_by_scene:
                identifier_result = self.get_identifier_result_scene(atomic_kpi)
                target = atomic_kpi[TARGET]
                filters = self.get_general_calculation_parameters(atomic_kpi, product_types=[SKU, OTHER])
                filters.update(self.get_availability_and_price_calculation_parameters(atomic_kpi))
                scene_result = 0
                if filters['scene_fk']:
                    scene_df = self.merged_matches_scif[self.tools.get_filter_condition(self.merged_matches_scif, **filters)]
                    if not scene_df.empty:
                        scene_result = self.get_price_result(scene_df, target, atomic_kpi, identifier_result)
                    self.add_scene_atomic_result_to_db(scene_result, atomic_kpi, identifier_result)

    def calculate_availability_scene(self, atomic_kpis_data):
        for i in xrange(len(atomic_kpis_data)):
            atomic_kpi = atomic_kpis_data.iloc[i]
            is_by_scene = self.is_by_scene(atomic_kpi)
            avail_type = atomic_kpi[AVAILABILITY_TYPE]
            if is_by_scene:
                identifier_result = self.get_identifier_result_scene(atomic_kpi)
                try:
                    self.availability_for_scene_calc[avail_type](atomic_kpi, identifier_result)
                except KeyError:
                    Log.info('Availablity of type {} is not supported by scene calculation'.format(avail_type))
                    continue

    def availability_against_competitors_scene(self, atomic_kpi, identifier_result):
        filters = {GENERAL_FILTERS: self.get_general_calculation_parameters(atomic_kpi, product_types=[SKU, OTHER])}
        filters['1'] = {atomic_kpi[TYPE1]: atomic_kpi[VALUE1]}
        filters['2'] = {atomic_kpi[TYPE2]: atomic_kpi[VALUE2]}
        filters['3'] = {atomic_kpi[TYPE3]: atomic_kpi[VALUE3]}
        if filters[GENERAL_FILTERS]['scene_fk']:
            scene_result = 0
            non_ko_filters = filters[GENERAL_FILTERS].copy()
            non_ko_filters.update(filters['1'])
            non_ko_filters.update({atomic_kpi[TYPE2]: (atomic_kpi[VALUE2], 0)})
            non_ko_facings = self.scif[self.tools.get_filter_condition(self.scif, **non_ko_filters)][
                'facings'].sum()
            if non_ko_facings >= 1:
                ko_filters = filters[GENERAL_FILTERS].copy()
                ko_filters.update(filters['1'])
                ko_filters.update(filters['2'])
                ko_filters.update(filters['3'])
                ko_facings = self.scif[self.tools.get_filter_condition(self.scif, **ko_filters)]['facings'].sum()
                scene_result = 100 if ko_facings >= 2 else 0
            self.add_scene_atomic_result_to_db(scene_result, atomic_kpi, identifier_result)

    def availability_and_or_scene(self, atomic_kpi, identifier_result):
        filters = {GENERAL_FILTERS: self.get_general_calculation_parameters(atomic_kpi, product_types=[SKU, OTHER]),
                   KPI_SPECIFIC_FILTERS: self.get_availability_and_price_calculation_parameters(atomic_kpi)}
        if filters[GENERAL_FILTERS]['scene_fk']:
            scene_result = 0
            scene_scif = self.scif[self.tools.get_filter_condition(self.scif, **filters[GENERAL_FILTERS])]
            if not scene_scif.empty:
                result = self.retrieve_availability_result_all_any_sku(scene_scif, atomic_kpi,
                                                                       filters[KPI_SPECIFIC_FILTERS],
                                                                       identifier_result, is_by_scene=True)
                scene_result = 100 if result else 0
            self.add_scene_atomic_result_to_db(scene_result, atomic_kpi, identifier_result)  # no scene = > no result

    def availability_brand_strips_scene(self, atomic_kpi, identifier_result):
        target = atomic_kpi[TARGET]
        filters = {GENERAL_FILTERS: self.get_general_calculation_parameters(atomic_kpi),
                   KPI_SPECIFIC_FILTERS: self.get_availability_and_price_calculation_parameters(atomic_kpi)}
        if filters[GENERAL_FILTERS]['scene_fk']:
            scene_result = 0
            scene_scif = self.scif[self.tools.get_filter_condition(self.scif, **filters[GENERAL_FILTERS])]
            brands_count = scene_scif[~scene_scif[PRODUCT_TYPE].isin([POS, IRRELEVANT, EMPTY])].groupby(BRAND_NAME)[
                BRAND_NAME].count()
            if not brands_count.empty:
                scene_brand_strips_df = scene_scif[self.tools.get_filter_condition(scene_scif,
                                                                                   **filters[KPI_SPECIFIC_FILTERS])]
                brand_strips_count = scene_brand_strips_df.groupby(POS_BRAND_FIELD)['facings'].sum()
                result = self.match_brand_strip_to_brand(brands_count, brand_strips_count, target,
                                                         self.data_provider.scene_id)
                scene_result = 100 if result else 0
            self.add_scene_atomic_result_to_db(scene_result, atomic_kpi, identifier_result)  # no scene = > no result

    def add_scene_atomic_result_to_db(self, result, atomic_kpi, identifier_result):
        atomic_name = atomic_kpi[ATOMIC_KPI_NAME]
        atomic_name_scene = '{}_scene'.format(atomic_name)
        kpi_fk_scene = self.common.get_kpi_fk_by_kpi_type(atomic_name_scene)
        custom_score = self.get_pass_fail(result)
        self.common.write_to_db_result(fk=kpi_fk_scene, numerator_id=KO_ID, score=custom_score,
                                       denominator_id=self.store_id,
                                       identifier_result=identifier_result, should_enter=True,
                                       result=result, by_scene=True)

    def get_identifier_result_scene(self, atomic_kpi):
        identifier_result = self.common.get_dictionary(kpi_fk=self.common.get_kpi_fk_by_kpi_type(atomic_kpi[ATOMIC_KPI_NAME]))
        identifier_result['session_fk'] = self.session_info['pk'].values[0]
        identifier_result['store_fk'] = self.session_info['store_fk'].values[0]
        return identifier_result

#----------------------session calculations--------------------------

    def main_calculation(self, *args, **kwargs):
        """
        This function calculates the KPI results.
        """
        if not self.template_data:
            Log.error('Template data is empty')
            return
        red_score = 0
        red_target = 0
        identifier_result_red_score = self.get_identifier_result_red_score()
        for kpi_set_name in self.kpi_sets:
            self.current_kpi_set_name = kpi_set_name
            set_score = 0
            set_target = 0
            is_bonus = False
            kpi_data = self.template_data[KPI_TAB][self.template_data[KPI_TAB][SET_NAME] == kpi_set_name]
            identifier_result_set = self.get_identifier_result_set(kpi_set_name)
            for index, kpi in kpi_data.iterrows():
                kpi_types = self.get_kpi_types_by_kpi(kpi)
                identifier_result_kpi = self.get_identifier_result_kpi(kpi)
                is_bonus = self.is_bonus_kpi(kpi)
                for kpi_type in kpi_types:
                    atomic_kpis_data = self.get_atomic_kpis_data(kpi_type, kpi)
                    if not atomic_kpis_data.empty:
                        if kpi_type == 'Survey':
                            self.calculate_survey_new(atomic_kpis_data, identifier_result_kpi)
                        elif kpi_type == 'Availability':
                            self.calculate_availability_session_new(atomic_kpis_data, identifier_result_kpi)
                        elif kpi_type == 'Count':
                            self.calculate_count(atomic_kpis_data, identifier_result_kpi)
                        elif kpi_type == 'Price':
                            self.calculate_price(atomic_kpis_data, identifier_result_kpi)
                        elif kpi_type == 'SOS':
                            self.calculate_sos(atomic_kpis_data, identifier_result_kpi)
                        elif kpi_type == 'Planogram':
                            self.calculate_planogram_compliance(atomic_kpis_data, identifier_result_kpi)
                        else:
                            Log.warning("KPI of type '{}' is not supported".format(kpi_type))
                            continue
                kpi_result, kpi_target = self.calculate_kpi_result(kpi, identifier_result_set, identifier_result_kpi)
                set_score += kpi_result
                set_target += kpi_target
            set_kpi_fk = self.common.get_kpi_fk_by_kpi_type(kpi_set_name)
            self.common.write_to_db_result(fk=set_kpi_fk, numerator_id=KO_ID, score=set_score,
                                           denominator_id=self.store_id,
                                           identifier_parent=identifier_result_red_score,
                                           identifier_result=identifier_result_set,
                                           target=set_target, should_enter=True)
            red_score += set_score
            if not is_bonus:
                red_target += set_target

        red_score_percent = float(red_score) / red_target if red_target != 0 else 0
        red_score_kpi_fk = self.common.get_kpi_fk_by_kpi_type(RED_SCORE)
        self.common.write_to_db_result(fk=red_score_kpi_fk, numerator_id=KO_ID, result=red_score,
                                       score=red_score_percent, identifier_result=identifier_result_red_score,
                                       denominator_id=self.store_id, target=red_target, should_enter=True)
        self.common.commit_results_data()

    def get_identifier_result_kpi(self, kpi):
        kpi_name = kpi[KPI_NAME]
        identifier_result = self.common.get_dictionary(kpi_fk=self.common.get_kpi_fk_by_kpi_type(kpi_name),
                                                       manufacturer_id=KO_ID)
        return identifier_result

    def get_identfier_result_atomic(self, atomic_kpi):
        kpi_name = atomic_kpi[ATOMIC_KPI_NAME]
        identifier_result = self.common.get_dictionary(kpi_fk=self.common.get_kpi_fk_by_kpi_type(kpi_name),
                                                       manufacturer_id=KO_ID)
        return identifier_result

    def get_identifier_result_set(self, set_name):
        kpi_name = set_name
        identifier_result = self.common.get_dictionary(kpi_fk=self.common.get_kpi_fk_by_kpi_type(kpi_name),
                                                       manufacturer_id=KO_ID)
        return identifier_result

    def get_identifier_result_red_score(self):
        kpi_name = RED_SCORE
        identifier_result = self.common.get_dictionary(kpi_fk=self.common.get_kpi_fk_by_kpi_type(kpi_name),
                                                       manufacturer_id=KO_ID)
        return identifier_result

    def get_store_data_by_store_id(self):
        store_id = self.store_id if self.store_id else self.session_info['store_fk'].values[0]
        query = CCBZA_SAND_Queries.get_store_data_by_store_id(store_id)
        query_result = pd.read_sql_query(query, self.rds_conn.db)
        return query_result

    def get_kpi_result_values_df(self):
        query = CCBZA_SAND_Queries.get_kpi_result_values()
        query_result = pd.read_sql_query(query, self.rds_conn.db)
        return query_result

    def get_kpi_score_values_df(self):
        query = CCBZA_SAND_Queries.get_kpi_score_values()
        query_result = pd.read_sql_query(query, self.rds_conn.db)
        return query_result

    def get_manufacturer_related_scif(self):
        scif = self.data_provider[Data.SCENE_ITEM_FACTS].copy()
        return scif[scif[MANUFACTURER_NAME] == KO_PRODUCTS]

    def get_template_path(self):
        store_type = self.store_data['store_type'].values[0]
        template_name = 'Template_{}.xlsx'.format(store_type)
        return os.path.join(os.path.dirname(os.path.realpath(__file__)), '..', 'Data', template_name)

    def get_template_data(self):
        template_data = {}
        try:
            sheet_names = pd.ExcelFile(self.template_path).sheet_names
            for sheet in sheet_names:
                template_data[sheet] = parse_template(self.template_path, sheet, lower_headers_row_index=0)
        except IOError as e:
            # raise DataError('Template for store type {} does not exist'.format(self.store_data['store_type'].values[0]))
            Log.error('Template for store type {} does not exist. {}'.format(self.store_data['store_type'].values[0], repr(e)))
        return template_data

    def get_merged_matches_scif(self):
        scif = self.scif
        matches = self.match_product_in_scene
        merged_df = matches.merge(scif, left_on='product_fk', right_on='item_id', how='left')
        return merged_df

    def get_planogram_results(self):
        scenes = self.scene_info['scene_fk'].unique().tolist()
        query = CCBZA_SAND_Queries.get_planogram_results(scenes)
        kpi_static_data = pd.read_sql_query(query, self.rds_conn.db)
        return kpi_static_data

    def get_kpi_static_data(self):
        """
            This function extracts the static KPI data and saves it into one global data frame.
            the data is taken from static.kpi / static.atomic_kpi / static.kpi_set.
        """
        query = CCBZA_SAND_Queries.get_all_kpi_data()
        kpi_static_data = pd.read_sql_query(query, self.rds_conn.db)
        return kpi_static_data

    def get_new_kpi_static_data(self):
        """
            This function extracts the static new KPI data (new tables) and saves it into one global data frame.
            The data is taken from static.kpi_level_2.
        """
        query = CCBZA_SAND_Queries.get_new_kpi_data()
        kpi_static_data = pd.read_sql_query(query, self.rds_conn.db)
        return kpi_static_data

    def get_kpi_types_by_kpi(self, kpi):
        kpi_types_list = []
        if kpi[KPI_TYPE]:
            kpi_types = self.template_data[KPI_TAB][self.template_data[KPI_TAB][KPI_NAME] == kpi[KPI_NAME]][KPI_TYPE].values[0]
            kpi_types_list = self.split_and_strip(kpi_types)
        return kpi_types_list

    def get_atomic_kpis_data(self, kpi_type, kpi):
        atomic_kpis_data = self.template_data[kpi_type][(self.template_data[kpi_type][KPI_NAME] == kpi[KPI_NAME]) &
                                                        (self.template_data[kpi_type][STORE_TYPE].isin(self.store_data['store_type'])) &
                                                        ((self.template_data[kpi_type][ATTRIBUTE_1].isin(self.store_data['additional_attribute_1']))
                                                        |(self.template_data[kpi_type][ATTRIBUTE_1]=='')) &
                                                        ((self.template_data[kpi_type][ATTRIBUTE_2].isin(self.store_data['additional_attribute_2']))
                                                        |(self.template_data[kpi_type][ATTRIBUTE_2]==''))]
        return atomic_kpis_data

    @staticmethod
    def does_kpi_have_split_score(kpi):
        split_score = kpi[SPLIT_SCORE].strip(' ')
        if split_score == 'Y':
            return True
        elif split_score == 'N':
            return False
        else:
            Log.error('{} field has invalid value for kpi {}. Should be Y or N'.format(SPLIT_SCORE, kpi[KPI_NAME]))

    @staticmethod
    def get_kpi_dependency(kpi):
        dependency = kpi[DEPENDENCY].strip(' ')
        return dependency if dependency else None

    @staticmethod
    def create_kpi_results_container():
        columns = [SET_NAME, KPI_NAME, ATOMIC_KPI_NAME, SCORE, MAX_SCORE]
        df = pd.DataFrame(columns=columns)
        return df

    @staticmethod
    def split_string(string, deviders):
        regex=''
        for devider in deviders:
            regex += str(devider)+'|'
        regex = regex[0:len(regex)-1]
        print regex
        result_list = re.split(regex, string)
        return result_list

    @staticmethod
    def split_and_strip(string):
        return map(lambda x: x.strip(' '), str(string).split(',')) if string else []

    def calculate_kpi_result(self, kpi, identifier_parent, identifier_result):
        dependency = self.get_kpi_dependency(kpi)
        if dependency:
            kpi_scores = []
            affecting_kpis = self.kpi_results_data[self.kpi_results_data[SET_NAME] == dependency]
            for index, affecting_kpi in affecting_kpis.iterrows():
                aff_kpi_name = affecting_kpi[KPI_NAME]
                aff_kpi_row = self.template_data[KPI_TAB][self.template_data[KPI_TAB][KPI_NAME] == aff_kpi_name].iloc[0]
                kpi_score, max_score_dep = self.calculate_kpi_score_no_dependency(aff_kpi_row)
                kpi_scores.append((kpi_score, max_score_dep))
            if sum(map(lambda x: x[0], kpi_scores)) == sum(map(lambda x: x[1], kpi_scores)):
                score = float(kpi[self.full_store_type])
            else:
                score = 0
            max_score = float(kpi[self.full_store_type])
        else:
            score, max_score = self.calculate_kpi_score_no_dependency(kpi)
        kpi_fk = self.common.get_kpi_fk_by_kpi_type(kpi[KPI_NAME])
        self.common.write_to_db_result(fk=kpi_fk, numerator_id=KO_ID, score=score,
                                       denominator_id=self.store_id,
                                       identifier_parent=identifier_parent,  identifier_result=identifier_result,
                                       target=max_score, should_enter=True)
        return score, max_score

    def get_kpi_result_value_pk_by_value(self, value):
        pk = None
        try:
            pk = self.kpi_result_values[self.kpi_result_values['value'] == value]['pk'].values[0]
        except:
            Log.error('Value {} does not exist'.format(value))
        return pk

    def get_kpi_score_value_pk_by_value(self, value):
        pk = None
        try:
            pk = self.kpi_score_values[self.kpi_score_values['value'] == value]['pk'].values[0]
        except:
            Log.error('Value {} does not exist'.format(value))
        return pk

    def calculate_kpi_score_no_dependency(self, kpi):
        is_split_score = self.does_kpi_have_split_score(kpi)
        kpi_name = kpi[KPI_NAME]
        if is_split_score:
            kpi_score = self.kpi_results_data[self.kpi_results_data[KPI_NAME] == kpi_name][SCORE].sum()
            max_score = self.kpi_results_data[self.kpi_results_data[KPI_NAME] == kpi_name][MAX_SCORE].sum()
        else:
            atomic_scores = self.kpi_results_data[self.kpi_results_data[KPI_NAME] == kpi_name][SCORE].values.tolist()
            kpi_score = float(kpi[self.full_store_type]) if all(atomic_scores) else 0
            max_score = float(kpi[self.full_store_type])
        return kpi_score, max_score

    def get_full_store_type(self, ):
        # store_attr_1 = self.store_data['additional_attribute_1'].values[0]
        store_type = self.store_data['store_type'].values[0]
        if store_type != GROCERY:
            store = '{} {}'.format(self.store_data['store_type'].values[0],
                                   self.store_data['additional_attribute_1'].values[0])
        else:
            store = self.store_data['store_type'].values[0]
        return store

    def calculate_planogram_compliance(self, atomic_kpis_data, identifier_parent):
        for i in xrange(len(atomic_kpis_data)):
            atomic_kpi = atomic_kpis_data.iloc[i]
            filters = self.get_general_calculation_parameters(atomic_kpi)
            max_score = atomic_kpi[SCORE]
            scenes = filters['scene_fk']
            atomic_score = 0
            session_results = []
            for scene in scenes:
                matches = self.match_product_in_scene.copy()
                status_tags = matches[matches['scene_fk'] == scene]['compliance_status_fk']
                scene_result = 100 if all([tag == CORRECT for tag in status_tags]) else 0
                session_results.append(scene_result)
            if session_results:
                atomic_result = 100 if any(session_results) else 0
                atomic_score = self.calculate_atomic_score(atomic_result, max_score)
            self.add_kpi_result_to_kpi_results_container(atomic_kpi, atomic_score)

            kpi_fk = self.common.get_kpi_fk_by_kpi_type(atomic_kpi[ATOMIC_KPI_NAME])
            self.common.write_to_db_result(fk=kpi_fk, numerator_id=KO_ID, score=atomic_score,
                                           denominator_id=self.store_id,
                                           identifier_parent=identifier_parent,
                                           target=int(float(max_score)), should_enter=True)

    def calculate_price(self, atomic_kpis_data, identifier_parent):
        for i in xrange(len(atomic_kpis_data)):
            atomic_kpi = atomic_kpis_data.iloc[i]
            target = atomic_kpi[TARGET]
            max_score = atomic_kpi[SCORE]
            is_by_scene = False
            atomic_result = 0
            filters = self.get_general_calculation_parameters(atomic_kpi, product_types=[SKU, OTHER])
            filters.update(self.get_availability_and_price_calculation_parameters(atomic_kpi))
            identifier_result = self.get_identfier_result_atomic(atomic_kpi)
            list_of_scenes = filters.pop('scene_fk')
            if list_of_scenes:
                is_by_scene = self.is_by_scene(atomic_kpi)
                if is_by_scene:
                    session_results = self.get_atomic_results_all_scenes(atomic_kpi, list_of_scenes)
                    if session_results:
                        atomic_result = 100 if all(session_results) else 0

                        identifier_result.update({'session_fk': self.session_info['pk'].values[0]})
                        self.add_scene_hierarchy_on_session_level(atomic_kpi, identifier_result)
                    else:
                        is_by_scene = False
                else:
                    filters['scene_id'] = list_of_scenes
                    filtered_df = self.merged_matches_scif[self.tools.get_filter_condition(self.merged_matches_scif, **filters)]
                    if not filtered_df.empty:
                        atomic_result = self.get_price_result(filtered_df, target, atomic_kpi, identifier_result)
            atomic_score = self.calculate_atomic_score(atomic_result, max_score)
            self.add_kpi_result_to_kpi_results_container(atomic_kpi, atomic_score)

            kpi_fk = self.common.get_kpi_fk_by_kpi_type(atomic_kpi[ATOMIC_KPI_NAME])
            if max_score:
                self.common.write_to_db_result(fk=kpi_fk, score=atomic_score, numerator_id=KO_ID,
                                               denominator_id=self.store_id, identifier_parent=identifier_parent,
                                               identifier_result=identifier_result,
                                               target=max_score, should_enter=True)
            else:
                custom_score = self.get_pass_fail(atomic_score)
                self.common.write_to_db_result(fk=kpi_fk, score=custom_score, numerator_id=KO_ID, result=atomic_score,
                                               denominator_id=self.store_id, identifier_parent=identifier_parent,
                                               identifier_result=identifier_result,
                                               should_enter=True)

    def add_scene_hierarchy_on_session_level(self, atomic_kpi, identifier_result):
        scene_kpi_no = self.common.get_kpi_fk_by_kpi_type('{}_scene'.format(atomic_kpi[ATOMIC_KPI_NAME]))
        scene_kpi_fks = self.scene_kpi_results[self.scene_kpi_results['kpi_level_2_fk'] == scene_kpi_no][
            'pk'].values
        for scene in scene_kpi_fks:
            self.common.write_to_db_result(fk=NON_KPI, should_enter=True, scene_result_fk=scene, result=scene,
                                           score=scene_kpi_no, identifier_parent=identifier_result,
                                           switch=True)

    def get_price_result(self, matches_scif_df, target, atomic_kpi, identifier_parent):
        unique_skus_list = matches_scif_df['item_id'].unique().tolist()
        if target:
            result = self.calculate_price_vs_target(matches_scif_df, unique_skus_list, target, atomic_kpi, identifier_parent)
        else:
            result = self.calculate_price_presence(matches_scif_df, unique_skus_list, identifier_parent)
        return result

    @staticmethod
    def calculate_price_presence(matches, skus_list, identifier_parent):
        price_presence = []
        for sku in skus_list:
            sku_prices = matches[matches['item_id'] == sku]['price'].values.tolist()
            is_price = True if any([price is not None for price in sku_prices]) else False
            price_presence.append(is_price)
        result = 100 if all(price_presence) else 0
        return result

    def calculate_price_vs_target(self, matches, skus_list, target, atomic_kpi, identifier_parent):
        target = float(target)
        price_reaching_target = []
        for sku in skus_list:
            sku_prices = matches[matches['item_id'] == sku]['price'].values.tolist()
            price_meets_target = True if any([(price is not None and price <= target)
                                              for price in sku_prices]) else False
            price_reaching_target.append(price_meets_target)
            self.add_sku_price_kpi_to_db(sku, sku_prices, atomic_kpi, target, identifier_parent)
        # result = 100 if all(price_reaching_target) else 0
        result = 100 if any(price_reaching_target) else 0
        return result

    def add_sku_price_kpi_to_db(self, sku, prices_list, atomic_kpi, target_price, identifier_parent):
        prices_list = filter(lambda x: x is not None, prices_list)
        result_price = min(prices_list) if prices_list else -1
        price_score = 100 if (result_price != -1 and result_price <= target_price) else 0
        custom_score = self.get_pass_fail(price_score)
        atomic_name = '{}_SKU'.format(atomic_kpi[ATOMIC_KPI_NAME])
        kpi_fk = self.common.get_kpi_fk_by_kpi_type(atomic_name)
        self.common.write_to_db_result(fk=kpi_fk, numerator_id=sku, score=custom_score,
                                       denominator_id=self.store_id, result=result_price,
                                       identifier_parent=identifier_parent,
                                       target=target_price, should_enter=True)

    def calculate_count(self, atomic_kpis_data, identifier_parent):
        for i in xrange(len(atomic_kpis_data)):
            atomic_kpi = atomic_kpis_data.iloc[i]
            max_score = atomic_kpi[SCORE]
            target = float(atomic_kpi[TARGET])
            filters = self.get_general_calculation_parameters(atomic_kpi)
            atomic_result = 0
            if filters['scene_fk']:
                filters['scene_id'] = filters.pop('scene_fk')
                relevant_matches = self.merged_matches_scif[self.tools.get_filter_condition(self.merged_matches_scif, **filters)]
                bays_by_scene = relevant_matches[['scene_id', 'bay_number']].drop_duplicates().groupby(['scene_id']).count()
                atomic_result = 100 if (bays_by_scene == target).any().values[0] else 0

            atomic_score = self.calculate_atomic_score(atomic_result, max_score)
            self.add_kpi_result_to_kpi_results_container(atomic_kpi, atomic_score)

            kpi_fk = self.common.get_kpi_fk_by_kpi_type(atomic_kpi[ATOMIC_KPI_NAME])
            self.common.write_to_db_result(fk=kpi_fk, numerator_id=KO_ID, score=atomic_score,
                                           denominator_id=self.store_id,
                                           identifier_parent=identifier_parent,
                                           target=int(float(max_score)), should_enter=True)

    def calculate_survey_new(self, atomic_kpis_data, identifier_parent):
        """
        This function calculates Survey-Question typed Atomics, and writes the result to the DB.
        """
        for i in xrange(len(atomic_kpis_data)):
            atomic_kpi = atomic_kpis_data.iloc[i]
            survey_ids = map(lambda x: str(int(float(x))), self.split_and_strip(atomic_kpi[SURVEY_QUESTION_CODE]))
            # score = 0
            atomic_result = 0
            max_score = atomic_kpi[SCORE]
            expected_answers = atomic_kpi[EXPECTED_RESULT]
            if atomic_kpi[QUESTION_TYPE] == 'Text':
                expected_answers = self.split_and_strip(expected_answers)
                survey_answers = []
                for id in survey_ids:
                    survey_answers.append(self.tools.get_survey_answer(('code', [id])))
                if survey_answers:
                    if all([answer in expected_answers for answer in survey_answers]):
                        atomic_result = 100
            elif atomic_kpi[QUESTION_TYPE] == 'Numeric':
                if expected_answers:
                    try:
                        expected_answers = map(lambda x: int(float(x)), expected_answers.split(':'))
                        survey_answers = []
                        for id in survey_ids:
                            answer = self.tools.get_survey_answer(('code', [id]))
                            try:
                                survey_answers.append(int(float(answer)))
                            except ValueError as e:
                                Log.error('Survey kpi: {}, error: {}'.format(atomic_kpi[ATOMIC_KPI_NAME], str(e)))
                        if survey_answers and len(survey_answers) == len(expected_answers):
                            if all([answer is not None for answer in survey_answers]):
                                reduced_survey_answers = map(lambda x: x/reduce(gcd, survey_answers), survey_answers)
                                if expected_answers == reduced_survey_answers:
                                    atomic_result = 100
                    except Exception as e:
                        Log.error('Survey kpi: {}, input error: {}'.format(atomic_kpi[ATOMIC_KPI_NAME], str(e)))
            else:
                Log.error('Survey of type {} is not supported'.format(QUESTION_TYPE))
                continue
            score = self.calculate_atomic_score(atomic_result, max_score)
            self.add_kpi_result_to_kpi_results_container(atomic_kpi, score)

            custom_score = self.get_pass_fail(score)
            kpi_fk = self.common.get_kpi_fk_by_kpi_type(atomic_kpi[ATOMIC_KPI_NAME])
            if max_score:
                self.common.write_to_db_result(fk=kpi_fk, numerator_id=KO_ID, score=score,
                                               denominator_id=self.store_id, result=score,
                                               identifier_parent=identifier_parent,
                                               target=int(float(max_score)), should_enter=True)
            else:
                self.common.write_to_db_result(fk=kpi_fk, numerator_id=KO_ID, score=custom_score,
                                               denominator_id=self.store_id, result=score,
                                               identifier_parent=identifier_parent,
                                               should_enter=True)


    def calculate_survey(self, atomic_kpis_data, identifier_parent):
        """
        This function calculates Survey-Question typed Atomics, and writes the result to the DB.
        """
        for i in xrange(len(atomic_kpis_data)):
            atomic_kpi = atomic_kpis_data.iloc[i]
            survey_id = [str(int(float(atomic_kpi[SURVEY_QUESTION_CODE])))]
            # survey_id = [atomic_kpi[SURVEY_QUESTION_CODE]]
            expected_answers = self.split_and_strip(atomic_kpi[EXPECTED_RESULT])
            survey_max_score = atomic_kpi[SCORE]
            survey_answer = self.tools.get_survey_answer(('code', survey_id))
            score = 0
            if survey_answer:
                if survey_max_score:
                    score = int(float(survey_max_score)) if survey_answer in expected_answers else 0
                else:
                    score = 100 if survey_answer in expected_answers else 0
            self.add_kpi_result_to_kpi_results_container(atomic_kpi, score)

            custom_score = self.get_pass_fail(score)
            kpi_fk = self.common.get_kpi_fk_by_kpi_type(atomic_kpi[ATOMIC_KPI_NAME])
            if survey_max_score:
                self.common.write_to_db_result(fk=kpi_fk, numerator_id=KO_ID, score=custom_score,
                                               denominator_id=self.store_id, result=score,
                                               identifier_parent=identifier_parent,
                                               target=int(float(survey_max_score)), should_enter=True)
            else:
                self.common.write_to_db_result(fk=kpi_fk, numerator_id=KO_ID, score=custom_score,
                                               denominator_id=self.store_id, result=score,
                                               identifier_parent=identifier_parent,
                                               should_enter=True)

    def get_atomic_results_all_scenes(self, atomic_kpi, list_of_scenes):
        scene_atomic_name = '{}_scene'.format(atomic_kpi[ATOMIC_KPI_NAME])
        kpi_fk = self.common.get_kpi_fk_by_kpi_type(scene_atomic_name)
        session_results = self.scene_kpi_results[(self.scene_kpi_results['scene_fk'].isin(list_of_scenes))&
                                                 (self.scene_kpi_results['kpi_level_2_fk'] == kpi_fk)]['result'].values.tolist()
        return session_results

    def get_pass_fail(self, score):
        value = 'Failed' if not score else 'Passed'
        custom_score = self.get_kpi_score_value_pk_by_value(value)
        return custom_score

    def get_x_v(self, score):
        value = 'X' if not score else 'V'
        custom_score = self.get_kpi_score_value_pk_by_value(value)
        return custom_score

    def get_pass_fail_result(self, result):
        value = 'Failed' if not result else 'Passed'
        custom_score = self.get_kpi_result_value_pk_by_value(value)
        return custom_score

    def get_x_v_result(self, result):
        value = 'X' if not result else 'V'
        custom_score = self.get_kpi_result_value_pk_by_value(value)
        return custom_score

    def add_kpi_result_to_kpi_results_container(self, atomic_kpi, score):
        kpi_name = atomic_kpi[KPI_NAME]
        atomic_name = atomic_kpi[ATOMIC_KPI_NAME]
        max_score = float(atomic_kpi[SCORE]) if atomic_kpi[SCORE] else atomic_kpi[SCORE]
        self.kpi_results_data = self.kpi_results_data.append([{SET_NAME: self.current_kpi_set_name, KPI_NAME: kpi_name,
                                                               ATOMIC_KPI_NAME: atomic_name, SCORE: score,
                                                               MAX_SCORE: max_score}],
                                                             ignore_index=True)

    def calculate_sos(self, atomic_kpis_data, identifier_parent):
        for i in xrange(len(atomic_kpis_data)):
            atomic_kpi = atomic_kpis_data.iloc[i]
            max_score = atomic_kpi[SCORE]
            general_filters = self.get_general_calculation_parameters(atomic_kpi)
            atomic_result = 0
            number_of_conditions = 0
            identifier_result = self.get_identfier_result_atomic(atomic_kpi)
            conditions_details = []
            if general_filters['scene_fk']:
                scif = self.scif.copy()
                filtered_scif = scif[self.tools.get_filter_condition(scif, **general_filters)]
                if not filtered_scif.empty:
                    sos_filters = self.get_sos_calculation_parameters(atomic_kpi)
                    number_of_conditions = len(sos_filters.items())
                    conditions_results = []
                    for condition, filters in sos_filters.items():
                        target = float(filters.pop('target')) / 100
                        ratio, num_res, denom_res = self.calculate_sos_for_condition(filtered_scif, sos_filters[condition])
                        condition_score = 100 if ratio >= target else 0
                        conditions_results.append(condition_score)
                        conditions_details.append({'ratio': ratio, 'num_res': num_res, 'denom_res': denom_res,
                                                   'target': target})


                        custom_score = self.get_pass_fail(condition_score)
                        atomic_name ='{}_{}'.format(atomic_kpi[ATOMIC_KPI_NAME], condition)
                        kpi_fk_cond = self.common.get_kpi_fk_by_kpi_type(atomic_name)

                        self.common.write_to_db_result(fk=kpi_fk_cond, numerator_id=KO_ID, numerator_result=num_res,
                                                       denominator_id=self.store_id, denominator_result=denom_res,
                                                       result=ratio, score=custom_score,
                                                       identifier_parent=identifier_result,
                                                       target=target, should_enter=True)

                    if conditions_results:
                        atomic_result = 100 if all(conditions_results) else 0

            atomic_score = self.calculate_atomic_score(atomic_result, max_score)
            self.add_kpi_result_to_kpi_results_container(atomic_kpi, atomic_score)
            # write atomic result to db aggregated conditions- need mobile mock-up grocery
            if number_of_conditions > 0:
                kpi_fk = self.common.get_kpi_fk_by_kpi_type(atomic_kpi[ATOMIC_KPI_NAME])
                if max_score:
                    self.common.write_to_db_result(fk=kpi_fk, numerator_id=KO_ID, denominator_id=self.store_id,
                                                   score=atomic_score, identifier_parent=identifier_parent,
                                                   identifier_result=identifier_result, should_enter=True,
                                                   target=max_score)
                else:
                    custom_score_db = self.get_pass_fail(atomic_score)
                    numerator_result = conditions_details[0]['num_res'] if number_of_conditions == 1 else 0
                    denominator_result = conditions_details[0]['denom_res'] if number_of_conditions == 1 else 0
                    actual_ratio = conditions_details[0]['ratio'] if number_of_conditions == 1 else 0
                    target_atomic = conditions_details[0]['ratio'] if number_of_conditions == 1 else None
                    self.common.write_to_db_result(fk=kpi_fk, numerator_id=KO_ID, numerator_result=numerator_result,
                                                   denominator_id=self.store_id, denominator_result=denominator_result,
                                                   result=actual_ratio, score=custom_score_db,
                                                   identifier_parent=identifier_parent,
                                                   identifier_result=identifier_result,
                                                   target=target_atomic, should_enter=True)

    def calculate_sos_for_condition(self, scif, filters):
        numer_result = scif[self.tools.get_filter_condition(scif, **filters['numer'])]['facings'].sum()
        denom_result = scif[self.tools.get_filter_condition(scif, **filters['denom'])]['facings'].sum()
        ratio = float(numer_result) / denom_result if denom_result != 0 else 0
        return ratio, numer_result, denom_result

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
        scenes_ids_filter = {'scene_fk': relevant_scenes['scene_fk'].unique().tolist()}
        calculation_parameters.update(scenes_ids_filter)

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

    def get_sos_calculation_parameters(self, atomic_kpi):
        columns = atomic_kpi.index.values
        conditions = [CONDITION_1, CONDITION_2]
        filters = {}
        for condition in conditions:
            condition_columns = filter(lambda y: y.startswith(condition), columns)
            if all([atomic_kpi[col] for col in condition_columns]):
                filters.update({condition: {'numer': {}, 'denom': {}}})
                filters[condition]['numer'].update({atomic_kpi[' '.join([condition, NUMER, TYPE])]:
                                                    self.string_or_list(atomic_kpi[' '.join([condition, NUMER])])})
                filters[condition]['denom'].update({atomic_kpi[' '.join([condition, DENOM, TYPE])]:
                                                    self.string_or_list(atomic_kpi[' '.join([condition, DENOM])])})
                filters[condition]['target'] = atomic_kpi[' '.join([condition, C_TARGET])]
                filters[condition]['numer'].update(filters[condition]['denom'])
        return filters

    def calculate_availability_session_new(self, atomic_kpis_data, identifier_parent):
        for i in xrange(len(atomic_kpis_data)):
            atomic_kpi = atomic_kpis_data.iloc[i]
            identifier_result = self.get_identfier_result_atomic(atomic_kpi)
            score = 0
            per_scene = self.is_by_scene(atomic_kpi)
            avail_type = atomic_kpi[AVAILABILITY_TYPE]
            if per_scene:
                try:
                    score, identifier_result = self.availability_by_scene_router[avail_type](atomic_kpi, identifier_result)
                except KeyError:
                    Log.info('Availability not found for scene. Proceeding to session')
                    try:
                        score = self.availability_router[avail_type](atomic_kpi, identifier_result)
                    except KeyError:
                        Log.error('Availability type {} is not supported. kpi: {}'.format(avail_type,
                                                                                          atomic_kpi[ATOMIC_KPI_NAME]))
                        continue
            else:
                try:
                    score = self.availability_router[avail_type](atomic_kpi, identifier_result)
                except Exception as e:
                    Log.error('Availability type {} is not supported by calculation. {}'.format(avail_type, str(e)))
                    continue

            self.add_kpi_result_to_kpi_results_container(atomic_kpi, score)
            max_score = atomic_kpi[SCORE]
            kpi_fk = self.common.get_kpi_fk_by_kpi_type(atomic_kpi[ATOMIC_KPI_NAME])
            if max_score:
                self.common.write_to_db_result(fk=kpi_fk, score=score, numerator_id=KO_ID,
                                               denominator_id=self.store_id, identifier_parent=identifier_parent,
                                               identifier_result=identifier_result, result=score,
                                               should_enter=True, target=float(max_score))
            else:
                custom_score = self.get_pass_fail(score)
                self.common.write_to_db_result(fk=kpi_fk, score=custom_score, numerator_id=KO_ID,
                                               denominator_id=self.store_id, identifier_parent=identifier_parent,
                                               identifier_result=identifier_result,
                                               should_enter=True, result=score)

    # always by scene
    def get_availability_results_scene_table(self, atomic_kpi, identifier_result):
        score = 0
        max_score = atomic_kpi[SCORE]
        filters = self.get_general_calculation_parameters(atomic_kpi)
        list_of_scenes = filters['scene_fk']
        if list_of_scenes:
            session_results = self.get_atomic_results_all_scenes(atomic_kpi, list_of_scenes)
            if session_results:
                atomic_result = 100 if all(session_results) else 0
                score = self.calculate_atomic_score(atomic_result, max_score)
                identifier_result.update({'session_fk': self.session_info['pk'].values[0]})
                self.add_scene_hierarchy_on_session_level(atomic_kpi, identifier_result)
        return score, identifier_result

    # calculated on session level
    def calculate_availability_posm(self, atomic_kpi, identifier_parent):
        max_score = atomic_kpi[SCORE]
        target = atomic_kpi[TARGET]
        filters = self.get_general_calculation_parameters(atomic_kpi)
        filters.update(self.get_availability_and_price_calculation_parameters(atomic_kpi))
        atomic_result = 0
        if filters['scene_fk']:
            facings = self.scif[self.tools.get_filter_condition(self.scif, **filters)]['facings'].sum()
            atomic_result = 100 if facings >= float(target) else 0
        score = self.calculate_atomic_score(atomic_result, max_score)
        return score

    def is_by_scene(self, atomic_kpi):
        return True if atomic_kpi[BY_SCENE] == 'Y' else False

    def is_bonus_kpi(self, kpi):
        return True if kpi[BONUS] == 'Y' else False

    def match_brand_strip_to_brand(self, brands_series, brand_strips_series, target, scene):
        for brand, count in brands_series.iteritems():
            try:
                if not count*int(float(target)) <= brand_strips_series[brand]:
                    return False
            except KeyError:
                Log.info('Brand strip for brand {} does not exist in scene {}'.format(brand, scene))
                return False
        return True

    #only on session level
    def calculate_availability_min_facings_unique_list(self, atomic_kpi, identifier_parent):
        max_score = atomic_kpi[SCORE]
        filters = {GENERAL_FILTERS: self.get_general_calculation_parameters(atomic_kpi, product_types=[SKU, OTHER]),
                   KPI_SPECIFIC_FILTERS: self.get_availability_and_price_calculation_parameters(atomic_kpi)}
        atomic_result = 0
        list_of_scenes = filters[GENERAL_FILTERS]['scene_fk']
        if list_of_scenes:
            target_facings = float(filters[KPI_SPECIFIC_FILTERS].pop('facings'))
            filtered_scif = self.scif[self.tools.get_filter_condition(self.scif, **filters[GENERAL_FILTERS])]
            if not filtered_scif.empty:
                atomic_result = self.retrieve_availability_result_all_any_sku(filtered_scif, atomic_kpi,
                                                                              filters[KPI_SPECIFIC_FILTERS],
                                                                              identifier_parent, is_by_scene=False,
                                                                              or_min_facings=target_facings)
        score = self.calculate_atomic_score(atomic_result, max_score)
        return score

    def calculate_availability_sku_and_or(self, atomic_kpi, identifier_parent):
        max_score = atomic_kpi[SCORE]
        filters = {GENERAL_FILTERS: self.get_general_calculation_parameters(atomic_kpi, product_types=[SKU, OTHER]),
                   KPI_SPECIFIC_FILTERS: self.get_availability_and_price_calculation_parameters(atomic_kpi)}
        atomic_result = 0
        list_of_scenes = filters[GENERAL_FILTERS]['scene_fk']
        if list_of_scenes:
            filtered_scif = self.scif[self.tools.get_filter_condition(self.scif, **filters[GENERAL_FILTERS])]
            if not filtered_scif.empty:
                atomic_result = self.retrieve_availability_result_all_any_sku(filtered_scif, atomic_kpi,
                                                                              filters[KPI_SPECIFIC_FILTERS],
                                                                              identifier_parent,
                                                                              is_by_scene=False)
        score = self.calculate_atomic_score(atomic_result, max_score)
        return score

    def retrieve_availability_result_all_any_sku(self, scif, atomic_kpi, filters, identifier_parent, is_by_scene, or_min_facings=None):
        target = float(atomic_kpi[TARGET])
        min_skus = atomic_kpi[MIN_SKU_TARGET]
        result = 0
        availability_type = atomic_kpi[AVAILABILITY_TYPE]
        result_df = scif[self.tools.get_filter_condition(scif, **filters)] if filters else scif
        sku_list = filters[EAN_CODE] if EAN_CODE in filters.keys() else result_df[EAN_CODE].unique().tolist()
        facings_by_sku = self.get_facing_number_by_item(result_df, sku_list, EAN_CODE, PRODUCT_FK)
        if facings_by_sku:
            self.add_sku_availability_kpi_to_db(facings_by_sku, atomic_kpi, target, identifier_parent, is_by_scene)
            if availability_type == AVAILABILITY_SKU_FACING_AND:
                result = 100 if all([facing >= target for facing in facings_by_sku.values()]) else 0
            elif availability_type == AVAILABILITY_SKU_FACING_OR:
                # result = 100 if any([facing >= target for facing in facings_by_sku.values()]) else 0
                count_skus_meeting_target = sum([facings >= target for facings in facings_by_sku.values()])
                result = 100 if count_skus_meeting_target >= min_skus else 0
            else:
                Log.warning('Availability of type {} is not supported'.format(availability_type))
        return result

#--------------------------------utility functions-----------------------------------#

    def get_facing_number_by_item(self, facings_df, iter_list, filter_field, result_field=None):
        facings_by_item = {}
        iter_list = iter_list if isinstance(iter_list, list) else [iter_list]
        for item in iter_list:
            try:
                key = self.all_products[self.all_products[filter_field] == item][result_field].values[0] if result_field else item
                facings = facings_df[facings_df[filter_field] == item]['facings'].sum()
                facings_by_item.update({key: facings})
            except Exception as e:
                Log.info('SKU {} is not in the DB. {}'.format(item, str(e)))
                continue
        return facings_by_item

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
                    # else:
                    #     Log.info('condition {} does not have corresponding value column'.format(column)) # should it be error?
        return condition_filters

    @staticmethod
    def get_string_or_number(field, value):
        if field in NUMERIC_FIELDS:
            try:
                value = float(value)
            except:
                value = value
        return value

    def add_sku_availability_kpi_to_db(self, facings_by_sku, atomic_kpi, target, identifier_parent, is_by_scene):
        if not is_by_scene:
            atomic_name = '{}_SKU'.format(atomic_kpi[ATOMIC_KPI_NAME])
            kpi_fk = self.common.get_kpi_fk_by_kpi_type(atomic_name)
            for sku, facings in facings_by_sku.items():
                score = 100 if facings >= target else 0
                custom_result = self.get_x_v_result(100) if facings >= target else self.get_x_v_result(0)
                self.common.write_to_db_result(fk=kpi_fk, numerator_id=sku, score=score, result=custom_result,
                                               numerator_result=facings, denominator_id=self.store_id, identifier_parent=identifier_parent,
                                               should_enter=True)

    @staticmethod
    def get_facings_by_item(facings_df, iter_list, filter_field):
        facings_by_item = {}
        iter_list = iter_list if isinstance(iter_list, list) else [iter_list]
        for item in iter_list:
            facings = facings_df[facings_df[filter_field] == item]['facings'].sum()
            facings_by_item.update({item: facings})
        return facings_by_item

    def check_brand_strip(self, brand, df, target):
        pass

    @staticmethod
    def calculate_atomic_score(atomic_result, max_score):
        if max_score:
            score = float(max_score) if atomic_result == 100 else 0
        else:
            score = atomic_result
        return score

    def string_or_list(self, string):
        string_to_list = self.split_and_strip(string)
        if len(string_to_list) == 1:
            return string
        else:
            return string_to_list

    def reflect_result_by_sku(self):
        pass

#-----------------functions not used for the time-being but could be used in future---------------#

    # always by scene
    def calculate_availability_brand_strips(self, atomic_kpi):
        score = 0
        max_score = atomic_kpi[SCORE]
        target = atomic_kpi[TARGET]
        filters = {GENERAL_FILTERS: self.get_general_calculation_parameters(atomic_kpi),
                   KPI_SPECIFIC_FILTERS: self.get_availability_and_price_calculation_parameters(atomic_kpi)}
        list_of_scenes = filters[GENERAL_FILTERS]['scene_fk']
        # removed_field = filters[GENERAL_FILTERS].pop('manufacturer_name') #for debugging

        #Option 1 - implement by scene calculations
        # if list_of_scenes:
        #     session_results = self.get_atomic_results_all_scenes(atomic_kpi, list_of_scenes)
        #     if session_results:
        #         atomic_result = 100 if all(session_results) else 0
        #         score = self.calculate_atomic_score(atomic_result, max_score)

        #Option 2- to imitate scene calculations
        session_results = []
        for scene in list_of_scenes:
            scene_score = 0
            filters[GENERAL_FILTERS]['scene_fk'] = scene
            scene_scif = self.scif[self.tools.get_filter_condition(self.scif, **filters[GENERAL_FILTERS])]
            brands_count = scene_scif[~scene_scif[PRODUCT_TYPE].isin([POS, IRRELEVANT, EMPTY])].groupby(BRAND_NAME)[BRAND_NAME].count()
            if not brands_count.empty:
                scene_brand_strips_df = scene_scif[self.tools.get_filter_condition(scene_scif,
                                                                                   **filters[KPI_SPECIFIC_FILTERS])]
                brand_strips_count = scene_brand_strips_df.groupby(POS_BRAND_FIELD)['facings'].sum()
                scene_results = self.match_brand_strip_to_brand(brands_count, brand_strips_count, target, scene)
                scene_score = 100 if scene_results else 0
            session_results.append(scene_score)
        if session_results:
            atomic_result = 100 if all(session_results) else 0
            score = self.calculate_atomic_score(atomic_result, max_score)
        return score

    #always by scene
    def calculate_avialability_against_competitors(self, atomic_kpi):
        max_score = atomic_kpi[SCORE]
        score = 0
        is_by_scene = self.is_by_scene(atomic_kpi)
        # filters = {GENERAL_FILTERS: self.get_general_calculation_parameters(atomic_kpi, product_types=[SKU, OTHER]),
        #            KPI_SPECIFIC_FILTERS: self.get_availability_and_price_calculation_parameters(atomic_kpi)}
        filters = {GENERAL_FILTERS: self.get_general_calculation_parameters(atomic_kpi, product_types=[SKU, OTHER])}
        filters['1'] = {atomic_kpi[TYPE1]: atomic_kpi[VALUE1]}
        filters['2'] = {atomic_kpi[TYPE2]: atomic_kpi[VALUE2]}
        filters['3'] = {atomic_kpi[TYPE3]: atomic_kpi[VALUE3]}
        list_of_scenes = filters[GENERAL_FILTERS]['scene_fk']
        if is_by_scene:

            #Option 1 - get scene results from DB
            # session_results = self.get_atomic_results_all_scenes(atomic_kpi, list_of_scenes)
            # if session_results:
            #     atomic_result = 100 if all(session_results) else 0
            #     score = self.calculate_atomic_score(atomic_result, max_score)

            #Option 2 - calculate scene results
            session_results = []
            for scene in list_of_scenes:
                scene_result = 0
                non_ko_filters = filters[GENERAL_FILTERS].copy()
                non_ko_filters['scene_fk'] = scene
                non_ko_filters.update(filters['1'])
                non_ko_filters.update({atomic_kpi[TYPE2]: (atomic_kpi[VALUE2], 0)})
                # scene_scif = self.scif[self.tools.get_filter_condition(self.scif, **non_ko_filters)]
                # non_ko_facings = scene_scif['facings'].sum()
                non_ko_facings = self.scif[self.tools.get_filter_condition(self.scif, **non_ko_filters)]['facings'].sum()
                if non_ko_facings >= 1:
                    ko_filters = filters[GENERAL_FILTERS].copy()
                    ko_filters['scene_fk'] = scene
                    ko_filters.update(filters['1'])
                    ko_filters.update(filters['2'])
                    ko_filters.update(filters['3'])
                    ko_facings = self.scif[self.tools.get_filter_condition(self.scif, **ko_filters)]['facings'].sum()
                    scene_result = 100 if ko_facings >= 2 else 0
                session_results.append(scene_result)
            if session_results:
                atomic_result = 100 if all(session_results) else 0
                score = self.calculate_atomic_score(atomic_result, max_score)
        else:
            Log.info("Calculation of kpi {} by session is not supported".format(atomic_kpi[ATOMIC_KPI_NAME]))
        return score

    @staticmethod
    def filter_df_based_on_filtering_dictionary(df, filters):
        filtered_df = df.copy()
        for column, criteria in filters.items():
            if column in filtered_df.columns:
                if isinstance(criteria, list):
                    filtered_df = filtered_df[filtered_df[column].isin(criteria)]
                else:
                    filtered_df = filtered_df[filtered_df[column] == criteria]
        return filtered_df

#-------------obsolete functions---------------------#
    #replaced by calculate_availability_scene with router
    def calculate_availability_scene_old(self, atomic_kpis_data):
        for i in xrange(len(atomic_kpis_data)):
            atomic_kpi = atomic_kpis_data.iloc[i]
            is_by_scene = self.is_by_scene(atomic_kpi)
            if is_by_scene:
                identifier_result = self.get_identifier_result_scene(atomic_kpi)
                if atomic_kpi[AVAILABILITY_TYPE] == AVAILABILITY_POS:
                    self.availability_brand_strips_scene(atomic_kpi, identifier_result)
                elif atomic_kpi[AVAILABILITY_TYPE] == AVAILABILITY_SKU_FACING_AND:
                    self.availability_and_or_scene(atomic_kpi, identifier_result)
                elif atomic_kpi[AVAILABILITY_TYPE] == AVAILABILITY_SKU_FACING_OR:
                    self.availability_and_or_scene(atomic_kpi, identifier_result)
                elif atomic_kpi[AVAILABILITY_TYPE] == AVAILABLITY_IF_THEN:
                    self.availability_against_competitors_scene(atomic_kpi, identifier_result)
                else:
                    Log.warning(
                        'Availablity of type {} is not supported by scene calculation'.format(atomic_kpi[AVAILABILITY_TYPE]))
                    continue

    #replaced by calculate_availability_session_new
    def calculate_availability_session(self, atomic_kpis_data, identifier_parent):
        for i in xrange(len(atomic_kpis_data)):
            atomic_kpi = atomic_kpis_data.iloc[i]
            identifier_result = self.get_identfier_result_atomic(atomic_kpi)
            score = 0
            per_scene = self.is_by_scene(atomic_kpi)
            if atomic_kpi[AVAILABILITY_TYPE] == AVAILABILITY_POSM:
                score = self.calculate_availability_posm(atomic_kpi, identifier_result)
            elif atomic_kpi[AVAILABILITY_TYPE] == AVAILABILITY_SKU_FACING_OR_MIN:
                score = self.calculate_availability_min_facings_unique_list(atomic_kpi, identifier_result)
            elif atomic_kpi[AVAILABILITY_TYPE] == AVAILABILITY_POS:
                if per_scene:
                    score, identifier_result = self.get_availability_results_scene_table(atomic_kpi, identifier_result)
                else:
                    Log.error('Availability of type {} is not supported on session level'.format(atomic_kpi[AVAILABILITY_TYPE]))
                    continue
            elif atomic_kpi[AVAILABILITY_TYPE] == AVAILABLITY_IF_THEN:
                if per_scene:
                    score, identifier_result = self.get_availability_results_scene_table(atomic_kpi, identifier_result)
                else:
                    Log.error('Availability of type {} is not supported on session level'.format(atomic_kpi[AVAILABILITY_TYPE]))
                    continue
            elif atomic_kpi[AVAILABILITY_TYPE] in [AVAILABILITY_SKU_FACING_AND, AVAILABILITY_SKU_FACING_OR]:
                if per_scene:
                    score, identifier_result = self.get_availability_results_scene_table(atomic_kpi, identifier_result)
                else:
                    score = self.calculate_availability_sku_and_or(atomic_kpi, identifier_result)
            else:
                Log.warning('Availablity of type {} is not supported by calculation'.format(atomic_kpi[AVAILABILITY_TYPE]))
                continue
            self.add_kpi_result_to_kpi_results_container(atomic_kpi, score)

            max_score = atomic_kpi[SCORE]
            kpi_fk = self.common.get_kpi_fk_by_kpi_type(atomic_kpi[ATOMIC_KPI_NAME])
            if max_score:
                self.common.write_to_db_result(fk=kpi_fk, score=score, numerator_id=KO_ID,
                                               denominator_id=self.store_id, identifier_parent=identifier_parent,
                                               identifier_result=identifier_result, result=score,
                                               should_enter=True, target=float(max_score))
            else:
                custom_score = self.get_pass_fail(score)
                self.common.write_to_db_result(fk=kpi_fk, score=custom_score, numerator_id=KO_ID,
                                               denominator_id=self.store_id, identifier_parent=identifier_parent,
                                               identifier_result=identifier_result,
                                               should_enter=True, result=score)

# def calculate_availability_sku_facing_and(self, atomic_kpi):
    #     max_score = atomic_kpi[SCORE]
    #     is_by_scene = self.is_by_scene(atomic_kpi)
    #     filters = {GENERAL_FILTERS: self.get_general_calculation_parameters(atomic_kpi,
    #                                                                                     product_types=[SKU, OTHER]),
    #                            KPI_SPECIFIC_FILTERS: self.get_availability_and_price_calculation_parameters(atomic_kpi)}
    #     atomic_result = 0
    #     list_of_scenes = filters[GENERAL_FILTERS]['scene_fk']
    #     if list_of_scenes:
    #         if is_by_scene:
    #             session_results = []
    #             for scene in list_of_scenes:
    #                 filters[GENERAL_FILTERS]['scene_fk'] = scene
    #                 scif = self.scif.copy()
    #                 scene_scif = scif[self.tools.get_filter_condition(scif, **filters[GENERAL_FILTERS])]
    #                 if not scene_scif.empty:
    #                     scene_result = self.calculate_availability_result_all_skus(scene_scif, filters, atomic_kpi)
    #                     session_results.append(scene_result)
    #                     # write result and score to DB by scene
    #             if session_results:
    #                 atomic_result = 100 if all([result == 100 for result in session_results]) else 0
    #         else:
    #             scif = self.scif.copy()
    #             filtered_scif = scif[self.tools.get_filter_condition(scif, **filters[GENERAL_FILTERS])]
    #             if not filtered_scif.empty:
    #                 atomic_result = self.calculate_availability_result_all_skus(filtered_scif, filters, atomic_kpi)
    #     score = self.calculate_atomic_score(atomic_result, max_score)
    #     return score
    #
    # def calculate_availability_sku_facing_or(self, atomic_kpi):
    #     max_score = atomic_kpi[SCORE]
    #     is_by_scene = self.is_by_scene(atomic_kpi)
    #     filters = {GENERAL_FILTERS: self.get_general_calculation_parameters(atomic_kpi,
    #                                                                                     product_types=[SKU, OTHER]),
    #                            KPI_SPECIFIC_FILTERS: self.get_availability_and_price_calculation_parameters(atomic_kpi)}
    #     atomic_result = 0
    #     list_of_scenes = filters[GENERAL_FILTERS]['scene_fk']
    #     if list_of_scenes:
    #         if is_by_scene:
    #             session_results = []
    #             for scene in list_of_scenes:
    #                 filters[GENERAL_FILTERS]['scene_fk'] = scene
    #                 scif = self.scif.copy()
    #                 scene_scif = scif[self.tools.get_filter_condition(scif, **filters[GENERAL_FILTERS])]
    #                 if not scene_scif.empty:
    #                     scene_result = self.calculate_availability_result_any_sku(scene_scif, filters, atomic_kpi)
    #                     session_results.append(scene_result)
    #                     # write result to DB by scene
    #             if session_results:
    #                 atomic_result = 100 if all([result == 100 for result in session_results]) else 0
    #         else:
    #             scif = self.scif.copy()
    #             filtered_scif = scif[self.tools.get_filter_condition(scif, **filters[GENERAL_FILTERS])]
    #             if not filtered_scif.empty:
    #                 atomic_result = self.calculate_availability_result_any_sku(filtered_scif, filters, atomic_kpi)
    #     score = self.calculate_atomic_score(atomic_result, max_score)
    #     return score

    # def calculate_availability_result_all_skus(self, scif, filters, atomic_kpi):
    #     target = float(atomic_kpi[TARGET])
    #     calc_filters = filters[KPI_SPECIFIC_FILTERS].copy()
    #     # result = 0
    #     unique_skus_list = calc_filters[EAN_CODE] if EAN_CODE in calc_filters.keys() \
    #         else scif[EAN_CODE].unique().tolist()
    #     calc_filters[EAN_CODE] = unique_skus_list
    #     result_df = scif[self.tools.get_filter_condition(scif, **calc_filters)]
    #     facings_by_sku = self.get_facings_by_item(result_df, unique_skus_list, EAN_CODE)
    #     result = 100 if all([facing >= target for facing in facings_by_sku.values()]) else 0
    #     identifier_result = self.get_identfier_result_atomic(atomic_kpi)
    #     self.add_sku_availability_kpi_to_db(facings_by_sku, atomic_kpi, target, identifier_result)
    #     return result
    #
    # def calculate_availability_result_any_sku(self, scif, filters, atomic_kpi):
    #     target = float(atomic_kpi[TARGET])
    #     calc_filters = filters[KPI_SPECIFIC_FILTERS].copy()
    #     unique_skus_list = calc_filters[EAN_CODE] if EAN_CODE in calc_filters.keys() \
    #         else scif[EAN_CODE].unique().tolist()
    #     calc_filters[EAN_CODE] = unique_skus_list
    #     result_df = scif[self.tools.get_filter_condition(scif, **filters[KPI_SPECIFIC_FILTERS])]
    #     facings_by_sku = self.get_facings_by_item(result_df, unique_skus_list, EAN_CODE)
    #     result = 100 if all([facing >= target for facing in facings_by_sku.values()]) else 0
    #     # facings = result_df['facings'].sum()
    #     # result = 100 if facings >= target else 0
    #     # I am not sure we will need to write to db by sku in this case - does not make much sense
    #     return result

    # def calculate_kpi_result(self, kpi):
        # is_split_score = self.does_kpi_have_split_score(kpi)
        # kpi_name = kpi[KPI_NAME]
        # if is_split_score:
        #     kpi_score = self.kpi_results_data[self.kpi_results_data[KPI_NAME] == kpi_name][SCORE].sum()
        # else:
        #     atomic_scores = self.kpi_results_data[self.kpi_results_data[KPI_NAME] == kpi_name][SCORE].values.tolist()
        #     kpi_score = kpi[self.full_store_type] if all(atomic_scores) else 0
        # return kpi_score

    # def calculate_count_with_scene_brkdwn(self, atomic_kpis_data, identifier_parent):
    #     for i in xrange(len(atomic_kpis_data)):
    #         atomic_kpi = atomic_kpis_data.iloc[i]
    #         max_score = atomic_kpi[SCORE]
    #         target = float(atomic_kpi[TARGET])
    #         filters = self.get_general_calculation_parameters(atomic_kpi)
    #         atomic_result = 0
    #         if filters['scene_fk']:
    #             filters['scene_id'] = filters.pop('scene_fk')
    #             relevant_matches = self.merged_matches_scif[self.tools.get_filter_condition(self.merged_matches_scif, **filters)]
    #             bays_by_scene = relevant_matches[['scene_id', 'bay_number']].drop_duplicates().groupby(['scene_id']).count()
    #             atomic_result = 100 if (bays_by_scene == target).any().values[0] else 0
    #
    #         # #option 2 - in case scene brdwn is required
    #         # session_door_count = []
    #         # list_of_scenes = filters['scene_fk']
    #         # for scene in list_of_scenes:
    #         #     scene_filter = filters.copy()
    #         #     scene_filter['scene_fk'] = scene
    #         #     # scene_filter = {'scene_fk': scene} # this was replaced with previous 2 lines to filter only KO Products
    #         #     matches = self.match_product_in_scene
    #         #     relevant_match_prod_in_scene = matches[self.tools.get_filter_condition(matches, **scene_filter)]
    #         #     number_of_bays = len(relevant_match_prod_in_scene['bay_number'].unique())
    #         #     session_door_count.append(number_of_bays)
    #         # atomic_result = 100 if any([doors == target for doors in session_door_count]) else 0
    #
    #         atomic_score = self.calculate_atomic_score(atomic_result, max_score)
    #         self.add_kpi_result_to_kpi_results_container(atomic_kpi, atomic_score)
    #         # constructing queries for DB
    #         kpi_fk = self.common.get_kpi_fk_by_kpi_type(atomic_kpi[ATOMIC_KPI_NAME])
    #         self.common.write_to_db_result(fk=kpi_fk, numerator_id=KO_ID, score=atomic_score,
    #                                        denominator_id=self.store_id,
    #                                        identifier_parent=identifier_parent,
    #                                        target=int(float(max_score)), should_enter=True)

    # def calculate_sos_version_1(self, atomic_kpis_data, identifier_parent):
    #     for i in xrange(len(atomic_kpis_data)):
    #         atomic_kpi = atomic_kpis_data.iloc[i]
    #         max_score = atomic_kpi[SCORE]
    #         general_filters = self.get_general_calculation_parameters(atomic_kpi)
    #         atomic_result = 0
    #         number_of_conditions = 0
    #         identifier_result = self.get_identfier_result_atomic(atomic_kpi)
    #         conditions_details = []
    #         if general_filters['scene_fk']:
    #             scif = self.scif.copy()
    #             filtered_scif = scif[self.tools.get_filter_condition(scif, **general_filters)]
    #             if not filtered_scif.empty:
    #                 sos_filters = self.get_sos_calculation_parameters(atomic_kpi)
    #                 number_of_conditions = len(sos_filters.items())
    #                 conditions_results = []
    #                 for condition, filters in sos_filters.items():
    #                     target = float(filters.pop('target')) / 100
    #                     ratio, num_res, denom_res = self.calculate_sos_for_condition(filtered_scif, sos_filters[condition])
    #                     condition_score = 100 if ratio >= target else 0
    #                     conditions_results.append(condition_score)
    #                     conditions_details.append({'ratio': ratio, 'num_res': num_res, 'denom_res': denom_res,
    #                                                'target': target})
    #
    #                     # write condition result to DB
    #                     custom_score = self.get_pass_fail(condition_score)
    #                     atomic_name ='{}_{}'.format(atomic_kpi[ATOMIC_KPI_NAME], condition)
    #                     kpi_fk_cond = self.common.get_kpi_fk_by_kpi_type(atomic_name)
    #                     # identifier_parent_condition = self.get_identfier_result_atomic(atomic_kpi)
    #                     self.common.write_to_db_result(fk=kpi_fk_cond, numerator_id=KO_ID, numerator_result=num_res,
    #                                                    denominator_id=self.store_id, denominator_result=denom_res,
    #                                                    result=ratio, score=custom_score,
    #                                                    identifier_parent=identifier_result,
    #                                                    target=target, should_enter=True)
    #
    #                     # custom_score = self.get_pass_fail(condition_score)
    #                     # atomic_name = atomic_kpi[ATOMIC_KPI_NAME] if number_of_conditions == 1 \
    #                     #                 else '{} {}'.format(atomic_kpi[ATOMIC_KPI_NAME], condition)
    #                     # kpi_fk = self.common.get_kpi_fk_by_kpi_type(atomic_name)
    #                     # identifier_parent_condition = identifier_parent if number_of_conditions == 1 else \
    #                     #     self.get_identfier_result_atomic(atomic_kpi)
    #                     # self.common.write_to_db_result(fk=kpi_fk, numerator_id=KO_ID, numerator_result=num_res,
    #                     #                                denominator_id=self.store_id, denominator_result=denom_res,
    #                     #                                result=ratio, score=custom_score,
    #                     #                                identifier_parent=identifier_parent_condition,
    #                     #                                target=target, should_enter=True)
    #                 if conditions_results:
    #                     atomic_result = 100 if all(conditions_results) else 0
    #
    #         atomic_score = self.calculate_atomic_score(atomic_result, max_score)
    #         self.add_kpi_result_to_kpi_results_container(atomic_kpi, atomic_score)
    #         # write atomic result to db aggregated conditions- need mobile mock-up grocery
    #         if number_of_conditions > 0:
    #             kpi_fk = self.common.get_kpi_fk_by_kpi_type(atomic_kpi[ATOMIC_KPI_NAME])
    #             if max_score:
    #                 self.common.write_to_db_result(fk=kpi_fk, numerator_id=KO_ID, denominator_id=self.store_id,
    #                                                score=atomic_score, identifier_parent=identifier_parent,
    #                                                identifier_result=identifier_result, should_enter=True,
    #                                                target=max_score)
    #             else:
    #                 custom_score_db = self.get_pass_fail(atomic_score)
    #                 numerator_result = conditions_details[0]['num_res'] if number_of_conditions == 1 else 0
    #                 denominator_result = conditions_details[0]['denom_res'] if number_of_conditions == 1 else 0
    #                 actual_ratio = conditions_details[0]['ratio'] if number_of_conditions == 1 else 0
    #                 target_atomic = conditions_details[0]['ratio'] if number_of_conditions == 1 else None
    #                 self.common.write_to_db_result(fk=kpi_fk, numerator_id=KO_ID, numerator_result=numerator_result,
    #                                                denominator_id=self.store_id, denominator_result=denominator_result,
    #                                                result=actual_ratio, score=custom_score_db,
    #                                                identifier_parent=identifier_parent,
    #                                                identifier_result=identifier_result,
    #                                                target=target_atomic, should_enter=True)
    #
    #             # kpi_fk = self.common.get_kpi_fk_by_kpi_type(atomic_kpi[ATOMIC_KPI_NAME])
    #             # self.common.write_to_db_result(fk=kpi_fk, numerator_id=KO_ID, denominator_id=self.store_id,
    #             #                                score=atomic_score, identifier_parent=identifier_parent,
    #             #                                identifier_result=self.get_identfier_result_atomic(atomic_kpi),
    #             #                                should_enter=True)

    # def match_brand_strip_to_brand_detailed(self, brands_series, brand_strips_series, target, scene):
        # Option 2 - to b kept till deployment in case detailed breakdown of brand availability is necessary
        # scene_results = []
        # for brand, count in brands_series.iteritems():
        #     try:
        #         if count*int(float(target)) <= brand_strips_series[brand]:
        #             brand_result = True
        #         else:
        #             brand_result = False
        #     except KeyError:
        #         Log.info('Brand strip for brand {} does not exist in scene {}'.format(brand, scene))
        #         brand_result = False
        #     scene_results.append(brand_result)
        # return scene_results
