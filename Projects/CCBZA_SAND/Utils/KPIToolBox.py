import pandas as pd
import os
import re

from Trax.Algo.Calculations.Core.DataProvider import Data
from Trax.Cloud.Services.Connector.Keys import DbUsers
from Trax.Data.Projects.Connector import ProjectConnector
from Trax.Utils.Logging.Logger import Log

# from KPIUtils_v2.DB.Common import Common
from KPIUtils_v2.DB.CommonV2 import Common
# from KPIUtils_v2.Calculations.AssortmentCalculations import Assortment
from KPIUtils_v2.Calculations.AvailabilityCalculations import Availability
# from KPIUtils_v2.Calculations.NumberOfScenesCalculations import NumberOfScenes
# from KPIUtils_v2.Calculations.PositionGraphsCalculations import PositionGraphs
from KPIUtils_v2.Calculations.SOSCalculations import SOS
# from KPIUtils_v2.Calculations.SequenceCalculations import Sequence
# from KPIUtils_v2.Calculations.SurveyCalculations import Survey

from KPIUtils_v2.Calculations.CalculationsUtils.GENERALToolBoxCalculations import GENERALToolBox

from Projects.CCBZA_SAND.Utils.Fetcher import CCBZA_SAND_Queries
from Projects.CCBZA_SAND.Utils.ParseTemplates import parse_template
from Projects.CCBZA_SAND.Utils.GeneralToolBox import CCBZA_SAND_GENERALToolBox
from Projects.CCBZA_SAND.Utils.Errors import DataError
from Projects.CCBZA_SAND.Utils.CustomSceneCommon import CCBZA_SANDSceneCommon

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


class CCBZA_SANDToolBox:
    LEVEL1 = 1
    LEVEL2 = 2
    LEVEL3 = 3

    def __init__(self, data_provider, output):
        self.output = output
        self.data_provider = data_provider

        self.common = Common(self.data_provider)
        # self.kpi_static_data = self.common.get_kpi_static_data()

        # self.general_tool_box = GENERALToolBox(self.data_provider)
        # self.common_sos = SOS(self.data_provider, self.output)
        # self.common_availability = Availability(self.data_provider)

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
        self.scif_KO_only = self.get_manufacturer_related_scif()
        self.rds_conn = ProjectConnector(self.project_name, DbUsers.CalculationEng)

        self.kpi_results_queries = []

        # self.kpi_static_data = self.get_kpi_static_data()
        # self.new_kpi_static_data = self.get_new_kpi_static_data()
        self.kpi_results_data = self.create_kpi_results_container()
        self.store_data = self.get_store_data_by_store_id()
        self.template_path = self.get_template_path()
        self.template_data = self.get_template_data()
        # self.kpi_sets = self.kpi_static_data['kpi_set_fk'].unique().tolist()
        self.kpi_sets = self.template_data[KPI_TAB][SET_NAME].unique().tolist()
        self.current_kpi_set_name = ''
        self.tools = CCBZA_SAND_GENERALToolBox(self.data_provider, self.output)
        self.kpi_result_values = self.get_kpi_result_values_df() # maybe for ps data provider, create mock
        self.kpi_score_values = self.get_kpi_score_values_df()
        # self.planogram_results = self.get_planogram_results()
        self.full_store_type = self.get_full_store_type()
        self.common_scene = CCBZA_SANDSceneCommon(self.data_provider, self.scene_info['pk'].unique().tolist())

    def main_calculation(self, *args, **kwargs):
        """
        This function calculates the KPI results.
        """
        # if self.template_data?
        red_score = 0
        for kpi_set_name in self.kpi_sets:
            self.current_kpi_set_name = kpi_set_name
            set_score = 0
            # kpi_set_name = self.kpi_static_data[self.kpi_static_data['kpi_set_fk'] == kpi_set]['kpi_set_name'].values[0]
            # self.current_kpi_set_name = self.kpi_static_data[self.kpi_static_data['kpi_set_fk'] == kpi_set]['kpi_set_name'].values[0] #alternative
            # kpi_types = self.get_kpi_types_by_set_name(kpi_set_name)
            kpi_data = self.template_data[KPI_TAB][self.template_data[KPI_TAB][SET_NAME] == kpi_set_name] # we get the relevant kpis for the set
            for index, kpi in kpi_data.iterrows():
                kpi_types = self.get_kpi_types_by_kpi(kpi)
                identifier_result = self.get_identifier_result_kpi(kpi)
                # atomic_scores = []
                for kpi_type in kpi_types:
                    atomic_kpis_data = self.get_atomic_kpis_data(kpi_type, kpi) # we get relevant atomics from the sheets
                    if not atomic_kpis_data.empty:
                        if kpi_type == 'Survey':
                            self.calculate_survey(atomic_kpis_data, identifier_result)
                        elif kpi_type == 'Availability':
                            self.calculate_availability_custom(atomic_kpis_data, identifier_result)
                        elif kpi_type == 'Count':
                            self.calculate_count(atomic_kpis_data, identifier_result)
                        elif kpi_type == 'Price':
                            self.calculate_price(atomic_kpis_data, identifier_result)
                        elif kpi_type == 'SOS':
                            self.calculate_sos(atomic_kpis_data, identifier_result)
                        elif kpi_type == 'Planogram':
                            self.calculate_planogram_compliance(atomic_kpis_data, identifier_result)
                        else:
                            Log.warning("KPI of type '{}' is not supported".format(kpi_type))
                            continue
                kpi_result = self.calculate_kpi_result(kpi)
                #write result to the DB
                set_score += kpi_result
            red_score += set_score
            # write set_score to db - maybe in KPIGenerator? Will see...
        # write red_score to db - maybe in KPIGenerator? Will see..
        self.common.commit_results_data()
        self.common_scene.commit_results_data(result_entity='scene')

    def get_identifier_result_kpi(self, kpi):
        kpi_name = kpi[KPI_NAME]
        identifier_result = self.common.get_dictionary(kpi_fk=self.common.get_kpi_fk_by_kpi_type(kpi_name),
                                                       manufacturer_id=KO_ID)
        return identifier_result

    def get_identifier_result_set(self, set_name):
        kpi_name = set_name
        identifier_result = self.common.get_dictionary(kpi_fk=self.common.get_kpi_fk_by_kpi_type(kpi_name),
                                                       manufacturer_id=KO_ID)
        return identifier_result

    def get_store_data_by_store_id(self):
        query = CCBZA_SAND_Queries.get_store_data_by_store_id(self.store_id)
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

    def get_relevant_template_data(self):
        pass


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

    # def get_kpi_types_by_set_name(self, kpi_set_name):
    #     kpi_types = self.template_data[SET_TAB][self.template_data[SET_TAB][KPI_NAME] == kpi_set_name][KPI_TYPE].values[0]
    #     # kpi_types_list = kpi_types.split(', ') if ', ' in kpi_types else kpi_types.split(',')
    #     kpi_types_list = re.split(r', |,| ,', kpi_types)
    #     return kpi_types_list

    def get_kpi_types_by_kpi(self, kpi):
        kpi_types_list = []
        if kpi[KPI_TYPE]:
            kpi_types = self.template_data[KPI_TAB][self.template_data[KPI_TAB][KPI_NAME] == kpi[KPI_NAME]][KPI_TYPE].values[0]
            # kpi_types_list = self.split_string(kpi_types, [', ', ' ,', ' , ', ','])
            # kpi_types_list = re.split(r', |,| ,', kpi_types)
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

        # split_score = kpi[SPLIT_SCORE].strip(' ')
        # return True if split_score == 'Y' else False #ask Istrael if we want error message in case of invalid value

    @staticmethod
    def get_kpi_dependency(kpi):
        dependency = kpi[DEPENDENCY].strip(' ')
        return dependency if dependency else None

    @staticmethod
    def create_kpi_results_container():
        columns = [SET_NAME, KPI_NAME, ATOMIC_KPI_NAME, SCORE, MAX_SCORE]
        df = pd.DataFrame(columns=columns)
        # df = df.fillna(0) # need to see if we want to do that
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

    def calculate_kpi_result(self, kpi):
        dependency = self.get_kpi_dependency(kpi)
        if dependency:
            kpi_scores = []
            affecting_kpis = self.kpi_results_data[self.kpi_results_data[SET_NAME] == dependency]
            for index, kpi in affecting_kpis.iterrows():
                kpi_score, max_score = self.calculate_kpi_score_no_dependency(kpi)
                kpi_scores.append((kpi_score, max_score))
            if sum(map(lambda x: x[0], kpi_scores)) == sum(map(lambda x: x[1], kpi_scores)):
                score = float(kpi[self.full_store_type])
            else:
                score = 0
        else:
            score, max_score = self.calculate_kpi_score_no_dependency(kpi)
        return score

    def get_kpi_result_value_pk_by_value(self, value):
        pk = None  # I want to stop code - maybe w/o try/except?
        try:
            pk = self.kpi_result_values[self.kpi_result_values['value'] == value]['pk'].values[0]
        except:
            Log.error('Value {} does not exist'.format(value))
        return pk

    def get_kpi_score_value_pk_by_value(self, value):
        pk = None  # I want to stop code - maybe w/o try/except?
        try:
            pk = self.kpi_score_values[self.kpi_score_values['value'] == value]['pk'].values[0]
        except:
            Log.error('Value {} does not exist'.format(value))
        return pk

    # def calculate_kpi_result(self, kpi):
        # is_split_score = self.does_kpi_have_split_score(kpi)
        # kpi_name = kpi[KPI_NAME]
        # if is_split_score:
        #     kpi_score = self.kpi_results_data[self.kpi_results_data[KPI_NAME] == kpi_name][SCORE].sum()
        # else:
        #     atomic_scores = self.kpi_results_data[self.kpi_results_data[KPI_NAME] == kpi_name][SCORE].values.tolist()
        #     kpi_score = kpi[self.full_store_type] if all(atomic_scores) else 0
        # return kpi_score

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
        store_attr_1 = self.store_data['additional_attribute_1'].values[0]
        if store_attr_1:
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
            # write session result to DB

    def calculate_price(self, atomic_kpis_data, identifier_parent):
        for i in xrange(len(atomic_kpis_data)):
            atomic_kpi = atomic_kpis_data.iloc[i]
            target = atomic_kpi[TARGET]
            max_score = atomic_kpi[SCORE]
            is_by_scene = self.is_by_scene(atomic_kpi)
            atomic_result = 0
            calculation_filters = self.get_general_calculation_parameters(atomic_kpi, product_types=[SKU, OTHER])
            calculation_filters.update(self.get_availability_and_price_calculation_parameters(atomic_kpi))
            list_of_scenes = calculation_filters['scene_fk']
            if is_by_scene:
                session_results = []
                for scene in list_of_scenes:
                    scene_result = 0
                    calculation_filters['scene_fk'] = scene
                    scif = self.scif.copy()
                    matches = self.match_product_in_scene.copy()
                    merged_df = matches.merge(scif, left_on='product_fk', right_on='item_id', how='left')
                    scene_df = merged_df[self.tools.get_filter_condition(merged_df, **calculation_filters)]
                    if not scene_df.empty:
                        scene_result = self.get_price_result(scene_df, target)
                    session_results.append(scene_result)
                    # write scene result to DB
                if session_results:
                    atomic_result = 100 if all(session_results) else 0
            else:
                scif = self.scif.copy()
                matches = self.match_product_in_scene.copy()
                merged_df = matches.merge(scif, left_on='product_fk', right_on='item_id', how='left')
                filtered_df = merged_df[self.tools.get_filter_condition(merged_df, **calculation_filters)]
                if not filtered_df.empty:
                    atomic_result = self.get_price_result(filtered_df, target)
            atomic_score = self.calculate_atomic_score(atomic_result, max_score)
            self.add_kpi_result_to_kpi_results_container(atomic_kpi, atomic_score)
            # write session result to DB

    def get_price_result(self, matches_scif_df, target):
        # result = 0
        unique_skus_list = matches_scif_df['product_fk'].unique().tolist()
        # price_filters = {'product_fk': unique_skus_list}
        # if scene:
        #     price_filters.update({'scene_fk': scene})
        # matches = self.match_product_in_scene.copy()
        # relevant_matches = matches[self.tools.get_filter_condition(matches, **price_filters)]
        if target:
            result = self.calculate_price_vs_target(matches_scif_df, unique_skus_list, target)
        else:
            result = self.calculate_price_presence(matches_scif_df, unique_skus_list)
        return result

    @staticmethod
    def calculate_price_presence(matches, skus_list):
        price_presence = []
        for sku in skus_list:
            sku_prices = matches[matches['product_fk'] == sku]['price'].values.tolist()
            is_price = True if any([price is not None for price in sku_prices]) else False
            price_presence.append(is_price)
        result = 100 if all(price_presence) else 0
        return result

    @staticmethod
    def calculate_price_vs_target(matches, skus_list, target):
        target = float(target)
        price_reaching_target = []
        for sku in skus_list:
            sku_prices = matches[matches['product_fk'] == sku]['price'].values.tolist()
            price_meets_target = True if any([(price is not None and price <= target)
                                              for price in sku_prices]) else False
            price_reaching_target.append(price_meets_target)
        result = 100 if all(price_reaching_target) else 0
        return result

# option w/o merge ************************************
#     def calculate_price(self, atomic_kpis_data):
#         for i in xrange(len(atomic_kpis_data)):
#             atomic_kpi = atomic_kpis_data.iloc[i]
#             target = atomic_kpi[TARGET]
#             max_score = atomic_kpi[SCORE]
#             is_by_scene = self.is_by_scene(atomic_kpi)
#             atomic_result = 0
#             # filters = {GENERAL_FILTERS: self.get_general_calculation_parameters(atomic_kpi),  # add product type??
#             #            KPI_SPECIFIC_FILTERS: self.get_availability_and_price_calculation_parameters(atomic_kpi)}
#             calculation_filters = self.get_general_calculation_parameters(atomic_kpi, product_types=[SKU, OTHER])
#             calculation_filters.update(self.get_availability_and_price_calculation_parameters(atomic_kpi))
#             list_of_scenes = calculation_filters['scene_fk']
#             if is_by_scene:
#                 session_results = []
#                 for scene in list_of_scenes:
#                     scene_result = 0
#                     calculation_filters['scene_fk'] = scene
#                     scif = self.scif.copy()
#                     scene_scif = scif[self.tools.get_filter_condition(scif, **calculation_filters)]
#                     if not scene_scif.empty:
#                         scene_result = self.get_price_result(scene_scif, target, scene=scene)
#                     session_results.append(scene_result)
#                     # write scene result to DB
#                 if session_results:
#                     atomic_result = 100 if all(session_results) else 0
#                         # unique_skus_list = scene_scif['item_id'].unique().tolist()
#                         # price_filters = {'scene_fk': scene, 'product_fk': unique_skus_list}
#                         # matches = self.match_product_in_scene.copy()
#                         # relevant_matches = matches[self.tools.get_filter_condition(matches, **price_filters)]
#                         # # relevant_matches = matches[self.tools.get_filter_condition(matches, **price_filters)][['scene_fk', 'product_fk', 'price']]
#                         # if target:
#                         #     scene_result = self.calculate_price_vs_target(relevant_matches, unique_skus_list, target)
#                         # else:
#                         #     scene_result = self.calculate_price_presence(relevant_matches, unique_skus_list)
#             else:
#                 scif = self.scif.copy()
#                 filtered_scif = scif[self.tools.get_filter_condition(scif, **calculation_filters)]
#                 if not filtered_scif.empty:
#                     atomic_result = self.get_price_result(filtered_scif, target)
#             atomic_score = self.calculate_atomic_score(atomic_result, max_score)
#             self.add_kpi_result_to_kpi_results_container(atomic_kpi, atomic_score)
#             # write session result to DB
#
#     def get_price_result(self, scif, target, scene=None):
#         result = 0
#         unique_skus_list = scif['item_id'].unique().tolist()
#         price_filters = {'product_fk': unique_skus_list}
#         if scene:
#             price_filters.update({'scene_fk': scene})
#         matches = self.match_product_in_scene.copy()
#         relevant_matches = matches[self.tools.get_filter_condition(matches, **price_filters)]
#         if target:
#             result = self.calculate_price_vs_target(relevant_matches, unique_skus_list, target)
#         else:
#             result = self.calculate_price_presence(relevant_matches, unique_skus_list)
#         return result
#
#     @staticmethod
#     def calculate_price_presence(matches, skus_list):
#         price_presence = []
#         for sku in skus_list:
#             sku_prices = matches[matches['product_fk'] == sku]['price'].values.tolist()
#             is_price = True if any(price is not None for price in sku_prices) else False
#             price_presence.append(is_price)
#         result = 100 if all(price_presence) else 0
#         return result
#
#     @staticmethod
#     def calculate_price_vs_target(matches, skus_list, target):
#         target = float(target)
#         price_reaching_target = []
#         for sku in skus_list:
#             sku_prices = matches[matches['product_fk'] == sku]['price'].values.tolist()
#             price_meets_target = True if any(price <= target for price in sku_prices) else False
#             price_reaching_target.append(price_meets_target)
#         result = 100 if all(price_reaching_target) else 0
#         return result
# option w/o merge - end code ************************************


    # def calculate_price(self, atomic_kpis_data):
    #     for i in xrange(len(atomic_kpis_data)):
    #         score = 0
    #         atomic_kpi = atomic_kpis_data.iloc[i]
    #         # max_score = atomic_kpi[SCORE]
    #         target = atomic_kpi[TARGET]
    #         if target:
    #             score = self.calculate_price_vs_target(atomic_kpi)
    #         else:
    #             score = self.calculate_price_presence(atomic_kpi)
    #         self.add_kpi_result_to_kpi_results_container(atomic_kpi, score)
    #         # write kpi result to DB
    #
    # def calculate_price_presence(self, atomic_kpi):
    #     score = 0
    #     max_score = atomic_kpi[SCORE]
    #     filters = {GENERAL_FILTERS: self.get_general_calculation_parameters(atomic_kpi),
    #                KPI_SPECIFIC_FILTERS: self.get_availability_and_price_calculation_parameters(atomic_kpi)}
    #     calculation_filters = filters[GENERAL_FILTERS]
    #     calculation_filters.update(filters[KPI_SPECIFIC_FILTERS])
    #     list_of_scenes = calculation_filters['scene_fk']
    #     session_results = []
    #     for scene in list_of_scenes:
    #         scene_result = 0
    #         calculation_filters['scene_fk'] = scene
    #         scif = self.scif.copy()
    #         scene_relevant_scif = scif[self.tools.get_filter_condition(scif, **calculation_filters)]
    #         sku_prices_array = scene_relevant_scif['median_price'].values
    #         if sku_prices_array:
    #             scene_result = 100 if all([price is not None for price in sku_prices_array]) else 0
    #         session_results.append(scene_result)
    #     if session_results:
    #         atomic_result = 100 if all(session_results) else 0
    #         score = self.calculate_atomic_score(atomic_result, max_score)
    #     return score
    #
    # def calculate_price_vs_target(self, atomic_kpi):
    #     score = 0
    #     max_score = atomic_kpi[SCORE]
    #     target = atomic_kpi[TARGET]
    #     filters = {GENERAL_FILTERS: self.get_general_calculation_parameters(atomic_kpi),
    #                KPI_SPECIFIC_FILTERS: self.get_availability_and_price_calculation_parameters(atomic_kpi)}
    #     calculation_filters = filters[GENERAL_FILTERS]
    #     calculation_filters.update(filters[KPI_SPECIFIC_FILTERS])
    #     list_of_scenes = calculation_filters['scene_fk']
    #     session_results = []
    #     for scene in list_of_scenes:

    def calculate_count(self, atomic_kpis_data, identifier_parent):
        for i in xrange(len(atomic_kpis_data)):
            atomic_kpi = atomic_kpis_data.iloc[i]
            max_score = atomic_kpi[SCORE]
            target = float(atomic_kpi[TARGET])
            calculation_filters = self.get_general_calculation_parameters(atomic_kpi)
            session_door_count = []
            for scene in calculation_filters['scene_fk']:
                scene_filter = {'scene_fk': scene}
                matches = self.match_product_in_scene.copy()
                relevant_match_prod_in_scene = matches[self.tools.get_filter_condition(matches, **scene_filter)]
                number_of_bays = len(relevant_match_prod_in_scene['bay_number'].unique())
                session_door_count.append(number_of_bays)
            atomic_result = 100 if any([doors == target for doors in session_door_count]) else 0
            atomic_score = self.calculate_atomic_score(atomic_result, max_score)
            self.add_kpi_result_to_kpi_results_container(atomic_kpi, atomic_score)

            # constructing queries for DB
            kpi_fk = self.common.get_kpi_fk_by_kpi_name(atomic_kpi[ATOMIC_KPI_NAME])
            self.common.write_to_db_result(fk=kpi_fk, numerator_id=KO_ID, score=atomic_score,
                                           denominator_id=self.store_id,
                                           identifier_parent=identifier_parent,
                                           target=int(float(max_score)), should_enter=True)

    def calculate_survey(self, atomic_kpis_data, identifier_parent):
        """
        This function calculates Survey-Question typed Atomics, and writes the result to the DB.
        """
        for i in xrange(len(atomic_kpis_data)):
            atomic_kpi = atomic_kpis_data.iloc[i]
            survey_id = int(float(atomic_kpi[SURVEY_QUESTION_CODE]))
            expected_answers = atomic_kpi[EXPECTED_RESULT]
            survey_max_score = float(atomic_kpi[SCORE])
            survey_answer = self.tools.get_survey_answer(('question_fk', survey_id))
            # atomic_fk = self.kpi_static_data[self.kpi_static_data['atomic_kpi_name'] == atomic_kpi[ATOMIC_KPI_NAME]] # fk from new tables
            score = None
            if survey_max_score:
                score = int(survey_max_score) if survey_answer in expected_answers else 0
            else:
                score = 100 if survey_answer in expected_answers else 0 #check if this is 100 or 1

            self.add_kpi_result_to_kpi_results_container(atomic_kpi, score)
            # constructing queries for DB
            custom_score = self.get_pass_fail(score)
            kpi_fk = self.common.get_kpi_fk_by_kpi_name(atomic_kpi[ATOMIC_KPI_NAME])
            self.common.write_to_db_result(fk=kpi_fk, numerator_id=KO_ID, score=custom_score,
                                           denominator_id=self.store_id, result=score,
                                           identifier_parent=identifier_parent,
                                           target=int(survey_max_score), should_enter=True)

            # append score to db queries - which tables should it be?
            # self.write_to_db_result(atomic_fk, self.LEVEL3, score, score) # verify after...

    # def get_kpi_data_by_set_name(self, kpi_set_name):
    #     kpi_data = self.template_data[KPI_TAB][self.template_data[KPI_TAB][SET_NAME] == kpi_set_name]

    def get_pass_fail(self, score):
        value = 'Failed' if not score else 'Passed'
        custom_score = self.get_kpi_score_value_pk_by_value(value)
        return custom_score

    def get_x_v(self, score):
        value = 'X' if not score else 'V'
        custom_score = self.get_kpi_score_value_pk_by_value(value)
        return custom_score

    def add_kpi_result_to_kpi_results_container(self, atomic_kpi, score):
        # columns = [SET_NAME, KPI_NAME, ATOMIC_KPI_NAME, SCORE]
        # set_name = self.current_kpi_set_name
        kpi_name = atomic_kpi[KPI_NAME]
        atomic_name = atomic_kpi[ATOMIC_KPI_NAME]
        max_score = float(atomic_kpi[SCORE]) if atomic_kpi[SCORE] else atomic_kpi[SCORE]
        # new_kpi_row = pd.DataFrame([self.current_kpi_set_name, kpi_name, atomic_name, score], columns=columns)
        # self.kpi_results_data = self.kpi_results_data.append(new_kpi_row, ignore_index=True)
        #
        self.kpi_results_data = self.kpi_results_data.append([{SET_NAME: self.current_kpi_set_name, KPI_NAME: kpi_name,
                                                               ATOMIC_KPI_NAME: atomic_name, SCORE: score,
                                                               MAX_SCORE: max_score}],
                                                             ignore_index=True)

    def calculate_sos(self, atomic_kpis_data, identifier_parent):
        for i in xrange(len(atomic_kpis_data)):
            atomic_kpi = atomic_kpis_data.iloc[i]
            max_score = atomic_kpi[SCORE]
            general_filters = self.get_general_calculation_parameters(atomic_kpi)
            scif = self.scif.copy()
            filtered_scif = scif[self.tools.get_filter_condition(scif, **general_filters)]
            atomic_result = 0
            if not filtered_scif.empty:
                sos_filters = self.get_sos_calculation_parameters(atomic_kpi)
                number_of_conditions = len(sos_filters.items())
                conditions_results = []
                for condition, filters in sos_filters.items():
                    target = float(filters[condition].pop('target'))/100
                    ratio, num_res, denom_res = self.calculate_sos_for_condition(filtered_scif, sos_filters[condition])
                    condition_score = 100 if ratio >= target else 0
                    conditions_results.append(condition_score)

                    # write condition result to DB
                    custom_score = self.get_pass_fail(condition_score)
                    atomic_name = atomic_kpi[ATOMIC_KPI_NAME] if number_of_conditions == 1 \
                                    else '{} {}'.format(atomic_kpi[ATOMIC_KPI_NAME], condition)
                    kpi_fk = self.common.get_kpi_fk_by_kpi_name(atomic_name)
                    self.common.write_to_db_result(fk=kpi_fk, numerator_id=KO_ID, numerator_result=num_res,
                                                   denominator_id=self.store_id, denominator_result=denom_res,
                                                   result=ratio, score=custom_score,
                                                   identifier_parent=identifier_parent,
                                                   target=target, should_enter=True)
                if conditions_results:
                    atomic_result = 100 if all(conditions_results) else 0
            atomic_score = self.calculate_atomic_score(atomic_result, max_score)
            self.add_kpi_result_to_kpi_results_container(atomic_kpi, atomic_score)
            # write atomic score / maybe also result to DB

    def calculate_sos_for_condition(self, scif, filters):
        numer_result = scif[self.tools.get_filter_condition(scif, **filters['numer'])]['facings'].sum()
        denom_result = scif[self.tools.get_filter_condition(scif, **filters['denom'])]['facings'].sum()
        ratio = float(numer_result) / denom_result if denom_result != 0 else 0
        return ratio, numer_result, denom_result


    # def get_facings_based_on_critera(self, scif, filters):
    #     filtered_scif = self.filter_df_based_on_filtering_dictionary(scif, filters)
    #     # for column, criteria in filters.items():
    #     #     if isinstance(criteria, list):
    #     #         filtered_scif = filtered_scif[filtered_scif[column].isin(criteria)]
    #     #     else:
    #     #         filtered_scif = filtered_scif[filtered_scif[column] == criteria]
    #     number_of_facings = sum(filtered_scif['facings'].values) if not filtered_scif.empty else 0
    #     return number_of_facings

    # def calculate_sos_by_scene(self, atomic_kpis_data):
    #     for i in xrange(len(atomic_kpis_data)):
    #         atomic_kpi = atomic_kpis_data.iloc[i]
    #         calculation_filters = {GENERAL_FILTERS: self.get_general_calculation_parameters(atomic_kpi),
    #                                KPI_SPECIFIC_FILTERS: self.get_sos_calculation_parameters(atomic_kpi)}
    #         list_of_scenes = calculation_filters[GENERAL_FILTERS]['scene_fk']
    #         condition_1_target = float(atomic_kpi[CONDITION_1_TARGET]) / 100
    #         condition_2_target = float(atomic_kpi[CONDITION_2_TARGET]) / 100
    #         max_score = atomic_kpi[SCORE]
    #
    #         scenes_results = []
    #         for scene in list_of_scenes:
    #             condition_scores = []
    #             calculation_filters[GENERAL_FILTERS]['scene_fk'] = scene
    #             filtered_scif = self.filter_df_based_on_filtering_dictionary(self.scif, calculation_filters[GENERAL_FILTERS])
    #             condition_1_ratio = self.calculate_sos_for_condition(filtered_scif, calculation_filters[KPI_SPECIFIC_FILTERS][CONDITION_1])
    #             condition_scores.append(100 if condition_1_ratio >= condition_1_target else 0)
    #
    #             condition_2_ratio = self.calculate_sos_for_condition(filtered_scif, calculation_filters[KPI_SPECIFIC_FILTERS][CONDITION_2])
    #             condition_scores.append(100 if condition_2_ratio >= condition_2_target else 0)
    #             scenes_results.append(100 if all(condition_scores) else 0)
    #             # do we need to write the result for each scene to DB
    #
    #         atomic_result = 100 if all(scenes_results) else 0
    #         atomic_score = self.calculate_atomic_score(atomic_result, max_score)
    #         self.add_kpi_result_to_kpi_results_container(atomic_kpi, atomic_score)
    #         # write atomic score / maybe also result to DB

    #         # OPTION 2- SOS for session but at specific scene types - uncomment template name parameter at sos_calculation parameters
    #         atomic_kpi = atomic_kpis_data.iloc[i]
    #         sos_calculation_filters = self.get_sos_calculation_parameters(atomic_kpi)
    #         condition_scores = []
    #
    #         condition_1_target = float(atomic_kpi[CONDITION_1_TARGET])/100
    #         condition_1_ratio = self.calculate_sos_per_condition(sos_calculation_filters[CONDITION_1])
    #         condition_scores.append(100 if condition_1_ratio >=condition_1_target else 0)
    #
    #         condition_2_target = float(atomic_kpi[CONDITION_2_TARGET])/100
    #         condition_2_ratio = self.calculate_sos_per_condition(sos_calculation_filters[CONDITION_2])
    #         condition_scores.append(100 if condition_2_ratio >= condition_2_target else 0)
    #
    #         atomic_score = atomic_kpi[SCORE] if all([score == 100 for score in condition_scores]) else 0
    #         self.add_kpi_result_to_kpi_results_container(atomic_kpi, atomic_score)
    #         # write atomic score / maybe also result to DB
    #
    # def calculate_sos_per_condition(self, condition_filters):
    #     numerator_result = self.get_facings_based_on_critera(self.scif, condition_filters['numerator'])
    #     denominator_result = self.get_facings_based_on_critera(self.scif, condition_filters['denominator'])
    #     return float(numerator_result)/denominator_result if denominator_result != 0 else 0


    def filter_df_based_on_filtering_dictionary(self, df, filters):
        filtered_df = df.copy()
        for column, criteria in filters.items():
            if isinstance(criteria, list):
                filtered_df = filtered_df[filtered_df[column].isin(criteria)]
            else:
                filtered_df = filtered_df[filtered_df[column] == criteria]
        return filtered_df

    #will be adding it when necessarry
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
        # if template_names:
        #     relevant_scenes = self.scif[(self.scif['template_name'].isin(template_names))]
        # else:
        #     relevant_scenes = self.scif
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
    # def get_sos_condition_filters(self, atomic_kpi, *conditions):
    #     columns = atomic_kpi.index.values
    #     for condition in conditions:
    #         condition_columns = filter(lambda y: y[0] == condition, map(lambda x: x.split(' '), columns))
    #

    def calculate_availability_custom(self, atomic_kpis_data, identifier_parent):
        for i in xrange(len(atomic_kpis_data)):
            atomic_kpi = atomic_kpis_data.iloc[i]
            score = 0
            if atomic_kpi[AVAILABILITY_TYPE] == AVAILABILITY_POS:
                score = self.calculate_availability_brand_strips(atomic_kpi)
            elif atomic_kpi[AVAILABILITY_TYPE] == AVAILABILITY_SKU_FACING_AND:
                score = self.calculate_availability_sku_facing_and(atomic_kpi)
            elif atomic_kpi[AVAILABILITY_TYPE] == AVAILABILITY_SKU_FACING_OR:
                score = self.calculate_availability_sku_facing_or(atomic_kpi)
            elif atomic_kpi[AVAILABILITY_TYPE] == AVAILABILITY_POSM:
                score = self.calculate_availability_posm(atomic_kpi)
            elif atomic_kpi[AVAILABILITY_TYPE] == AVAILABLITY_IF_THEN:
                score = self.calculate_avialability_against_competitors(atomic_kpi)
            else:
                Log.warning('Availablity of type {} is not supported by calculation'.format(atomic_kpi[AVAILABILITY_TYPE]))
                continue
            self.add_kpi_result_to_kpi_results_container(atomic_kpi, score)
            # write atomic score (maybe also result) to DB

    def calculate_avialability_against_competitors(self, atomic_kpi):
        max_score = atomic_kpi[SCORE]
        score = 0
        is_by_scene = self.is_by_scene(atomic_kpi)
        filters = {GENERAL_FILTERS: self.get_general_calculation_parameters(atomic_kpi,
                                                                            product_types=[SKU, OTHER]),
                   KPI_SPECIFIC_FILTERS: self.get_availability_and_price_calculation_parameters(atomic_kpi)}
        if is_by_scene:
            session_results = []
            list_of_scenes = filters[GENERAL_FILTERS]['scene_fk']
            for scene in list_of_scenes:
                scene_result = 0
                filters[GENERAL_FILTERS]['scene_fk'] = scene
                non_ko_filters = filters[GENERAL_FILTERS].copy()
                non_ko_filters.update(filters[KPI_SPECIFIC_FILTERS]['category'])
                scif = self.scif.copy()
                scene_scif = scif[self.tools.get_filter_condition(scif, **non_ko_filters)]
                non_ko_facings = scene_scif[~scene_scif[MANUFACTURER_NAME] == KO_PRODUCTS]['facings'].sum()
                if non_ko_facings >= 1:
                    ko_filters = filters[GENERAL_FILTERS].copy()
                    ko_filters.update(filters[KPI_SPECIFIC_FILTERS])
                    removed_filter = ko_filters.pop('manufacturer_except')
                    scene_scif = self.scif.copy()
                    ko_facings = scene_scif[self.tools.get_filter_condition(scif, **ko_filters)]['facings'].sum()
                    scene_result = 100 if ko_facings >= 2 else 0
                session_results.append(scene_result)
            if session_results:
                atomic_result = 100 if all(session_results) else 0
                score = self.calculate_atomic_score(atomic_result, max_score)
        return score

    # calculated on session level
    def calculate_availability_posm(self, atomic_kpi):
        max_score = atomic_kpi[SCORE]
        target = atomic_kpi[TARGET]
        calculation_filters = self.get_general_calculation_parameters(atomic_kpi)
        calculation_filters.update(self.get_availability_and_price_calculation_parameters(atomic_kpi))
        scif = self.scif.copy()
        facings = scif[self.tools.get_filter_condition(scif, **calculation_filters)]['facings'].sum()
        atomic_result = 100 if facings >= float(target) else 0
        score = self.calculate_atomic_score(atomic_result, max_score)
        return score

    def calculate_availability_brand_strips(self, atomic_kpi):
        score = 0
        max_score = atomic_kpi[SCORE]
        target = atomic_kpi[TARGET]
        # is_by_scene = self.is_by_scene(atomic_kpi)
        calculation_filters = {GENERAL_FILTERS: self.get_general_calculation_parameters(atomic_kpi),
                               KPI_SPECIFIC_FILTERS: self.get_availability_and_price_calculation_parameters(atomic_kpi)}
        list_of_scenes = calculation_filters[GENERAL_FILTERS]['scene_fk']
        session_results = []
        for scene in list_of_scenes:
            scene_score = 0
            calculation_filters[GENERAL_FILTERS]['scene_fk'] = scene
            scif = self.scif.copy()
            scene_scif = scif[self.tools.get_filter_condition(scif, **calculation_filters[GENERAL_FILTERS])]
            brands_count = scene_scif[~scene_scif[PRODUCT_TYPE].isin([POS, IRRELEVANT, EMPTY])].groupby(BRAND_NAME)[BRAND_NAME].count()
            # scene_scif = self.filter_df_based_on_filtering_dictionary(self.scif, calculation_filters[GENERAL_FILTERS])
            # scene_skus_scif = scene_scif[scene_scif[PRODUCT_TYPE] == SKU]
            # brands_count = scene_skus_scif.groupby(BRAND_NAME)[BRAND_NAME].count()
            if not brands_count.empty:
                scene_brand_strips_df = scene_scif[self.tools.get_filter_condition(scene_scif,
                                                                                   **calculation_filters[KPI_SPECIFIC_FILTERS])]
                # scene_brand_strips_df = self.filter_df_based_on_filtering_dictionary(scene_scif, calculation_filters[KPI_SPECIFIC_FILTERS])
                brand_strips_count = scene_brand_strips_df.groupby(POS_BRAND_FIELD)[POS_BRAND_FIELD].count()
                scene_results = self.match_brand_strip_to_brand(brands_count, brand_strips_count, target, scene)
                # for brand, count in brands_count.iteritems():
                #     # brand_strip_result = None # need to discuss what we do with results - writing / not to DB?
                #     try:
                #         if count*int(float(target)) <= brand_strips_count[brand]:
                #             brand_result = True
                #         else:
                #             brand_result = False
                #     except KeyError:
                #         Log.info('Brand strip for brand {} does not exist in scene {}'.format(brand, scene))
                #         brand_result = False
                #     scene_results.append(brand_result)
                scene_score = 100 if all(scene_results) else 0
            session_results.append(scene_score)
            # write results per scene to DB
        if session_results:
            atomic_result = 100 if all(session_results) else 0
            score = self.calculate_atomic_score(atomic_result, max_score)
        return score

    def is_by_scene(self, atomic_kpi):
        return True if atomic_kpi[BY_SCENE] == 'Y' else False

    def match_brand_strip_to_brand(self, brands_series, brand_strips_series, target, scene):
        scene_results = []
        for brand, count in brands_series.iteritems():
            # brand_strip_result = None # need to discuss what we do with results - writing / not to DB?
            try:
                if count * int(float(target)) <= brand_strips_series[brand]:
                    brand_result = True
                else:
                    brand_result = False
            except KeyError:
                Log.info('Brand strip for brand {} does not exist in scene {}'.format(brand, scene))
                brand_result = False
            scene_results.append(brand_result)
        return scene_results

    # def calculate_availability_sku_facing_and(self, atomic_kpi):
    #     max_score = atomic_kpi[SCORE]
    #     calculation_filters = {GENERAL_FILTERS: self.get_general_calculation_parameters(atomic_kpi),
    #                            KPI_SPECIFIC_FILTERS: self.get_availability_and_price_calculation_parameters(atomic_kpi)}
    #     atomic_result = 0
    #     scene_results = []
    #     list_of_scenes = calculation_filters[GENERAL_FILTERS]['scene_fk']
    #     for scene in list_of_scenes:
    #         calculation_filters[GENERAL_FILTERS]['scene_fk'] = scene
    #         scene_relevant_scif = self.filter_df_based_on_filtering_dictionary(self.scif,
    #                                                                            calculation_filters[GENERAL_FILTERS])
    #         scene_result = self.calculate_availability_result_all_skus(scene_relevant_scif, calculation_filters,
    #                                                                    atomic_kpi)
    #         scene_results.append(scene_result)
    #     if scene_results:
    #         # write result to DB by scene????
    #         atomic_result = 100 if all([result == 100 for result in scene_results]) else 0
    #     score = self.calculate_atomic_score(atomic_result, max_score)
    #     return score

    def calculate_availability_sku_facing_and(self, atomic_kpi):
        max_score = atomic_kpi[SCORE]
        is_by_scene = self.is_by_scene(atomic_kpi)
        calculation_filters = {GENERAL_FILTERS: self.get_general_calculation_parameters(atomic_kpi,
                                                                                        product_types=[SKU, OTHER]),
                               KPI_SPECIFIC_FILTERS: self.get_availability_and_price_calculation_parameters(atomic_kpi)}
        # filtered_scif = self.filter_df_based_on_filtering_dictionary(self.scif, calculation_filters[GENERAL_FILTERS])
        atomic_result = 0
        if is_by_scene:
            session_results = []
            list_of_scenes = calculation_filters[GENERAL_FILTERS]['scene_fk']
            for scene in list_of_scenes:
                calculation_filters[GENERAL_FILTERS]['scene_fk'] = scene
                scif = self.scif.copy()
                scene_scif = scif[self.tools.get_filter_condition(scif, **calculation_filters[GENERAL_FILTERS])]
                if not scene_scif.empty:
                    scene_result = self.calculate_availability_result_all_skus(scene_scif, calculation_filters, atomic_kpi)
                    session_results.append(scene_result)
                    # write result and score to DB by scene
                # scene_relevant_scif = self.filter_df_based_on_filtering_dictionary(self.scif,
                #                                                                    calculation_filters[GENERAL_FILTERS])

                # unique_skus_list = self.split_and_strip(atomic_kpi['product_ean_code']) if 'product_ean_code' in calculation_filters[KPI_SPECIFIC_FILTERS].keys() \
                #                                                                         else scene_relevant_scif['product_ean_code'].unique().tolist()
                # calculation_filters[KPI_SPECIFIC_FILTERS]['product_ean_code'] = unique_skus_list
                # result_scene_df = self.filter_df_based_on_filtering_dictionary(scene_relevant_scif, calculation_filters[KPI_SPECIFIC_FILTERS])
                # facings_by_sku = self.get_facings_by_sku(result_scene_df, unique_skus_list)
                # scene_atomic_result = 100 if all([facing >= target for facing in facings_by_sku.values()]) and all(
                #     [sku in unique_skus_list for sku in facings_by_sku.keys()]) else 0

            if session_results:
                atomic_result = 100 if all([result == 100 for result in session_results]) else 0
        else:
            scif = self.scif.copy()
            filtered_scif = scif[self.tools.get_filter_condition(scif, **calculation_filters[GENERAL_FILTERS])]
            # filtered_scif = self.filter_df_based_on_filtering_dictionary(self.scif, calculation_filters[GENERAL_FILTERS])
            if not filtered_scif.empty:
                atomic_result = self.calculate_availability_result_all_skus(filtered_scif, calculation_filters, atomic_kpi)
            # unique_skus_list = self.split_and_strip(atomic_kpi['product_ean_code']) if 'product_ean_code' in calculation_filters[KPI_SPECIFIC_FILTERS].keys() \
            #                                                                         else filtered_scif['product_ean_code'].unique().tolist()
            # calculation_filters[KPI_SPECIFIC_FILTERS]['product_ean_code'] = unique_skus_list
            # result_session_df = self.filter_df_based_on_filtering_dictionary(filtered_scif, calculation_filters[KPI_SPECIFIC_FILTERS])
            # facings_by_sku = self.get_facings_by_sku(result_session_df, unique_skus_list)
            # atomic_result = 100 if all([facing>=target for facing in facings_by_sku.values()]) and all ([sku in unique_skus_list for sku in facings_by_sku.keys()]) else 0

        score = self.calculate_atomic_score(atomic_result, max_score)
        return score

    # def calculate_availability_sku_facing_or(self, atomic_kpi):
    #     max_score = atomic_kpi[SCORE]
    #     score = 0
    #     calculation_filters = {GENERAL_FILTERS: self.get_general_calculation_parameters(atomic_kpi, product_types=[SKU, OTHER]),
    #                            KPI_SPECIFIC_FILTERS: self.get_availability_and_price_calculation_parameters(atomic_kpi)}
    #     atomic_result = 0
    #     scene_results = []
    #     list_of_scenes = calculation_filters[GENERAL_FILTERS]['scene_fk']
    #     for scene in list_of_scenes:
    #         calculation_filters[GENERAL_FILTERS]['scene_fk'] = scene
    #         scif = self.scif.copy()
    #         scene_scif = scif[self.tools.get_filter_condition(scif, **calculation_filters[GENERAL_FILTERS])]
    #         # scene_relevant_scif = self.filter_df_based_on_filtering_dictionary(self.scif,
    #         #                                                                    calculation_filters[GENERAL_FILTERS])
    #         scene_result = self.calculate_availability_result_any_sku(scene_scif, calculation_filters, atomic_kpi)
    #         scene_results.append(scene_result)
    #     if scene_results:
    #         # write result to DB by scene????
    #         atomic_result = 100 if all([result == 100 for result in scene_results]) else 0
    #     score = self.calculate_atomic_score(atomic_result, max_score)
    #     return score

    def calculate_availability_sku_facing_or(self, atomic_kpi):
        max_score = atomic_kpi[SCORE]
        is_by_scene = self.is_by_scene(atomic_kpi)
        calculation_filters = {GENERAL_FILTERS: self.get_general_calculation_parameters(atomic_kpi, product_types=[SKU, OTHER]),
                               KPI_SPECIFIC_FILTERS: self.get_availability_and_price_calculation_parameters(atomic_kpi)}
        atomic_result = 0
        if is_by_scene:
            session_results = []
            list_of_scenes = calculation_filters[GENERAL_FILTERS]['scene_fk']
            for scene in list_of_scenes:
                calculation_filters[GENERAL_FILTERS]['scene_fk'] = scene
                scif = self.scif.copy()
                scene_scif = scif[self.tools.get_filter_condition(scif, **calculation_filters[GENERAL_FILTERS])]
                if not scene_scif.empty:
                    scene_result = self.calculate_availability_result_any_sku(scene_scif, calculation_filters, atomic_kpi)
                    session_results.append(scene_result)
                    # write result to DB by scene
            if session_results:
                atomic_result = 100 if all([result == 100 for result in session_results]) else 0
        else:
            scif = self.scif.copy()
            filtered_scif = scif[self.tools.get_filter_condition(scif, **calculation_filters[GENERAL_FILTERS])]
            if not filtered_scif.empty:
                atomic_result = self.calculate_availability_result_any_sku(filtered_scif, calculation_filters, atomic_kpi)
        score = self.calculate_atomic_score(atomic_result, max_score)
        return score

    def get_availability_and_price_calculation_parameters(self, atomic_kpi):
        # print atomic_kpi.index.values
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
                        # current_type = atomic_kpi[column]
                        # current_value = self.get_string_or_number(atomic_kpi[column], atomic_kpi[value_col])
                        # condition_filters.update({current_type: current_value if len(value_list) <= 1 else value_list})
                        # condition_filters.update({atomic_kpi[column]: self.get_string_or_number(atomic_kpi[column], atomic_kpi[value_col]) if len(value_list) <= 1 else value_list})
                    else:
                        Log.error('condition {} does not have corresponding value column'.format(column)) # should it be error?
        # if atomic_kpi[TEMPLATE_NAME]:
        #     condition_filters.update({'template_name': self.split_and_strip(atomic_kpi[TEMPLATE_NAME]), 'manufacturer_name': KO_PRODUCTS})
        # else:
        #     condition_filters.update({'manufacturer_name': KO_PRODUCTS})
        return condition_filters

    # see later if I want to use this function
    @staticmethod
    def get_string_or_number(field, value):
        if field in NUMERIC_FIELDS:
            try:
                value = float(value)
            except:
                value = value
        return value

    def calculate_availability_result_all_skus(self, scif, filters, atomic_kpi):
        target = atomic_kpi[TARGET]
        unique_skus_list = self.split_and_strip(atomic_kpi[EAN_CODE]) if EAN_CODE in filters[KPI_SPECIFIC_FILTERS].keys() \
            else scif[EAN_CODE].unique().tolist()
        filters[KPI_SPECIFIC_FILTERS].update({EAN_CODE: unique_skus_list})
        result_df = scif[self.tools.get_filter_condition(scif, **filters[KPI_SPECIFIC_FILTERS])]
        # result_df = self.filter_df_based_on_filtering_dictionary(scif, filters[KPI_SPECIFIC_FILTERS])
        facings_by_sku = self.get_facings_by_item(result_df, unique_skus_list, EAN_CODE)
        result = 100 if all([facing >= target for facing in facings_by_sku.values()]) and \
                        all([sku in unique_skus_list for sku in facings_by_sku.keys()]) else 0
        return result

    def calculate_availability_result_any_sku(self, scif, filters, atomic_kpi):
        target = atomic_kpi[TARGET]
        unique_skus_list = self.split_and_strip(atomic_kpi[EAN_CODE]) if EAN_CODE in filters[KPI_SPECIFIC_FILTERS].keys() \
            else scif[EAN_CODE].unique().tolist()
        filters[KPI_SPECIFIC_FILTERS][EAN_CODE] = unique_skus_list
        result_df = scif[self.tools.get_filter_condition(scif, **filters[KPI_SPECIFIC_FILTERS])]
        facings = result_df['facings'].sum()
        result = 100 if facings >= target else 0
        return result

    @staticmethod
    def get_facings_by_item(facings_df, iter_list, filter_field):
        facings_by_item = {}
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