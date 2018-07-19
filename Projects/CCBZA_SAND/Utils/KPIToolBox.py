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

#Other constants
CONDITION_1 = 'Condition_1'
CONDITION_2 = 'Condition_2'
AVAILABILITY_POS = 'Availability POS'
AVAILABILITY_SKU_FACING_AND = 'Availability SKU facing And'
AVAILABILITY_SKU_FACING_OR = 'Availability SKU facing Or'
KO_PRODUCTS = 'KO PRODUCTS'
GENERAL_FILTERS = 'general_filters'
KPI_SPECIFIC_FILTERS = 'kpi_specific_filters'

# scif fields
EAN_CODE = 'product_ean_code'
BRAND_NAME = 'brand_name'
POS_BRAND_FIELD = 'attr5' # in the labels will need to be changed
PRODUCT_TYPE = 'product_type'
NUMERIC_FIELDS = ['size']

#scif values
SKU='SKU'
POS='POS'

class CCBZA_SANDToolBox:
    LEVEL1 = 1
    LEVEL2 = 2
    LEVEL3 = 3

    def __init__(self, data_provider, output):
        self.output = output
        self.data_provider = data_provider

        # self.common = Common(self.data_provider)
        # self.general_tool_box = GENERALToolBox(self.data_provider)
        # self.common_sos = SOS(self.data_provider, self.output)
        self.common_availability = Availability(self.data_provider)

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
        self.rds_conn = ProjectConnector(self.project_name, DbUsers.CalculationEng)
        # self.kpi_static_data = self.common.get_kpi_static_data()

        self.kpi_results_queries = []

        self.kpi_static_data = self.get_kpi_static_data()
        self.new_kpi_static_data = self.get_new_kpi_static_data()
        self.kpi_results_data = self.create_kpi_results_container()
        self.store_data = self.get_store_data_by_store_id()
        self.template_path = self.get_template_path()
        self.template_data = self.get_template_data()
        # self.kpi_sets = self.kpi_static_data['kpi_set_fk'].unique().tolist()
        self.kpi_sets = self.template_data[KPI_TAB][SET_NAME].unique().tolist()
        self.current_kpi_set_name = ''
        self.tools = CCBZA_SAND_GENERALToolBox(self.data_provider, self.output)

    def main_calculation(self, *args, **kwargs):
        """
        This function calculates the KPI results.
        """
        # if self.template_data:
        for kpi_set_name in self.kpi_sets:
            self.current_kpi_set_name = kpi_set_name
            set_score = 0
            # kpi_set_name = self.kpi_static_data[self.kpi_static_data['kpi_set_fk'] == kpi_set]['kpi_set_name'].values[0]
            # self.current_kpi_set_name = self.kpi_static_data[self.kpi_static_data['kpi_set_fk'] == kpi_set]['kpi_set_name'].values[0] #alternative

            # kpi_types = self.get_kpi_types_by_set_name(kpi_set_name)
            kpi_data = self.template_data[KPI_TAB][self.template_data[KPI_TAB][SET_NAME] == kpi_set_name] # we get the relevant kpis for the set
            for index, kpi in kpi_data.iterrows():
                kpi_types = self.get_kpi_types_by_kpi(kpi)
                # atomic_scores = []
                for kpi_type in kpi_types:
                    atomic_kpis_data = self.get_atomic_kpis_data(kpi_type, kpi) # we get relevant atomics from the sheets
                    if kpi_type == 'Survey':
                        self.calculate_survey(atomic_kpis_data)
                    elif kpi_type == 'Availability':
                        self.calculate_availability_custom(atomic_kpis_data)
                    elif kpi_type == 'Count':
                        pass
                    elif kpi_type == 'Price':
                        pass
                    elif kpi_type == 'SOS':
                        self.calculate_sos(atomic_kpis_data)
                    else:
                        Log.warning("KPI of type '{}' is not supported".format(kpi_type.encode('utf8')))
                        continue
                kpi_result = self.calculate_kpi_result(kpi)
                #write result to the DB
                set_score += kpi_result
            # write set_score to db - maybe in KPIGenerator? Will see...
            self.commit_results_data()

    def get_store_data_by_store_id(self):
        query = CCBZA_SAND_Queries.get_store_data_by_store_id(self.store_id)
        query_result = pd.read_sql_query(query, self.rds_conn.db)
        return query_result

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
        return True if split_score == 'Y' else False

    @staticmethod
    def does_kpi_have_dependency(kpi):
        dependency = kpi[DEPENDENCY].strip(' ')
        return dependency if dependency else None

    @staticmethod
    def create_kpi_results_container():
        columns = [SET_NAME, KPI_NAME, ATOMIC_KPI_NAME, SCORE]
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
        return map(lambda x: x.strip(' '), str(string).split(','))

    def calculate_kpi_result(self, kpi):
        is_split_score = self.does_kpi_have_split_score(kpi)
        dependency = self.does_kpi_have_dependency(kpi)
        kpi_name = kpi[KPI_NAME]
        if is_split_score:
            # filter out relevant atomic scores and add them up
            # kpi_results = self.kpi_results_data.groupby(KPI_NAME).aggregate('sum')
            pass
        else:
            # filter out relevant kpi lines
            # see if all the atomics pass
            # take the score from the kpi tab for the relevant store
            pass

    def calculate_survey(self, atomic_kpis_data):
        """
        This function calculates Survey-Question typed Atomics, and writes the result to the DB.
        """
        for i in xrange(len(atomic_kpis_data)):
            atomic_kpi = atomic_kpis_data.iloc[i]
            survey_id = int(atomic_kpi[SURVEY_QUESTION_CODE])
            expected_answers = atomic_kpi[EXPECTED_RESULT]
            survey_max_score = atomic_kpi[SCORE]
            survey_answer = self.tools.get_survey_answer(('question_fk', survey_id))
            atomic_fk = self.kpi_static_data[self.kpi_static_data['atomic_kpi_name'] == atomic_kpi[ATOMIC_KPI_NAME]]
            score = None
            if survey_max_score:
                score = survey_max_score if survey_answer in expected_answers else 0
            else:
                score = 100 if survey_answer in expected_answers else 0 #check if this is 100 or 1
            self.add_kpi_result_to_kpi_results_container(atomic_kpi, score)
            # append score to db queries - which tables should it be?
            # self.write_to_db_result(atomic_fk, self.LEVEL3, score, score) # verify after...

    # def get_kpi_data_by_set_name(self, kpi_set_name):
    #     kpi_data = self.template_data[KPI_TAB][self.template_data[KPI_TAB][SET_NAME] == kpi_set_name]

    def add_kpi_result_to_kpi_results_container(self, atomic_kpi, score):
        columns = [SET_NAME, KPI_NAME, ATOMIC_KPI_NAME, SCORE]
        # set_name = self.current_kpi_set_name
        kpi_name = atomic_kpi[KPI_NAME]
        atomic_name = atomic_kpi[ATOMIC_KPI_NAME]
        new_kpi_row = pd.DataFrame([self.current_kpi_set_name, kpi_name, atomic_name, score], columns=columns)
        self.kpi_results_data = self.kpi_results_data.append(new_kpi_row, ignore_index=True)

    def calculate_sos(self, atomic_kpis_data):
        for i in xrange(len(atomic_kpis_data)):
            atomic_kpi = atomic_kpis_data.iloc[i]
            sos_calculation_filters = self.get_sos_calculation_parameters(atomic_kpi)
            condition_scores = []

            condition_1_target = float(atomic_kpi[CONDITION_1_TARGET])/100
            condition_1_ratio = self.calculate_sos_per_condition(sos_calculation_filters[CONDITION_1])
            condition_scores.append(100 if condition_1_ratio >=condition_1_target else 0)

            condition_2_target = float(atomic_kpi[CONDITION_2_TARGET])/100
            condition_2_ratio = self.calculate_sos_per_condition(sos_calculation_filters[CONDITION_2])
            condition_scores.append(100 if condition_2_ratio >= condition_2_target else 0)

            atomic_score = atomic_kpi[SCORE] if all([score == 100 for score in condition_scores]) else 0
            self.add_kpi_result_to_kpi_results_container(atomic_kpi, atomic_score)
            # write atomic score / maybe also result to DB

    def calculate_sos_per_condition(self, condition_filters):
        numerator_result = self.get_facings_based_on_critera(self.scif, condition_filters['numerator'])
        denominator_result = self.get_facings_based_on_critera(self.scif, condition_filters['denominator'])
        return float(numerator_result)/denominator_result if denominator_result != 0 else 0

    def get_facings_based_on_critera(self, scif, filters):
        filtered_scif = self.filter_df_based_on_filtering_dictionary(scif, filters)
        # for column, criteria in filters.items():
        #     if isinstance(criteria, list):
        #         filtered_scif = filtered_scif[filtered_scif[column].isin(criteria)]
        #     else:
        #         filtered_scif = filtered_scif[filtered_scif[column] == criteria]
        number_of_facings = sum(filtered_scif['facings'].values) if not filtered_scif.empty else 0
        return number_of_facings

    def filter_df_based_on_filtering_dictionary(self, df, filters):
        filtered_df = df.copy()
        for column, criteria in filters.items():
            if isinstance(criteria, list):
                filtered_df = filtered_df[filtered_df[column].isin(criteria)]
            else:
                filtered_df = filtered_df[filtered_df[column] == criteria]
        return filtered_df

    #will be adding it when necessarry
    def get_general_calculation_parameters(self, atomic_kpi):
        calculation_parameters = {}
        try:
            template_names = self.split_and_strip(atomic_kpi[TEMPLATE_NAME])
        except KeyError:
            template_names = None
        if template_names:
            relevant_scenes = self.scif[(self.scif['template_name'].isin(template_names))]
        else:
            relevant_scenes = self.scif
        scenes_ids_filter = {'scene_fk': relevant_scenes['scene_fk'].unique().tolist()}
        calculation_parameters.update(scenes_ids_filter)
        calculation_parameters.update({'manufacturer_name': KO_PRODUCTS})  # will see if i need it based on updated template
        return calculation_parameters

    def get_sos_calculation_parameters(self, atomic_kpi):
        calculation_parameters = {CONDITION_1: {'numerator':{}, 'denominator': {}},
                                  CONDITION_2: {'numerator':{}, 'denominator': {}}}
        calculation_parameters[CONDITION_1]['numerator'].update({atomic_kpi[CONDITION_1_NUMERATOR_TYPE]: atomic_kpi[CONDITION_1_NUMERATOR].lower()})
        calculation_parameters[CONDITION_1]['denominator'].update({atomic_kpi[CONDITION_1_DENOMINATOR_TYPE]: atomic_kpi[CONDITION_1_DENOMINATOR].lower()})
        calculation_parameters[CONDITION_1]['denominator'].update({'product_type': 'SKU'}) # talk to Israel
        if atomic_kpi[TEMPLATE_NAME]:
            calculation_parameters[CONDITION_1]['denominator'].update({'template_name': self.split_and_strip(atomic_kpi[TEMPLATE_NAME])})
        calculation_parameters[CONDITION_1]['numerator'].update(calculation_parameters[CONDITION_1]['denominator'])

        calculation_parameters[CONDITION_2]['numerator'].update({atomic_kpi[CONDITION_2_NUMERATOR_TYPE]: atomic_kpi[CONDITION_2_NUMERATOR].lower()})
        calculation_parameters[CONDITION_2]['denominator'].update({atomic_kpi[CONDITION_2_DENOMINATOR_TYPE]: atomic_kpi[CONDITION_2_DENOMINATOR].lower()})
        calculation_parameters[CONDITION_2]['denominator'].update({'product_type': 'SKU'})  # talk to Israel
        if atomic_kpi[TEMPLATE_NAME]:
            calculation_parameters[CONDITION_2]['denominator'].update({'template_name': self.split_and_strip(atomic_kpi[TEMPLATE_NAME])})
        calculation_parameters[CONDITION_2]['numerator'].update(calculation_parameters[CONDITION_2]['denominator'])
        return calculation_parameters

    # def get_sos_condition_filters(self, atomic_kpi, *conditions):
    #     columns = atomic_kpi.index.values
    #     for condition in conditions:
    #         condition_columns = filter(lambda y: y[0] == condition, map(lambda x: x.split(' '), columns))
    #

    def calculate_availability_custom(self, atomic_kpis_data):
        for i in xrange(len(atomic_kpis_data)):
            atomic_kpi = atomic_kpis_data.iloc[i]
            score = 0 # or None
            if atomic_kpi[AVAILABILITY_TYPE] == AVAILABILITY_POS:
                score = self.calculate_availability_pos(atomic_kpi)
            elif atomic_kpi[AVAILABILITY_TYPE] == AVAILABILITY_SKU_FACING_AND:
                score = self.calculate_availability_sku_facing_and(atomic_kpi)
            elif atomic_kpi[AVAILABILITY_TYPE] == AVAILABILITY_SKU_FACING_OR:
                score = self.calculate_availability_sku_facing_or(atomic_kpi)
            else:
                Log.warning('Availablity of type {} is not supported by calculation'.format(atomic_kpi[AVAILABILITY_TYPE]))
                continue
            self.add_kpi_result_to_kpi_results_container(atomic_kpi, score)
            # write atomic score (maybe also result) to DB

    def calculate_availability_pos(self, atomic_kpi):
        score = 0
        max_score = atomic_kpi[SCORE]
        target = atomic_kpi[TARGET]
        calculation_filters = {GENERAL_FILTERS: self.get_general_calculation_parameters(atomic_kpi),
                               KPI_SPECIFIC_FILTERS: self.get_availability_and_price_calculation_parameters(atomic_kpi)}
        session_results = []
        for scene in calculation_filters[GENERAL_FILTERS]['scene_fk']:
            scene_results = []
            calculation_filters[GENERAL_FILTERS]['scene_fk'] = scene
            scene_scif = self.filter_df_based_on_filtering_dictionary(self.scif, calculation_filters[GENERAL_FILTERS])
            scene_skus_scif = scene_scif[scene_scif[PRODUCT_TYPE]==SKU]
            brands_count = scene_skus_scif.groupby(BRAND_NAME)[BRAND_NAME].count()
            scene_brand_strips_df = self.filter_df_based_on_filtering_dictionary(scene_scif, calculation_filters[KPI_SPECIFIC_FILTERS])

            #option 1
            brand_strips_count = scene_brand_strips_df.groupby(POS_BRAND_FIELD)[POS_BRAND_FIELD].count()
            for brand, count in brands_count.iteritems():
                # brand_strip_result = None # need to discuss what we do with results - writing / not to DB?
                try:
                    if count*int(target) <= brand_strips_count[brand]:
                        brand_strip_result = True
                    else:
                        brand_strip_result = False
                except KeyError:
                    Log.info('Brand strip for brand {} does not exist in scene {}'.format(brand, scene))
                    brand_strip_result = False
                scene_results.append(brand_strip_result)
            if all(scene_results):
                session_results.append(100)
            else:
                session_results.append(0)
            # do we write results per scene to DB??

            # # option 2: alternative calculation taking into account target
            # scene_brand_strips_df = self.filter_df_based_on_filtering_dictionary(scene_scif, calculation_filters[KPI_SPECIFIC_FILTERS])
            # for index, sku_row in scene_skus_scif.iterrows():
            #     brand = sku_row[BRAND_NAME].values[0]
            #     brand_strip_result = self.check_brand_strip(brand, scene_brand_strips_df, target)

        atomic_result = 100 if all(session_results) else 0
        score = self.calculate_atomic_score(atomic_result, max_score)
        # if max_score:
        #     score = max_score if atomic_result == 100 else 0
        # else:
        #     score = atomic_result
        return score

    def calculate_availability_sku_facing_and(self, atomic_kpi):
        max_score = atomic_kpi[SCORE]
        score = 0
        calculation_filters = {GENERAL_FILTERS: self.get_general_calculation_parameters(atomic_kpi),
                               KPI_SPECIFIC_FILTERS: self.get_availability_and_price_calculation_parameters(atomic_kpi)}
        # filtered_scif = self.filter_df_based_on_filtering_dictionary(self.scif, calculation_filters[GENERAL_FILTERS])
        atomic_result = 0 # lets see if we want to make it None
        if atomic_kpi[TEMPLATE_NAME]:
            scene_results = []
            for scene in calculation_filters[GENERAL_FILTERS]['scene_fk']:
                calculation_filters[GENERAL_FILTERS]['scene_fk'] = scene
                scene_relevant_scif = self.filter_df_based_on_filtering_dictionary(self.scif, calculation_filters[GENERAL_FILTERS])
                scene_result = self.calculate_availability_result_all_skus(scene_relevant_scif, calculation_filters, atomic_kpi)
                # unique_skus_list = self.split_and_strip(atomic_kpi['product_ean_code']) if 'product_ean_code' in calculation_filters[KPI_SPECIFIC_FILTERS].keys() \
                #                                                                         else scene_relevant_scif['product_ean_code'].unique().tolist()
                # calculation_filters[KPI_SPECIFIC_FILTERS]['product_ean_code'] = unique_skus_list
                # result_scene_df = self.filter_df_based_on_filtering_dictionary(scene_relevant_scif, calculation_filters[KPI_SPECIFIC_FILTERS])
                # facings_by_sku = self.get_facings_by_sku(result_scene_df, unique_skus_list)
                # scene_atomic_result = 100 if all([facing >= target for facing in facings_by_sku.values()]) and all(
                #     [sku in unique_skus_list for sku in facings_by_sku.keys()]) else 0
                scene_results.append(scene_result)
            if scene_results:
                # write result to DB by scene????
                atomic_result=100 if all([result==100 for result in scene_results]) else 0
        else:
            filtered_scif = self.filter_df_based_on_filtering_dictionary(self.scif, calculation_filters[GENERAL_FILTERS])
            atomic_result = self.calculate_availability_result_all_skus(filtered_scif, calculation_filters, atomic_kpi)
            # unique_skus_list = self.split_and_strip(atomic_kpi['product_ean_code']) if 'product_ean_code' in calculation_filters[KPI_SPECIFIC_FILTERS].keys() \
            #                                                                         else filtered_scif['product_ean_code'].unique().tolist()
            # calculation_filters[KPI_SPECIFIC_FILTERS]['product_ean_code'] = unique_skus_list
            # result_session_df = self.filter_df_based_on_filtering_dictionary(filtered_scif, calculation_filters[KPI_SPECIFIC_FILTERS])
            # facings_by_sku = self.get_facings_by_sku(result_session_df, unique_skus_list)
            # atomic_result = 100 if all([facing>=target for facing in facings_by_sku.values()]) and all ([sku in unique_skus_list for sku in facings_by_sku.keys()]) else 0
            # write result to DB ????

        score = self.calculate_atomic_score(atomic_result, max_score)
        # if max_score:
        #     score = max_score if atomic_result==100 else 0
        # else:
        #     score = atomic_result
        return score

    def calculate_availability_sku_facing_or(self, atomic_kpi):
        max_score = atomic_kpi[SCORE]
        atomic_result = None
        score = 0
        calculation_filters = {GENERAL_FILTERS: self.get_general_calculation_parameters(atomic_kpi),
                               KPI_SPECIFIC_FILTERS: self.get_availability_and_price_calculation_parameters(atomic_kpi)}
        atomic_result = 0
        if atomic_kpi[TEMPLATE_NAME]:
            scene_results = []
            for scene in calculation_filters[GENERAL_FILTERS]['scene_fk']:
                calculation_filters[GENERAL_FILTERS]['scene_fk'] = scene
                scene_relevant_scif = self.filter_df_based_on_filtering_dictionary(self.scif, calculation_filters[GENERAL_FILTERS])
                scene_result = self.calculate_availability_result_any_sku(scene_relevant_scif, calculation_filters, atomic_kpi)
                scene_results.append(scene_result)
            if scene_results:
                # write result to DB by scene????
                atomic_result = 100 if all([result == 100 for result in scene_results]) else 0
        else:
            filtered_scif = self.filter_df_based_on_filtering_dictionary(self.scif, calculation_filters[GENERAL_FILTERS])
            atomic_result = self.calculate_availability_result_any_sku(filtered_scif, calculation_filters, atomic_kpi)

        score = self.calculate_atomic_score(atomic_result, max_score)
        # if max_score:
        #     score = max_score if atomic_result == 100 else 0
        # else:
        #     score = atomic_result
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
                        value_list = self.split_and_strip(atomic_kpi[value_col])
                        condition_filters.update({atomic_kpi[column]: atomic_kpi[value_col] if len(value_list) <= 1 else value_list})
                    else:
                        Log.error('condition {} does not have corresponding value column'.format(column)) # should it be error?
        # if atomic_kpi[TEMPLATE_NAME]:
        #     condition_filters.update({'template_name': self.split_and_strip(atomic_kpi[TEMPLATE_NAME]), 'manufacturer_name': KO_PRODUCTS})
        # else:
        #     condition_filters.update({'manufacturer_name': KO_PRODUCTS})
        return condition_filters

    # see later if I want to use this function
    def get_string_or_number(self, field, value):
        if field in NUMERIC_FIELDS:
            try:
                value = float(value)
            except:
                value = value
        return value

    def calculate_availability_result_all_skus(self, scif, filters, atomic_kpi):
        target = atomic_kpi[TARGET]
        unique_skus_list = self.split_and_strip(atomic_kpi['product_ean_code']) if 'product_ean_code' in filters[KPI_SPECIFIC_FILTERS].keys() \
            else scif['product_ean_code'].unique().tolist()
        filters[KPI_SPECIFIC_FILTERS]['product_ean_code'] = unique_skus_list
        result_df = self.filter_df_based_on_filtering_dictionary(scif, filters[KPI_SPECIFIC_FILTERS])
        facings_by_sku = self.get_facings_by_item(result_df, unique_skus_list, EAN_CODE)
        result = 100 if all([facing >= target for facing in facings_by_sku.values()]) and \
                        all([sku in unique_skus_list for sku in facings_by_sku.keys()]) else 0
        return result

    def calculate_availability_result_any_sku(self, scif, filters, atomic_kpi):
        target = atomic_kpi[TARGET]
        unique_skus_list = self.split_and_strip(atomic_kpi['product_ean_code']) if 'product_ean_code' in filters[KPI_SPECIFIC_FILTERS].keys() \
            else scif['product_ean_code'].unique().tolist()
        filters[KPI_SPECIFIC_FILTERS]['product_ean_code'] = unique_skus_list
        result_df = self.filter_df_based_on_filtering_dictionary(scif, filters[KPI_SPECIFIC_FILTERS])
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


    def calculate_atomic_score(self, atomic_result, max_score):
        if max_score:
            score = max_score if atomic_result == 100 else 0
        else:
            score = atomic_result
        return score