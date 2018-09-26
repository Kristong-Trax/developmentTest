
from Trax.Algo.Calculations.Core.DataProvider import Data
from Trax.Cloud.Services.Connector.Keys import DbUsers
from Trax.Data.Projects.Connector import ProjectConnector
from Trax.Utils.Logging.Logger import Log
import pandas as pd
import os
from KPIUtils_v2.DB.Common import Common
# from KPIUtils_v2.Calculations.AssortmentCalculations import Assortment
from KPIUtils_v2.Calculations.AvailabilityCalculations import Availability
# from KPIUtils_v2.Calculations.NumberOfScenesCalculations import NumberOfScenes
# from KPIUtils_v2.Calculations.PositionGraphsCalculations import PositionGraphs
from KPIUtils_v2.Calculations.SOSCalculations import SOS
from KPIUtils_v2.Calculations.SequenceCalculations import Sequence
# from KPIUtils_v2.Calculations.SurveyCalculations import Survey

# from KPIUtils_v2.Calculations.CalculationsUtils import GENERALToolBoxCalculations

__author__ = 'jasmine'

KPI_RESULT = 'report.kpi_results'
KPK_RESULT = 'report.kpk_results'
KPS_RESULT = 'report.kps_results'

######################
relevant_cols = 'relevant columns'
template_kpi_type = 'type of condition'
GENERAL_COLS = ['template_name']
EXCLUDE = 0
SURVEY_QUEST = 'Survey Question Text'
STORE_LVL_1 = 'store_type'
# chance to additional attribute!!!!!
# STORE_LVL_2 = 'address_line_1'
STORE_LVL_2 = 'retailer_name'
STORE_LVL_3 = 'additional_attribute_1'
SURVEY_SHEET ='Survey'
######################

class GSKSGToolBox:
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
        self.store_info = self.data_provider[Data.STORE_INFO]
        self.scif = self.data_provider[Data.SCENE_ITEM_FACTS]
        self.rds_conn = ProjectConnector(self.project_name, DbUsers.CalculationEng)
        self.kpi_static_data = self.common.get_kpi_static_data()
        self.kpi_results_queries = []

        self.templates_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), '..', 'Data')
        self.excel_file_path = os.path.join(self.templates_path, 'template.xlsx')
        self.survey_file = pd.read_excel(self.excel_file_path, sheetname=SURVEY_SHEET)
        self.msl_list = pd.read_excel(self.excel_file_path,
                   header=[[0, 1, 2]],
                   sheetname='MSL-test').dropna()

        self.calculations = {'SOS': self.calculate_sos, 'MSL': self.calculate_MSL, 'sequence': self.calculate_sequence,
                             'presence': self.calculate_presence, 'facings': self.calculate_facings,
                             'No facings': self.calculate_no_facings, 'Survey': self.calculate_survey}
        self.sequence = Sequence(data_provider, ignore_stacking=True)
        self.availability = Availability(data_provider, ignore_stacking=True)
        self.sos = SOS(data_provider,self.output)
        self.survey_data = self.data_provider.survey_responses

    def main_calculation(self, *args, **kwargs):
        """
        This function calculates the KPI results.
        """
        template = self.get_relevant_calculations()
        self.handle_calculation(template)

        score = 0
        return score

    def handle_calculation(self, kpis):
        kpi_results = dict()

        # for each level3:
        for i in xrange(len(kpis)):
            current_kpi = kpis.iloc[i]
            result = self.calculate_atomic(current_kpi)

        #all caculation below for main kpis
            # save result to db
            kpi_key = (current_kpi['1st level'],current_kpi['2nd Level'],current_kpi['KPI Weight'],current_kpi['Conditional Weight'])
            if kpi_key not in kpi_results:
                kpi_results[kpi_key] = 0
            if current_kpi['Score Method'] == 'Proportional':
                kpi_results[kpi_key] = result * current_kpi['Weight']
            else:
                result = 100 if(result >= current_kpi['Benchmark']) else 0
                kpi_results[kpi_key] = result * current_kpi['Weight']

        kpi_3_results = dict()
        for kpi in kpi_results.keys():
            #write result to db level 2
            if kpi['1st level'] not in kpi_3_results:
                kpi_3_results[kpi['1st level']] = 0
            if kpi_results[kpi] >= kpi['Conditional Weight']:
                kpi_3_results[kpi['1st level']] = kpi['KPI Weight'] * kpi_results[kpi]
    # #
    #     for score in kpi_3_results.keys():
    # #write ti db


    def get_relevant_calculations(self):
        # Gets the store type name
        store_type = self.store_info['store_type'].values[0].title()

        # Gets the relevant kpis from template
        template = pd.read_excel(self.excel_file_path, sheetname=store_type)
        return template

    def calculate_atomic(self, row):
        # gets the atomic kpi's calculation type
        kpi_type = row[template_kpi_type]

        # runs the relevant calculation
        calculation = self.calculations.get(kpi_type, '')
        if calculation:
            return calculation(row)
        return 0

    def calculate_sos(self, row):
        filters, general_filters = self.get_filters(row)
        return self.sos.calculate_share_of_shelf(self, sos_filters=filters, **general_filters)

    def calculate_presence(self, row):

        return self.calculate_facings(row)

    def calculate_facings(self, row, no_facing=False):

        target = row['target'] if not pd.isnull(row['target']) else 0
        row_filter, general_filters = self.get_filters(row)
        row_filter.update(general_filters)
        result = self.availability.calculate_availability(**row_filter)
        if no_facing:
            return int(result < target)
        return int(result >= target)

    def calculate_no_facings(self, row):
        return self.calculate_facings(row, no_facing=True)

    def calculate_MSL(self, row):
        """This function gets the relevant assortment, and returns the number of shown is session out of assortment"""
        # ToDo: use the benchmark as needed if needed.

        target = row['target'] if not pd.isnull(row['target']) else 0

        # Gets relevant assortment
        store_type = self.store_info[STORE_LVL_1].values[0]
        att1 = self.store_info[STORE_LVL_2].values[0]
        att2 = self.store_info[STORE_LVL_3].values[0]

        # gets the assortment product's ean codes relevant for store
        store_assortment = self.msl_list[store_type][att1][att2]
        store_assortment = store_assortment[store_assortment == 1]
        products = store_assortment.keys()

        kpi_filters, general = self.get_filters(row)
        kpi_filters.update(general)
        total_products = len(products)
        exist_products = 0
        # Checks for each product if passed, if so, count it.
        for product in set(products):
            kpi_filters['product_ean_code'] = product
            res = self.availability.calculate_availability(**kpi_filters)
            if res:
                exist_products += 1

        if exist_products >= target:
            return exist_products / total_products if total_products else 0
        else:
            return 0

    def calculate_sequence(self, row):
        sequence_filter, general_filters = self.get_filters(row)

        # running sequence kpi, allowing empty spaces, not allowing Irrelevant.
        # assuming should pass in ALL relevant scenes.
        # If an entity in sequence is missing (less than 1 facing)- will fail.
        result = self.sequence.calculate_product_sequence(sequence_filter, direction='left', **general_filters)
        return result


#need to check below
    def calculate_survey(self, row):
        group_of_question = self.survey_file[(self.survey_file['KPI Name'] == row['3rd Level']) & (self.store_info[STORE_LVL_1] in self.survey_file['Store Policy'])]
        target = group_of_question.iloc[0]['target']
        counter = 0
        for quest in group_of_question.itertuples():
            answer = self.survey_data[self.survey_data['question_text'] == quest['Survey Question Text']]
            if ~ self.quest[answer['selected_option_text'] == quest['Accepted Answers']].empty: #not empty
                counter = counter + 1
                if counter >= target:
                    return 100
        return 0

    def get_filters(self, row):
        filters = {}
        general_filters = {}
        # gets the relevant column names to consider in kpi
        cols = map(str.strip, str(row[relevant_cols]).split(','))
        for col in cols:
            # column must exist
            if col in row.keys():
                # handle the values in column
                if col == 'exclude':
                    value = self.handle_exclude(row[col])
                if col in ['target','Store Type']:
                    continue
                elif self.is_string_a_list(str(row[col])):
                    value = map(str.strip, str(row[col]).split(','))
                else:
                    value = [row[col]]

                # add the filter to relevant dictionary
                if col in GENERAL_COLS:
                    general_filters[col] = value
                else:
                    filters[col] = value
            else:
                Log.info('attribute {} is not in template'.format(col))

        return filters, general_filters

    @staticmethod
    def is_string_a_list(str_value):
        # checks whether a string is representing a list of values
        return len(str_value.split(',')) > 0

    def handle_exclude(self, value):
        # value is string of dictionary format with multi values, for example 'product_type:Irrelevant, Empty;
        # scene_id:34,54'

        exclude_dict = {}

        # gets the different fields
        fields = value.split(';')
        for field in fields:

            # gets the key and value of field
            field = field.split[':']
            key = field[0]
            values = field[1].split(',')
            exclude_dict[key] = (values, EXCLUDE)

        return exclude_dict