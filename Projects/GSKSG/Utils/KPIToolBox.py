
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
# from KPIUtils_v2.Calculations.SOSCalculations import SOS
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
        self.msl_list = pd.read_excel(self.excel_file_path, sheetname='MSL List')
        self.calculations = {'SOS': self.calculate_sos, 'MSL': self.calculate_MSL, 'sequence': self.calculate_sequence,
                             'presence': self.calculate_presence, 'facings': self.calculate_facings,
                             'Survey': self.calculate_survey, 'No facings': self.calculate_no_facings}
        self.sequence = Sequence(data_provider, ignore_stacking=True)
        self.availability = Availability(data_provider, ignore_stacking=True)

    def main_calculation(self, *args, **kwargs):
        """
        This function calculates the KPI results.
        """
        template = self.get_relevant_calculations()
        self.handle_calculation(template)

        score = 0
        return score

    def handle_calculation(self, kpis):
        # for each level3:
        for i in xrange(len(kpis)):
            current_kpi = kpis.iloc[i]
            result = self.calculate_atomic(current_kpi)
            # save result to db


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
            calculation(row)

    def calculate_sos(self, row):
        return

    def calculate_presence(self, row):
        return

    def calculate_facings(self, row, no_facing=False):

        target = row['target'] if not pd.isnull(row['target']) else 0
        row_filter, general_filters = self.get_filters(row)
        row_filter.update(general_filters)
        result = self.availability.calculate_availability(**row_filter)
        if no_facing:
            return result < target
        return result >= target

    def calculate_no_facings(self, row):
        return self.calculate_facings(row, no_facing=True)

    def calculate_MSL(self, row):
        if not pd.isnull(row['target']):
            target = row['target']

        return

    def calculate_sequence(self, row):
        sequence_filter, general_filters = self.get_filters(row)

        # running sequence kpi, allowing empty spaces, not allowing Irrelevant.
        # assuming should pass in ALL relevant scenes.
        result = self.sequence.calculate_product_sequence(sequence_filter, direction='left', **general_filters)
        return result

    def calculate_survey(self, row):
        return

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
                if col == 'target':
                    continue
                elif self.is_string_a_list(row[col]):
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