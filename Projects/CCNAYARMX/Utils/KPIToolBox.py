from Trax.Algo.Calculations.Core.DataProvider import Data
from Trax.Utils.Logging.Logger import Log
from KPIUtils_v2.Utils.GlobalScripts.Scripts import GlobalSessionToolBox
from KPIUtils_v2.GlobalDataProvider.PsDataProvider import PsDataProvider

import pandas as pd
import os
from datetime import datetime

from Projects.CCNAYARMX.Data.LocalConsts import Consts

# from KPIUtils_v2.Utils.Consts.DataProvider import 
# from KPIUtils_v2.Utils.Consts.DB import 
# from KPIUtils_v2.Utils.Consts.PS import 
# from KPIUtils_v2.Utils.Consts.GlobalConsts import 
# from KPIUtils_v2.Utils.Consts.Messages import 
# from KPIUtils_v2.Utils.Consts.Custom import 
# from KPIUtils_v2.Utils.Consts.OldDB import 

# from KPIUtils_v2.Calculations.AssortmentCalculations import Assortment
# from KPIUtils_v2.Calculations.AvailabilityCalculations import Availability
# from KPIUtils_v2.Calculations.NumberOfScenesCalculations import NumberOfScenes
# from KPIUtils_v2.Calculations.PositionGraphsCalculations import PositionGraphs
# from KPIUtils_v2.Calculations.SOSCalculations import SOS
# from KPIUtils_v2.Calculations.SequenceCalculations import Sequence
# from KPIUtils_v2.Calculations.SurveyCalculations import Survey

# from KPIUtils_v2.Calculations.CalculationsUtils import GENERALToolBoxCalculations

__author__ = 'krishnat'

KPI_NAME = 'KPI Name'
KPI_TYPE = 'Type'



PRODUCT_TYPE = 'product_type'
NUMERATOR_PARAM = 'numerator param'
NUMERATOR_VALUE = 'numerator value'
DENOMINATOR_PARAM = 'denominator param'
DENOMINATOR_VALUE = 'denominator value'

SOS = 'SOS'
Sheets = [SOS]
TEMPLATE_PATH = os.path.join(os.path.dirname(os.path.realpath(__file__)), '..', 'Data', 'CC Nayar Template v0.2.xlsx')


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


class ToolBox(GlobalSessionToolBox):

    def __init__(self, data_provider, output):
        GlobalSessionToolBox.__init__(self, data_provider, output)
        self.ps_data_provider = PsDataProvider(data_provider)
        self.templates = {}
        self.parse_template()

    def parse_template(self):
        for sheet in Sheets:
            self.templates[sheet] = pd.read_excel(TEMPLATE_PATH, sheet_name=sheet)

    def main_calculation(self):
        a = self.calculate_share_of_shelf()
        return a

    def calculate_share_of_shelf(self):
        pass


    @staticmethod
    def does_exist(kpi_line, column_name):
        """
        checks if kpi_line has values in this column, and if it does -returns a list of these values
        :param kpi_line: line from template
        :param column_name: str
        :return: list of values if there are, otherwise None
        """

        if column_name in kpi_line.keys() and kpi_line[column_name] != "":
            cell = kpi_line[column_name]
            if type(cell) in [int, float]:
                return [cell]
            elif type(cell) in [unicode, str]:
                return [x.strip() for x in cell.split(",")]
        return None

    def get_numerator_scif(self, kpi_line, denominator_scif):
        numerator_scif = self.filter_scif_by_template_columns(kpi_line, NUMERATOR_PARAM, NUMERATOR_VALUE,
                                                              denominator_scif)
        return numerator_scif

    def get_denominator_scif(self, kpi_line, relevant_scif):
        denominator_scif = self.filter_scif_by_template_columns(kpi_line, )
        denominator_scif = self.filter_scif_by_template_columns(kpi_line, DENOMINATOR_PARAM, DENOMINATOR_VALUE,
                                                                relevant_scif)

        return denominator_scif

    @staticmethod
    def filter_scif_by_template_columns(kpi_line, type_base, value_base, relevant_scif, exclude=False):
        filters = {}

        # get the denominator filters
        for den_column in [col for col in kpi_line.keys() if type_base in col]:
            if kpi_line[den_column]:  # check to make sure this kpi has the is denominator param
                filters[kpi_line[den_column]] = \
                    [value.strip() for value in
                     kpi_line[den_column.replace(type_base, value_base)].split(',')]  # get associated values

        for key in filters.iterkeys():
            if key not in relevant_scif.columns.tolist():
                Log.error("{} is not a valid parameter type").format(key)
                continue
            if exclude:
                relevant_scif = relevant_scif[~(relevant_scif[key].isin(filters[key]))]
            else:
                relevant_scif = relevant_scif[relevant_scif[key].isin(filters[key])]





    # def calculate_sos(self):
    #     for i, row in self.templates[SOS].iterrows():
    #         kpi_name = row['KPI Name']
    #         kpi_fk = self.common.get_kpi_fk_by_kpi_name(kpi_name)
    #
    #         kpi_type = row['KPI Type']
    #
    #         product_type = self.sanitize_values(row['product_type'])
    #
    #         num_param1 = self.sanitize_values(row['numerator param 1'])
    #         num_value1 = self.sanitize_values(row['numerator value 1'])
    #
    #         den_param1 = self.sanitize_values(row['denominator param 1'])
    #         den_value1 = self.sanitize_values(row['denominator value 1'])
    #
    #         filters = {'product_type':product_type, num_param1:num_value1}
    #         filters = self.delete_filter_nan(filters)
    #
    #         general_filters = {den_param1:den_value1, 'product_type': product_type}
    #         general_filters = self.delete_filter_nan(general_filters)
    #
    #
    # def sanitize_values(self, item):
    #     if pd.isna(item):
    #         return item
    #     else:
    #         items = [x.strip() for x in item.split(',')]
    #         return items
    #



# def calculate_main_kpi(self, main_line):
    #     """
    #     This function gets as line from the main_sheet, transfers it to the match function, and checks all of the
    #     KPIs in the same name in the match sheet.
    #     :param main_line: series from the template of the main_sheet.
    #     """
    #     # kpi_name = main_line[KPI_NAME]
    #     # relevant_scif = self.scif
    #     # result = self.calculate_kpi_by_type(main_line, relevant_scif)
    #     #
    #     # return result
    #     pass
    #
    # # Determines the kpi to use by the defining the function. Comeback to define it
    # def calculate_kpi_by_type(self, main_line, filtered_scif):
    #     """
    #     the function calculates all the kpis
    #     :param main_line: one kpi line from the main template
    #     :param filtered_scif:
    #     :return: boolean, but it can be None if we don't want to write in the Database
    #     """
    #
    #     # kpi_type = main_line[KPI_TYPE]
    #     # relevant_template = self.templates[kpi_type]
    #     # relevant_template = relevant_template[relevant_template[KPI_NAME] == main_line[KPI_NAME]]
    #     # kpi_function = self.get_kpi_function(kpi_type)
    #     #
    #     #
    #     pass
    #
    # #Come back to this function when you get relevant scif
    # def calculate_sos(self, kpi_line, relevant_scif):
    #     pass
    #
    # #Come back to this function when the finish the sos function
    # def get_kpi_function(self, kpi_type):
    #
    #     if kpi_type == SOS:
    #         pass