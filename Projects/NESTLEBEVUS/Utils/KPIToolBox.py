
from Trax.Algo.Calculations.Core.DataProvider import Data
from Trax.Utils.Logging.Logger import Log
from KPIUtils_v2.Utils.GlobalScripts.Scripts import GlobalSessionToolBox
import pandas as pd
import os

from Projects.NESTLEBEVUS.Data.LocalConsts import Consts

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

TEMPLATE_PATH = os.path.join(os.path.dirname(os.path.realpath(__file__)), '..', 'Data',
                             'NestleRTD_Template_v1.xlsx')

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

SHEETS = [Consts.KPIS,Consts.SOS]

class ToolBox(GlobalSessionToolBox):
    def __init__(self, data_provider, output):
        GlobalSessionToolBox.__init__(self, data_provider, output)
        self.templates = {}
        self.parse_template()
        self.results_df = pd.DataFrame(columns=['kpi_name', 'kpi_fk', 'numerator_id', 'numerator_result',
                                                'denominator_id', 'denominator_result', 'result'])
    def parse_template(self):
        for sheet in SHEETS:
            self.templates[sheet] = pd.read_excel(TEMPLATE_PATH, sheet_name=sheet)

    def main_calculation(self):
        relevant_kpi_template = self.templates[Consts.KPIS]
        foundation_kpi_types= [Consts.SOS]
        foundation_kpi_template = relevant_kpi_template[relevant_kpi_template[Consts.KPI_TYPE].isin(foundation_kpi_types)]

        self._calculate_kpis_from_template(foundation_kpi_template)
        return

    def _calculate_kpis_from_template(self, template_df):
        for i, row in template_df.iterrows():
            calculation_function = self._get_calculation_function_by_kpi_type(row[Consts.KPI_TYPE])
            try:
                kpi_row = self.templates[row[Consts.KPI_TYPE]][
                    self.templates[row[Consts.KPI_TYPE]][Consts.KPI_NAME].str.encode('utf-8') == row[Consts.KPI_NAME].encode('utf-8')].iloc[
                    0]
            except IndexError:
                pass
            result_data = calculation_function(kpi_row)

    def _get_calculation_function_by_kpi_type(self, kpi_type):
        if kpi_type == Consts.SOS:
            return self.calculate_sos

    def calculate_sos(self, row):
        kpi_name = row[Consts.KPI_NAME]
        kpi_fk = self.get_kpi_fk_by_kpi_type(kpi_name)


