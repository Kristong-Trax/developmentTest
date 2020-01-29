from Trax.Algo.Calculations.Core.DataProvider import Data
from Trax.Utils.Logging.Logger import Log
from KPIUtils_v2.Utils.GlobalScripts.Scripts import GlobalSessionToolBox
from KPIUtils_v2.GlobalDataProvider.PsDataProvider import PsDataProvider
from KPIUtils_v2.Calculations.SurveyCalculations import Survey
from KPIUtils_v2.Calculations.BlockCalculations_v2 import Block
from KPIUtils_v2.Calculations.AssortmentCalculations import Assortment

import pandas as pd
import numpy as np
import os
from datetime import datetime

# Sheet names
KPIS = 'KPIs'
AVALIABILITY = 'Availability'
SOS = 'SOS'
SHARE_OF_EMPTY = 'Share of Empty'
SURVEY = 'Survey'
DISTRIBUTION = 'Distribution'
ASSORTMENT = 'Assortment'
SCORING = 'Scoring'

# Dataframe Column Names
KPI_NAME = 'KPI Name'
KPI_TYPE = 'KPI Type'
PARENT_KPI = 'Parent KPI'
TASK_TEMPLATE_GROUP = 'Task/ Template Group'
TEMPLATE_NAME = 'template_name'
MANUFACTURER_NAME = 'manufacturer_name'

# Excel Sheets
SHEETS = [KPIS, AVALIABILITY, SOS, SHARE_OF_EMPTY, SURVEY, DISTRIBUTION, ASSORTMENT, SCORING]

TEMPLATE_PATH = os.path.join(os.path.dirname(os.path.realpath(__file__)), '..', 'Data',
                             'Nayar_Comidas_Fondas_Template.xlsx')


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


class FONDASToolBox(GlobalSessionToolBox):
    def __init__(self, data_provider, output, common):
        GlobalSessionToolBox.__init__(self, data_provider, output, common)
        self.common_v2 = common
        self.ps_data_provider = PsDataProvider(data_provider)
        self.store_type = self.store_info['store_type'].iloc[0]
        self.templates = {}
        self.parse_template()
        self.results_df = pd.DataFrame(columns=['kpi_name', 'kpi_fk', 'numerator_id', 'numerator_result',
                                                'denominator_id', 'denominator_result', 'result', 'score',
                                                'identifier_result', 'identifier_parent', 'should_enter'])

    def parse_template(self):
        for sheet in SHEETS:
            self.templates[sheet] = pd.read_excel(TEMPLATE_PATH, sheet_name=sheet)

    def main_calculation(self):
        relevant_kpi_template = self.templates[KPIS]
        foundation_kpi_types = [SURVEY]
        foundation_kpi_template = relevant_kpi_template[relevant_kpi_template[KPI_TYPE].isin(foundation_kpi_types)]

        self._calculate_kpis_from_template(foundation_kpi_template)

        def _calculate_kpis_from_template(self, template_df):
            for i, row in template_df.iterrows():
                calculation_function = self._get_calculation_function_by_kpi_type(row[KPI_TYPE])

        def _get_calculation_function_by_kpi_type(self, kpi_type):
            if kpi_type == SOS:
                return self.calculate_sos
            elif kpi_type == AVALIABILITY:
                return self.calculate_availability
            elif kpi_type == SURVEY:
                return self.calculate_survey
            elif kpi_type == DISTRIBUTION:
                return self.calculate_assortment
            elif kpi_type == SHARE_OF_EMPTY:
                return self.calculate_share_of_empty
            elif kpi_type == SCORING:
                return self.calculate_scoring

        def calculate_survey(self, row):
            b = 2
            a = row
            b = self.templates


