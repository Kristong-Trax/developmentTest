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
AVALIABILITY = 'Avalibility'
SOS = 'SOS'
SHARE_OF_EMPTY = 'Share of Empty'
SURVEY = 'Survey'
DISTRIBUTION = 'Distribution'
ASSORTMENT = 'Assortment'
SCORING = 'Scoring'

# Excel Sheets
SHEETS = [KPIS, AVALIABILITY, SOS, SHARE_OF_EMPTY, SURVEY, DISTRIBUTION, ASSORTMENT, SCORING]

TEMPLATE_PATH = os.path.join(os.path.dirname(os.path.realpath(__file__)), '..', 'Data',
                             'CCNayarTemplate_Nationalv.5.xlsx')


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

    def parse_template(self):
        for sheet in SHEETS:
            self.templates[sheet] = pd.read_excel(TEMPLATE_PATH, sheet_name=sheet)

    def main_calculation(self):
        a = 1


