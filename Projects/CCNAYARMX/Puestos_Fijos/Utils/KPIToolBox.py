from datetime import datetime
import pandas as pd
import numpy as np
import operator as op
import os

from Trax.Algo.Calculations.Core.DataProvider import Data
from Trax.Utils.Logging.Logger import Log

from KPIUtils_v2.Utils.GlobalScripts.Scripts import GlobalSessionToolBox
from KPIUtils_v2.GlobalDataProvider.PsDataProvider import PsDataProvider
from KPIUtils_v2.Calculations.SurveyCalculations import Survey

__author__ = 'krishnat'

# Sheet names
KPIS = 'KPIs'
SOS = 'SOS'
POSM_AVAILABILITY = 'POSM Availability'
SHARE_OF_EMPTY = 'Share of Empty'
SURVEY = 'Survey'
DISTRIBUTION = 'Distribution'
COMBO = 'Combo'
SCORING = 'Scoring'

# Column Name
KPI_NAME = 'KPI Name'
KPI_TYPE = 'KPI Type'
PARENT_KPI = 'Parent KPI'
NUMERATOR_ENTITY = 'Numerator Entity'
DENOMINATOR_ENTITY = 'Denominator Entity'
NUMERATOR_PARAM_1 = 'numerator param 1'
NUMERATOR_VALUE_1 = 'numerator value 1'
TEMPLATE_GROUP = 'template_group'


TEMPLATE_PATH = os.path.join(os.path.dirname(os.path.realpath(__file__)), '..', 'Data',
                             'NayarPuestosFijos2020v1.xlsx')

SHEETS = [KPIS, SOS, POSM_AVAILABILITY, DISTRIBUTION, COMBO, SCORING]
COLUMNS = ['scene_match_fk', 'scene_fk', TEMPLATE_GROUP, 'template_fk', 'brand_fk', 'brand_name', 'manufacturer_fk',
           'manufacturer_name', 'category_fk', 'category', 'product_type', 'product_fk', 'facings']

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


class PuestosFijosToolBox(GlobalSessionToolBox):
    def __init__(self, data_provider, output, common):
        GlobalSessionToolBox.__init__(self, data_provider, output, common)
        self.ps_data_provider = PsDataProvider(data_provider)
        self.own_manufacturer = int(self.data_provider.own_manufacturer.param_value.values[0])
        self.all_templates = self.data_provider[Data.ALL_TEMPLATES]
        self.project_templates = {}
        self.parse_template()
        self.store_type = self.store_info['store_type'].iloc[0]
        self.survey = Survey(self.data_provider, output, ps_data_provider=self.ps_data_provider, common=self.common)
        self.att2 = self.store_info['additional_attribute_2'].iloc[0]
        self.results_df = pd.DataFrame(columns=['kpi_name', 'kpi_fk', 'numerator_id', 'numerator_result',
                                                'denominator_id', 'denominator_result', 'result', 'score',
                                                'identifier_result', 'identifier_parent', 'should_enter'])

        self.products = self.data_provider[Data.PRODUCTS]
        scif = self.scif[['brand_fk', 'facings', 'product_type']].groupby(by='brand_fk').sum()
        self.mpis = self.matches \
            .merge(self.products, on='product_fk', suffixes=['', '_p']) \
            .merge(self.scene_info, on='scene_fk', suffixes=['', '_s']) \
            .merge(self.all_templates[['template_fk', TEMPLATE_GROUP]], on='template_fk') \
            .merge(scif, on='brand_fk')[COLUMNS]
        self.mpis['store_fk'] = self.store_id

    def parse_template(self):
        for sheet in SHEETS:
            self.project_templates[sheet] = pd.read_excel(TEMPLATE_PATH, sheet_name=sheet)

    def main_calculation(self):
        if not self.store_type == 'Puestos Fijos':
            return
