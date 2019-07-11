import os
import pandas as pd
from datetime import date

from Projects.RINIELSENUS.Utils.Const import DOG_MAIN_MEAL_WET
from Projects.RINIELSENUS.Utils.Utils import strip_df

TEMPLATE_PATH = os.path.join(os.path.dirname(os.path.realpath(__file__)), '..', 'Data')
TEMPLATE_NAME = 'Template.xlsx'


class SosConsts(object):

    KPI_NAME = 'KPI Name'
    SOS_NUM_ENTITY = 'Entity Type Numerator'
    SOS_NUMERATOR = 'Numerator'
    SOS_DENOM_ENTITY = 'Entity Type Denominator'
    SOS_DENOMINATOR = 'Denominator'


class KPIConsts(object):

    SHEET_NAME = 'Hierarchy'

    STORE_TYPE = 'Store Type'
    KPI_NAME = 'KPI name Eng'
    KPI_GROUP = 'KPI Group'
    SCENE_TYPE = 'Scene Type'
    KPI_TYPE = 'KPI Type'

    SURVEY_ID = 'Survey Q ID'
    SURVEY_ANSWER = 'Accepted Survey Answer'
    TESTED_KPI_GROUP = 'Tested KPI Group'
    TARGET = 'Target'
    SCORE = 'SCORE'
    WEIGHT = 'WEIGHT'
    GAP_PRIORITY = 'GAP PRIORITY'
    TARGET_PREFIX = 'SEE '

    TWO_BLOCK_SHEET = 'Two Blocks'
    BLOCK_SHEET = 'Block'
    ANCHOR_SHEET = 'Anchor'
    SHELF_LEVEL_SHEET = 'Shelf Level'
    ADJACENCY_SHEET = 'Adjacency'
    SHARE_OF_ASSORTMENT_SHEET = 'Share of assortment'
    LINEAR_PREFERRED_RANGE_SHARE_SHEET = 'Linear preferred range share'
    LINEAR_FAIR_SHARE_SHEET = 'Linear fair share'
    ASSORTMENT_FAIR_SHARE_SHEET = 'Assortment fair share'
    PREFERRED_RANGE_SHEET = 'Preferred Range'
    NBIL_SHEET = 'NBIL'
    CHANNEL_CONVERSION_SHEET = 'Channel Conversion'


class ParseMarsUsTemplates(object):

    @staticmethod
    def _get_template_path(set_name, i):
        template_name = 'Template_2019 SPT Midyear_v1.03_EM' if i == 0 else 'Template_2019_BDB Midyear_v1.03_ES'
        return os.path.join(TEMPLATE_PATH, '{}.xlsx'.format(template_name))

    def parse_template(self, set_name, i):
        template_file = pd.ExcelFile(self._get_template_path(set_name, i))
        templates_data = {sheet_name: self.parse_sheet(
            template_file, sheet_name) for sheet_name in template_file.sheet_names}
        templates_data[KPIConsts.SHEET_NAME] = templates_data[KPIConsts.SHEET_NAME][templates_data[KPIConsts.SHEET_NAME]['Set name'] == set_name]
        return templates_data

    def get_nbil_data(self):
        return self.parse_sheet(pd.ExcelFile(self._get_template_path('NBIL')), 'NBIL')

    @strip_df
    def parse_sheet(self, template_file, sheet_name):
        data = template_file.parse(sheet_name)
        return data

    def get_mars_sales_data(self):
        # return pd.read_csv(os.path.join(TEMPLATE_PATH, 'Sales.csv'))
        template_file = pd.ExcelFile(self._get_template_path('sales', 1))
        return self.parse_sheet(template_file, 'sales')

    def get_mars_spt_sales_data(self):
        template_file = pd.ExcelFile(self._get_template_path('sales', 0))
        return self.parse_sheet(template_file, 'sales')
        # return pd.read_csv(os.path.join(TEMPLATE_PATH, 'Sales_spt.csv'))
