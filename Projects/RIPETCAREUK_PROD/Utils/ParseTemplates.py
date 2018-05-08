import os
import pandas as pd
from datetime import date

from Projects.RIPETCAREUK_PROD.Utils.Utils import strip_df

TEMPLATE_PATH = os.path.join(os.path.dirname(os.path.realpath(__file__)), '..', 'Data')
TEMPLATE_NAME_2016 = '170711 2016 MARS_Perfect_Store_Template.xlsx'
TEMPLATE_NAME_2017 = '170711 MARS_Perfect_Store_Mars_UK_Template_v1.7.xlsx'


class SosConsts(object):

    KPI_NAME = 'KPI Name'
    SOS_NUM_ENTITY = 'Entity Type Numerator'
    SOS_NUMERATOR = 'Numerator'
    SOS_DENOM_ENTITY = 'Entity Type Denominator'
    SOS_DENOMINATOR = 'Denominator'


class KPIConsts(object):

    SHEET_NAME = 'KPIs'


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

    ASSORTMENT_SHEET = 'ASSORTMENT'
    SOS_SHEET = 'SOS_Targets'
    FACINGS_SHEET = 'Facings_Targets'
    SHELVES_POS_SHEET = 'Shelves_Pos_Products'
    MACRO_SPACES_SHEET = 'Macro_Spaces_Targets'
    CLIP_STRIP_SHEET = 'Clip_Strips'


class ParseMarsUkTemplates(object):

    def __init__(self, visit_date=date(2017, 1, 1)):
        template_name = TEMPLATE_NAME_2017 if visit_date.year >= 2017 else TEMPLATE_NAME_2016
        self.template_path = os.path.join(TEMPLATE_PATH, template_name)

    def parse_templates(self):
        return {
            KPIConsts.SHEET_NAME: self.parse_kpi(),
            KPIConsts.ASSORTMENT_SHEET: self.parse_sheet(KPIConsts.ASSORTMENT_SHEET),
            KPIConsts.SOS_SHEET: self.parse_sheet(KPIConsts.SOS_SHEET),
            KPIConsts.CLIP_STRIP_SHEET: self.parse_sheet(KPIConsts.CLIP_STRIP_SHEET),
            KPIConsts.FACINGS_SHEET: self.parse_sheet(KPIConsts.FACINGS_SHEET),
            KPIConsts.SHELVES_POS_SHEET: self.parse_sheet(KPIConsts.SHELVES_POS_SHEET),
            KPIConsts.MACRO_SPACES_SHEET: self.parse_sheet(KPIConsts.MACRO_SPACES_SHEET)
        }

    @strip_df
    def parse_sheet(self, sheet_name):
        data = pd.read_excel(self.template_path, sheet_name, skiprows=3)
        return data

    @strip_df
    def parse_kpi(self):
        data = pd.read_excel(self.template_path, KPIConsts.SHEET_NAME)
        return data

    @staticmethod
    def pad_columns(columns):
        previous_column = columns[0]
        for c in xrange(1, len(columns)):
            if 'Unnamed: ' in columns[c]:
                if 'Unnamed: ' not in previous_column:
                    columns[c] = previous_column
            previous_column = columns[c]
        return columns
