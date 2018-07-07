
import os
import pandas as pd

TEMPLATE_PATH = os.path.join(os.path.dirname(os.path.realpath(__file__)), '..', 'Data')


class CCTHDEMOGapConsts(object):

    SHEET_NAME = 'BPPC_GAPS_PRIORITY'

    KPI_NAME = 'KPI Name'


class CCTHDEMOSurveyConsts(object):

    SHEET_NAME = 'BPPC_SURVEY_Q'

    KPI_NAME = 'KPI Name'
    SURVEY_TEXT = 'Survey Q Text'
    SURVEY_ID = 'Survey Q ID'
    SURVEY_ANSWER = 'Accepted Answer'


class CCTHDEMOAvailabilityConsts(object):

    SHEET_NAME = 'BPPC_AVAILABILITY'

    KPI_NAME = 'KPI Name'
    PRODUCT_EAN_CODES = 'Products EANs'


class CCTHDEMOKPIConsts(object):

    SHEET_NAME = 'KPIs'
    SEPARATOR = ','
    SEPARATOR2 = ':'
    SEPARATOR3 = '\n'

    STORE_TYPE = 'Store Type'
    SEGMENTATION = 'Segmentation (Store.Additional_attribute_2)'
    EXECUTION_CONDITION = 'KPI Execution Condition (When no condition then Mandatory)'
    MANDATORY_SCENES = 'Mandatory Scenes'
    KPI_NAME = 'KPI name Eng'
    KPI_GROUP = 'KPI Group'
    SCENE_TYPE = 'Scene Type'
    KPI_TYPE = 'KPI Type'
    SOS_ENTITY = 'SOVI Etitiy Type'
    SOS_NUMERATOR = 'SOVI Numerator'
    SOS_DENOMINATOR = 'SOVI Denominator'
    SURVEY_ID = 'Survey Q ID'
    SURVEY_ANSWER = 'Accepted Survey Answer'
    SURVEY_MODE = 'Multiple Survey Mode'
    TESTED_KPI_GRUOP = 'Tested KPI Group'
    TARGET = 'Target'
    SCORE = 'SCORE'
    WEIGHT = 'WEIGHT'
    GAP_PRIORITY = 'GAP PRIORITY'


def strip_df(func):
    def wrapper(*args, **kwargs):
        data = func(*args, **kwargs)
        if data is None:
            return data
        data = data.fillna('')
        columns = data.columns
        for column in columns:
            if 'Unnamed: ' in column:
                del data[column]
            else:
                data[column] = data[column].apply(lambda x: x if not isinstance(x, (str, unicode)) else x.strip())
        return data
    return wrapper


class CCTHDEMOParseTemplates(CCTHDEMOKPIConsts):

    TEMPLATE_TT_UNTIL_NOV2017 = 'Template_TT_until_November2017'
    TEMPLATE_TT_AFTER_NOV2017 = 'Template_TT_after_November2017'
    TEMPLATE_7_11_UNTIL_JULY2017 = 'Template_7_11_until_July2017'
    TEMPLATE_7_11_AFTER_JULY2017 = 'Template_7_11_after_July2017'
    TEMPLATE_7_11 = 'Template_7_11_2018'
    TEMPLATE_TT = 'Template_TT_2018'
    TEMPLATE_7_11_AFTER_FEB2018 = 'Template_7_11_after_Feb2018'
    TEMPLATE_TT_AFTER_FEB2018 = 'Template_TT_after_Feb2018'

    def __init__(self, template):
        self.template = template
        self.template_path = os.path.join(TEMPLATE_PATH, '{}.xlsx'.format(template))
        self.survey_consts = CCTHDEMOSurveyConsts()
        self.availability_consts = CCTHDEMOAvailabilityConsts()
        self.gap_consts = CCTHDEMOGapConsts()

    def parse_templates(self):
        return dict(kpi=self.parse_kpi(),
                    availability=self.parse_availability(),
                    survey=self.parse_survey())

    def parse_sheet(self, sheet_name):
        data = pd.read_excel(self.template_path, sheet_name)
        return data

    @strip_df
    def parse_kpi(self):
        data = pd.read_excel(self.template_path, self.SHEET_NAME)
        return data

    @strip_df
    def parse_gap(self):
        if self.template in (self.TEMPLATE_TT, self.TEMPLATE_TT_AFTER_FEB2018, self.TEMPLATE_TT_AFTER_NOV2017):
            regions = pd.read_excel(self.template_path, self.gap_consts.SHEET_NAME, skiprows=3)
            regions_columns = self.padd_columns(regions.columns.tolist())
            template_data = pd.read_excel(self.template_path, self.gap_consts.SHEET_NAME, skiprows=4)

            starting_column = template_data.columns.tolist().index(self.gap_consts.KPI_NAME) + 1
            new_columns = {}

            columns = template_data.columns
            for c in xrange(starting_column, len(columns)):
                column = columns[c]
                data = template_data[column]
                region, store_type = regions_columns[c], column.split('.')[0]
                for r in region.split(self.SEPARATOR):
                    for s in store_type.split(self.SEPARATOR):
                        new_columns['{};{}'.format(r, s)] = data
                del template_data[column]
            for column in template_data.columns:
                if 'Unnamed: ' in column:
                    del template_data[column]
            for column in new_columns.keys():
                template_data[column] = new_columns[column]
            return template_data

        elif self.template in (self.TEMPLATE_7_11, self.TEMPLATE_7_11_AFTER_FEB2018, self.TEMPLATE_7_11_AFTER_JULY2017):
            return pd.read_excel(self.template_path, self.gap_consts.SHEET_NAME, skiprows=4)

    @strip_df
    def parse_availability(self):
        if self.template in (self.TEMPLATE_TT, self.TEMPLATE_TT_AFTER_FEB2018, self.TEMPLATE_TT_AFTER_NOV2017):
            regions = pd.read_excel(self.template_path, self.availability_consts.SHEET_NAME, skiprows=3)
            regions_columns = self.padd_columns(regions.columns.tolist())
            template_data = pd.read_excel(self.template_path, self.availability_consts.SHEET_NAME, skiprows=4)

            starting_column = template_data.columns.tolist().index(self.availability_consts.PRODUCT_EAN_CODES) + 1
            new_columns = {}

            columns = template_data.columns
            for c in xrange(starting_column, len(columns)):
                column = columns[c]
                data = template_data[column]
                region, store_type = regions_columns[c], column.split('.')[0]
                for r in region.split(self.SEPARATOR):
                    for s in store_type.split(self.SEPARATOR):
                        new_columns['{};{}'.format(r, s)] = data
                del template_data[column]
            for column in template_data.columns:
                if 'Unnamed: ' in column:
                    del template_data[column]
            for column in new_columns.keys():
                template_data[column] = new_columns[column]
            return template_data

        elif self.template in (self.TEMPLATE_7_11, self.TEMPLATE_7_11_AFTER_FEB2018, self.TEMPLATE_7_11_AFTER_JULY2017):
            return pd.read_excel(self.template_path, self.availability_consts.SHEET_NAME, skiprows=4)

    @strip_df
    def parse_survey(self):
        if self.template in (self.TEMPLATE_TT, self.TEMPLATE_TT_AFTER_FEB2018, self.TEMPLATE_TT_AFTER_NOV2017):
            regions = pd.read_excel(self.template_path, self.survey_consts.SHEET_NAME, skiprows=3)
            regions_columns = self.padd_columns(regions.columns.tolist())
            store_types = pd.read_excel(self.template_path, self.survey_consts.SHEET_NAME, skiprows=4)
            store_types_columns = self.padd_columns(store_types.columns.tolist())
            template_data = pd.read_excel(self.template_path, self.survey_consts.SHEET_NAME, skiprows=5)

            starting_column = template_data.columns.tolist().index(self.survey_consts.SURVEY_ANSWER) + 1
            new_columns = {}

            columns = template_data.columns
            for c in xrange(starting_column, len(columns)):
                column = columns[c]
                data = template_data[column]
                region, store_type, segmentation = regions_columns[c], store_types_columns[c].split('.')[0], column.split('.')[0]
                for r in region.split(self.SEPARATOR):
                    for st in store_type.split(self.SEPARATOR):
                        for se in segmentation.split(self.SEPARATOR):
                            new_columns['{};{};{}'.format(r, st, se)] = data
                del template_data[column]
            for column in template_data.columns:
                if 'Unnamed: ' in column:
                    del template_data[column]
            for column in new_columns.keys():
                template_data[column] = new_columns[column]
            return template_data

    @staticmethod
    def padd_columns(columns):
        previous_column = columns[0]
        for c in xrange(1, len(columns)):
            if 'Unnamed: ' in columns[c]:
                if 'Unnamed: ' not in previous_column:
                    columns[c] = previous_column
            previous_column = columns[c]
        return columns
