import os

__author__ = 'hunter'


class Const(object):
    TEMPLATE_PATH = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'Liberty_bottlers_Template_Final_Final.xlsx')
    LIBERTY = 'Liberty'
    RED_SCORE_PARENT = 'RED SCORE'

    # sheets
    KPIS = 'KPIs'
    SOS = 'SOS'
    AVAILABILITY = 'Availability'
    COUNT_OF_DISPLAY = 'Count of Display'
    SHARE_OF_DISPLAY = 'Share of Display'
    MARKET_SHARE = 'market_share'
    SURVEY = 'Survey'

    SHEETS = [KPIS, SOS, AVAILABILITY, COUNT_OF_DISPLAY, SHARE_OF_DISPLAY, MARKET_SHARE]

    # KPIs columns
    KPI_NAME = 'KPI Name'
    KPI_TYPE = 'Type'
    SCENE_TYPE = 'Scene Type'
    EXCLUDED_SCENE_TYPE = 'Excluded Scene Type'
    STORE_TYPE = 'store_type'
    WEIGHT = 'Weight'
    TEMPLATE_NAME = 'Template'
    TEMPLATE_GROUP = 'Template Group'

    # SOS columns
    MANUFACTURER = 'manufacturer'
    MARKET_SHARE_TARGET = 'market_share_target'

    # Availability columns
    BRAND = 'Brand'
    CATEGORY = 'category'
    EXCLUDED_BRAND = 'Excluded_Brand'
    MINIMUM_NUMBER_OF_SKUS = 'number_required_SKUS'
    SURVEY_QUESTION_SKUS_REQUIRED = 'Survey_Question_SKUs_required'

    # Count of Display
    ATT4 = 'att4'
    SIZE_SUBPACKAGES_NUM = 'size;subpackages_num'
    SUBPACKAGES_NUM = 'subpackages_num'
    MINIMUM_FACINGS_REQUIRED = 'facings_threshold'

    # Share of Display


    # KPI result values
    PASS = 'Pass'
    FAIL = 'Fail'

    NUMERIC_VALUES_TYPES = ['size']
