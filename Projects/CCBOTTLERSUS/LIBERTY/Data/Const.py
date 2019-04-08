import os

__author__ = 'hunter'


class Const(object):
    TEMPLATE_PATH = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'Liberty_bottlers_Template_Final_Final_Final.xlsx')
    LIBERTY = ' - Liberty'
    DRILLDOWN = ' - Drilldown'
    RED_SCORE_PARENT = 'RED SCORE - Liberty'
    BODY_ARMOR_BRAND_FK = 203
    MANUFACTURER_FK = 1

    # sheets
    KPIS = 'KPIs'
    SOS = 'SOS'
    AVAILABILITY = 'Availability'
    COUNT_OF_DISPLAY = 'Count of Display'
    SHARE_OF_DISPLAY = 'Share of Display'
    SURVEY = 'Survey'
    MINIMUM_FACINGS = 'minimum_facings'
    SURVEY_QUESTION_SKUS = 'Survey_Question_SKUs'
    MARKET_SHARE = 'market_share'
    BODY_ARMOR = 'Body_Armor_Zip'


    SHEETS = [KPIS, SOS, AVAILABILITY, COUNT_OF_DISPLAY, SHARE_OF_DISPLAY, SURVEY, MINIMUM_FACINGS,
              SURVEY_QUESTION_SKUS, MARKET_SHARE, BODY_ARMOR]

    # KPIs columns
    KPI_NAME = 'KPI Name'
    KPI_TYPE = 'Type'
    SCENE_TYPE = 'Scene Type'
    EXCLUDED_SCENE_TYPE = 'Excluded Scene Type'
    STORE_TYPE = 'store_type'
    WEIGHT = 'Weight'
    TEMPLATE_NAME = 'Template'
    TEMPLATE_GROUP = 'Template Group'
    ADDITIONAL_ATTRIBUTE_7 = 'additional_attribute_7'

    # SOS columns
    MANUFACTURER = 'manufacturer'
    MARKET_SHARE_TARGET = 'market_share_target'

    # Availability columns
    BRAND = 'brand'
    CATEGORY = 'category'
    EXCLUDED_BRAND = 'Excluded_Brand'
    MINIMUM_NUMBER_OF_SKUS = 'number_required_SKUS'
    SURVEY_QUESTION_SKUS_REQUIRED = 'Survey_Question_SKUs_required'

    # Count of Display columns
    ATT4 = 'att4'
    SIZE_SUBPACKAGES_NUM = 'size;subpackages_num'
    SUBPACKAGES_NUM = 'subpackages_num'
    MINIMUM_FACINGS_REQUIRED = 'facings_threshold'

    # Share of Display columns
    INCLUDE_BODY_ARMOR = 'include_body_armor'

    # Survey columns
    QUESTION_TEXT = 'question_text'

    # minimum_facings columns
    SIZE = 'size'
    UNIT_OF_MEASURE = 'unit_of_measure'
    MINIMUM_FACINGS_REQUIRED_FOR_DISPLAY = 'Facings_Required_for_Display'

    # Survey_Question_SKUs columns
    EAN_CODE = 'ean_code'

    # Body_Armor_Zip
    ZIP = 'Zip'

    # market_share columns
    RETAILER = 'retailer_name'
    BRANCH = 'branch_name'
    SSD_AND_STILL = 'Total_SSD_and_Still'
    SSD = 'SSD'
    STILL = 'Still'

    # KPI result values
    PASS = 'Pass'
    FAIL = 'Fail'

    NOT_NULL = 'NOT NULL'
    NUMERIC_VALUES_TYPES = ['size']
