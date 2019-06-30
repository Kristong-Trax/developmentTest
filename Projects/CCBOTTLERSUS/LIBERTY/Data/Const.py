import os

__author__ = 'hunter'


class Const(object):
    TEMPLATE_PATH = os.path.join(os.path.dirname(os.path.realpath(__file__)),
                                 'Liberty_bottlers_Template_subquestions_added_2019_06_27_RB.xlsx')
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
    SHARE_OF_COOLERS = 'Share of Coolers'
    SURVEY = 'Survey'
    MINIMUM_FACINGS = 'minimum_facings'
    SURVEY_QUESTION_SKUS = 'Survey_Question_SKUs'
    MARKET_SHARE = 'market_share'


    SHEETS = [KPIS, SOS, AVAILABILITY, COUNT_OF_DISPLAY, SHARE_OF_DISPLAY, SURVEY, MINIMUM_FACINGS,
              SURVEY_QUESTION_SKUS, MARKET_SHARE, SHARE_OF_COOLERS]

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
    PARENT_KPI_NAME = 'Parent KPI'

    # SOS columns
    MANUFACTURER = 'manufacturer'
    MARKET_SHARE_TARGET = 'market_share_target'
    LIBERTY_KEY_MANUFACTURER = 'Liberty Key Manufacturer'

    # Availability columns
    BRAND = 'brand'
    CATEGORY = 'category'
    EXCLUDED_BRAND = 'excluded_brand'
    EXCLUDED_SKU = 'excluded_SKU'
    MINIMUM_NUMBER_OF_SKUS = 'number_required_SKUS'
    SECONDARY_MINIMUM_NUMBER_OF_SKUS = 'number_required_SKUS - secondary'
    SURVEY_QUESTION_SKUS_REQUIRED = 'Survey_Question_SKUs_required'

    # Count of Display columns
    ATT4 = 'att4'
    SIZE_SUBPACKAGES_NUM = 'Base Size;Multi-Pack Size'
    SUBPACKAGES_NUM = 'Multi-Pack Size'
    MINIMUM_FACINGS_REQUIRED = 'facings_threshold'
    GREATER_THAN_ONE = '>1'
    EXCLUDED_CATEGORY = 'excluded_category'
    EXCLUDED_SIZE_SUBPACKAGES_NUM = 'Excluded Base Size; Multi Pack Size'

    # Share of Display columns
    INCLUDE_BODY_ARMOR = 'body_armor_delivered'

    # Share of Coolers columns
    COKE_FACINGS_THRESHOLD = 'cooler_facings_threshold'

    # Survey columns
    QUESTION_TEXT = 'question_text'

    # minimum_facings columns
    SIZE = 'size'
    UNIT_OF_MEASURE = 'unit_of_measure'
    MINIMUM_FACINGS_REQUIRED_FOR_DISPLAY = 'Facings_Required_for_Display'
    BASE_SIZE_MIN = 'Base Size Min'
    BASE_SIZE_MAX = 'Base Size Max'
    MULTI_PACK_SIZE = 'Multi-Pack Size'

    # Survey_Question_SKUs columns
    EAN_CODE = 'ean_code'
    SECONDARY_GROUP = 'secondary_group'

    # market_share columns
    ADDITIONAL_ATTRIBUTE_4 = 'additional_attribute_4'
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
