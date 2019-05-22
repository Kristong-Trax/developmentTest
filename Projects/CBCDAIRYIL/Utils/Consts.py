# coding=utf-8
import os


class Consts(object):
    # Template consts
    PROJECT_TEMPLATE_NAME = 'Template.xlsx'
    TEMPLATE_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.realpath(__file__))), 'Data',
                                 PROJECT_TEMPLATE_NAME)
    KPI_SHEET = 'KPI'
    KPI_WEIGHT = 'kpi weights'
    KPI_GAP = 'Kpi Gap'
    SEPARATOR = ','

    # Template columns
    KPI_SET = 'KPI Set'
    KPI_NAME = 'KPI Name'
    KPI_ATOMIC_NAME = 'Atomic Name'
    KPI_TYPE = 'KPI Family'
    TEMPLATE_NAME = 'Template Name'
    TEMPLATE_GROUP = 'Template group'
    PARAMS_TYPE_1 = 'Param Type (1)/ Numerator'
    PARAMS_VALUE_1 = 'Param (1) Values'
    PARAMS_TYPE_2 = 'Param Type (2)/ Denominator'
    PARAMS_VALUE_2 = 'Param (2) Values'
    PARAMS_TYPE_3 = 'Param Type (3)'
    PARAMS_VALUE_3 = 'Param (3) Values'
    TARGET = 'Target'
    SPLIT_SCORE = 'Split Score'
    WEIGHT = 'Weight'
    ORDER = 'Order'
    KPI_FILTER_VALUE_LIST = [(PARAMS_TYPE_1, PARAMS_VALUE_1), (PARAMS_TYPE_2, PARAMS_VALUE_2),
                             (PARAMS_TYPE_3, PARAMS_VALUE_3)]

    # KPIs
    TOTAL_SCORE = 'Total Score'
    AVAILABILITY = 'Availability'
    AVAILABILITY_FROM_BOTTOM = 'Availability from bottom'
    MIN_2_AVAILABILITY = 'Min 2 Availability'
    BRAND_BLOCK = 'Brand Block'
    EYE_LEVEL = 'Eye Level'
    SURVEY = 'Survey'

    # General Attributes
    CODE = 'code'
    QUESTION_ID = 'question_id'
    SHELF_NUM_FROM_BOTTOM = 'shelf_number_from_bottom'
    MIN_BLOCK_RATIO = 0.8
    MIN_FACINGS_IN_BLOCK = 3
    PRODUCT_TYPE = 'product_type'
    OTHER = 'Other'
    PRODUCT_FK = 'product_fk'
    FACINGS = 'facings'
    FACINGS_IGN_STACK = 'facings_ign_stack'
    EAN_CODE = 'product_ean_code'

    # Store attributes
    ADDITIONAL_ATTRIBUTE_1 = 'additional_attribute_1'
    ADDITIONAL_ATTRIBUTE_2 = 'additional_attribute_2'
    ADDITIONAL_ATTRIBUTE_3 = 'additional_attribute_3'
    STORE_TYPE = 'store_type'
    STORE_ATTRIBUTES_TO_FILTER_BY = [STORE_TYPE, ADDITIONAL_ATTRIBUTE_1, ADDITIONAL_ATTRIBUTE_2, ADDITIONAL_ATTRIBUTE_3]

    # Session attributes
    SCENE_FK = 'scene_fk'
    SCENE_ID = 'scene_id'

    # Filters
    KPI_FILTERS = 'filters'
    EXCLUDE_VAL = 0
    INCLUDE_VAL = 1

    # Eye level calculation
    EYE_LEVEL_PER_SHELF = [{'min': 3, 'max': 4, 'top': 0, 'bottom': 1}, {'min': 5, 'max': 6, 'top': 1, 'bottom': 2},
                           {'min': 7, 'max': 100, 'top': 1, 'bottom': 2}]

    # CBC data
    CBCIL_MANUFACTURER = 45
    CBC_COOLERS = u'מקרר חברה מרכזית'
    COMPETITOR_COOLERS = [u'מקרר מתחרה', u'מקרר קמעונאי']
    SURVEY_ANSWERS_TO_IGNORE = [u'הסידור אינו קטגוריאלי', u'אין מותגים אחרים']
    HEBREW_YES = u'כן'
    HEBREW_NO = u'לא'
    GOLDEN_SHELVES = [3, 4]
    LOWEST_SHELF = 1
    DYNAMO = u'דינמי פרטי'
    MINI_MARKET = u'מינימרקט'
    GENERAL = u'כללי'
    ARAB = u'ערבי'
    ORTHODOX = u'חרדי'
    QUESTION_CODES_FOR_EMPTY_SESSIONS = [9]

    # Gaps
    GAPS_QUERY = "insert into pservice.custom_gaps (session_fk, gap_category, name, priority) values ({},{},{},{})"
    PRIORITY = 'priority'
    SCORE = 'score'

    # Logs
    MISSING_QUESTION_LOG = "Missing question ID field! Please check the template"
    EMPTY_TEMPLATE_DATA_LOG = "There isn't relevant data in the template for store fk = {}! Exiting..."
    UNSUPPORTED_KPI_LOG = "KPI of type '{}' is not supported"
