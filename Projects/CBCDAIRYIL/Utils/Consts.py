# coding=utf-8
import os


class Consts(object):
    # Template consts
    PROJECT_TEMPLATE_NAME = 'Template.xlsx'
    TEMPLATE_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.realpath(__file__))), 'CBCDAIRYIL', 'Data',
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

    # KPIs
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
    FILTERS = 'filters'
    ALL = 'All'
    FILTER_PARAM_1 = 'param1'
    FILTER_PARAM_2 = 'param2'
    FILTER_PARAM_3 = 'param3'

    # CBC data
    CBCIL_MANUFACTURER = 1  # todo todo todo CHECK FOR THE RELEVANT ONE!!!!!!!
    CBC_COOLERS = u'מקרר חברה מרכזית'
    COMPETITOR_COOLERS = [u'מקרר מתחרה', u'מקרר קמעונאי']
