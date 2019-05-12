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

    # Store attributes
    ADDITIONAL_ATTRIBUTE_1 = 'additional_attribute_1'
    STORE_TYPE = 'store_type'

    # Session attributes
    SCENE_FK = 'scene_fk'
    SCENE_ID = 'scene_id'

    # P&B data
    CBCIL_MANUFACTURER = 1  # todo todo todo CHECK FOR THE RELEVANT ONE!!!!!!!
