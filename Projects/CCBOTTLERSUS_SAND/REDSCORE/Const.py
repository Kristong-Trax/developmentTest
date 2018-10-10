import os
__author__ = 'Elyashiv'


class Const(object):
    TEMPLATE_PATH = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'Data', 'KPITemplateV4.6.xlsx')
    SURVEY_TEMPLATE_PATH = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'Data',
                                        'SurveyTemplateV2.xlsx')
    STORE_TYPES = {
        "CR SOVI RED": "CR&LT",
        "DRUG SOVI RED": "Drug",
        "VALUE SOVI RED": "Value",
        "FSOP - QSR": "QSR",
    }

    # sheets:
    KPIS = "KPIs"
    AVAILABILITY = "Availability"
    SOS = "SOS"
    SOS_MAJOR = "SOS_majority"
    SURVEY = "Survey"
    SKU_EXCLUSION = "SKU_Exclusion"
    CONVERTERS = "converters"
    SCENE_AVAILABILITY = "Availability_scene"
    SHEETS = [KPIS, AVAILABILITY, SOS, SOS_MAJOR, SURVEY, SKU_EXCLUSION, CONVERTERS]
    SHEETS_MANUAL = [KPIS, SURVEY]

    # generic columns:
    KPI_NAME = "KPI_name"
    PACKAGE_TYPE = "package_type"
    SSD_STILL = "SSD/still"
    TRADEMARK = "trademark"
    TARGET = "target"
    UNITED_DELIVER = "United Deliver"
    BRAND = "brand"

    # columns of KPIS:
    REGION = "Region"
    DISPLAY_TEXT = 'display_text'
    STORE_TYPE = "store_type"
    SCENE_TYPE = "scene_type"
    SCENE_TYPE_GROUP = "scene_type_group"
    STORE_ATTRIBUTE = "store_attribute"
    SESSION_LEVEL = "session_level"
    GROUP_TARGET = "group_target"
    CONDITION = "condition"
    INCREMENTAL = "incremental"
    SHEET = "sheet"
    EXCLUSION_SHEET = "exclusion_sheet"
    WEIGHT = "weight"
    SAME_PACK = "Same Pack"
    REUSE_SCENE = "reuse_scene"

    # columns of AVAILABILITY:
    SCENE_SKU = "scene/SKU"
    MANUFACTURER = "manufacturer"
    SIZE = "Size"
    PREMIUM_SSD = "Premium SSD"
    INNOVATION_BRAND = "Innovation Brand"
    NUM_SUB_PACKAGES = "Number of Subpackages"
    PRODUCT_EAN = "product_ean_code"

    # columns of sos&majority:
    DEN_TYPES_1 = "denominator_types 1"
    DEN_VALUES_1 = "denominator_values 1"
    DEN_TYPES_2 = "denominator_types 2"
    DEN_VALUES_2 = "denominator_values 2"
    NUM_TYPES_1 = "numerator_types 1"
    NUM_VALUES_1 = "numerator_values 1"
    NUM_TYPES_2 = "numerator_types 2"
    NUM_VALUES_2 = "numerator_values 2"
    GROUP = "Group"
    MAJ_DOM = "Majority/Dominant"
    ADD_IF_NOT_DP = "add if not DP"

    # columns of survey:
    Q_TEXT = "question_text"
    Q_ID = "question_ID"
    ACCEPTED_ANSWER = "accepted_answer"
    REQUIRED_ANSWER = "required_answer"

    # converters columns:
    NAME_IN_TEMP = "name_in_template"
    NAME_IN_DB = "name_in_db"

    # tables of results:
    DB_SCENE_KPI_FK = "kpi_level_2_fk"
    DB_SCENE_FK = "scene_fk"
    DB_RESULT = "result"
    KPI_FK = "kpi_fk"
    SCORE = "score"
    COLUMNS_OF_RESULTS = [KPI_NAME, DB_RESULT]

    # constants:
    ALL = "ALL"
    V = "V"
    DP_MANU = ['Dr Pepper', 'Dr Pepper Snapple Group Inc', 'DPSG']
    DP = "DP"
    MAJOR = "majority"
    DOMINANT = "dominant"
    MAJORITY_TARGET = 0.5
    NUMERIC_VALUES_TYPES = ['size']

    RED_SCORE = 'Red SCORE'
    RED_SCORE_INTEG = 'Red SCORE Integration'
    MANUAL_RED_SCORE = 'Red Score Survey'
    MANUAL_RED_SCORE_INTEG = 'Red Score Survey Integration'

    SOVI = "SOVI"
    MANUAL = "MANUAL"
    CALCULATION_TYPES = [
        SOVI,
        MANUAL
    ]
    SCENE_SUFFIX = " - scene"
    PASS = "Pass"
    FAIL = "Fail"
