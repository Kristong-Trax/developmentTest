
__author__ = 'Uri'


class Const(object):

    SOVI = "SOVI"

    REGIONS = ['SOUTHWEST']

    # sheets:
    KPIS = "KPIs"
    AVAILABILITY = "Availability"
    SOS = "SOS"
    SHELVES = "shelves"
    SHELVES_BONUS = "shelves bonus"
    SOS_MAJOR = "SOS_majority"
    SURVEY = "Survey"
    SKU_EXCLUSION = "SKU_Exclusion"
    CONVERTERS = "converters"
    SCENE_AVAILABILITY = "Availability_scene"
    TARGETS = "Targets"
    FACINGS = 'Facings NTBA'
    RATIO = 'ratio'
    PURITY = 'purity'
    SHEETS = [KPIS, AVAILABILITY, SOS, SOS_MAJOR, SURVEY, SKU_EXCLUSION, CONVERTERS]
    SHEETS_CMA = [KPIS, SOS, SHELVES, FACINGS, RATIO, PURITY, TARGETS]


    # generic columns:
    KPI_NAME = "KPI name"
    PACKAGE_TYPE = "package_type"
    SSD_STILL = "SSD/still"
    TRADEMARK = "trademark"
    TARGET = "target"
    UNITED_DELIVER = "United Deliver"
    BRAND = "brand"

    # columns of KPIS:
    REGION = "Region"
    DISPLAY_TEXT = 'display_text'
    SCENE_LEVEL = 'scene level'
    STORE_TYPE = "store_type"
    SCENE_TYPE = "scene type"
    SCENE_TYPE_GROUP = "template_group"
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
    TYPE = "type"
    TEMPLATE_GROUP = 'template group'

    # columns of AVAILABILITY:
    SCENE_SKU = "scene/SKU"
    MANUFACTURER = "manufacturer"
    SIZE = "Size"
    PREMIUM_SSD = "Premium SSD"
    INNOVATION_BRAND = "Innovation Brand"
    NUM_SUB_PACKAGES = "Number of Subpackages"
    PRODUCT_EAN = "product_ean_code"

    # columns of sos&majority:
    DEN_TYPES_1 = "denominator param 1"
    DEN_VALUES_1 = "denominator value 1"
    DEN_TYPES_2 = "denominator param 2"
    DEN_VALUES_2 = "denominator value 2"
    NUM_TYPES_1 = "numerator param 1"
    NUM_VALUES_1 = "numerator value 1"
    NUM_TYPES_2 = "numerator param 2"
    NUM_VALUES_2 = "numerator value 2"
    GROUP = "Group"
    MAJ_DOM = "Majority/Dominant"

    # Generic Columns
    A_PARAM = 'Param 1'
    A_VALUE = 'Value 1'
    B_PARAM = 'Param 2'
    B_VALUE = 'Value 2'
    C_PARAM = 'Param 3'
    C_VALUE = 'Value 3'
    PROGRAM = 'program (Additional Attribute 3)'

    # columns of survey:
    Q_TEXT = "question_text"
    Q_ID = "question_ID"
    ACCEPTED_ANSWER = "accepted_answer"
    REQUIRED_ANSWER = "required_answer"

    # converters columns:
    NAME_IN_TEMP = "name_in_template"
    NAME_IN_DB = "name_in_db"

    # table of scene results:
    SCENE_FK = "scene_fk"
    RESULT = "result"
    KPI_FK = "kpi_fk"
    SCORE = "score"
    THRESHOLD = "threshold"
    COLUMNS_OF_SCENE = [SCENE_FK, KPI_NAME, RESULT, SCORE]
    COLUMNS_OF_SESSION = [KPI_NAME, RESULT]

    # seperators
    SEPERATOR = '; '
    COMMA = ','

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

    MANUAL = "MANUAL"
    CALCULATION_TYPES = [SOVI, MANUAL]

    KPI_FAMILY_KEY = {
                    18: 'CMA Compliance SW # of Shelves',
                    19: 'CMA Compliance SW Impulse Zone Cooler',
                    20: 'CMA Compliance SW % of Facings',
                    2: 'CMA Compliance SW % of Facings',
                    21: 'CMA Compliance SW # of Shelves Bonus'
                }

