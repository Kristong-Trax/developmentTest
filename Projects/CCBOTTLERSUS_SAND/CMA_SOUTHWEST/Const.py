
__author__ = 'Uri'


class Const(object):

    SOVI = "SOVI"

    REGIONS = 'SOUTHWEST'
    TEMPLATE_PATH = 'Southwest CMA Compliance Template_v8.xlsx'

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
    PASS = 'Pass'
    FAIL = 'Fail'

    RED_SCORE = 'Red SCORE'
    RED_SCORE_INTEG = 'Red SCORE Integration'
    MANUAL_RED_SCORE = 'Red Score Survey'
    MANUAL_RED_SCORE_INTEG = 'Red Score Survey Integration'
    MANUAL = "MANUAL"
    CALCULATION_TYPES = [SOVI, MANUAL]
    ALL_SCENE_KPIS = ['Impulse Zone Cooler']

    # Hierarchy
    CMA = 'CMA Compliance SW'
    NUM_OF_SHELVES = 'CMA Compliance SW # of Shelves'
    IMPULSE_COOLER_ZONE = 'CMA Compliance SW Impulse Zone Cooler'
    NUM_OF_FACINGS = 'CMA Compliance SW # of Facings'
    PERCENT_OF_FACINGS = 'CMA Compliance SW % of Facings'
    NUM_SHELVES_BONUS = 'CMA Compliance SW # of Shelves Bonus'
    TOTAL_COKE = 'Total Coke Cooler Purity'
    COKE_COOLER_PURITY_BRAND = 'Coke Cooler Purity - Brand'
    ''

    KPI_FAMILY_KEY = {
                      18: NUM_OF_SHELVES,
                      # 19: IMPULSE_COOLER_ZONE,
                       20:  NUM_OF_FACINGS,
                       2: PERCENT_OF_FACINGS,
                       # 21: NUM_SHELVES_BONUS,
                       21: NUM_OF_SHELVES
                     }

    NO_PRESSURE = ['# of Shelves Bonus']
    SCENE_SESSION_KPI = {
                            2160: 2161,
                            3048: 2161,
                            3022: 3047,
                            3023: 3047,
                            3024: 3047,
                            3025: 3047,
                            3026: 3047,
                            3027: 3047,
                            3028: 3047,
                            3029: 3047,
                            3030: 3047,
                            3031: 3047,
                        }
    PARENT_HIERARCHY = {
                        NUM_OF_SHELVES: CMA,
                        IMPULSE_COOLER_ZONE: CMA,
                        NUM_OF_FACINGS: CMA,
                        PERCENT_OF_FACINGS: CMA,
                        NUM_SHELVES_BONUS: None,
                        TOTAL_COKE: None,
                        }
    BEHAVIOR = {
                3045: 'SUM',
                2161: 'SUM',
                3047: 'PASS'
                }
    PARENT_NOT_RATIO = []

    MANUFACTURER_FK = 1
