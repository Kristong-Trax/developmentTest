from Trax.Utils.Classes.Enums import Enum

__author__ = 'nethanel'

CAT_MAIN_MEAL_WET = 'CAT MAIN MEAL WET'
CAT_MAIN_MEAL_WET_2018 = 'CAT MAIN MEAL WET 2018'
DOG_MAIN_MEAL_DRY = 'DOG MAIN MEAL DRY'
DOG_MAIN_MEAL_DRY_2018 = 'DOG MAIN MEAL DRY 2018'
DOG_TREATS = 'DOG TREATS'
DOG_TREATS_2018 = 'DOG TREATS 2018'
CAT_TREATS = 'CAT TREATS'
CAT_TREATS_2018 = 'CAT TREATS 2018'
CAT_MAIN_MEAL_DRY = 'CAT MAIN MEAL DRY'
CAT_MAIN_MEAL_DRY_2018 = 'CAT MAIN MEAL DRY 2018'
DOG_MAIN_MEAL_WET = 'DOG MAIN MEAL WET'

SPT_DOG_TREATS = 'SPT DOG TREATS'
SPT_CAT_TREATS = 'SPT CAT TREATS'
SPT_CAT_MAIN_MEAL = 'SPT CAT MAIN MEAL'
SPT_DOG_MAIN_MEAL = 'SPT DOG MAIN MEAL'

SPT_DOG_TREATS_Q1_2018 = 'SPT DOG TREATS Q1 2018'
SPT_CAT_TREATS_Q1_2018 = 'SPT CAT TREATS Q1 2018'
SPT_CAT_MAIN_MEAL_Q1_2018 = 'SPT CAT MAIN MEAL Q1 2018'
SPT_DOG_MAIN_MEAL_Q1_2018 = 'SPT DOG MAIN MEAL Q1 2018'

SEPARATOR = ','
FILTER_NAME_SEPARATOR = ';'
PRODUCT_EAN_CODES = 'Products EANs'
PRODUCT_TYPE = 'Product Type'
BRAND = 'Brand Name'
SUB_CATEGORY = 'Sub category'
RELEVANT_SCENE_TYPES = 'Scene Types to Include'
# DOG_MAIN_MEAL_WET = 'Dog Main Meal Wet'
DOG_MAIN_MEAL_WET_2018 = 'DOG MAIN MEAL WET 2018'

BDB_RETAILERS = ['Ahold Delhaize',
                'Ahold',
                'Albertsons',
                'Family Dollar',
                'Meijer',
                'Target',
                'Walmart',
                'Wegmans',
                'Kroger',
                'Publix',
                'Stater Bros',
                'Dollar General'
                ]

SPT_RETAILERS = ['PetSmart',
                'Petco'
                ]

BDB_CHANNELS = ['MASS',
               'FOOD',
               'Small Format'
               ]

SPT_CHANNELS = ['Pet Specialty']


ATT1 = 'att1'
KPI_NAME = 'KPI name'
TARGET = 'Target'
KPI_GROUP = 'KPI Group'
KPI_TYPE = 'KPI Type'
WEIGHT = 'WEIGHT'
DEPEND_ON = 'Depend on'
DEPEND_SCORE = 'Depend score'
RANGE_MIN = 'num. of shelves min'
RANGE_MAX = 'num. of shelves max'
IGNORE_TOP = 'num. ignored from top'
IGNORE_BOTTOM = 'num. ignored from bottom'
SUB_SECTION = 'Sub-section'
BRAND_NAME = 'brand_name'
MANUFACTURER_NAME = 'manufacturer_name'
PRIVATE_LABEL = 'Private Label'
SUB_BRAND_NAME = 'Sub Brand'
EXCLUDE = 'EXCLUDE'
ALLOWED = 'ALLOWED'
PACK_TYPE = 'Pack Type'
SINGLE_MULTIPLE = 'Single/Multuple'
BREED_SIZE = 'Breed Size'
MM_TO_FT_RATIO = 304.8
SECTION = 'Section'
SEGMENT_SPT = 'Segment_SPT'
FACINGS = 'facings_ign_stack'
USE_PROBES = 'Separate Stitching Group'
PACKAGE_SIZE = 'Package Size'

TEMPLATE_NAME = 'template_name'

IRRELEVANT_FIELDS = ['Score Card Name', 'KPI name', 'Tested Group Type', 'Target', 'Sales', 'NBIL', 'Stacking',
                     'Vertical Block', 'Note']

NOT_SEPARATE_FIELDS = ['Question Text;A']

DENOMINATOR_FILTER_FIELDS = [TEMPLATE_NAME, SUB_SECTION, SECTION, SEGMENT_SPT]
ALLOWED_TYPES = {'product_type': ['Empty', 'Other']}

MPIP_SVR_COLS = ['match_product_in_probe_fk', 'match_product_in_probe_state_reporting_fk']

FILTER_NAMING_DICT = {
    'Manufacturer': MANUFACTURER_NAME,
    'Brand': BRAND_NAME,
    'SubSection': SUB_SECTION,
    'Sub Brand': SUB_BRAND_NAME,
    'PACK TYPE': PACK_TYPE,
    'SINGLE/MULTI COUNT': SINGLE_MULTIPLE,
    'Breed size': BREED_SIZE,
    'Facings Minimum': FACINGS,
    'Package Size': PACKAGE_SIZE,
}

BLOCK_THRESHOLD = 0.5
VERTICAL_BLOCK_THRESHOLD = .5

SET_CATEGORIES = {
    DOG_MAIN_MEAL_WET: 13,
    CAT_MAIN_MEAL_DRY: 13,
    CAT_MAIN_MEAL_WET: 13,
    DOG_MAIN_MEAL_DRY: 13,
    DOG_TREATS: 13,
    CAT_TREATS: 13
}


class Sets(Enum):
    DOG_MAIN_MEAL_WET = 'DOG MAIN MEAL WET'
    CAT_MAIN_MEAL_DRY = 'CAT MAIN MEAL DRY'


class CalculationDependencyCheck(Enum):
    CALCULATE = 1
    PUSH_BACK = 2
    IGNORE = 3


SET_PRE_CALC_CHECKS = {
    DOG_MAIN_MEAL_WET: {SUB_SECTION: 'DOG MAIN MEAL WET', TEMPLATE_NAME: 'Pet Food & Edible Treats Section'},
    CAT_MAIN_MEAL_DRY: {SUB_SECTION: 'CAT MAIN MEAL DRY', TEMPLATE_NAME: 'Pet Food & Edible Treats Section'},
    CAT_MAIN_MEAL_WET: {SUB_SECTION: 'CAT MAIN MEAL WET', TEMPLATE_NAME: 'Pet Food & Edible Treats Section'},
    DOG_MAIN_MEAL_DRY: {SUB_SECTION: 'DOG MAIN MEAL DRY', TEMPLATE_NAME: 'Pet Food & Edible Treats Section'},
    DOG_TREATS: {SECTION: 'DOG TREATS', TEMPLATE_NAME: 'Pet Food & Edible Treats Section'},
    CAT_TREATS: {SECTION: 'CAT TREATS', TEMPLATE_NAME: 'Pet Food & Edible Treats Section'},
    DOG_MAIN_MEAL_WET_2018: {SUB_SECTION: 'DOG MAIN MEAL WET', TEMPLATE_NAME: 'Pet Food & Edible Treats Section'},
    CAT_MAIN_MEAL_DRY_2018: {SUB_SECTION: 'CAT MAIN MEAL DRY', TEMPLATE_NAME: 'Pet Food & Edible Treats Section'},
    CAT_MAIN_MEAL_WET_2018: {SUB_SECTION: 'CAT MAIN MEAL WET', TEMPLATE_NAME: 'Pet Food & Edible Treats Section'},
    DOG_MAIN_MEAL_DRY_2018: {SUB_SECTION: 'DOG MAIN MEAL DRY', TEMPLATE_NAME: 'Pet Food & Edible Treats Section'},
    DOG_TREATS_2018: {SECTION: 'DOG TREATS', TEMPLATE_NAME: 'Pet Food & Edible Treats Section'},
    CAT_TREATS_2018: {SECTION: 'CAT TREATS', TEMPLATE_NAME: 'Pet Food & Edible Treats Section'},

    SPT_DOG_TREATS: {SECTION: 'DOG TREATS', TEMPLATE_NAME: 'Pet Food & Edible Treats Section'},
    SPT_CAT_TREATS: {SECTION: 'CAT TREATS', TEMPLATE_NAME: 'Pet Food & Edible Treats Section'},
    SPT_CAT_MAIN_MEAL: {SECTION: 'CAT MAIN MEAL', TEMPLATE_NAME: 'Pet Food & Edible Treats Section'},
    SPT_DOG_MAIN_MEAL: {SECTION: 'DOG MAIN MEAL', TEMPLATE_NAME: 'Pet Food & Edible Treats Section'},

    SPT_DOG_TREATS_Q1_2018: {SECTION: 'DOG TREATS', TEMPLATE_NAME: 'Pet Food & Edible Treats Section'},
    SPT_CAT_TREATS_Q1_2018: {SECTION: 'CAT TREATS', TEMPLATE_NAME: 'Pet Food & Edible Treats Section'},
    SPT_CAT_MAIN_MEAL_Q1_2018: {SECTION: 'CAT MAIN MEAL', TEMPLATE_NAME: 'Pet Food & Edible Treats Section'},
    SPT_DOG_MAIN_MEAL_Q1_2018: {SECTION: 'DOG MAIN MEAL', TEMPLATE_NAME: 'Pet Food & Edible Treats Section'}
}

EYELIGHT_KPIS = [
                    'Is the Customized Care Dry Cat Food feeding philosophy segment blocked?',
                    'Is IAMS Wet Cat food shelved within the Customized Care feeding philosophy segment?',
                    'Is the Cat Treats category blocked?',
                    'Is IAMS ProActive Health Dry Dog Food shelved with the Customized Care feeding philosophy segment?',
                    'Is PEDIGREE Dry Dog Food shelved with the Basic & Balanced feeding philosophy segment?',
                    'Is the Wet Dog Food category blocked?',
                    'Is the Basic & Balanced Wet Dog Food feeding philosophy segment blocked?',
                    'Is IAMS Wet Dog Food shelved with the Customized Care feeding philosophy segment?',
                    'Is the Dog Treats Regular category blocked? <=8 ft',
                    'Are Cesar Dog Treats blocked? <=8 ft',
                    'Is Nutro Culinary Dog food feeding philosophy segment blocked?"',
                    'Is Nutro Ingredient Transparency Dog food feeding philosophy segment blocked?',
                    'Is Nutro Dry Dog food blocked?',
                    'Is Nutro Wet Dog food blocked?',
                    'Is Nutro Wet Cat Food blocked?',
                    ]