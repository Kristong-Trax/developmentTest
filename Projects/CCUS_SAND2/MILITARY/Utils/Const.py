def get(coll, keys):
    return [coll.get(key) for key in keys]


# kpi results
FK = 'fk'
NUMERATOR_ID = 'numerator_id'
NUMERATOR_RESULT = 'numerator_result'
DENOMINATOR_ID = 'denominator_id'
DENOMINATOR_RESULT = 'denominator_result'
RESULT = 'result'
CONTEXT_ID = 'context_id'

# kpi specs keys
NAME = 'name'
TEMPLATE = 'template'
MANUFACTURER = 'manufacturer'
CATEGORY = 'category'
PRODUCT = 'product'
RESULTS = 'results'
DATASET_A = 'dataset_a'
TEST_A = 'test_a'
FACINGS = 'facings'

COMPLIANT_BAY_COUNT = 'Compliant Bay Count'
SCENE_AVAILABILITY = 'Scene Availability'
FACINGS_SOS = 'Facings SOS'
SHARE_OF_SCENES = 'Share of Scenes'

# db columns
SCENE_FK = 'scene_fk'
PRODUCT_FK = 'product_fk'
PRODUCT_NAME = 'product_name'
TEMPLATE_FK = 'template_fk'

# template keys
MAIN_COOLERS = 'main_coolers'
SELF_COOLERS = 'self_coolers'
DISPLAY = 'display'
ENDCAP = 'display_endcap'
FRONT_ENTRANCE = 'front_entrance'
BEVERAGE_AISLE = 'beverage_aisle'

# manufacturer keys
COKE = 'coke'
FAIRLIFE = 'fairlife'
PEPSI = 'pepsi'

#
KPI_NAME = 'KPI Name'
EMPTY = 'empty'
QUESTION_TEXT = 'question_text'
FILTER = 'filter'
FILTERS = 'filters'
NUMERATOR = 'numerator'
DENOMINATOR = 'denominator'
CONTEXT = 'context'
KEY_PACKAGE = 'KEY PACKAGE'
TEMPLATE_NAME = 'template_name'
IDENTIFIER_PARENT = 'identifier_parent'
IDENTIFIER_RESULT = 'identifier_result'
KPI_ID = 'KPI ID'
KPI_PARENT_ID = 'KPI Parent ID'

KPI = 'KPI'
KPI_TYPE = 'KPI Type'
# survey
PALLET = 'pallet'
RACK = 'rack'
SHOULD_ENTER = 'should_enter'
LOCATION = 'location'
PRODUCT_TYPE = 'product_type'
CATEGORY_FK = 'category_fk'
MANUFACTURER_FK = 'manufacturer_fk'
NUMERATOR_ENTITY = 'Numerator Entity'
DENOMINATOR_ENTITY = 'Denominator Entity'

TEMPLATES = {
    MAIN_COOLERS: 'M - Main Checkout Coolers Only',
    SELF_COOLERS: 'M - Self Check-Out Coolers',
    DISPLAY: 'M - Display (Pallet Drop/Rack/ Shipper)',
    ENDCAP: 'M - Displays Endcap Only',
    FRONT_ENTRANCE: 'M - Front Entrance Primary Displays Only',
    BEVERAGE_AISLE: 'M - Beverage Aisle/Shelf - All (Separated by Category)',
}

MANUFACTURERS = {
    COKE: 'CCNA',
    FAIRLIFE: 'FAIRLIFE',  # 'FairLife LLC',
    PEPSI: 'PBNA'
}

PRODUCTS = {
    EMPTY: 'General Empty'
}

SURVEY_RESPONSES = {
    PALLET: 'Pallat Drop',
    RACK: 'Rack'
}

KPIs = {
    COMPLIANT_BAY_COUNT: [
        {
            NAME: 'How Many Coca-Cola Branded Coolers are in the Front End Checkout Area?',
            TEMPLATE: TEMPLATES[MAIN_COOLERS],
            MANUFACTURER: get(MANUFACTURERS, [COKE, FAIRLIFE]),
            'exclude_manufacturers': False
        },
        {
            NAME: 'How Many Pepsi Branded Coolers are in the Front End Checkout Area?',
            TEMPLATE: TEMPLATES[MAIN_COOLERS],
            MANUFACTURER: [MANUFACTURERS[PEPSI]],
            'exclude_manufacturers': False
        },
        {
            NAME: 'How Many Coolers From Other Brands are in the Front End Checkout Area?',
            TEMPLATE: TEMPLATES[MAIN_COOLERS],
            MANUFACTURER: get(MANUFACTURERS, [COKE, FAIRLIFE, PEPSI]),
            'exclude_manufacturers': True
        },
        {
            NAME: 'How Many Coca-Cola Branded Coolers are in the Self Checkout Area?',
            TEMPLATE: TEMPLATES[SELF_COOLERS],
            MANUFACTURER: get(MANUFACTURERS, [COKE, FAIRLIFE]),
            'exclude_manufacturers': False
        },
        {
            NAME: 'How Many Pepsi Branded Coolers are in the Self Checkout Area?',
            TEMPLATE: TEMPLATES[SELF_COOLERS],
            MANUFACTURER: [MANUFACTURERS[PEPSI]],
            'exclude_manufacturers': False
        },
        {
            NAME: 'How Many Other Coolers From Other Brands are in the Self Checkout Area?',
            TEMPLATE: TEMPLATES[SELF_COOLERS],
            MANUFACTURER: get(MANUFACTURERS, [COKE, FAIRLIFE, PEPSI]),
            'exclude_manufacturers': True
        }
    ],
    FACINGS_SOS: [
        {
            NAME: 'SOVI Main Beverage Aisle',
            TEMPLATE: [TEMPLATES[BEVERAGE_AISLE]],
            NUMERATOR: MANUFACTURER,
            DENOMINATOR: CATEGORY,
            CONTEXT: ''
        },
        {
            NAME: '% Empty by Manufacturer Within Category',
            TEMPLATE: [TEMPLATES[BEVERAGE_AISLE]],
            NUMERATOR: EMPTY,
            DENOMINATOR: MANUFACTURER,
            CONTEXT: CATEGORY
        },
        {
            NAME: 'SOVI Displays',
            TEMPLATE: get(TEMPLATES, [DISPLAY, ENDCAP, FRONT_ENTRANCE]),
            NUMERATOR: MANUFACTURER,
            DENOMINATOR: CATEGORY,
            CONTEXT: ''
        },
    ]
}

REGION = 'Military'
