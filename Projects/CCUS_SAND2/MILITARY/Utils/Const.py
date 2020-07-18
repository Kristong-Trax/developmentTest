def get(coll, keys):
    return [coll.get(key) for key in keys]


FK = 'fk'
NUMERATOR_ID = 'numerator_id'
NUMERATOR_RESULT = 'numerator_result'
DENOMINATOR_ID = 'denominator_id'
DENOMINATOR_RESULT = 'denominator_result'
RESULT = 'result'
CONTEXT_ID = 'context_id'

# kpi keys
NAME = 'name'
TEMPLATE = 'template'
MANUFACTURER = 'manufacturer'
CATEGORY = 'category'
PRODUCT = 'product'
RESULTS = 'results'
DATASET_A = 'dataset_a'
TEST_A = 'test_a'
FACINGS = 'facings'

COMPLIANT_BAY_COUNT = 'compliant_bay_count'
SCENE_AVAILABILITY = 'scene_availability'
FACINGS_SOS = 'facings_sos'
SHARE_OF_SCENES = 'share_of_scenes'

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
EMPTY = 'empty'

FILTER = 'filter'
NUMERATOR = 'numerator'
DENOMINATOR = 'denominator'
CONTEXT = 'context'
KEY_PACKAGE = 'Key Package'

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

KPIs = {
    COMPLIANT_BAY_COUNT: [
        {
            NAME: 'How many Coca-Cola branded coolers does this store have at the front end checkout area?',
            TEMPLATE: TEMPLATES[MAIN_COOLERS],
            MANUFACTURER: get(MANUFACTURERS, [COKE, FAIRLIFE]),
            'exclude_manufacturers': False
        },
        {
            NAME: 'How many Pepsi branded coolers does this store have at the front end checkout area?',
            TEMPLATE: TEMPLATES[MAIN_COOLERS],
            MANUFACTURER: [MANUFACTURERS[PEPSI]],
            'exclude_manufacturers': False
        },
        {
            NAME: 'How many branded coolers from other brands does this store have at the front end checkout area?',
            TEMPLATE: TEMPLATES[MAIN_COOLERS],
            MANUFACTURER: get(MANUFACTURERS, [COKE, FAIRLIFE, PEPSI]),
            'exclude_manufacturers': True
        },
        {
            NAME: 'How many Coca-Cola branded coolers does this store have at the self checkout area?',
            TEMPLATE: TEMPLATES[SELF_COOLERS],
            MANUFACTURER: get(MANUFACTURERS, [COKE, FAIRLIFE]),
            'exclude_manufacturers': False
        },
        {
            NAME: 'How many Pepsi branded coolers does this store have at the self checkout area?',
            TEMPLATE: TEMPLATES[SELF_COOLERS],
            MANUFACTURER: [MANUFACTURERS[PEPSI]],
            'exclude_manufacturers': False
        },
        {
            NAME: 'How many branded coolers from other brands does this store have at the self checkout area?',
            TEMPLATE: TEMPLATES[SELF_COOLERS],
            MANUFACTURER: get(MANUFACTURERS, [COKE, FAIRLIFE, PEPSI]),
            'exclude_manufacturers': True
        }
    ],
    SCENE_AVAILABILITY: [
        {
            'name': 'Does this store have a display of Coca - Cola CSD Brands of 24 pack /12 oz cans ?',
            TEMPLATE: TEMPLATES[DISPLAY],
            'datasets': [
                {
                    MANUFACTURER: MANUFACTURERS[COKE],
                    KEY_PACKAGE: '12OZ 24PK CAN'
                }
            ],
            'tests': [
                {FACINGS: 3}
            ],
        },
        {
            'name': 'Where is the display of Coca-Cola CSD Brands of 24 pack/12 oz. cans located?',
        },
        {
            'name': 'Does this store have a Coca-Cola branded endcap display?',
        },
        {
            'name': 'If Yes, does the end cap include 6pk .5L Coca-Cola',
        },
        {
            'name': 'Does this store have a 24 pack 500 ml Dasani Water display anywhere in the store?',
        },
        {
            'name': 'Does this store have a rack with Coca-Cola branding of Powerade and Core Power?',
        },
        {
            'name': 'Does the store have a rack containing Coca-Cola 1.25 liter CSD brands and Gold Peak Tea?',
        },
        {
            'name': 'Does the store have a rack containing Coca-Cola 7.5oz mini cans?',
        },
        {
            'name': 'Does this store have a rack with SmartWater and Vitamin Water?',
        },
        {
            'name': 'Does this store have an Aha sparkling pallet?',
        },
        {
            'name': 'Does the store have a mass display in the Lobby?',
        }
    ],
    FACINGS_SOS: [
        {
            NAME: 'SOVI Main Bev Aisle Man win Cat',
            TEMPLATE: [TEMPLATES[BEVERAGE_AISLE]],
            NUMERATOR: MANUFACTURER,
            DENOMINATOR: CATEGORY,
            CONTEXT: ''
        },
        {
            NAME: '% Empty By Manufacturer within Category',
            TEMPLATE: [TEMPLATES[BEVERAGE_AISLE]],
            NUMERATOR: EMPTY,
            DENOMINATOR: MANUFACTURER,
            CONTEXT: CATEGORY
        },
        {
            NAME: 'SOVI Displays (all scenes) Man within Cat',
            TEMPLATE: get(TEMPLATES, [DISPLAY, ENDCAP, FRONT_ENTRANCE]),
            NUMERATOR: MANUFACTURER,
            DENOMINATOR: CATEGORY,
            CONTEXT: ''
        },
    ],
    SHARE_OF_SCENES: [
        {
            NAME: 'Share of Displays',
            TEMPLATE: get(TEMPLATES, [DISPLAY, ENDCAP, FRONT_ENTRANCE])
        }
    ]
}

REGION = 'Military'
