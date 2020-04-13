import os

def get_parent_dir(path, levels=1):
    for _ in range(levels):
        path = os.path.dirname(path)
    return path

DATA_PATH = os.path.join(get_parent_dir(__file__, 2), 'Data')
TEST_SESSIONS_PATH = os.path.join(DATA_PATH, 'Nestle Waters Test Sessions.xlsx')
SHELF_MAP_PATH = os.path.join(DATA_PATH, 'ShelfMap.xlsx')


KPIs = {
    'facings': {
        'DB': 'FACINGS_SKU_STACKING_OUT_OF_TEMPLATE',
        'DF': 'facings',
        'ID': 909
    },
    'facings_ign_stack': {
        'DB': 'FACINGS_SKU_IGN_STACKING_OUT_OF_TEMPLATE',
        'DF': 'facings_ign_stack',
        'ID': 910
    },
    '': {
        'DB': 'LINEAR_FEET_SKU_STACKING_OUT_OF_TEMPLATE',
        'DF': 'net_len_add_stack',
        'ID': 911
    },
    '': {
        'DB': 'LINEAR_FEET_SKU_IGN_STACKING_OUT_OF_TEMPLATE',
        'DF': 'net_len_ign_stack',
        'ID': 912
    },
    '': {
        'DB': 'WATER_AISLE_BASE_FOOTAGE',
        'DF': None,
        'ID': 913
    },
    '': {
        'DB': 'FACINGS_BY_SHELF_POSITION',
        'DF': None,
        'ID': 914
    },
    '': {
        'DB': 'COUNT_OF_DISPLAY_TYPE',
        'DF': None,
        'ID': 915
    },
    '': {
        'DB': 'COUNT_OF_NESTLE_DISPLAYS',
        'DF': None,
        'ID': 916
    }
}

WATER_TEMPLATES = {
    'water_aisle': 2,
    'water_display': 7
}

WATER_CATEGORY = {
    'water': 29
}

SHELF_POSITION = {
    'BOTTOM': 1,
    'MIDDLE': 2,
    'EYE': 3,
    'TOP': 4
}

def mm_to_feet(n): return n / 1000.0 * 3.28084