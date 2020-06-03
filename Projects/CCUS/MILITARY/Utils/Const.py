COMPLIANT_BAY_COUNT = 'compliant_bay_count'
MAIN_COOLERS = 'main_coolers'
SELF_COOLERS = 'self_coolers'

COKE = 'coke'
PEPSI = 'pepsi'

TEMPLATES = {
    'main_coolers': 'M - Main Checkout Coolers Only',
    'self_coolers': 'M - Self Check-Out Coolers'
}

MANUFACTURERS = {
    'coke': ['CCNA', 'FairLife LLC'],
    'pepsi': ['PBNA']
}

KPIs = {
    'compliant_bay_count': [
        {
            'name': 'How many Coca-Cola branded coolers does this store have at the front end checkout area?',
            'template': TEMPLATES[MAIN_COOLERS],
            'manufacturers': MANUFACTURERS[COKE],
            'exclude_manufacturers': False
        },
        {
            'name': 'How many Pepsi branded coolers does this store have at the front end checkout area?',
            'template': TEMPLATES[MAIN_COOLERS],
            'manufacturers': MANUFACTURERS[PEPSI],
            'exclude_manufacturers': False
        },
        {
            'name': 'How many branded coolers from other brands does this store have at the front end checkout area?',
            'template': TEMPLATES[MAIN_COOLERS],
            'manufacturers': MANUFACTURERS[COKE] + MANUFACTURERS[PEPSI],
            'exclude_manufacturers': True
        },
        {
            'name': 'How many Coca-Cola branded coolers does this store have at the self checkout area?',
            'template': TEMPLATES[SELF_COOLERS],
            'manufacturers': MANUFACTURERS[COKE],
            'exclude_manufacturers': False
        },
        {
            'name': 'How many Pepsi branded coolers does this store have at the self checkout area?',
            'template': TEMPLATES[SELF_COOLERS],
            'manufacturers': MANUFACTURERS[PEPSI],
            'exclude_manufacturers': False
        },
        {
            'name': 'How many branded coolers from other brands does this store have at the self checkout area?',
            'template': TEMPLATES[SELF_COOLERS],
            'manufacturers': MANUFACTURERS[COKE] + MANUFACTURERS[PEPSI],
            'exclude_manufacturers': True
        }
    ],
}

REGION = 'Military'
