__author__ = 'Hunter'

class Const(object):
    WAREHOUSE_JUICE = 'Warehouse Juice'
    KPI_NAME = 'Warehouse Juice - Set Size'

    # relevant scene types
    DRINK_JUICE_TEA = 'Drink/Juice/Tea'
    MILK = 'Milk'
    RELEVANT_SCENE_TYPES = [DRINK_JUICE_TEA, MILK]

    # relevant_categories
    RELEVANT_CATEGORIES = {
        DRINK_JUICE_TEA: ['Juice', 'Tea', 'Coffee'],
        MILK: ['Dairy']
    }

