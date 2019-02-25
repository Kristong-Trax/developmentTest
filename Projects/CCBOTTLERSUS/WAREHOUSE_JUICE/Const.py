import os

__author__ = 'Hunter'

class Const(object):
    WAREHOUSE_JUICE = 'Warehouse Juice'
    SET_SIZE_KPI_NAME = 'Warehouse Juice - Set Size'
    ASSORTMENT_KPI_NAME = 'Warehouse Juice - On Shelf Availability'
    NIELSEN_UPC = 'Nielsen_UPC'
    TOTAL_CATEGORY = 'Total Category'
    CATEGORY = 'Category'
    STORAGE_TYPE = 'Storage Type'
    KEY_MANUFACTURER = 'Key Manufacturer'

    # template stuff
    TEMPLATE_PATH = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'Data', 'Warehouse Juice Must Have SKUs.xlsx')
    RETAILERS = ['C&S NE Key Foods', 'Krasdale', 'General Trading', 'C&S Foodtown', 'C&S Heritage']
    UPC = 'UPC'
    PRODUCT_NAME = 'Product/No'
    SET_SIZE = 'Set Size'

    # relevant scene types
    DRINK_JUICE_TEA = 'Drink/Juice/Tea'
    MILK = 'Milk'
    NATURAL_HEALTH = 'Natural Health'
    RELEVANT_SCENE_TYPES = [DRINK_JUICE_TEA, MILK, NATURAL_HEALTH]

    # relevant filters by scene type
    RELEVANT_FILTERS = {
        DRINK_JUICE_TEA: {
            TOTAL_CATEGORY: ['TTL JUICE/DRINK', 'TTL TEA'],
            STORAGE_TYPE: ['CH RTD']
        },
        MILK: {
            TOTAL_CATEGORY: ['TTL DRINKABLE DAIRY'],
            STORAGE_TYPE: ['CH RTD'],
            CATEGORY: ['VALUE ADDED DAIRY']
        },
        NATURAL_HEALTH: {
            TOTAL_CATEGORY: ['TTL NATURAL HEALTH BEVERAGES'],
            STORAGE_TYPE: ['CH RTD']
        }
    }
