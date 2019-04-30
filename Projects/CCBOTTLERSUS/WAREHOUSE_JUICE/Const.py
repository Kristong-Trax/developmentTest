import os

__author__ = 'Hunter'

class Const(object):
    WAREHOUSE_JUICE = 'Warehouse Juice'
    SET_SIZE_KPI_NAME = 'Warehouse Juice - Set Size'
    ASSORTMENT_KPI_NAME = 'Warehouse Juice - On Shelf Availability'
    NIELSEN_UPC = 'Nielsen_UPC'
    TOTAL_CATEGORY = 'Total Category'
    CATEGORY = 'Category'
    CLIENT_CATEGORY = 'category'
    STORAGE_TYPE = 'Storage Type'
    KEY_MANUFACTURER = 'Key Manufacturer'

    # template stuff
    TEMPLATE_PATH = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'Data', 'Must Have SKUs v3.xlsx')
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
            CLIENT_CATEGORY: ['Juice', 'Tea']
        },
        MILK: {
            CLIENT_CATEGORY: ['Dairy']
        },
        NATURAL_HEALTH: {
            CLIENT_CATEGORY: ['Juice', 'Tea']
        }
    }
