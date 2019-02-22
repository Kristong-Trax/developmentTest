import os

__author__ = 'Hunter'

class Const(object):
    WAREHOUSE_JUICE = 'Warehouse Juice'
    SET_SIZE_KPI_NAME = 'Warehouse Juice - Set Size'
    ASSORTMENT_KPI_NAME = 'Warehouse Juice - On Shelf Availability'
    NIELSEN_UPC = 'Nielsen_UPC'
    TOTAL_CATEGORY = 'Total Category'

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

    # relevant_categories
    RELEVANT_CATEGORIES = {
        DRINK_JUICE_TEA: ['TTL JUICE/DRINK', 'TTL TEA', 'TTL PLANT WATER/JUICE',
                          'TTL N-RTD FT FLAVORED BEVS'],
        MILK: ['TTL DRINKABLE DAIRY'],
        NATURAL_HEALTH: ['TTL COMPLETE NUTRITIONAL', 'TTL NATURAL HEALTH BEVERAGES', 'TTL KOMBUCHA']
    }

