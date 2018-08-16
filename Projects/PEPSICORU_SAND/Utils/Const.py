
__author__ = 'Idanr'


class Const(object):
    # Categories
    SNACKS = 'SNACK'
    BEVERAGES = 'BEVERAGE'
    JUICES = 'JUICE'

    # Filters
    TEMPLATE_GROUP = 'template_group' #DELETE?
    TEMPLATE_NAME = 'template_name'
    MANUFACTURER_NAME = 'manufacturer_name'
    MANUFACTURER = 'manufacturer'
    CATEGORY = 'category'
    SUB_CATEGORY = 'sub_category'
    BRAND = 'brand'
    BRAND_NAME = 'brand_name'
    MAIN_SHELF = 'Main Shelf'
    MAIN_SHELF_SNACKS = 'Main Shelf snacks'
    MAIN_SHELF_BEVERAGES = 'Main Shelf beverages'
    MAIN_SHELF_JUICES = 'Main Shelf juices'
    PEPSICO = 'PepsiCo'
    FK = '_fk'
    NAME = '_name'

    # SOS plaster
    EXCLUDE_FILTER = 0
    INCLUDE_FILTER = 1
    CONTAIN_FILTER = 2
    EXCLUDE_EMPTY = False
    INCLUDE_EMPTY = True
    EMPTY = 'Empty'

    # KPI Names
    FACINGS_MANUFACTURER = 'FACINGS_SOS_BY_MANUFACTURER'
    FACINGS_LOCATION = 'FACINGS_SOS_BY_LOCATION'
    FACINGS_CATEGORY = 'FACINGS_SOS_BY_CATEGORY'
    FACINGS_SUB_CATEGORY = 'FACINGS_SOS_BY_SUB_CATEGORY'
    FACINGS_BRAND = 'FACINGS_SOS_BY_BRAND'
    LINEAR_MANUFACTURER = 'LINEAR_SOS_BY_MANUFACTURER'
    LINEAR_LOCATION = 'LINEAR_SOS_BY_LOCATION'
    LINEAR_CATEGORY = 'LINEAR_SOS_BY_CATEGORY'
    LINEAR_SUB_CATEGORY = 'LINEAR_SOS_BY_SUB_CATEGORY'
    LINEAR_BRAND = 'LINEAR_SOS_BY_BRAND'

    # Saving to DB
    RESULT = 'result'
    SCORE = 'score'
