
__author__ = 'Idanr'


class Const(object):
    # Categories
    SNACKS = 'SNACK'
    BEVERAGES = 'BEVERAGE'
    JUICES = 'JUICE'
    LRB = 'LRB'

    # Filters
    ADDITIONAL_ATTRIBUTE_2 = 'additional_attribute_2'
    TEMPLATE_GROUP = 'template_group'
    TEMPLATE_NAME = 'template_name'
    SCENE_FK = 'scene_fk'
    MANUFACTURER_NAME = 'manufacturer_name'
    MANUFACTURER = 'manufacturer'
    CATEGORY = 'category'
    CATEGORY_FK = 'category_fk'
    SUB_CATEGORY = 'sub_category'
    SUB_CATEGORY_FK = 'sub_category_fk'
    BRAND = 'brand'
    TEMPLATE = 'template'
    BRAND_NAME = 'brand_name'
    MAIN_SHELF = 'MAIN SHELF'
    MAIN_SHELF_SNACKS = 'Main Shelf Snacks'
    MAIN_SHELF_BEVERAGES = 'Main Shelf Beverages'
    MAIN_SHELF_JUICES = 'Main Shelf Juices'
    PEPSICO = 'PepsiCo'
    FK = '_fk'
    NAME = '_name'
    PRODUCT_TYPE = 'product_type'
    IRRELEVANT = 'Irrelevant'


    # SOS plaster
    EXCLUDE_FILTER = 0
    INCLUDE_FILTER = 1
    CONTAIN_FILTER = 2
    EXCLUDE_EMPTY = False
    INCLUDE_EMPTY = True
    EMPTY = 'Empty'

    # KPI Names
    FACINGS_MANUFACTURER_SOS = 'PEPSICO_FACINGS_SOS_BY_MANUFACTURER'
    FACINGS_CATEGORY_SOS = 'PEPSICO_FACINGS_SOS_BY_CATEGORY'
    FACINGS_SUB_CATEGORY_SOS = 'PEPSICO_FACINGS_SOS_BY_SUB_CATEGORY'
    FACINGS_BRAND_SOS = 'PEPSICO_FACINGS_SOS_BY_BRAND'
    LINEAR_MANUFACTURER_SOS = 'PEPSICO_LINEAR_SOS_BY_MANUFACTURER'
    LINEAR_CATEGORY_SOS = 'PEPSICO_LINEAR_SOS_BY_CATEGORY'
    LINEAR_SUB_CATEGORY_SOS = 'PEPSICO_LINEAR_SOS_BY_SUB_CATEGORY'
    LINEAR_BRAND_SOS = 'PEPSICO_LINEAR_SOS_BY_BRAND'
    DISPLAY_COUNT_STORE_LEVEL = 'PEPSICO_COUNT_OF_DISPLAYS_STORE_LEVEL'
    DISPLAY_COUNT_CATEGORY_LEVEL = 'PEPSICO_COUNT_OF_DISPLAYS_CATEGORY_LEVEL'
    DISPLAY_COUNT_SCENE_LEVEL = 'PEPSICO_COUNT_OF_DISPLAYS_SCENE_TYPE_LEVEL'
    OSA_STORE_LEVEL = 'OSA_Store_level'
    OSA_CATEGORY_LEVEL = 'OSA_Category_level'
    OSA_SKU_LEVEL = 'OSA_Store_level - SKU'
    OOS_STORE_LEVEL = 'OOS_Store_level'
    OOS_CATEGORY_LEVEL = 'OOS_Category_level'
    OOS_SKU_LEVEL = 'OOS_Store_level - SKU'

    # Saving to DB
    RESULT = 'result'
    SCORE = 'score'
    DISTRIBUTION = '4'
    OOS = '5'

