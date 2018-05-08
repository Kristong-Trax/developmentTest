
__author__ = 'ilays'


class Const(object):

    # sheet names
    KPIS = 'KPIS'
    COUNT = 'COUNT'
    SURVEY = 'SURVEY'
    SOS = 'SOS'
    GROUP_COUNT = 'GROUP_COUNT'

    # count types
    SCENE = 'scene'
    UNIQUE_SKU = 'unique sku'
    FACING = 'facing'

    # const
    KPI_GROUP = 'Tested KPI Group'
    ENGLISH_KPI_NAME = 'English KPI Name'
    KPI_DISPLAY_NAME = 'KPI Display Name'
    GROUP_KPI_NAME = 'Group KPI Name'
    KPI_TYPE = 'KPI Type'
    STORE_TYPE_TEMPLATE = 'Store Type'
    SURVEY_QUESTION_ID = 'Survey Question Id'
    TARGET_ANSWER = 'Target Answer'
    COUNT_TYPE = 'Count Type'
    TARGET = 'Target'
    TARGET_TYPE = 'Target Type'
    TARGET_OPERATOR = 'Target Operator'
    PERCENTAGE = 'percentage'
    TEMPLATE_GROUP = 'Template Group'
    TEMPLATE_NAME = 'Template Name'
    BRAND = 'Brand'
    CATEGORY = 'Category'
    MANUFACTURER = 'Manufacturer'
    PRODUCT_TYPE = 'Product Type'
    PRODUCT_SIZE = 'Product Size'
    PRODUCT_SIZE_OPERATOR = 'Product Size Operator'
    PRODUCT = 'Product'
    EXPECTED_RESULT = "Expected Result"
    NUMERIC = "Numeric"
    MEASUREMENT_UNIT = "Measurement Unit"
    WEIGHT = 'Weight'
    EXCLUDE_MANUFACTURER = 'Exclude manufacturer'
    MULTIPACK= 'Multipack'

    # delete fields
    DELETE_FIELDS = [ENGLISH_KPI_NAME, COUNT_TYPE, STORE_TYPE_TEMPLATE, PRODUCT, TARGET_OPERATOR,
                     PRODUCT_SIZE_OPERATOR, PRODUCT_SIZE, TARGET, MEASUREMENT_UNIT, EXPECTED_RESULT,  MULTIPACK]#, EXCLUDE_MANUFACTURER]

    # include exclude filters
    EXCLUDE_FILTER = 0
    INCLUDE_FILTER = 1

    # pk
    # COUNT_OF_SCENES_PK = 1
    # COUNT_OF_UNIQUE_SKUS_PK = 2
    # COUNT_OF_FACINGS_PK = 3
    # SURVEY_PK = 4
    AVAILABILITY_PK = 5
    PRICING_PK = 6
