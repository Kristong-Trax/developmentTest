
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
    SCENE_SOS = 'scene_sos'

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
    EXCLUDE_CATEGORY = 'Exclude category'
    EXCLUDE_PRODUCT = 'Exclude Product'
    MANUFACTURER = 'Manufacturer'
    PRODUCT_TYPE = 'Product Type'
    PRODUCT_SIZE = 'Product Size'
    PRODUCT_SIZE_OPERATOR = 'Product Size Operator'
    PRODUCT = 'Product'
    EXPECTED_RESULT = "Expected Result"
    NUMERIC = "Numeric"
    STRING = "STRING"
    MEASUREMENT_UNIT = "Measurement Unit"
    WEIGHT = 'Weight'
    EXCLUDE_MANUFACTURER = 'Exclude manufacturer'
    MULTIPACK= 'Multipack'
    CONSIDER_FEW = 'Consider few brands'

    # delete fields
    DELETE_FIELDS = [ENGLISH_KPI_NAME, COUNT_TYPE, STORE_TYPE_TEMPLATE, PRODUCT, TARGET_OPERATOR, CONSIDER_FEW,
                     PRODUCT_SIZE_OPERATOR, PRODUCT_SIZE, TARGET, MEASUREMENT_UNIT, EXPECTED_RESULT, MULTIPACK]

    # include exclude filters
    EXCLUDE_FILTER = 0
    INCLUDE_FILTER = 1

    # pk
    AVAILABILITY_PK = 5
    PRICING_PK = 6
