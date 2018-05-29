
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
    FACING = 'facings'
    SCENE_SOS = 'scene_sos'

    # const
    KPI_TYPE = 'KPI Type'
    KPI_ID = 'KPI ID'
    ENGLISH_KPI_NAME = 'English KPI Name'
    TARGET = 'Target'
    TEMPLATE_GROUP = 'Template Group'
    COUNT_TYPE = 'Count Type'
    WEIGHT = 'Weight'
    SCORE = 'Score'
    BRAND = 'Brand'
    SUB_BRAND = 'Subbrand'
    CATEGORY = 'Category'
    SUB_CATEGORY = 'Subcategory'
    REGION_TEMPLATE = 'Region'
    STATE_TEMPLATE = 'State'
    STORE_TYPE_TEMPLATE = 'Store Type'
    TARGET_OPERATOR = 'Target operator'
    NUMERIC = "Numeric"
    PERCENTAGE = '%'
    CONTAINER_TYPE = 'Container type'


    KPI_GROUP = 'Tested KPI Group'
    KPI_DISPLAY_NAME = 'KPI Display Name'
    GROUP_KPI_NAME = 'Group KPI Name'
    SURVEY_QUESTION_ID = 'Survey Question Id'
    TARGET_ANSWER = 'Target Answer'
    TARGET_TYPE = 'Target Type'
    TEMPLATE_NAME = 'Template Name'
    EXCLUDE_CATEGORY = 'Exclude category'
    MANUFACTURER = 'Manufacturer'
    PRODUCT_TYPE = 'Product Type'
    PRODUCT_SIZE = 'Product Size'
    PRODUCT_SIZE_OPERATOR = 'Product Size Operator'
    PRODUCT = 'Product'
    EXPECTED_RESULT = "Expected Result"
    MEASUREMENT_UNIT = "Measurement Unit"
    EXCLUDE_MANUFACTURER = 'Exclude manufacturer'
    MULTIPACK= 'Multipack'
    CONSIDER_FEW = 'Consider few brands'

    # delete fields
    DELETE_FIELDS = [KPI_ID, REGION_TEMPLATE, ENGLISH_KPI_NAME, COUNT_TYPE, TARGET, EXPECTED_RESULT, TARGET_OPERATOR,
                                                            CONTAINER_TYPE, STORE_TYPE_TEMPLATE, WEIGHT]

    # DELETE_FIELDS = [ENGLISH_KPI_NAME, COUNT_TYPE, STORE_TYPE_TEMPLATE, PRODUCT, TARGET_OPERATOR, CONSIDER_FEW,
    #                  PRODUCT_SIZE_OPERATOR, PRODUCT_SIZE, TARGET, MEASUREMENT_UNIT, EXPECTED_RESULT, MULTIPACK]

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
