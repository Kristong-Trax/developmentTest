
__author__ = 'ilays'


class Const(object):

    # sheet names
    KPIS = 'KPIS'
    SOS = 'SOS'
    SOS_PACKS = 'SOS_PACKS'
    COUNT = 'COUNT'
    GROUP_COUNT = 'GROUP_COUNT'
    SURVEY = 'SURVEY'
    PROD_SEQ = 'PROD_SEQ'
    PROD_SEQ_2 = 'PROD_SEQ_2'
    PROD_WEIGHT = 'PROD_WEIGHT'
    PROD_WEIGHT_SKU = 'PROD_WEIGHT_SKU'
    PROD_WEIGHT_SUBBRAND = 'PROD_WEIGHT_SUBBRAND'

    # count types
    FACING = 'facing'
    SCENES = 'scene'

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
    EXCLUDE_CATEGORY = 'Exclude category'
    SUB_CATEGORY = 'Subcategory'
    REGION_TEMPLATE = 'Region'
    STATE_TEMPLATE = 'State'
    STORE_TYPE_TEMPLATE = 'Store Type'
    NUMERIC = "Numeric"
    PERCENTAGE = '%'
    CONTAINER_TYPE = 'Container Type'
    ATT1 = 'att1'
    RESULT_TYPE = 'Result Type'
    BEER_TYPE = 'Beer Type'
    TARGET_OPERATOR = 'Target operator'
    SECONDARY_TARGET = 'Secondary Target'
    PACKS_TARGET = 'Packs Target'
    BRAND_GROUP_OUTSIDE = 'Brand group outside'
    BRAND_GROUP_INSIDE = 'Brand group inside'
    MANUFACTURER = 'Manufacturer'
    EXCLUDE_MANUFACTURER = 'Exclude manufacturer'
    MEASUREMENT_UNIT = "Measurement Unit"
    PRODUCT_SIZE = 'Product Size'
    SURVEY_QUESTION_ID = 'Survey Question Id'
    LEFT_RIGHT_SUBCATEGORY = 'Left or Right Subcategory'
    FLAVOR = 'Flavor'
    LIMIT_SCORE = 'Limit Score'
    TARGET_ANSWER = 'Target Answer'
    TEMPLATE_NAME = 'Template name'
    EXPECTED_RESULT = "Expected Result"
    PARENT_KPI_GROUP = 'Parent kpi group'

    KPI_GROUP = 'Tested KPI Group'
    KPI_DISPLAY_NAME = 'KPI Display Name'
    GROUP_KPI_NAME = 'Group KPI Name'
    TARGET_TYPE = 'Target Type'
    PRODUCT_TYPE = 'Product Type'
    PRODUCT_SIZE_OPERATOR = 'Product Size Operator'
    PRODUCT = 'Product'
    MULTIPACK= 'Multipack'
    CONSIDER_FEW = 'Consider few brands'
    CERVEJA = 'CERVEJA'
    GAME_PLAN = 'GAME PLAN'
    NAB = 'NAB'

    # delete fields
    DELETE_FIELDS = [KPI_ID, REGION_TEMPLATE, ENGLISH_KPI_NAME, COUNT_TYPE, TARGET, EXPECTED_RESULT, TARGET_OPERATOR,
                     MEASUREMENT_UNIT,PRODUCT,PRODUCT_SIZE, STORE_TYPE_TEMPLATE, WEIGHT, STATE_TEMPLATE,
                     SECONDARY_TARGET, GROUP_KPI_NAME,SCORE, PACKS_TARGET, RESULT_TYPE]

    # include exclude filters
    EXCLUDE_FILTER = 0
    INCLUDE_FILTER = 1

    # pk
    # AVAILABILITY_PK = 5
    # PRICING_PK = 6
