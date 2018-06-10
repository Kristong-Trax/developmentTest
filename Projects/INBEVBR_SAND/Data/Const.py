
__author__ = 'ilays'


class Const(object):

    # sheet names
    KPIS = 'KPIS'
    SOS = 'SOS'
    COUNT = 'COUNT'
    GROUP_COUNT = 'GROUP_COUNT'
    SURVEY = 'SURVEY'
    PROD_SEQ = 'PROD_SEQ'
    PROD_SEQ_2 = 'PROD_SEQ_2'
    PROD_WEIGHT = 'PROD_WEIGHT'

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
    SUB_CATEGORY = 'Subcategory'
    REGION_TEMPLATE = 'Region'
    STATE_TEMPLATE = 'State'
    STORE_TYPE_TEMPLATE = 'Store Type'
    TARGET_OPERATOR = 'Target operator'
    NUMERIC = "Numeric"
    PERCENTAGE = '%'
    ATT1 = 'att1'
    CONTAINER_TYPE = 'Container Type'
    BEER_TYPE = 'Beer Type'
    SECONDARY_TARGET = 'Secondary Target'
    BRAND_GROUP_OUTSIDE = 'Brand group outside'
    BRAND_GROUP_INSIDE = 'Brand group inside'



    KPI_GROUP = 'Tested KPI Group'
    KPI_DISPLAY_NAME = 'KPI Display Name'
    GROUP_KPI_NAME = 'Group KPI Name'
    SURVEY_QUESTION_ID = 'Survey Question Id'
    TARGET_ANSWER = 'Target Answer'
    TARGET_TYPE = 'Target Type'
    TEMPLATE_NAME = 'Template name'
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
                                                ATT1, STORE_TYPE_TEMPLATE, WEIGHT, STATE_TEMPLATE, SECONDARY_TARGET,
                                                            GROUP_KPI_NAME,SCORE,BEER_TYPE, CONTAINER_TYPE]

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
