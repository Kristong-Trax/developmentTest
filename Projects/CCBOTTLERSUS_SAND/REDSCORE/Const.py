
__author__ = 'Elyashiv'


class Const(object):

    # sheets:
    KPIS = "KPIs"
    AVAILABILITY = "Availability"
    SOS = "SOS"
    SURVEY = "Survey"
    SKU_EXCLUSION = "SKU_Exclusion"
    SHEETS = [
        KPIS, AVAILABILITY, SOS, SURVEY, SKU_EXCLUSION
    ]

    # generic columns:
    KPI_NAME = "KPI_name"
    SCENE_TYPE = "scene_type"
    SCENE_TYPE_GROUP = "scene_type_group"
    PACKAGE_TYPE = "package_type"
    SSD_STILL = "SSD/still"
    STORE_ATTRIBUTE = "store_attribute"
    TRADEMARK = "trademark"
    TARGET = "target"

    # columns of KPIS
    REGION = "Region"
    STORE_TYPE = "store_type"
    SCENE_LEVEL = "scene_level"
    TESTED_GROUP = "tested_group"
    GROUP_TARGET = "group_target"
    CONDITION = "condition"
    INCREMENTAL = "incremental"
    SHEET = "sheet"
    EXCLUSION_SHEET = "exclusion_sheet"
    WEIGHT = "weight"

    # columns of AVAILABILITY:
    SCENE_SKU = "scene/SKU"
    MANUFACTURER = "manufacturer"
    BRAND = "brand"
    PRODUCT_NAME = "product_name"
    PRODUCT_EAN = "product_ean_code"
    BRAND_TARGET = "brand_target"

    # columns of SOS:
    DEN_TYPES = "denominator_types"
    DEN_VALS = "denominator_values"
    NUM_TYPES = "numerator_types"
    NUM_VALS = "numerator_values"
    TRADEMARK_CONDITION = "trademark_conditional"

    # columns of survey:
    Q_TEXT = "question_text"
    Q_ID = "question_ID"
    ACCEPTED_ANSWER = "accepted_answer"
    REQUIRED_ANSWER = "required_answer"

    # columns of exclusion:
    ENTITY_TYPES = "entity_types"
    ENTITY_VALUES = "entity_values"
