
__author__ = 'Elyashiv'


class Const(object):
    LEVEL1 = 1  # TODO move consts to its own object
    LEVEL2 = 2
    LEVEL3 = 3

    ATTR3 = "additional_attribute_3"
    # sheets
    KPIS = "KPIs"
    LIST_OF_ENTITIES = "list_of_entities"
    SOS_WEIGHTS = "sos_weights"
    SOS_TARGETS = "sos_targets"
    PRICING_WEIGHTS = "Pricing_weights"
    PRICING_TARGETS = "Pricing_targets"
    SURVEY_QUESTIONS = "survey_questions"
    # main sheet fields
    KPI_NAME = "KPI Name"
    KPI_GROUP = "KPI Group"
    KPI_TYPE = "KPI Type"
    TARGET = "Target"
    WEIGHT_SHEET = "Weight Sheet"
    # second sheet fields
    ATOMIC_NAME = "Atomic Kpi NAME"
    ENTITY_TYPE = "Entity Type"
    ENTITY_VAL = "Entity Value"
    ENTITY_TYPE2 = "Entity 2 Type"
    ENTITY_VAL2 = "Entity 2 Value"

    ENTITY_TYPE_NUMERATOR = "Entity Type Numerator"
    NUMERATOR = "Numerator"
    ENTITY_TYPE_DENOMINATOR = "Entity Type Denominator"
    DENOMINATOR = "Denominator"
    SCORE = "score"
    IN_NOT_IN = "In/Not In"
    TYPE_FILTER = "Type Filter"
    VALUE_FILTER = "Filter Value"

    SURVEY_Q_CODE = "Survey Q CODE"
    SURVEY_Q_ID = "Survey Q ID"
    ACCEPTED_ANSWER_RESULT = "Accepted Answer/Result"
    # inner values
    NA = "N/A"
    SURVEY = "Survey"
    SCENE_COUNT = "Scene count"
    PLANOGRAM = "Planogram"
    AVAILABILITY = "Availability"
    SOS_FACINGS = "SOS Facings"
    SURVEY_QUESTION = "Survey Question"
    FLOW = "Flow"
    NUMERIC = "numeric"
    BINARY = "binary"
    RED_SCORE = "Red Score"
    # consts
    targets_line = "target"
    type = "type"

    sheet_names_and_rows = {KPIS: 1, LIST_OF_ENTITIES: 3, SOS_WEIGHTS: 3, SOS_TARGETS: 3,
                            PRICING_WEIGHTS: 3, PRICING_TARGETS: 3, SURVEY_QUESTIONS: 3}
    #for fix_type function
    templateNames_realFieldNames = {'SKU': 'product_ean_code', 'EAN': 'product_ean_code', 'Category': 'category',
                                    'Sub-Cateogry': 'sub_category', 'Sub_Category': 'sub_category',
                                    'Sub_category': 'sub_category', 'Brand': 'brand_name',
                                    'Template Name': 'template_name', 'Manufacturer': 'manufacturer_name',
                                    'Product_type': 'product_type', 'Location Types': 'location_type'}
    #levels keys and values columns in DB
    column_key1 = 'kpi_set_fk'
    column_name1 = 'kpi_set_name'
    column_key2 = 'kpi_fk'
    column_name2 = 'kpi_name'
    column_key3 = 'atomic_kpi_fk'
    column_name3 = 'atomic_kpi_name'

    IGNORE_STACKING = True
    SOS_OWN_MANUF_OUT_OF_STORE = 'SOS OWN MANUFACTURER OUT OF STORE'
    SOS_CAT_OUT_OF_STORE = 'SOS CATEGORY OUT OF STORE'
    SOS_MANUF_OUT_OF_CAT = 'SOS MANUFACTURER OUT OF CATEGORY'
    SOS_BRAND_OUT_CAT = 'SOS BRAND OUT OF CATEGORY'
