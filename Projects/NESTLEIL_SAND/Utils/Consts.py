

class Consts(object):

    NUMERATOR_ID = 'numerator_id'
    DENOMINATOR_ID = 'denominator_id'
    NUMERATOR_RESULT = 'numerator_result'
    DENOMINATOR_RESULT = 'denominator_result'
    IN_STORE = 'in_store'
    PRODUCT_FK = 'product_fk'
    ENTITIES_FOR_DB = [NUMERATOR_ID, DENOMINATOR_ID, NUMERATOR_RESULT, DENOMINATOR_RESULT]
    SOS_SKU_LVL_RENAME = {PRODUCT_FK: NUMERATOR_ID, IN_STORE: NUMERATOR_RESULT}

    # KPI names
    DISTRIBUTION_STORE_LEVEL = 'Distribution'
    DISTRIBUTION_SKU_LEVEL = 'Distribution - SKU'
    OOS_STORE_LEVEL = 'OOS'
    OOS_SKU_LEVEL = 'OOS - SKU'

    # KPI result values
    OOS_VALUE = 'OOS'
    DISTRIBUTED_VALUE = 'DISTRIBUTED'

    # Logs
    EMPTY_ASSORTMENT_DATA = "There isn't relevant assortment data for this visit"
