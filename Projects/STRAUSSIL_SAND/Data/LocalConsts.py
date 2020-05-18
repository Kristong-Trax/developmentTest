
class Consts(object):
    # OOS KPI
    OOS = ' OOS'
    OOS_SKU = ' OOS - SKU'

    # Distribution KPI
    DISTRIBUTION = ' Distribution'
    DISTRIBUTION_CAT = ' Distribution - Category'
    DISTRIBUTION_SKU = ' Distribution - SKU'

    # SCORE SOS KPIs
    LSOS_SCORE_KPI = 'LSOS_score'
    SOS_KPIS = 'SOS_KPIs'
    KPI_FK = 'kpi_fk'
    NUM_TYPE = 'numerator_type'
    NUM_VALUE = 'numerator_value'
    DEN_TYPE = 'denominator_type'
    DEN_VALUE = 'denominator_value'
    TARGET = 'target'
    TARGET_RANGE = 'target_range'
    RELEVANT_FIELDS = [KPI_FK, NUM_TYPE, NUM_VALUE, DEN_TYPE, DEN_VALUE, TARGET, TARGET_RANGE]

    # LINEAR and FACINGS SOS KPIs
    SOS_BY_BRAND = '_SOS_BY_BRAND'
    SOS_BY_CAT_BRAND = '_SOS_BY_CAT_BRAND'
    SOS_BY_CAT_BRAND_SKU = '_SOS_BY_CAT_BRAND_SKU'

    # external targets keys
    KEY_FIELDS = ['store_type', 'numerator_type', 'denominator_value', 'additional_attribute_6', 'retailer',
                  'additional_attribute_2', 'numerator_value', 'denominator_type', 'additional_attribute_8',
                  'additional_attribute_12']
    DATA_FIELDS = ['target_range', 'target']