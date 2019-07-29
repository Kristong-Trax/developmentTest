# coding=utf-8

class Consts(object):
    PRODUCT_TYPE = 'product_type'
    TYPES_TO_IGNORE_IN_SOS = ['Irrelevant', 'Empty']
    FACINGS_FOR_SOS = 'facings_ign_stack'
    MANUFACTURER_FK = 'manufacturer_fk'
    CATEGORY_FK = 'category_fk'
    PRODUCT_FK = 'product_fk'
    IN_STORE = 'in_store'
    NUMERATOR_ID = 'numerator_id'
    DENOMINATOR_ID = 'denominator_id'
    PS_CALC_STAGE = 3
    OOS_TYPE = 'OOS'
    DISTRIBUTION_TYPE = 'DISTRIBUTED'

    NUMERATOR_RESULT = 'numerator_result'
    DENOMINATOR_RESULT = 'denominator_result'
    ENTITIES_FOR_DB = [MANUFACTURER_FK, CATEGORY_FK, NUMERATOR_RESULT, DENOMINATOR_RESULT]
    OBLIGATORY_ASSORTMENT = u'חובה'
    OPTIONAL_SKU_ASSORTMENT = u'אופציונאלי'
    PRODUCT_POLICY_ATTR = 'att3'
    MILKY_POLICY = u'חלבי'
    TIRAT_TSVI_POLICY = u'טירת צבי'
    TEMPLATE_NAME = 'template_name'
    AGGREGATION_COLUMNS_RENAMING = {'sum': NUMERATOR_RESULT, 'count': DENOMINATOR_RESULT}

    # KPIs names
    SOS_MANU_OUT_OF_STORE_KPI_TIRAT_TSVI = 'TIRAT_TSVI_MANUFACTURER_OUT_OF_STORE_SOS'
    SOS_OWN_MANU_OUT_OF_CAT_KPI_TSVI = 'TIRAT_TSVI_OWN_MANUFACTURER_OUT_OF_CATEGORY_SOS'
    SOS_ALL_MANU_OUT_OF_CAT_KPI_TSVI = 'TIRAT_TSVI_ALL_MANUFACTURERS_OUT_OF_ALL_CATEGORIES_SOS'
    SOS_MANU_OUT_OF_STORE_KPI_MILKY = 'MILKY_MANUFACTURER_OUT_OF_STORE_SOS'
    SOS_OWN_MANU_OUT_OF_CAT_KPI_MILKY = 'MILKY_OWN_MANUFACTURER_OUT_OF_CATEGORY_SOS'
    SOS_ALL_MANU_OUT_OF_CAT_KPI_MILKY = 'MILKY_ALL_MANUFACTURERS_OUT_OF_ALL_CATEGORIES_SOS'
    DIST_STORE_LEVEL_MILKY = 'DISTRIBUTION_STORE_LEVEL_MILKY'
    DIST_STORE_LEVEL_TIRAT_TSVI = 'DISTRIBUTION_STORE_LEVEL_TIRAT_TSVI'
    DIST_CATEGORY_LEVEL_MILKY = 'DISTRIBUTION_CATEGORY_LEVEL_MILKY'
    DIST_CATEGORY_LEVEL_TIRAT_TSVI = 'DISTRIBUTION_CATEGORY_LEVEL_TIRAT_TSVI'
    DIST_SKU_LEVEL_MILKY = 'DISTRIBUTION_SKU_LEVEL_MILKY'
    DIST_SKU_LEVEL_TIRAT_TSVI = 'DISTRIBUTION_SKU_LEVEL_TIRAT_TSVI'
    OOS_STORE_LEVEL_MILKY = 'OOS_STORE_LEVEL_MILKY'
    OOS_STORE_LEVEL_TIRAT_TSVI = 'OOS_STORE_LEVEL_TIRAT_TSVI'
    OOS_CATEGORY_LEVEL_MILKY = 'OOS_CATEGORY_LEVEL_MILKY'
    OOS_CATEGORY_LEVEL_TIRAT_TSVI = 'OOS_CATEGORY_LEVEL_TIRAT_TSVI'
    OOS_SKU_LEVEL_MILKY = 'OOS_SKU_LEVEL_MILKY'
    OOS_SKU_LEVEL_TIRAT_TSVI = 'OOS_SKU_LEVEL_TIRAT_TSVI'

    # Logs
    EMPTY_ASSORTMENT_DATA = "There isn't relevant assortment data for this visit"
    LOG_EMPTY_ASSORTMENT_DATA_PER_POLICY = "There isn't relevant assortment data for the following policy: {}"
    LOG_EMPTY_PREVIOUS_SESSIONS = "Couldn't fetch previous results for the the following session: {}"
