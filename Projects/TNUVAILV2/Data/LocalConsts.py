# coding=utf-8

from KPIUtils_v2.Utils.Consts.DataProvider import ScifConsts
from KPIUtils_v2.Utils.Consts.DB import SessionResultsConsts
from KPIUtils_v2.Utils.Consts.GlobalConsts import ProductTypeConsts


class Consts(object):

    # Scene item facts attributes
    PRODUCT_POLICY_ATTR = 'att3'
    MILKY_POLICY = u'חלבי'
    TIRAT_TSVI_POLICY = u'טירת צבי'

    # SOS
    TYPES_TO_IGNORE_IN_SOS = [ProductTypeConsts.IRRELEVANT, ProductTypeConsts.EMPTY]

    # DB attributes
    ENTITIES_FOR_DB = [ScifConsts.MANUFACTURER_FK, ScifConsts.CATEGORY_FK,
                       SessionResultsConsts.NUMERATOR_RESULT, SessionResultsConsts.DENOMINATOR_RESULT]

    # Assortment Consts
    IN_STORE = 'in_store'
    PS_CALC_STAGE = 3
    OOS_TYPE = 'OOS'
    DISTRIBUTION_TYPE = 'DISTRIBUTED'
    OOS = 0
    AVAILABLE = 1
    OBLIGATORY_ASSORTMENT = u'חובה'
    OPTIONAL_SKU_ASSORTMENT = u'אופציונאלי'
    AGGREGATION_COLUMNS_RENAMING = {'sum': SessionResultsConsts.NUMERATOR_RESULT, 'count':
        SessionResultsConsts.DENOMINATOR_RESULT}
    SOS_SKU_LVL_RENAME = {IN_STORE: SessionResultsConsts.NUMERATOR_RESULT, ScifConsts.CATEGORY_FK:
        SessionResultsConsts.DENOMINATOR_ID,
                          ScifConsts.FACINGS: SessionResultsConsts.DENOMINATOR_RESULT}

    # KPIs names
    OOS_STORE_LEVEL = 'OOS_STORE_LEVEL'
    OOS_SKU_IN_STORE_LEVEL = 'OOS_SKU_IN_STORE_LEVEL'
    SOS_MANU_OUT_OF_STORE_KPI_TIRAT_TSVI = 'TIRAT_TSVI_MANUFACTURER_OUT_OF_STORE_SOS'
    SOS_OWN_MANU_OUT_OF_CAT_KPI_TSVI = 'TIRAT_TSVI_OWN_MANUFACTURER_OUT_OF_CATEGORY_SOS'
    SOS_ALL_MANU_OUT_OF_CAT_KPI_TSVI = 'TIRAT_TSVI_ALL_MANUFACTURERS_OUT_OF_ALL_CATEGORIES_SOS'
    SOS_MANU_OUT_OF_STORE_KPI_DAIRY = 'DAIRY_MANUFACTURER_OUT_OF_STORE_SOS'
    SOS_OWN_MANU_OUT_OF_CAT_KPI_DAIRY = 'DAIRY_OWN_MANUFACTURER_OUT_OF_CATEGORY_SOS'
    SOS_ALL_MANU_OUT_OF_CAT_KPI_DAIRY = 'DAIRY_ALL_MANUFACTURERS_OUT_OF_ALL_CATEGORIES_SOS'
    DIST_STORE_LEVEL_DAIRY = 'DISTRIBUTION_STORE_LEVEL_DAIRY'
    DIST_STORE_LEVEL_TIRAT_TSVI = 'DISTRIBUTION_STORE_LEVEL_TIRAT_TSVI'
    DIST_CATEGORY_LEVEL_DAIRY = 'DISTRIBUTION_CATEGORY_LEVEL_DAIRY'
    DIST_CATEGORY_LEVEL_TIRAT_TSVI = 'DISTRIBUTION_CATEGORY_LEVEL_TIRAT_TSVI'
    DIST_SKU_LEVEL_DAIRY = 'DISTRIBUTION_SKU_LEVEL_DAIRY'
    DIST_SKU_LEVEL_TIRAT_TSVI = 'DISTRIBUTION_SKU_LEVEL_TIRAT_TSVI'
    OOS_STORE_LEVEL_DAIRY = 'OOS_STORE_LEVEL_DAIRY'
    OOS_STORE_LEVEL_TIRAT_TSVI = 'OOS_STORE_LEVEL_TIRAT_TSVI'
    OOS_CATEGORY_LEVEL_DAIRY = 'OOS_CATEGORY_LEVEL_DAIRY'
    OOS_CATEGORY_LEVEL_TIRAT_TSVI = 'OOS_CATEGORY_LEVEL_TIRAT_TSVI'
    OOS_SKU_LEVEL_DAIRY = 'OOS_SKU_LEVEL_DAIRY'
    OOS_SKU_LEVEL_TIRAT_TSVI = 'OOS_SKU_LEVEL_TIRAT_TSVI'

    # Logs
    EMPTY_ASSORTMENT_DATA = "There isn't relevant assortment data for this visit"
    LOG_EMPTY_ASSORTMENT_DATA_PER_POLICY = "There isn't relevant assortment data for the following policy: {}"
    LOG_EMPTY_PREVIOUS_SESSIONS = "Couldn't fetch previous results for the the following session: {}"
