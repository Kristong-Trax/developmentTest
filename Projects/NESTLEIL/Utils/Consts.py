# -*- coding: utf-8 -*-


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

    DISTR_SNACKS = 'Distribution Snacks'
    DISTR_SNACKS_SKU = 'Distribution Snacks - SKU'
    OOS_SNACKS = 'OOS Snacks'
    OOS_SNACKS_SKU = 'OOS Snacks - SKU'

    DISTR_SABRA = 'Distribution Sabra'
    DISTR_SABRA_SKU = 'Distribution Sabra - SKU'
    OOS_SABRA = 'OOS Sabra'
    OOS_SABRA_SKU = 'OOS Sabra - SKU'

    # DIST_OOS_KPIS_MAP = {DISTR_SNACKS: OOS_SNACKS, DISTR_SABRA: OOS_SABRA, DISTR_SNACKS_SKU: OOS_SNACKS_SKU,
    #                      DISTR_SABRA_SKU: OOS_SABRA_SKU, DISTRIBUTION_STORE_LEVEL: OOS_STORE_LEVEL,
    #                      DISTRIBUTION_SKU_LEVEL: OOS_SKU_LEVEL}
    DIST_OOS_KPIS_MAP = {DISTR_SNACKS: OOS_SNACKS, DISTR_SABRA: OOS_SABRA, DISTR_SNACKS_SKU: OOS_SNACKS_SKU,
                         DISTR_SABRA_SKU: OOS_SABRA_SKU}

    COLLECTION_COMPLETENESS_SNACKS = 'Collection_Completeness_Snacks'
    COLLECTION_COMPLETENESS_SABRA = 'Collection_Completeness_Sabra'

    CRISPS_N_SNACKS = 'Crisps & Snacks'
    SALADS = 'Salads'
    PRODUCT_GROUP_FK = 'product_group_fk'
    COMPLETENESS_CHECK = 'completeness_check'
    TRAX_CATEGORY_FK = 'trax_category_fk'
    IS_INCLUDED = 'is_included'

    # KPI result values
    OOS_VALUE = 'OOS'
    DISTRIBUTED_VALUE = 'DISTRIBUTED'

    # Logs
    EMPTY_ASSORTMENT_DATA = "There isn't relevant assortment data for this visit"

    #assortment
    KPI_FK_LVL2 = 'kpi_fk_lvl2'
    KPI_FK_LVL3 = 'kpi_fk_lvl3'
    STORE_ASS_KPI_TYPE = 'store_lvl_kpi_type'
    SKU_ASS_KPI_TYPE = 'sku_lvl_kpi_type'
