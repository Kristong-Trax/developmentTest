# coding=utf-8


class Consts(object):

    # from KPIToolBox

    KPI_RESULT = 'report.kpi_results'
    KPK_RESULT = 'report.kpk_results'
    KPS_RESULT = 'report.kps_results'
    PLN_BLOCK = 'GSK_PLN_BLOCK_SCORE'
    POSITION_SCORE = 'GSK_PLN_POSITION_SCORE'
    PRODUCT_PRESENCE = 'GSK_PLN_ECAPS_PRODUCT_PRESENCE'
    PLN_MSL = 'GSK_PLN_MSL_SCORE'
    PLN_LSOS = 'GSK_PLN_LSOS_SCORE'
    COMPLIANCE_ALL_BRANDS = 'GSK_PLN_COMPLIANCE_ALL_BRANDS'
    ECAP_SUMMARY = 'GSK_PLN_ECAPS_SUMMARY'
    COMPLIANCE_SUMMARY = 'GSK_PLN_COMPLIANCE_SUMMARY'
    ECAP_ALL_BRAND = 'GSK_PLN_ECAPS_ALL BRANDS'
    GLOBAL_LSOS_BRAND_BY_STORE = 'GSK_LSOS_All_Brand_In_Whole_Store'
    PLN_ASSORTMENT_KPI = 'PLN_ECAPS - SKU'
    ECAPS_FILTER_IDENT = 'GSK_ECAPS'

    POSM = "POSM"
    POSM_SKU = "POSM - SKU"
    GSK_POS_DISTRIBUTION_STORE = "GSK_POS_DISTRIBUTION_STORE"
    GSK_POS_DISTRIBUTION_BRAND = "GSK_POS_DISTRIBUTION_BRAND"
    GSK_POS_DISTRIBUTION_SKU = "GSK_POS_DISTRIBUTION_SKU"

    KPI_DICT = {
        "GSK_PLN_BLOCK_SCORE": "GSK_PLN_BLOCK_SCORE",
        "GSK_ECAPS": "GSK_ECAPS",
        "GSK_PLN_MSL_SCORE": "GSK_PLN_MSL_SCORE",
        "GSK_PLN_POSITION_SCORE": "GSK_PLN_POSITION_SCORE",
        "GSK_PLN_LSOS_SCORE": "GSK_PLN_LSOS_SCORE",
        "POSM": "POSM"
    }
    SHELVES = 'shelves'
    POSITION_TARGET = 'position_target'

    # external targets keys
    KEY_FIELDS = ['brand_fk', 'store_number', 'store_name', 'address_city', 'additional_attribute_1',
                  'additional_attribute_2', 'region_fk']
    DATA_FIELDS = ['brand_target', 'position_target', 'shelves', 'block_target']
