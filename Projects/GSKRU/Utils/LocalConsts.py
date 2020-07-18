# coding=utf-8

from KPIUtils.GlobalProjects.GSK.Utils.KPIToolBox import Const


class Consts(object):

    # from KPIToolBox

    FRACTIONAL_FACINGS_PARAMETERS = \
        [
            {'product_filter':
                 {'product_ean_code': [],
                  'brand_name': [],
                  'manufacturer_name': ['GSK'],
                  'sub_category': ['Toothbrush'],
                  'category': []},
             'ff_threshold': 0.5,  # ratio between width and height (min/max)
             'ff_factor': 0.25},  # fractional factor to apply to the number of facings
        ]

    SOA = 'SOA'
    SOA_MANUFACTURER_INTERNAL_TARGET_KPI = 'GSK_SOA_in_Manufacturer_vs_Internal_Target'
    SOA_MANUFACTURER_EXTERNAL_TARGET_KPI = 'GSK_SOA_in_Manufacturer_vs_External_Target'
    SOA_SUBCAT_INTERNAL_TARGET_KPI = 'GSK_SOA_in_SubCategory_vs_Internal_Target'
    SOA_SUBCAT_EXTERNAL_TARGET_KPI = 'GSK_SOA_in_SubCategory_vs_External_Target'

    KPI_DICT = {
        SOA: SOA
    }

    SET_UP_DATA = {
        (SOA, Const.KPI_TYPE_COLUMN): Const.NO_INFO
    }


    # jp
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

    # external targets keys
    KEY_FIELDS = ['brand_fk', 'store_number', 'store_name', 'address_city', 'additional_attribute_1',
                  'additional_attribute_2', 'region_fk']
    DATA_FIELDS = ['brand_target', 'position_target', 'shelves', 'block_target']
