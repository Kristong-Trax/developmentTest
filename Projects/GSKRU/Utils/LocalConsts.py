# coding=utf-8

from KPIUtils.GlobalProjects.GSK.Data.LocalConsts import Consts as Const


class Consts(object):

    # from KPIToolBox

    # FRACTIONAL_FACINGS_PARAMETERS = None
    FRACTIONAL_FACINGS_PARAMETERS = \
        [
            {'product_filter':
                 {'product_ean_code': [],
                  'brand_name': [],
                  'manufacturer_name': ['GSK'],
                  'sub_category': ['***SKIP***'],
                  'category': []},
             'ff_threshold': 0.5,  # ratio between width and height (min/max)
             'ff_factor_horizontal': 1,  # fractional factor to apply to the number of facings
             'ff_factor_vertical': 1,
             'ff_factor_backside': 0.25},
        ]

    FACINGS_SOS = 'Facings SOS'
    LINEAR_SOS = 'Linear SOS'
    AVAILABILITY = 'Availability'

    SOA = 'SOA'
    SOA_KPI = 'GSK_SOA'
    SOA_MANUFACTURER_INTERNAL_TARGET_KPI = 'GSK_SOA_in_Manufacturer_vs_Internal_Target'
    SOA_MANUFACTURER_EXTERNAL_TARGET_KPI = 'GSK_SOA_in_Manufacturer_vs_External_Target'
    SOA_SUBCAT_INTERNAL_TARGET_KPI = 'GSK_SOA_in_SubCategory_vs_Internal_Target'
    SOA_SUBCAT_EXTERNAL_TARGET_KPI = 'GSK_SOA_in_SubCategory_vs_External_Target'

    CRA = 'CRA'
    CRA_KPI = 'GSK_CRA'
    CRA_MANUFACTURER_KPI = 'GSK_CRA_in_Manufacturer_Stacking_Included'
    CRA_SUBCAT_KPI = 'GSK_CRA_in_SubCategory_Stacking_Included'
    CRA_SUBCAT_BY_PRODUCT_KPI = 'GSK_CRA_in_SubCategory_by_Product_Stacking_Included'

    KPI_DICT = {
        SOA: SOA,
        CRA: CRA
    }

    SET_UP_DATA = {
        (FACINGS_SOS, Const.KPI_TYPE_COLUMN): Const.NO_INFO,
        (LINEAR_SOS, Const.KPI_TYPE_COLUMN): Const.NO_INFO,
        (AVAILABILITY, Const.KPI_TYPE_COLUMN): Const.NO_INFO,
        (SOA, Const.KPI_TYPE_COLUMN): Const.NO_INFO,
        (CRA, Const.KPI_TYPE_COLUMN): Const.NO_INFO
    }
