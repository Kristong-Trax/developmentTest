
__author__ = 'nidhinb'


class Const(object):

    # template path
    TEMPLATE_PATH = '07_PNGHK_template_2019_30_04.xlsx'

    # sheet names
    KPIS = 'KPIS'
    OSD_RULES = 'OSD_RULES'

    # KPIS fields
    KPI_ID = 'KPI_ID'
    KPI_TYPE = 'KPI_Type'
    KPI_NAME = 'KPI_Name'
    PER_SCENE_TYPE = 'Per scene_type'
    SCENE_TYPE = 'Scene_type'
    CATEGORY = 'Category'
    SCENE_SIZE = 'Scene_size'
    NUMERATOR_ENTITY = 'Numerator_entity'
    DENOMINATOR_ENTITY = 'Denominator_entity'
    NUMERATOR = 'Numerator'
    RESULT = 'Result'
    STACKING = 'Stacking'

    # Excluded parameters
    EXCLUDE_OTHER = 'Other'
    EXCLUDE_IRRELEVANT = 'Irrelevant'
    EXCLUDE_EMPTY = 'Empty'
    EXCLUDE_POSM = 'POSM'
    EXCLUDE_HANGER = 'Smart_attribute_hanger'
    EXCLUDE_OSD = 'OSD'
    EXCLUDE_STOCK = 'Stock'
    EXCLUDE_SKU = 'SKU'
    EACH = 'EACH'

    # OSD_RULES fields
    RETAILER = 'Retailer'
    HAS_OSD = 'Has_OSD'
    HAS_HOTSPOT = 'Has_hotspot'
    OSD_NUMBER_OF_SHELVES = 'OSD_Number_of_shelves'
    POSM_EAN_CODE = 'POSM_EAN_CODE'
    POSM_EAN_CODE_HOTSPOT = 'HOTSPOT_EAN_CODE'
    STORAGE_EXCLUSION_PRICE_TAG = 'STORAGE_EXCLUSION_PRICE_TAG'

    # KPIs types
    FSOS = 'Facing Share of Shelf'
    LSOS = 'Linear Share of Shelf'
    DISPLAY_NUMBER = 'Display Number'

    # Entities types
    MANUFACTURER = 'manufacturer_name'
    BRAND = 'brand_name'
    SKU = 'product_ean_code'

    # include exclude filters
    EXCLUDE = 'Excluded'
    INCLUDE = 'Included'
    YES = 'Y'
    NO = 'N'

    NAME_TO_FK = {"manufacturer_name": "manufacturer_fk",
                  "brand_name": "brand_fk", "product_fk": "product_fk"}
    CATEGORIES = ['Hair Care', 'Baby Care', 'Feminine Care',
                  'Toothpaste', 'Skin Care', 'Grooming', 'Laundry']

    # database aliases
    DB_STOCK_NAME = 'stock'
    DB_HANGER_NAME = 'additional display'

    OSD_KPI = 'OSD'
    MATCH_PRODUCT_IN_PROBE_FK = 'match_product_in_probe_fk'
    MATCH_PRODUCT_IN_PROBE_STATE_REPORTING_FK = 'match_product_in_probe_state_reporting_fk'