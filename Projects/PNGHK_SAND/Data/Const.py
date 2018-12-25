
__author__ = 'ilays'


class Const(object):

    # sheet names
    KPIS = 'KPIS'
    OSD_RULES = 'OSD_RULES'

    # KPIS fields
    KPI_ID = 'KPI_ID'
    KPI_TYPE = 'KPI_Type'
    KPI_NAME = 'KPI_Name'
    PER_CATEGORY = 'Per_category'
    SCENE_TYPE = 'Scene_type'
    CATEGORY = 'Category'
    SCENE_SIZE = 'Scene_size'
    STACKING = 'Stacking'
    EXCLUDE_OTHER = 'Other'
    EXCLUDE_IRRELEVANT = 'Irrelevant'
    EXCLUDE_EMPTY = 'Empty'
    EXCLUDE_POSM = 'POSM'
    EXCLUDE_HANGER = 'Smart_attribute_hanger'
    EXCLUDE_OSD = 'OSD'
    EXCLUDE_STOCK = 'Stock'
    EXCLUDE_SKU = 'SKU'
    NUMERATOR_ENTITY = 'Numerator_entity'
    DENOMINATOR_ENTITY = 'Denominator_entity'
    NMERATOR = 'Numerator'
    RESULT = 'Result'

    # OSD_RULES fields
    RETAILER = 'Retailer'
    HAS_OSD = 'Has_OSD'
    HAS_HOTSPOT = 'Has_hotspot'
    NUMBER_OF_SHELVES = 'Number_of_shelves'
    POSM_EAN_CODE = 'POSM_EAN_CODE'

    # KPIs types
    FSOS = 'Facing Share of Shelf'
    LSOS = 'Linear Share of Shelf'
    DISPLAY_NUMBER = 'Display Number'

    # Entities types
    MANUFACTURER = 'Manufacturer'
    BRAND = 'Brand'
    SKU = 'SKU'

    # include exclude filters
    EXCLUDE = 'Excluded'
    INCLUDE = 'Included'
    YES = 'Y'
    NO = 'N'