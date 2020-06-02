
class Consts(object):
    # KPIs
    NUMBER_OF_UNQIUE_SKUS_KPI = 'Number of Unique SKUs'
    NUMBER_OF_UNQIUE_SKUS_SKU_KPI = 'Number of Unique SKUs - SKU'
    NUMBER_OF_UNQIUE_BRANDS_KPI = 'Number of Unique Brands'
    NUMBER_OF_UNQIUE_BRANDS_BRAND_KPI = 'Number of Unique Brands - BRAND'
    LSOS_MANUFACTURER_OUT_OF_CATEGORY_KPI = 'LSOS Manufacturer out of Category'
    LSOS_OWN_BRAND_OUT_OF_CATEGORY_KPI = 'LSOS OWN Brand out of Category'
    OOS_MUST_HAVE_KPI = 'OOS Must Have'
    OOS_MUST_HAVE_SKU_KPI = 'OOS Must Have - SKU'
    OOS_NPA_KPI = 'OOS NPA'
    OOS_NPA_SKU_KPI = 'OOS NPA - SKU'
    OOS_OTHER_A_KPI = 'OOS Other A'
    OOS_OTHER_A_SKU_KPI = 'OOS Other A - SKU'
    NUMBER_OF_FACINGS_MUST_HAVE_KPI = 'Number of Facings Must Have Assortment'
    SKU_FACINGS_KPI = 'SKU Facings'
    SKU_LINEAR_KPI = 'SKU Linear'

    # Consts
    PRIMARY_SHELF = 'Primary Shelf'
    REPLACMENT_EAN_CODES = "Replacement Ean Codes"
    ADDITIONAL_DISPLAY = 'additional display'
    RELEVENT_FIELDS = ['scene_fk', 'product_fk', 'product_ean_code', 'template_name', 'product_type', 'manufacturer_fk',
                       'category_fk', 'sub_brand', 'sub_brand_fk', 'location_type']

    # external targets keys
    KEY_FIELDS = ['Store Type', 'region', 'category', 'brand', 'EAN Code']
    DATA_FIELDS = ['Field', 'Target']
