
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
    OOS_OTHER_A_KPI = 'OOS OTHER A'
    OOS_OTHER_A_SKU_KPI = 'OOS OTHER A - SKU'
    NUMBER_OF_FACINGS_MUST_HAVE_KPI = 'Number of Facings Must Have Assortment'
    SKU_FACINGS_KPI = 'SKU Facings'
    SKU_LINEAR_KPI = 'SKU Linear'

    # Template params
    TARGET = "Target"
    CATEGORY = 'category'
    REGION = 'region'
    STORE_TYPE = 'Store Type'
    BRAND = 'brand'
    EAN_CODE = 'EAN Code'
    FIELD = 'Field'

    # Consts
    PRIMARY_SHELF = 'Primary Shelf'
    REPLACMENT_EAN_CODES = "Replacement Ean Codes"
    ADDITIONAL_DISPLAY = 'additional display'
    RELEVENT_FIELDS = ['scene_fk', 'product_fk', 'product_ean_code', 'template_name', 'product_type', 'manufacturer_fk',
                       'category', 'category_fk', 'sub_brand', 'sub_brand_fk', 'location_type']

    # external targets keys
    KEY_FIELDS = [STORE_TYPE, REGION, CATEGORY, BRAND, EAN_CODE, FIELD]
    DATA_FIELDS = [TARGET]

    # external targets operation type
    SOS_KPIS = 'SOS_KPIs'
    AVA_KPIS = 'AVAILABILITY_KPIs'

    # KPIs threshold
    UNIQUE_SKUS_THRESHOLD = 5

    # kpi score values
    PASS = 4
    FAIL = 5
    NO_TARGET = 6