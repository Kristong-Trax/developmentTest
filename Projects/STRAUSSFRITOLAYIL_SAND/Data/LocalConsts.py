
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
    OSA_STORE_KPI = 'OSA - STORE'
    OOS_NPA_KPI = 'OOS NPA'
    OOS_NPA_SKU_KPI = 'OOS NPA - SKU'
    OOS_OTHER_A_KPI = 'OOS OTHER A'
    OOS_OTHER_A_SKU_KPI = 'OOS OTHER A - SKU'
    NUMBER_OF_FACINGS_MUST_HAVE_KPI = 'Number of Facings Must Have Assortment'
    SKU_FACINGS_KPI = 'SKU Facings'
    SKU_LINEAR_KPI = 'SKU Linear'

    # Template params
    TARGET = "Target"
    TARGET_MIN = "Target_Min"
    TARGET_MAX = "Target_Max"
    CATEGORY = 'Category'
    ADDITIONAL_ATTRIBUTE_2 = 'additional_attribute_2'
    ADDITIONAL_ATTRIBUTE_3 = 'additional_attribute_3'
    ADDITIONAL_ATTRIBUTE_4 = 'additional_attribute_4'
    BRAND_MIX = 'Brand_Mix'
    EAN_CODE = 'EAN Code'
    FIELD = 'Field'

    # Consts
    PRIMARY_SHELF = 'Primary Shelf'
    REPLACMENT_EAN_CODES = "Replacement Ean Codes"
    ADDITIONAL_DISPLAY = 'additional display'
    RELEVENT_FIELDS = ['scene_fk', 'product_fk', 'product_ean_code', 'template_name', 'product_type', 'manufacturer_fk',
                       'category', 'category_fk', 'Sub_Brand_Local', 'sub_brand_fk', 'location_type', 'Brand_Mix',
                       'brand_mix_fk']

    # external targets keys
    KEY_FIELDS = [ADDITIONAL_ATTRIBUTE_2, ADDITIONAL_ATTRIBUTE_3, ADDITIONAL_ATTRIBUTE_4, CATEGORY, BRAND_MIX,
                  EAN_CODE, FIELD]
    DATA_FIELDS = [TARGET, TARGET_MIN, TARGET_MAX]

    # external targets operation type
    SOS_KPIS = 'SOS_KPIs'
    AVA_KPIS = 'AVAILABILITY_KPIs'

    # KPIs threshold
    UNIQUE_SKUS_THRESHOLD = 5

    # kpi score values
    OOS = 1
    DISTRIBUTED = 2
    PASS = 4
    FAIL = 5
    NO_TARGET = 6

    SUB_BRAND_NO_VALUE = 1
    BRAND_MIX_NO_VALUE = 2