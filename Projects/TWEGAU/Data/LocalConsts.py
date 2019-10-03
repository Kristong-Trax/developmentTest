
class Consts(object):

    INCLUDE = 'include'
    EXCLUDE = 'exclude'
    FRONT_FACING = 'front_facing'
    LINEAR = 'width_mm_advance'
    OTHER = "Other"
    IRRELEVANT = "Irrelevant"
    POSM = "POS"
    EMPTY = "Empty"
    STORE = "Store"
    CATEGORY = "Category"
    SUB_CATEGORY = "SubCategory"

    MANUFACTURE_NAME = "TWEG"
    COUNTRY = "AU"

    # KPIs
    DISTRIBUTION = 'Distribution - SKU'
    DISTRIBUTION_PREFIX = MANUFACTURE_NAME + COUNTRY + '_Dst_Manufacturer_'
    DISTRIBUTION_SKU_PREFIX = MANUFACTURE_NAME + COUNTRY + '_Product_Presence_'
    OOS_PREFIX = MANUFACTURE_NAME + COUNTRY + '_OOS_Manufacturer_'
    OOS_SKU_PREFIX = MANUFACTURE_NAME + COUNTRY + '_OOS_Product_'
    AVAILABILITY_SUFFIX_STORE = 'in_Whole_Store'
    AVAILABILITY_SUFFIX_CATEGORY = 'in_Category'
    AVAILABILITY_SUFFIX_SUB_CAT = 'in_SubCategory'
    AVAILABILITY_SUFFIX_STORE_RE = 'In_Store_RE'
    AVAILABILITY_SUFFIX_CATEGORY_RE = 'In_Category_RE'
    AVAILABILITY_SUFFIX_SUB_CAT_RE = 'In_SubCategory_RE'
    SUB_CATEGORY_PARENT = "By_SubCategory_Category"
    SUFFIX_SUB_CAT = "_Category"

    STORE_KPI_SUFFIX = "In_Whole_Store"
    CATEGORY_KPI_SUFFIX = "By_Category"
    SUB_CATEGORY_KPI_SUFFIX = "By_SubCategory"
    OWN_MANUFACTURER = "_Own_Manufacturer_"
    ALL_MANUFACTURER = "_All_Manufacturer_"
    ALL_BRAND_KPI = "_All_Brand_"
    ALL_PRODUCT_KPI = "_All_Product_"
    SOS_FACINGS = MANUFACTURE_NAME + COUNTRY + "_FSOS"
    SOS_LINEAR = MANUFACTURE_NAME + COUNTRY + "_LSOS"
    OOS = 'OOS'
    DISTRIBUTED = 'DISTRIBUTED'
    EXTRA = 'EXTRA'

    #  flags - for use
    NO_INFO = 0
    INFO = 1

    # table columns filters
    PRODUCTS_COLUMNS = ['product_fk',
                        'category',
                        'category_fk',
                        'product_type',
                        'sub_category',
                        'sub_category_fk',
                        'manufacturer_fk',
                        'brand_name',
                        'brand_fk',
                        'product_ean_code',
                        'substitution_product_fk']

    SCIF_COLUMNS = ['product_fk', 'scene_id', 'template_name']

    # files consts
    SET_UP_COLUMNS_MULTIPLE_VALUES = ['Channel', 'Scene type / Tasks',
                                      'Include Brands', 'Exclude SKUs',
                                      'Include Category', 'Include SubCategory']

    SET_UP_COLUMNS_BOOLEAN_VALUES = ['Include Stacking', 'Include Others',
                                     'Include Empty', 'Include Irrelevant', 'Include POSM']
    KPI_TYPE_COLUMN = 'KPI Type'
    CHANNEL = 'Channel'
    SCENE_TYPE = 'Scene type / Tasks'
    SUB_CATEGORY_INCLUDE = 'Include SubCategory'
    CATEGORY_INCLUDE = 'Include Category'
    BRANDS_INCLUDE = 'Include Brands'
    SKUS_EXCLUDE = 'Exclude SKUs'
    INCLUDE_STACKING = 'Include Stacking'
    INCLUDE_OTHERS = 'Include Others'
    INCLUDE_EMPTY = 'Include Empty'
    INCLUDE_IRRELEVANT = 'Include Irrelevant'
    INCLUDE_POSM = 'Include POSM'

    # KPI_DICT : translate terms to kpis names in template
    KPI_DICT = {MANUFACTURE_NAME + COUNTRY + "_FSOS": "Facings SOS",
                "Availability": "Availability"}

    # AVAILABILITY_DICT = Dict kpis, according to DB for each kpi type there is an array.
    # dict from type of calculation to kpi TYPE in DB
    # value in position 0 its suffix of kpi of total score,
    # value in position 1 suffix of kpi of product level
    AVAILABILITY_DICT = {('OOS', CATEGORY): [CATEGORY_KPI_SUFFIX, AVAILABILITY_SUFFIX_CATEGORY_RE],
                         ('OOS', SUB_CATEGORY): [SUB_CATEGORY_KPI_SUFFIX, AVAILABILITY_SUFFIX_SUB_CAT_RE],
                         ('OOS', SUB_CATEGORY_PARENT): [SUB_CATEGORY_PARENT],
                         ('OOS', STORE): [AVAILABILITY_SUFFIX_STORE, AVAILABILITY_SUFFIX_STORE_RE],
                         ('DIST', STORE): [AVAILABILITY_SUFFIX_STORE, AVAILABILITY_SUFFIX_STORE],
                         ('DIST', CATEGORY): [CATEGORY_KPI_SUFFIX, AVAILABILITY_SUFFIX_CATEGORY],
                         ('DIST', SUB_CATEGORY): [SUB_CATEGORY_KPI_SUFFIX, AVAILABILITY_SUFFIX_SUB_CAT],
                         ('DIST', SUB_CATEGORY_PARENT): [SUB_CATEGORY_PARENT]

                         }

    def __init__(self):
        pass

