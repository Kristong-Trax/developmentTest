__author__ = 'Elyashiv'


class INBEVCISANDConst(object):
    #level 0
    SOS_VS_TARGET_AGGREGATE_MR = 'SOS_vs_Target_Aggregate_MR'

    # set_names:
    BRAND_FACING_TARGET = "Top Brand Facings"
    BRAND_COMPARISON = "Total Brand Comparison"
    TOP_BRAND_BLOCK = "Top Brand Block"
    EYE_LEVEL = "Eye Level"
    BRAND_VARIANT_GROUP = 'Brand Variant ASSORTMENT GROUP'
    ASSORTMENT = "Must have ASSORTMENT GROUP"
    SOS = "SOS_manufacturer_category"
    SOS_VS_TARGET = "SOS vs Target"
    MANUFACTURER_DISPLAY_COUNT = "Manufacturer Displays Count"
    SOS_PER_MANUFACTURER_AND_LOCATION_TYPE = "SOS per Manufacturer and Location Type"
    LINEAR_SOS_CATEGORY_LOCATION_TYPE = "Linear SOS SKU out of Category per Location Type"

    # level 2:
    BRAND_BLOCK = "Brand Block"
    ATOMIC_FACINGS = "Brand Facings"
    EYE_LEVEL_SKU = "Eye Level - SKU"
    MUST_HAVE_SKU = "Must have - SKU"

    BRAND_VARIANT = "Brand Variant"
    BRAND_VARIANT_SKU = "Brand Variant - SKU"

    COMPARISON_LEVEL_2 = "Brand Comparison"
    COMPARISON_LEVEL_3 = "Brand Comparison - BRAND"
    COMPARISON_LEVEL_4 = "Brand Comparison - SKU"

    OOS_SKU_KPI = 'OOS - SKU'
    OOS_KPI = 'OOS'
    SOS_MANUFACT_SUB_CAT_ALL = 'SOS_manufacturer_sub_category_all'
    SOS_VS_TARGET_SECONDARY_CORE = 'SOS vs Target Secondary Shelf Core Products'
    SOS_VS_TARGET_SECONDARY_HIGH_END = 'SOS vs Target Secondary Shelf High End Products'

    # columns:
    NUMERATOR = 'numerator'
    DENOMINATOR = 'denominator'
    ATTR5 = "Attribute_5"
    BRAND = "brand"
    TARGET = "target"
    ATOMIC_NAME = "Atomic Name"
    SCENE_FK = 'scene_fk'
    LOCATION_TYPE_FK = 'location_type_fk'
    MANUFACTURER_FK = 'manufacturer_fk'
    EXCLUDE_FILTER = 0
    EMPTY = 'Empty'
    IRRELEVANT = 'Irrelevant'
    PRODUCT_TYPE = 'product_type'
    CATEGORY_FK = 'category_fk'
    BEER_CATEGORY_FK = 1
    ABINBEV_MAN_FK = 7
    COOLER_FK = 3
    SECONDARY_DISPLAY_FK = 2
    PRIMARY_SHELF_FK = 1
    PRICE_GROUP = 'Price Group'

    ASSORTMENT_TYPE = "Assortment type"
    GRANULAR_GROUP_NAME = "Granular Group Name"

    INBEV_SKUS = "SKUs Inbev"
    COMPETITOR_SKUS = "SKUs Competitor"

    START_DATE = "Start date"
    END_DATE = "End date"

    GOLDEN_SHELVES = "golden_shelves"

    GROUP_NAME = "group_name"

    STORE_POLICY = "Store Policy"
    SHEET_NAMES = [TOP_BRAND_BLOCK, BRAND_COMPARISON, BRAND_FACING_TARGET, GOLDEN_SHELVES]
    SET_NAMES = [
        BRAND_COMPARISON,
        TOP_BRAND_BLOCK,
        BRAND_FACING_TARGET,
        SOS,
        ASSORTMENT,
        SOS_VS_TARGET,
        MANUFACTURER_DISPLAY_COUNT,
        LINEAR_SOS_CATEGORY_LOCATION_TYPE
    ]

    # eye levels:
    ALL_PALLETS = ['Full Pallet', 'Metal Bracket', 'Half Pallet']
    PALLET = ['Full Pallet', 'Metal Bracket']
    PALLET_FACTOR = 6
    HALF_PALLET_FACTOR = 4
    ABI_INBEV = 'ABI Inbev'
    CORE = 'Core'
    HIGH_END = 'HighEnd'
    BEER = 'BEER'
    PRIMARY_SHELF = 'Primary Shelf'
    SECONDARY_SHELF = 'Secondary Shelf'
    COOLER = 'Cooler'
    LOCATION_TYPE = 'location_type'

    STACK_PER_LOCATION = {PRIMARY_SHELF: 'gross_len_ign_stack', SECONDARY_SHELF: 'gross_len_add_stack',
                          COOLER: 'gross_len_ign_stack'}
    SOS_SKU_LOCATIONS = [PRIMARY_SHELF, SECONDARY_SHELF, COOLER]
    STACK_PER_LOCATION_TOTAL = {PRIMARY_SHELF: 'gross_len_ign_stack_total_location', SECONDARY_SHELF: 'gross_len_add_stack_total_location',
                                COOLER: 'gross_len_ign_stack_total_location'}