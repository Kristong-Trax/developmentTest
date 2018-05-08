__author__ = 'Elyashiv'


class Const(object):
    # set_names:
    BRAND_FACING_TARGET = "Top Brand Facings"
    BRAND_COMPARISON = "Total Brand Comparison"
    TOP_BRAND_BLOCK = "Top Brand Block"
    EYE_LEVEL = "Eye Level"
    BRAND_VARIANT_GROUP = 'Brand Variant ASSORTMENT GROUP'
    ASSORTMENT = "Must have ASSORTMENT GROUP"
    SOS = "SOS_manufacturer_category"

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

    # columns:

    NUMERATOR = 'numerator'
    DENOMINATOR = 'denominator'

    ATTR5 = "Attribute_5"
    BRAND = "brand"
    TARGET = "target"
    ATOMIC_NAME = "Atomic Name"

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
    ]

    # eye levels:

    ALL_PALLETS = ['Full Pallet', 'Metal Bracket', 'Half Pallet']
    PALLET = ['Full Pallet', 'Metal Bracket']
    PALLET_FACTOR = 6
    HALF_PALLET_FACTOR = 4
