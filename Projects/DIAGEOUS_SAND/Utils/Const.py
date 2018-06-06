
__author__ = 'Elyashiv'


class Const(object):

    # sheets:
    KPIS_SHEET = "KPIs"
    SHELF_FACING_SHEET = "Shelf Facings"
    PRICING_SHEET = "Pricing"
    SHELF_PLACMENTS_SHEET = "Shelf Placement"
    DISPLAY_TARGET_SHEET = "Display_Target"
    MINIMUM_SHELF_SHEET = "Minimum Shelf"
    SHELF_GROUPS_SHEET = "convert shelves groups"
    SHEETS = [
        KPIS_SHEET,
        SHELF_FACING_SHEET,
        PRICING_SHEET,
        SHELF_PLACMENTS_SHEET,
        DISPLAY_TARGET_SHEET,
        MINIMUM_SHELF_SHEET,
        SHELF_GROUPS_SHEET,
    ]

    # KPIs columns:
    KPI_NAME = "KPI Name"
    KPI_GROUP = "KPI Group"
    TEMPLATE_GROUP = "Template Group/ Scene Type"
    TESTED_GROUP = "Tested Group"
    SCORE = "Score"
    TARGET = "Target"
    WEIGHT = "Weight"

    # Shelf 1 columns:
    STATE = "State"
    OUR_EAN_CODE = "Product EAN Code"
    COMP_EAN_CODE = "Competitor EAN Code"
    BENCH_ENTITY = "BENCHMARK Entity"
    BENCH_VALUE = "BENCHMARK Value"

    # pricing columns:
    MIN_MSRP_RELATIVE = "SHELF MSRP RELATIVE GUIDENCE: MIN"
    MAX_MSRP_RELATIVE = "SHELF MSRP RELATIVE GUIDENCE: MAX"
    MIN_MSRP_ABSOLUTE = "SHELF MSRP ABSOLUTE GUIDENCE: MIN"
    MAX_MSRP_ABSOLUTE = "SHELF MSRP ABSOLUTE GUIDENCE: MAX"

    # shelf 2 columns:
    MIN_SHELF_LOCATION = "MINIMUM SHELF LOCATION"
    PRODUCT_EAN_CODE = "Product EAN Code"

    # display columns:
    SCENE_TYPE = "Scene Type"
    MIN_FACINGS = "Minimum # of Facings"

    # minimum shelf columns:
    SHELF_NAME = "Shelf Name"
    NUM_SHLEVES_MIN = "num. of shelves min"
    NUM_SHLEVES_MAX = "num. of shelves max"
    SHELVES_FROM_BOTTOM = "shelf number from bottom"

    # shelf groups
    NUMBER_GROUP = "number group"
    SHELF_GROUP = "shelf groups"

    # sets:
    POD = "POD"
    DISPLAY_BRAND = "Display Brand"
    DISPLAY_SHARE = "Display Share"
    SHELF_FACINGS = "Shelf Facings"
    SHELF_PLACEMENT = "Shelf Placement"
    MSRP = "MSRP"
    STORE_SCORE = "Store Score"

    SEGMENT = "segment"
    NATIONAL = "national"
    TOTAL = "total"
    BRAND = "brand"
    SUB_BRAND = "sub_brand"
    SKU = "sku"
    COMPETITION = "competition"
    MANUFACTURER = "manufacturer"

    DB_NAMES = {
        POD: {
            TOTAL: 'POD', NATIONAL: 'POD_score_national_%', SEGMENT: 'POD_score_segment_%',
            BRAND: 'POD_Brand_%', SUB_BRAND: 'POD_Sub_brand_%', SKU: 'POD - SKU'},
        DISPLAY_BRAND: {
            TOTAL: 'DISPLAY', NATIONAL: 'Display_Compliance_score_national_%',
            SEGMENT: 'Display_Compliance_score_segment_%', BRAND: 'Display_Compliance_Brand_%',
            SUB_BRAND: 'Display_Compliance_Sub_Brand_%', SKU: 'DISPLAY - SKU'},
        SHELF_FACINGS: {
            TOTAL: 'Shelf_facing_VS_Competition_score_total_%',
            NATIONAL: 'Shelf_facing_VS_Competition_score_national_%',
            SEGMENT: 'Shelf_facing_VS_Competition_score_segment_%', BRAND: 'Shelf_facing_Compliance_Brand_%',
            SUB_BRAND: 'Shelf_facings_SKU_VS_Competition', COMPETITION: 'Shelf_facings_SKU_VS_Competition',
            SKU: 'Shelf_facings_SKU'},
        SHELF_PLACEMENT: {
            TOTAL: 'Display_Shelf_Placements_score_total_%', NATIONAL: 'Display_Shelf_Placements_score_national_%',
            SEGMENT: 'Display_Shelf_Placements_score_segment_%', BRAND: 'Display_Shelf_Placements_Brand_%',
            SUB_BRAND: 'Display_Shelf_Placements_Sub_brand_%', SKU: 'Display_Shelf_Placements_SKU_status'},
        MSRP: {TOTAL: 'Msrp_score_total', BRAND: 'Msrp_Brand', SUB_BRAND: 'Msrp_Sub_Brand', COMPETITION: 'Msrp_sku',
               SKU: 'SKU_Price'},
        DISPLAY_SHARE: {TOTAL: 'Display_Share_score_total_%', MANUFACTURER: 'Display_Share_Manufacturer_%',
                        SKU: 'Display_Share_SKU'}
    }

    PERCENT_FOR_EYE_LEVEL = 0
    TARGET_FOR_DISPLAY_SHARE = 0.25

    # columns of db:
    PRODUCT_EAN_CODE_DB = "product_ean_code"
    PRODUCT_FK_DB = "product_fk"
    BRAND_FK_DB = "brand_fk"
    SUB_BRAND_FK_DB = "sub_brand"

    # names in DB:
    DB_SCORE_TOTAL = 'score_total_offpremis'
    DB_SCORE_NATIONAL = 'score_national_offpremis'
    DB_SCORE_SEGMENT = 'score_segment_offpremis'

    DB_TOTAL_ONPREMIS = 'score_total_onpremis'
    DB_TOTAL_ONPREMIS_MENU = 'Menu_total_onpremis'
    DB_BACK_BAR_TOTAL = 'Back_Bar_total_onpremis'
    DB_MENU_SHARE_MANUFATURER = 'Menu_share_Manufacturer_%'
    DB_MENU_SHARE_BRAND = 'Menu_share_Brand_%'
    DB_BACK_BAR_BRAND = 'Back_Bar_Brand_%'
    DB_BACK_BAR_SUB_BRAND = 'Back_Bar_Sub_Brand_%'
    DB_BACK_BAR_SKU = 'Back_Bar_SKU_status'

    PRODUCT_FK = "product_fk"
    STANDARD_TYPE = "standard_type"
    PASSED = "passed"
    MANUFACTURER = "manufacturer"
    COLUMNS_FOR_DISPLAY = [MANUFACTURER, PRODUCT_FK, PASSED]
    COLUMNS_FOR_PRODUCT = [PRODUCT_FK, STANDARD_TYPE, PASSED, BRAND, SUB_BRAND]

    EXTRA = "EXTRA"
