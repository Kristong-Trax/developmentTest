
__author__ = 'Elyashiv'


class Const(object):

    # sheets:
    KPIS_SHEET, SHELF_FACING_SHEET, PRICING_SHEET = "KPIs", "Shelf Facings", "Pricing"
    SHELF_PLACMENTS_SHEET, MINIMUM_SHELF_SHEET = "Shelf Placement", "Minimum Shelf"
    DISPLAY_TARGET_SHEET, SHELF_GROUPS_SHEET = "Display_Target", "convert shelves groups"
    SHEETS = [
        KPIS_SHEET, SHELF_FACING_SHEET, PRICING_SHEET, SHELF_PLACMENTS_SHEET,
        DISPLAY_TARGET_SHEET, MINIMUM_SHELF_SHEET, SHELF_GROUPS_SHEET,
    ]

    # KPIs columns:
    KPI_NAME, KPI_GROUP, SCORE, TARGET, WEIGHT = "KPI Name", "KPI Group", "Score", "Target", "Weight"
    TEMPLATE_GROUP = "Template Group/ Scene Type"

    # Shelf 1 columns:
    STATE, OUR_EAN_CODE, COMP_EAN_CODE = "State", "Product EAN Code", "Competitor EAN Code"
    BENCH_ENTITY, BENCH_VALUE = "BENCHMARK Entity", "BENCHMARK Value"

    # pricing columns:
    MIN_MSRP_RELATIVE, MAX_MSRP_RELATIVE = "SHELF MSRP RELATIVE GUIDENCE: MIN", "SHELF MSRP RELATIVE GUIDENCE: MAX"
    MIN_MSRP_ABSOLUTE = "SHELF MSRP ABSOLUTE GUIDENCE: MIN"
    MAX_MSRP_ABSOLUTE = "SHELF MSRP ABSOLUTE GUIDENCE: MAX"

    # shelf 2 columns:
    MIN_SHELF_LOCATION = "MINIMUM SHELF LOCATION"
    PRODUCT_EAN_CODE = "Product EAN Code"

    # display columns:
    SCENE_TYPE, MIN_FACINGS = "Scene Type", "Minimum # of Facings"

    # minimum shelf columns:
    SHELF_NAME, SHELVES_FROM_BOTTOM = "Shelf Name", "shelf number from bottom"
    NUM_SHLEVES_MIN, NUM_SHLEVES_MAX = "num. of shelves min", "num. of shelves max"

    # shelf groups
    NUMBER_GROUP, SHELF_GROUP = "number group", "shelf groups"

    # sets:
    POD = "POD"
    DISPLAY_BRAND = "Display Brand"
    DISPLAY_SHARE = "Display Share"
    SHELF_FACINGS = "Shelf Facings"
    SHELF_PLACEMENT = "Shelf Placement"
    MSRP = "MSRP"
    STORE_SCORE = "Store Score"

    SEGMENT, NATIONAL, TOTAL = "segment", "national", "total"
    BRAND, SUB_BRAND, SKU = "brand", "sub_brand", "sku"
    COMPETITION, MANUFACTURER = "competition", "manufacturer"

    DB_NAMES = {
        POD: {
            TOTAL: 'POD - Total Score', NATIONAL: 'POD - Natianal Score', SEGMENT: 'POD - Segment Score',
            BRAND: 'Diageo_POD_Brand_%', SUB_BRAND: 'Diageo_POD_Sub_brand_%', SKU: 'POD - Brand Variant Size Level'},
        DISPLAY_BRAND: {
            TOTAL: 'Display Brand - Total Score', NATIONAL: 'Brand Display - National Score',
            SEGMENT: 'Brand Display - Segment Score', BRAND: 'Diageo_Dispaly_Compliance_Brand_%',
            SUB_BRAND: 'Diageo_Dispaly_Compliance_Sub_Brand_%', SKU: 'Display Brand - Brand Variant Size'},
        SHELF_FACINGS: {
            TOTAL: 'Shelf Facings - Total Score',
            NATIONAL: 'Shelf Facings - National Score',
            SEGMENT: 'Shelf Facings - Segment Score', BRAND: 'Diageo_Shelf_facing_Compliance_Brand_%',
            SUB_BRAND: 'Diageo_Shelf_facing_VS_Competition_Sub_Brand%',
            COMPETITION: 'Shelf Facings - Brand Variant Size', SKU: 'Shelf Facings - BVS + Brand Benchmark'},
        SHELF_PLACEMENT: {
            TOTAL: 'Display Placement - Total Score', NATIONAL: 'Shelf Placement - National Score',
            SEGMENT: 'Shelf Placement - Segment Score', BRAND: 'Dispaly_Shelf_Placements_Brand_%',
            SUB_BRAND: 'Dispaly_Shelf_Placements_Sub_brand_%', SKU: 'Shelf Placement - Brand Variant Size'},
        MSRP: {TOTAL: 'Diageo_Msrp_score_total', BRAND: 'Diageo_Msrp_Brand', SUB_BRAND: 'Diageo_Msrp_Sub_Brand',
               COMPETITION: 'MSRP - Brand Variant Size', SKU: 'MSRP - BVS + Brand Benchmark'},
        DISPLAY_SHARE: {TOTAL: 'Display Share - Total Score', MANUFACTURER: 'Dispaly_Share_Manufacturer_%',
                        SKU: 'Display Share - Brand Variant Size'}
    }

    PERCENT_FOR_EYE_LEVEL = 0
    TARGET_FOR_DISPLAY_SHARE = 0.25

    # names in DB:
    DB_SCORE_TOTAL = 'Diageo_score_total_offpremis'
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

    PRODUCT_FK, STANDARD_TYPE, PASSED = "product_fk", "standard_type", "passed"
    COLUMNS_FOR_DISPLAY = [MANUFACTURER, PRODUCT_FK, PASSED]
    COLUMNS_FOR_PRODUCT = [PRODUCT_FK, STANDARD_TYPE, PASSED, BRAND, SUB_BRAND]

    EXTRA = "EXTRA"
    OOS = "OOS"
    DISTRIBUTED = "DISTRIBUTED"
