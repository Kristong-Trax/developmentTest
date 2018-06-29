
__author__ = 'Elyashiv'


class Const(object):

    # sheets:
    KPIS_SHEET, SHELF_FACING_SHEET, PRICING_SHEET = "KPIs", "Shelf Facings", "Pricing"
    SHELF_PLACMENTS_SHEET, MINIMUM_SHELF_SHEET = "Shelf Placement", "Minimum Shelf"
    DISPLAY_TARGET_SHEET, SHELF_GROUPS_SHEET = "Display_Target", "convert shelves groups"
    OFF_SHEETS = [
        KPIS_SHEET, SHELF_FACING_SHEET, PRICING_SHEET, SHELF_PLACMENTS_SHEET,
        DISPLAY_TARGET_SHEET, MINIMUM_SHELF_SHEET, SHELF_GROUPS_SHEET,
    ]
    ON_SHEETS = [KPIS_SHEET]
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

    # sets in off-premise:
    POD = "POD"
    DISPLAY_BRAND = "Display Brand"
    DISPLAY_SHARE = "Display Share"
    SHELF_FACINGS = "Shelf Facings"
    SHELF_PLACEMENT = "Shelf Placement"
    MSRP = "MSRP"
    STORE_SCORE = "Store Score"

    # sets in on-premise:
    BACK_BAR = "Back Bar"
    MENU = "Menu"

    SEGMENT, NATIONAL, TOTAL = "S", "N", "total"
    BRAND, SUB_BRAND, SKU = "brand", "sub_brand", "sku"
    COMPETITION, MANUFACTURER = "competition", "manufacturer"
    DISPLAY, NATIONAL_SEGMENT = "display", "national_segment_ind"
    # names in DB:
    DB_OFF, DB_ON = "off_premise", "on_premise"
    DB_OFF_TOTAL = 'Total Score - Off Premise'
    DB_OFF_NATIONAL = 'National Score - Off Premise'
    DB_OFF_SEGMENT = 'Segment Score - Off Premise'
    DB_OFF_NAMES = {
        POD: {
            TOTAL: 'POD - Total Score', NATIONAL: 'POD - National Score', SEGMENT: 'POD - Segment Score',
            BRAND: 'POD - Brand Level', SUB_BRAND: 'POD - Brand Variant Level', SKU: 'POD - Brand Variant Size Level'},
        DISPLAY_BRAND: {
            TOTAL: 'Display Brand - Total Score', NATIONAL: 'Brand Display - National Score',
            SEGMENT: 'Brand Display - Segment Score', BRAND: 'Diageo Display Compliance Brand %',
            SUB_BRAND: 'Diageo Display Compliance Brand Variant %', SKU: 'Display Brand - Brand Variant Size'},
        SHELF_FACINGS: {
            TOTAL: 'Shelf Facings - Total Score',
            NATIONAL: 'Shelf Facings - National Score', SEGMENT: 'Shelf Facings - Segment Score',
            BRAND: 'Shelf Facings - Compliance Brand',
            SUB_BRAND: 'Shelf Facings - Brand Variant',
            COMPETITION: 'Shelf Facings - Brand Variant Size', SKU: 'Shelf Facings - BVS + Brand Benchmark'},
        SHELF_PLACEMENT: {
            TOTAL: 'Shelf Placement - Total Score', BRAND: 'Shelf Placement - Brand',
            NATIONAL: 'Shelf Placement - National Score', SEGMENT: 'Shelf Placement - Segment Score',
            SUB_BRAND: 'Shelf Placement - Brand Variant', SKU: 'Shelf Placement - Brand Variant Size'},
        MSRP: {TOTAL: 'MSRP Total Score', BRAND: 'MSRP - Brand', SUB_BRAND: 'MSRP - Brand Variant',
               COMPETITION: 'MSRP - Brand Variant Size', SKU: 'MSRP - BVS + Brand Benchmark'},
        DISPLAY_SHARE: {TOTAL: 'Display Share - Total Score', MANUFACTURER: 'Display Share - Manufacturer',
                        SKU: 'Display Share - Brand Variant Size'}}
    DB_ON_TOTAL = 'Total Score - On Premise'
    DB_ON_NATIONAL = 'National Score - On Premise'
    DB_ON_SEGMENT = 'Segment Score - On Premise'
    DB_ON_NAMES = {
        POD: {
            TOTAL: 'On_POD - Total Score', SEGMENT: 'On_POD - Segment Score', NATIONAL: 'On_POD - National Score',
            BRAND: 'On_POD - Generic Brand', SUB_BRAND: 'On_POD -  Brand Variant',
            SKU: 'On_POD -  Brand Variant Size'},
        BACK_BAR: {
            TOTAL: 'Back Bar - Total Score', NATIONAL: 'Back Bar - National Score', SEGMENT: 'Back Bar - Segment Score',
            BRAND: 'Back Bar - Generic Brand', SUB_BRAND: 'Back Bar - Brand Variant',
            SKU: 'Back Bar - Brand Variant Size'},
        MENU: {
            TOTAL: 'Menu Share - Total Score', MANUFACTURER: 'Menu Share - Manufacturer Level',
            # BRAND: 'Menu Share - Brand Variant Level',
            SUB_BRAND: 'Menu Share - Brand Variant Level'}}
    DB_ASSORTMENTS_NAMES = {DB_OFF: "Assortment off Trade", DB_ON: "Assortment on Trade"}
    PERCENT_FOR_EYE_LEVEL = 0

    PRODUCT_FK, STANDARD_TYPE, PASSED, FACINGS = "product_fk", "standard_type", "passed", "facings"
    COLUMNS_FOR_DISPLAY = [MANUFACTURER, PRODUCT_FK, PASSED]
    COLUMNS_FOR_PRODUCT = [PRODUCT_FK, PASSED, BRAND, SUB_BRAND]
    COLUMNS_FOR_PRODUCT_ASSORTMENT = [PRODUCT_FK, STANDARD_TYPE, PASSED, BRAND, SUB_BRAND]
    COLUMNS_FOR_PRODUCT_PLACEMENT = [PASSED, SHELF_NAME, FACINGS]
    COLUMNS_FOR_MENU = [FACINGS, BRAND, MANUFACTURER]

    EXTRA = "EXTRA"
    OOS = "0"
    DISTRIBUTED = "1"
    OTHER = "OTHER"

    NO_DISPLAY_ALLOWED_QUESTION = "Confirm that there are no displays allowed in this outlet"
    NO_MENU_ALLOWED_QUESTION = "Confirm that there are no menus allowed in this outlet"
    SURVEY_ANSWER = "Yes"
