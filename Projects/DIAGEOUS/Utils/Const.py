
__author__ = 'Elyashiv'


class Const(object):

    OFF, ON, INDEPENDENT, OPEN, NATIONAL_STORE = "off_premise", "on_premise", "Independent", "Open", "National"
    NOT_INDEPENDENT_STORES = [OPEN, NATIONAL_STORE]
    # sheets:
    ON_TRADE_MAIN, OFF_TRADE_MAIN = "main - on_trade", "main - off_trade"
    ON_TRADE_INDEPENDENT, OFF_TRADE_INDEPENDENT = "independent - on_trade", "independent - off_trade"
    MINIMUM_SHELF_SHEET = "Minimum Shelf"
    SHELF_GROUPS_SHEET = "convert shelves groups"
    SHEETS = {
        OPEN: {ON: [ON_TRADE_MAIN], OFF: [OFF_TRADE_MAIN, MINIMUM_SHELF_SHEET, SHELF_GROUPS_SHEET]},
        NATIONAL_STORE: {ON: [ON_TRADE_MAIN], OFF: [OFF_TRADE_MAIN, MINIMUM_SHELF_SHEET, SHELF_GROUPS_SHEET]},
        INDEPENDENT: {ON: [ON_TRADE_INDEPENDENT], OFF: [OFF_TRADE_INDEPENDENT]}}
    # KPIs columns:
    KPI_NAME, KPI_GROUP, TARGET, WEIGHT = "KPI Name", "KPI Group", "Target", "Weight"
    TEMPLATE_GROUP = "Template Group/ Scene Type"
    # minimum shelf columns:
    SHELF_NAME = "Shelf Name"
    # shelf groups columns
    NUMBER_GROUP, SHELF_GROUP = "number group", "shelf groups"
    # sets in off-premise:
    POD, DISPLAY_BRAND, DISPLAY_SHARE, SHELF_FACINGS = "POD", "Display Brand", "Display Share", "Shelf Facings"
    SHELF_PLACEMENT, MSRP, STORE_SCORE = "Shelf Placement", "MSRP", "Store Score"
    # sets in on-premise:
    BACK_BAR, MENU = "Back Bar", "Menu"
    BACK_BAR_NATIONAL, MENU_NATIONAL = "National Back Bar", "National Menu"

    SEGMENT, NATIONAL, TOTAL = "S", "N", "total"
    TEMPLATE = "template"
    BRAND, SUB_BRAND, SKU = "brand", "sub_brand", "sku"
    COMPETITION, MANUFACTURER = "competition", "manufacturer"
    DISPLAY, NATIONAL_SEGMENT = "display", "national_segment_ind"
    # names in DB:
    DB_TOTAL_KPIS = {
        ON: {TOTAL: 'Total Score - On Premise',
             SEGMENT: 'Segment Score - On Premise', NATIONAL: 'National Score - On Premise'},
        OFF: {TOTAL: 'Total Score - Off Premise',
              SEGMENT: 'Segment Score - Off Premise', NATIONAL: 'National Score - Off Premise'}
    }
    DB_OFF_NAMES = {
        POD: {
            TOTAL: 'POD - Total Score', NATIONAL: 'POD - National Score', SEGMENT: 'POD - Segment Score', KPI_NAME: POD,
            BRAND: 'POD - Brand Level', SUB_BRAND: 'POD - Brand Variant Level', SKU: 'POD - Brand Variant Size Level'},
        DISPLAY_BRAND: {
            TOTAL: 'Display Brand - Total Score', NATIONAL: 'Display Brand - National Score', KPI_NAME: DISPLAY_BRAND,
            SEGMENT: 'Display Brand - Segment Score', BRAND: 'Display Brand - Brand Level',
            SUB_BRAND: 'Display Brand - Brand Variant Level', SKU: 'Display Brand - Brand Variant Size'},
        SHELF_FACINGS: {
            TOTAL: 'Shelf Facings - Total Score', KPI_NAME: SHELF_FACINGS,
            NATIONAL: 'Shelf Facings - National Score', SEGMENT: 'Shelf Facings - Segment Score',
            BRAND: 'Shelf Facings - Compliance Brand', SUB_BRAND: 'Shelf Facings - Brand Variant',
            COMPETITION: 'Shelf Facings - Brand Variant Size', SKU: 'Shelf Facings - BVS + Brand Benchmark'},
        SHELF_PLACEMENT: {
            TOTAL: 'Shelf Placement - Total Score', BRAND: 'Shelf Placement - Brand', KPI_NAME: SHELF_PLACEMENT,
            NATIONAL: 'Shelf Placement - National Score', SEGMENT: 'Shelf Placement - Segment Score',
            SUB_BRAND: 'Shelf Placement - Brand Variant', SKU: 'Shelf Placement - Brand Variant Size'},
        MSRP: {TOTAL: 'MSRP - Total Score', BRAND: 'MSRP - Brand', SUB_BRAND: 'MSRP - Brand Variant', KPI_NAME: MSRP,
               COMPETITION: 'MSRP - Brand Variant Size', SKU: 'MSRP - BVS + Brand Benchmark'},
        DISPLAY_SHARE: {TOTAL: 'Display Share - Total Score', MANUFACTURER: 'Display Share - Manufacturer',
                        SKU: 'Display Share - Brand Variant Size', KPI_NAME: DISPLAY_SHARE}}
    FUNCTION = "function"
    DB_ON_NAMES = {
        POD: {
            TOTAL: 'On_POD - Total Score', SEGMENT: 'On_POD - Segment Score', NATIONAL: 'On_POD - National Score',
            BRAND: 'On_POD - Generic Brand', SUB_BRAND: 'On_POD -  Brand Variant',
            SKU: 'On_POD -  Brand Variant Size', KPI_NAME: POD},
        BACK_BAR: {
            TOTAL: 'Back Bar - Total Score', NATIONAL: 'Back Bar - National Score', SEGMENT: 'Back Bar - Segment Score',
            KPI_NAME: BACK_BAR, SKU: 'Back Bar - Brand Variant Size',
            BRAND: 'Back Bar - Generic Brand', SUB_BRAND: 'Back Bar - Brand Variant'},
        BACK_BAR_NATIONAL: {
            TOTAL: 'Back Bar National - Total Score', SKU: 'Back Bar - Brand Variant Size',
            TEMPLATE: "Back Bar - Template Score", KPI_NAME: BACK_BAR,
            BRAND: 'Back Bar National - Generic Brand', SUB_BRAND: 'Back Bar National - Brand Variant'},
        MENU: {
            TOTAL: 'Menu Share - Total Score', MANUFACTURER: 'Menu Share - Manufacturer Level', KPI_NAME: MENU,
            SUB_BRAND: 'Menu Share - Brand Variant Level', TEMPLATE: "Menu Share - Template Score"},
        MENU_NATIONAL: {
            TOTAL: 'Menu Share National - Total Score',
            MANUFACTURER: 'Menu Share National - Manufacturer Level', KPI_NAME: MENU,
            TEMPLATE: "Menu Share - Template Score", SUB_BRAND: 'Menu Share - Brand Variant Level'}}
    DB_ASSORTMENTS_NAMES = {OFF: "Assortment off Trade", ON: "Assortment on Trade",
                            INDEPENDENT: "independent_display", BACK_BAR: "Assortment on Trade"}

    PRODUCT_FK, STANDARD_TYPE, PASSED, FACINGS = "product_fk", "standard_type", "passed", "facings"
    COLUMNS_FOR_DISPLAY = [MANUFACTURER, PRODUCT_FK, PASSED]
    COLUMNS_FOR_PRODUCT = [PRODUCT_FK, PASSED, BRAND, SUB_BRAND]
    COLUMNS_FOR_PRODUCT_PLACEMENT = [PASSED, SHELF_NAME, FACINGS]

    EXTRA, OOS, DISTRIBUTED, OTHER, NO_PLACEMENT = "EXTRA", "0", "1", "OTHER", "0"
    NO_DISPLAY_ALLOWED_QUESTION = "Confirm that there are no displays in this outlet."
    NO_MENU_ALLOWED_QUESTION = "Confirm that there are no menus in this outlet."
    NO_BACK_BAR_ALLOWED_QUESTION = "Confirm that there are no back bars in this outlet."
    SURVEY_ANSWER = "Yes"

    # operation types:
    DISPLAY_TARGET_OP, SHELF_FACINGS_OP = "display_target", "shelf_facings"
    SHELF_PLACEMENT_OP, MSRP_OP = "shelf_placement", "MSRP"
    OPEN_OPERATION_TYPES = [DISPLAY_TARGET_OP, SHELF_PLACEMENT_OP, MSRP_OP, SHELF_FACINGS_OP]
    INDEPENDENT_OPERATION_TYPES = [DISPLAY_TARGET_OP]
    # columns in external targets:
    EX_PRODUCT_FK, EX_STATE_FK, EX_OPERATION_TYPE,  = "product_fk", "state_fk", "operation_type"
    EX_ATTR2, EX_STORE_NUMBER = "attr2", "store_number_1"
    EX_SCENE_TYPE, EX_BENCHMARK_VALUE, EX_COMPETITOR_FK = "scene_type", "BENCHMARK Value", "competitor_product_fk"
    EX_MIN_FACINGS, EX_RELATIVE_MAX, EX_RELATIVE_MIN = "minimum facings", "relative_target_max", "relative_target_min"
    EX_TARGET_MAX, EX_TARGET_MIN, EX_MINIMUM_SHELF = "target_max", "target_min", "MINIMUM SHELF LOCATION"
    SHELF_FACINGS_COLUMNS = [EX_PRODUCT_FK, EX_COMPETITOR_FK, EX_BENCHMARK_VALUE]
    SHELF_PLACEMENT_COLUMNS = [EX_PRODUCT_FK, EX_MINIMUM_SHELF]
    MSRP_COLUMNS = [EX_PRODUCT_FK, EX_COMPETITOR_FK, EX_RELATIVE_MIN, EX_RELATIVE_MAX, EX_TARGET_MAX, EX_TARGET_MIN]
    DISPLAY_TARGET_COLUMNS = [EX_SCENE_TYPE, EX_MIN_FACINGS, EX_ATTR2]

    ALL = "ALL"

    MENU_EXCLUDE_SUB_CATEGORIES = ["SPIRIT DRINK", "COCKTAIL", "LIQUEUR"]
