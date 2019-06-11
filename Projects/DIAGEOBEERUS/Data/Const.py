__author__ = 'hunter'


class Const(object):
    ON = 'on'
    OFF = 'off'

    # sheets
    ON_PREMISE_MAIN = 'Total Visit Score (On Premise)'
    OFF_PREMISE_MAIN = 'Total Visit Score (Off Premise)'
    SHELF_FACING_SHEET = '# of facings, # Displays'
    PRICING_SHEET = 'Pricing'
    DISPLAY_TARGET_SHEET = 'Display Share - Targets'
    TAP_HANDLE_BEER_GLASSES_SHEET = 'Tap Handles and Beer Glasses'

    SHEETS = {ON: [ON_PREMISE_MAIN, TAP_HANDLE_BEER_GLASSES_SHEET],
              OFF: [OFF_PREMISE_MAIN, SHELF_FACING_SHEET, PRICING_SHEET,
                    DISPLAY_TARGET_SHEET]}

    TOTAL = 'total'
    EXTRA, OOS, DISTRIBUTED, OTHER, NO_PLACEMENT = "EXTRA", "0", "1", "OTHER", "0"
    BRAND, SUB_BRAND, SKU, VARIANT = "brand", "sub_brand", "sku", "sub_brand_name"
    COMPETITION, MANUFACTURER = "competition", "manufacturer"

    # KPIs columns:
    KPI_NAME, KPI_GROUP, SCORE, TARGET, WEIGHT, TYPE = "KPI Name", "KPI Group", "Score", "Target", "Weight", "KPI type"
    TEMPLATE_GROUP = "Template Group/ Scene Type"

    # pricing columns:
    STATE, OUR_EAN_CODE, COMP_EAN_CODE = "State", "Product EAN Code", "Competitor EAN Code"
    OUR_SUB_BRAND, COMP_SUB_BRAND = "Product Sub English Name (Brand Family)", "Competitor Sub English Name (Brand Family)"
    BENCH_ENTITY, BENCH_VALUE = "BENCHMARK Entity", "Target"
    MSRP = "Pricing"
    MIN_MSRP_RELATIVE, MAX_MSRP_RELATIVE = "SHELF MSRP RELATIVE GUIDENCE: MIN", "SHELF MSRP RELATIVE GUIDENCE: MAX"
    MIN_MSRP_ABSOLUTE = "SHELF MSRP ABSOLUTE GUIDENCE: MIN"
    MAX_MSRP_ABSOLUTE = "SHELF MSRP ABSOLUTE GUIDENCE: MAX"

    # display columns:
    SCENE_TYPE, MIN_FACINGS = "Scene Type", "Minimum # of Facings"

    # sets in off-premise:
    DISPLAY_SHARE, SHELF_FACINGS = "Display Share", "# of Facings (Main Shelf)"
    SHELF_FACINGS_MAIN_SHELF = "# of Facings (Main Shelf)"
    SHELF_FACINGS_COLD_BOX = "# of Facings (Cold Box)"
    NUMBER_OF_DISPLAYS = "# of Displays"
    MPA = 'MPA'
    MPA_SKU = 'MPA - SKU'
    MPA_MR = 'MPA - MR'
    MPA_SKU_MR = 'MPA - SKU - MR'
    ON_SHELF = 'On Shelf'
    NOT_ON_SHELF = 'Not on Shelf'

    # names in DB:
    DB_TOTAL_KPIS = {
        ON: {TOTAL: 'Total Score - On Premise'},
        OFF: {TOTAL: 'Total Score - Off Premise'}
    }
    DB_OFF_NAMES = {
        SHELF_FACINGS: {
            TOTAL: 'Shelf Facings - Total Score',
            BRAND: 'Shelf Facings - Compliance Brand', SUB_BRAND: 'Shelf Facings - Brand Variant',
            COMPETITION: 'Shelf Facings - Brand Variant Size', SKU: 'Shelf Facings - BVS + Brand Benchmark'},
        SHELF_FACINGS_MAIN_SHELF: {
            TOTAL: '# of Facings (Main Shelf)',
            BRAND: '# of Facings (Main Shelf) - Brand', SUB_BRAND: '# of Facings (Main Shelf) - Brand Variant',
            COMPETITION: '# of Shelf Facings - Brand Variant Size', SKU: '# of Shelf Facings - BVS + Brand Benchmark',
            VARIANT: '# of Facings (Main Shelf) - Brand Variant vs. Benchmark'},
        SHELF_FACINGS_COLD_BOX: {
            TOTAL: '# of Facings (Cold Box)',
            BRAND: '# of Facings (Cold Box) - Brand', SUB_BRAND: '# of Facings (Cold Box) - Brand Variant',
            COMPETITION: '# of Shelf Facings - Brand Variant Size', SKU: '# of Shelf Facings - BVS + Brand Benchmark',
            VARIANT: '# of Facings (Cold Box) - Brand Variant vs. Benchmark'},
        NUMBER_OF_DISPLAYS: {
            TOTAL: '# of Displays',
            BRAND: '# of Displays - Brand', SUB_BRAND: '# of Displays - Brand Variant',
            VARIANT: '# of Displays - Brand Variant vs. Benchmark'},
        MSRP: {TOTAL: 'MSRP', BRAND: 'MSRP - Brand', SUB_BRAND: 'MSRP - Brand Variant',
               COMPETITION: 'MSRP - Brand Variant Size', SKU: 'MSRP - BVS + Brand Benchmark'},
        DISPLAY_SHARE: {TOTAL: 'Display Share', MANUFACTURER: 'Display Share - Manufacturer',
                        SKU: 'Display Share - Brand Variant Size'}}

    # these are the off-premise KPI type values for use with hierarchies and mobile reports
    # this entire infrastructure is dumb, but is needed to duplicate KPIs so the API structure stays the same
    DB_OFF_NAMES_MR = {
        SHELF_FACINGS: {
            TOTAL: 'Shelf Facings - Total Score - MR',
            BRAND: 'Shelf Facings - Compliance Brand - MR', SUB_BRAND: 'Shelf Facings - Brand Variant - MR',
            COMPETITION: 'Shelf Facings - Brand Variant Size - MR', SKU: 'Shelf Facings - BVS + Brand Benchmark - MR'},
        SHELF_FACINGS_MAIN_SHELF: {
            TOTAL: '# of Facings (Main Shelf) - MR',
            BRAND: '# of Facings (Main Shelf) - Brand - MR', SUB_BRAND: '# of Facings (Main Shelf) - Brand Variant - MR',
            COMPETITION: '# of Shelf Facings - Brand Variant Size - MR', SKU: '# of Shelf Facings - BVS + Brand Benchmark - MR',
            VARIANT: '# of Facings (Main Shelf) - Brand Variant vs. Benchmark - MR'},
        SHELF_FACINGS_COLD_BOX: {
            TOTAL: '# of Facings (Cold Box) - MR',
            BRAND: '# of Facings (Cold Box) - Brand - MR', SUB_BRAND: '# of Facings (Cold Box) - Brand Variant - MR',
            COMPETITION: '# of Shelf Facings - Brand Variant Size - MR', SKU: '# of Shelf Facings - BVS + Brand Benchmark - MR',
            VARIANT: '# of Facings (Cold Box) - Brand Variant vs. Benchmark - MR'},
        NUMBER_OF_DISPLAYS: {
            TOTAL: '# of Displays - MR',
            BRAND: '# of Displays - Brand - MR', SUB_BRAND: '# of Displays - Brand Variant - MR',
            VARIANT: '# of Displays - Brand Variant vs. Benchmark - MR'},
        MSRP: {TOTAL: 'MSRP - MR', BRAND: 'MSRP - Brand - MR', SUB_BRAND: 'MSRP - Brand Variant - MR',
               COMPETITION: 'MSRP - Brand Variant Size - MR', SKU: 'MSRP - BVS + Brand Benchmark - MR'},
        DISPLAY_SHARE: {TOTAL: 'Display Share - MR', MANUFACTURER: 'Display Share - Manufacturer - MR',
                        SKU: 'Display Share - Brand Variant Size - MR'}}

    MENU = 'Menu Presence'
    TAP_HANDLE = 'Tap Handle Availability'
    BEER_GLASSES = 'Beer Glass Availability'
    DISPLAY = 'display'
    DB_ON_NAMES = {
        MENU: {
            TOTAL: 'Menu Presence', MANUFACTURER: 'Menu Presence - Manufacturer',
            BRAND: 'Menu Presence - Brand'},
        TAP_HANDLE: {
            TOTAL: 'Tap Handle Availability', BRAND: 'Tap Handle Availability - Brand',
            DISPLAY: 'Tap Handle Availability - Handle'
        },
        BEER_GLASSES: {
            TOTAL: 'Guinness Glasses', BRAND: 'Guinness Glasses - Brand',
            DISPLAY: 'Guinness Glasses - Glass'
        }}

    # these are the on-premise KPI type values for use with hierarchies and mobile reports
    # this entire infrastructure is dumb, but is needed to duplicate KPIs so the API structure stays the same
    DB_ON_NAMES_MR = {
        MENU: {
            TOTAL: 'Menu Presence - MR', MANUFACTURER: 'Menu Presence - Manufacturer - MR',
            BRAND: 'Menu Presence - Brand - MR'},
        TAP_HANDLE: {
            TOTAL: 'Tap Handle Availability - MR', BRAND: 'Tap Handle Availability - Brand - MR',
            DISPLAY: 'Tap Handle Availability - Handle - MR'
        },
        BEER_GLASSES: {
            TOTAL: 'Guinness Glasses - MR', BRAND: 'Guinness Glasses - Brand - MR',
            DISPLAY: 'Guinness Glasses - Glass - MR'
        }}

    # tap handle and beer glass columns
    ATT1 = 'additional_attribute_1'
    DISPLAY_BRAND = 'display_brand'

    PRODUCT_FK, STANDARD_TYPE, PASSED, FACINGS = "product_fk", "standard_type", "passed", "facings"
    COLUMNS_FOR_DISPLAY = [MANUFACTURER, PRODUCT_FK, PASSED]
    COLUMNS_FOR_PRODUCT = [PRODUCT_FK, PASSED, BRAND, SUB_BRAND]
    COLUMNS_FOR_PRODUCT_ASSORTMENT = [PRODUCT_FK, STANDARD_TYPE, PASSED, BRAND, SUB_BRAND]

    NO_MENU_ALLOWED_QUESTION = "Check this box if no menu was available in this outlet."
    SURVEY_ANSWER = "No Menu"
