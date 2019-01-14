__author__ = 'hunter'


class Const(object):
    ON = 'on'
    OFF = 'off'

    # sheets
    ON_PREMISE_MAIN = 'Total Visit Score (On Premise)'
    OFF_PREMISE_MAIN = 'Total Visit Score (Off Premise)'
    SHELF_FACING_SHEET = '# of facings, #of Displays'
    PRICING_SHEET = 'Pricing'
    DISPLAY_TARGET_SHEET = 'Display Share - Targets'

    SHEETS = {ON: [ON_PREMISE_MAIN],
              OFF: [OFF_PREMISE_MAIN, SHELF_FACING_SHEET, PRICING_SHEET,
                    DISPLAY_TARGET_SHEET]}

    TOTAL = 'total'
    BRAND, SUB_BRAND, SKU = "brand", "sub_brand", "sku"
    COMPETITION, MANUFACTURER = "competition", "manufacturer"

    # KPIs columns:
    KPI_NAME, KPI_GROUP, SCORE, TARGET, WEIGHT = "KPI Name", "KPI Group", "Score", "Target", "Weight"
    TEMPLATE_GROUP = "Template Group/ Scene Type"

    # pricing columns:
    STATE, OUR_EAN_CODE, COMP_EAN_CODE = "State", "Product EAN Code", "Competitor EAN Code"
    MSRP = "MSRP"
    MIN_MSRP_RELATIVE, MAX_MSRP_RELATIVE = "SHELF MSRP RELATIVE GUIDENCE: MIN", "SHELF MSRP RELATIVE GUIDENCE: MAX"
    MIN_MSRP_ABSOLUTE = "SHELF MSRP ABSOLUTE GUIDENCE: MIN"
    MAX_MSRP_ABSOLUTE = "SHELF MSRP ABSOLUTE GUIDENCE: MAX"

    # sets in off-premise:
    DISPLAY_SHARE, SHELF_FACINGS = "Display Share", "Shelf Facings"

    # names in DB:
    DB_TOTAL_KPIS = {
        ON: {TOTAL: 'Total Score - On Premise'},
        OFF: {TOTAL: 'Total Score - Off Premise'}
    }
    DB_OFF_NAMES = {
        DISPLAY_BRAND: {
            TOTAL: 'Display Brand - Total Score', NATIONAL: 'Display Brand - National Score',
            SEGMENT: 'Display Brand - Segment Score', BRAND: 'Display Brand - Brand Level',
            SUB_BRAND: 'Display Brand - Brand Variant Level', SKU: 'Display Brand - Brand Variant Size'},
        SHELF_FACINGS: {
            TOTAL: 'Shelf Facings - Total Score',
            NATIONAL: 'Shelf Facings - National Score', SEGMENT: 'Shelf Facings - Segment Score',
            BRAND: 'Shelf Facings - Compliance Brand', SUB_BRAND: 'Shelf Facings - Brand Variant',
            COMPETITION: 'Shelf Facings - Brand Variant Size', SKU: 'Shelf Facings - BVS + Brand Benchmark'},
        SHELF_PLACEMENT: {
            TOTAL: 'Shelf Placement - Total Score', BRAND: 'Shelf Placement - Brand',
            NATIONAL: 'Shelf Placement - National Score', SEGMENT: 'Shelf Placement - Segment Score',
            SUB_BRAND: 'Shelf Placement - Brand Variant', SKU: 'Shelf Placement - Brand Variant Size'},
        MSRP: {TOTAL: 'MSRP - Total Score', BRAND: 'MSRP - Brand', SUB_BRAND: 'MSRP - Brand Variant',
               COMPETITION: 'MSRP - Brand Variant Size', SKU: 'MSRP - BVS + Brand Benchmark'},
        DISPLAY_SHARE: {TOTAL: 'Display Share - Total Score', MANUFACTURER: 'Display Share - Manufacturer',
                        SKU: 'Display Share - Brand Variant Size'}}
    DB_ON_NAMES = {
        MENU: {
            TOTAL: 'Menu Share - Total Score', MANUFACTURER: 'Menu Share - Manufacturer Level',
            SUB_BRAND: 'Menu Share - Brand Variant Level'}}
    DB_ASSORTMENTS_NAMES = {OFF: "Assortment off Trade", ON: "Assortment on Trade"}

    PRODUCT_FK, STANDARD_TYPE, PASSED, FACINGS = "product_fk", "standard_type", "passed", "facings"
    COLUMNS_FOR_DISPLAY = [MANUFACTURER, PRODUCT_FK, PASSED]
    COLUMNS_FOR_PRODUCT = [PRODUCT_FK, PASSED, BRAND, SUB_BRAND]
    COLUMNS_FOR_PRODUCT_ASSORTMENT = [PRODUCT_FK, STANDARD_TYPE, PASSED, BRAND, SUB_BRAND]
