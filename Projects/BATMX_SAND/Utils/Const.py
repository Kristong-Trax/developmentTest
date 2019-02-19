
__author__ = 'Elyashiv'


class Const(object):

    FIXTURE_LEVEL = "fixture_level"
    STATUS_LEVEL = "status_level"
    VISIT_LEVEL = "visit_level"
    SKU_LEVEL = "sku_level"

    OOS = "oos"
    OOS_VISIT = "oos - visit"
    OOS_SKU = "oos - sku"

    TOBACCO_CENTER = "tobacco_center"
    PROMOTIONAL_TRAY = "promotional_tray"

    POG_KPI_NAMES = {
        TOBACCO_CENTER: {
            FIXTURE_LEVEL: "tobacco center pog compliance",
            STATUS_LEVEL: "tobacco center pog status",
            VISIT_LEVEL: "tobacco center pog visit",
            SKU_LEVEL: "tobacco center pog sku"},
        PROMOTIONAL_TRAY: {
            FIXTURE_LEVEL: "promotional tray pog compliance",
            STATUS_LEVEL: "promotional tray pog status",
            VISIT_LEVEL: "promotional tray pog visit",
            SKU_LEVEL: "promotional tray pog sku"},
    }

    SOS_LEVELS = {
        FIXTURE_LEVEL: "facings_sos",
        VISIT_LEVEL: "facings_sos - visit"
    }

    BAT_MANUFACTURERS = ["British American Tobacco"]

    ENTRY = "ENTRY"
    EXIT = "EXIT"

    ENTRY_TEMPLATE = "Entrada"
    EXIT_TEMPLATE = "Salida"
