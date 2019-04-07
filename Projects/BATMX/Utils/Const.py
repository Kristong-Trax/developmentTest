
__author__ = 'Elyashiv'


class Const(object):

    PLANOGRAM_LEVEL = "planogram_level"
    FIXTURE_LEVEL = "fixture_level"
    STATUS_LEVEL = "status_level"
    VISIT_LEVEL = "visit_level"
    SKU_LEVEL = "sku_level"

    OOS_FIXTURE = "OOS - fixture level"
    OOS_VISIT = "OOS - visit level"
    OOS_SKU = "OOS - sku level"

    TOBACCO_CENTER = "tobacco_center"
    PROMOTIONAL_TRAY = "promotional_tray"

    POG_KPI_NAMES = {
        TOBACCO_CENTER: {
            PLANOGRAM_LEVEL: "POG planogram level - tobacco center",
            FIXTURE_LEVEL: "POG fixture level - tobacco center",
            STATUS_LEVEL: "POG status level - tobacco center",
            VISIT_LEVEL: "POG visit level - tobacco center",
            SKU_LEVEL: "POG sku level - tobacco center"},
        PROMOTIONAL_TRAY: {
            PLANOGRAM_LEVEL: "POG planogram level - promotional tray",
            FIXTURE_LEVEL: "POG fixture level - promotional tray",
            STATUS_LEVEL: "POG status level - promotional tray",
            VISIT_LEVEL: "POG visit level - promotional tray",
            SKU_LEVEL: "POG sku level - promotional tray"},
    }

    SOS_LEVELS = {
        FIXTURE_LEVEL: "facings SOS - fixture level",
        VISIT_LEVEL: "facings SOS - visit level"
    }

    BAT_MANUFACTURERS = ["British American Tobacco"]

    ENTRY = "ENTRY"
    EXIT = "EXIT"

    ENTRY_TEMPLATE = "Entrada"
    EXIT_TEMPLATE = "Salida"
