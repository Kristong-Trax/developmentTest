
__author__ = 'Uri, Sam'


class Const(object):

    REGIONS = 'UNITED'
    DELIVER = 'United Deliver'
    TEMPLATE_PATH = 'ARA Template v0.1.xlsx'

    # sheets:
    KPIS = "KPIs"
    SOS = "SOS"
    MIN_SHELVES = 'Min Shelves'
    MIN_FACINGS = 'Min Facings'
    LOCATION = 'Location'
    MIN_SKUS = "Min SKUs"
    RATIO = 'Ratio'
    TARGETS = "Targets"
    SHEETS = [KPIS, SOS, MIN_SHELVES, MIN_FACINGS, LOCATION, MIN_SKUS, RATIO, TARGETS]

    # generic columns:
    KPI_NAME = "KPI name"
    SHELVES = 'Shelves'
    TARGET = "Target"
    EXCLUDE = 'exclude'

    # columns of KPIS:
    TYPE = "Type"
    PARENT = 'Parent'
    REGION = "Region"
    SCENE_TYPE_GROUP = "Template Group"
    SCENE_TYPE = "Scene Type"
    STORE_TYPE = "Store_Type"
    PROGRAM = 'Program'
    SESSION_LEVEL = "Session Level"

    # seperators
    SEPERATOR = '; '
    COMMA = ','

    # constants:
    PASS = 'Pass'
    FAIL = 'Fail'
    MANUFACTURER_FK = 1

    BEHAVIOR = {
                3045: 'SUM',
                2161: 'SUM',
                3047: 'PASS'
                }


