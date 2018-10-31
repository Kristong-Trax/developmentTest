
__author__ = 'Elyashiv'


class Const(object):

    FACINGS = 'facings'
    BRAND = 'brand_name'
    NOTABRAND = {'General', 'General.'}
    EXCLUDE_FILTERS = {
        'product_type': ['Irelevant', 'Empty']
    }

    GOOGLE_BRAND = 'Google Play'
    SOS_SCENE = 'SOS BRAND out of SCENE'
    SOS_RELATIVE = 'SOS BRAND out of GOOGLE in SCENE'
    SOS_OUT_OF_SCENE = 'SOS BRAND out of SCENE'
    SOS_IN_SCENE = 'SOS BRAND out of BRANDS in SCENE'
    FIXTURE_COMPLIANCE = 'FIXTURE COMPLIANCE'
    FIXTURE_POG = "FIXTURE POG COMPLIANCE"
    VISIT_POG = "VISIT POG COMPLIANCE"
    FIXTURE_OSA = "FIXTURE OOS/OSA"
    VISIT_OSA = "VISIT OOS/OSA"
    MISSING_DENOMINATIONS = "missing_denominations"
    FIXTURE_HIGH_LEVEL = "FIXTURE COMPLIANCE HIGH LEVEL"
    POG_HIGH_LEVEL = "POG COMPLIANCE HIGH LEVEL"
    POG_STATUS = "POG STATUS"
    POG_PRODUCT = "POG PRODUCT"

    FIXTURE_TARGETS = 'Fixture Targets'
    PK = 'pk'
    ENTRY_EXIT = "entry_exit_names"
    ENTRY_NAME = "entry_name"
    EXIT_NAME = "exit_name"
    ENTRY_PK = "entry_pk"
    EXIT_PK = "exit_pk"

    ENTRY_SCENES = "entry_chosen_scenes"
    EXIT_SCENES = "exit_chosen_scenes"
    REQUIRED_AMOUNT = "required_amount"
    ACTUAL_AMOUNT = "actual_amount"
    FIXTURE_FK = "fixture_fk"

    NON_KPI = 0

    SOS_KPIs = [SOS_OUT_OF_SCENE, SOS_IN_SCENE]
    SHEETS = [FIXTURE_TARGETS, PK, ENTRY_EXIT]
    GOOGLE = "Google"
