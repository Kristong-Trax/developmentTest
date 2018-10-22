
__author__ = 'Elyashiv'


class Const(object):

    FACINGS = 'facings'
    BRAND = 'brand_name'
    NOTABRAND = {'General', 'General.'}
    EXCLUDE_FILTERS = {
        'product_type': ['Irelevant', 'Empty']
    }
    SOS_KPIs = {
        'SOS BRAND out of SCENE': {'pk': 300000,
                                   'den': None},
        'SOS BRAND out of BRANDS in SCENE': {'pk': 300001,
                                             'den': None}
    }
    FIXTURE_KPIs = {
        'FIXTURE COMPLIANCE': {'pk': 300002}
    }

    FIXTURE_POG = "FIXTURE POG COMPLIANCE"
    VISIT_POG = "VISIT POG COMPLIANCE"
    FIXTURE_OSA = "FIXTURE OOS/OSA"
    VISIT_OSA = "VISIT OOS/OSA"
    MISSING_DENOMINATIONS = "missing_denominations"

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

    SHEETS = [FIXTURE_TARGETS, PK, ENTRY_EXIT]
