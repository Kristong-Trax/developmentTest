
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

    FIXTURE_TARGETS = 'Fixture Targets'
    PK = 'pk'
    SHEETS = [FIXTURE_TARGETS, PK]
