import os

__author__ = 'Nicolas'


class Const(object):
    KPI_TEMPLATE = os.path.join(os.path.dirname(os.path.realpath(__file__)),
                                '..', 'Data', 'KPI Template - Heineken MX -feb18.xlsx')

    sheetname_Bebidas = 'Acomodo_Bebidas'
    sheetname_Invasion = 'Invasion'
    RELEVANT_SCENES_TYPES = 'PJ'

    SOS_EXCLUDE_FILTERS = {'product_type': ['Irrelevant', 'Empty']}
    ALLOWED_FILTERS = {'product_type': ['Other', 'Empty']}
    EMPTY = 'Empty'
    PJ = 'PJ'
    IGN_STACKING = {"stacking_layer": 1}

    KPIS_HIERACHY = {
        'Refrescos PJ': 'Refrescos',
        'Mercadeo - PJ': 'Refrescos PJ',
        'Huecos - PJ': 'Mercadeo - PJ',
        'Frentes - PJ': 'Mercadeo - PJ',
        'Frentes - PJ - SKU': 'Frentes - PJ',
        'Acomodo - PJ': 'Mercadeo - PJ',
        'Acomodo Scene - PJ': 'Acomodo - PJ',
        'Acomodo - PJ - SKU': 'Acomodo Scene - PJ',
        'Invasion - PJ': 'Mercadeo - PJ',
        'Surtido - PJ': 'Refrescos PJ',
        'Surtido - PJ - SKU': 'Surtido - PJ'}

    KPI_EMPTY = 'Huecos - PJ'
    KPI_FACINGS = 'Frentes - PJ - SKU'
    KPI_COUNTING = 'Frentes - PJ - SKU'
    KPI_INVASION = 'Invasion - PJ'
    KPI_POSITION = 'Acomodo - PJ - SKU'
    KPI_DISTRIBUTION = 'Surtido - PJ - SKU'

    KPI_SURTIDO = 'Surtido - PJ'
    KPI_MERCADEO = 'Mercadeo - PJ'
    KPI_REFRESCO = 'Refrescos PJ'

    KPI_WEIGHTS = {
        'Refrescos': 1,
        'PJ': 0.4,
        'Mercadeo - PJ': 0.068,
        'Huecos - PJ': 0.068,
        'Frentes - PJ - SKU': 0.132,
        'Scene - PJ': 0.132,
        'Acomodo - PJ - SKU': 0.6,
    }
