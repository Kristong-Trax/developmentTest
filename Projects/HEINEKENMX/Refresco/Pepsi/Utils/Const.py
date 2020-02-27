import os

__author__ = 'Nicolas'


class Const(object):
    KPI_TEMPLATE = os.path.join(os.path.dirname(os.path.realpath(__file__)),
                                '..', 'Data', 'KPI Template - Heineken MX -feb18.xlsx')

    sheetname_Bebidas = 'Acomodo_Bebidas'
    sheetname_Invasion = 'Invasion'
    RELEVANT_SCENES_TYPES = 'Pepsi'

    SOS_EXCLUDE_FILTERS = {'product_type': ['Irrelevant', 'Empty']}
    ALLOWED_FILTERS = {'product_type': ['Other', 'Empty']}
    EMPTY = 'Empty'
    PEPSI = 'PEPSI'
    IGN_STACKING = {"stacking_layer": 1}

    KPIS_HIERACHY = {
        'Refrescos Pepsi': 'Refrescos',
        'Mercadeo - Pepsi': 'Refrescos Pepsi',
        'Huecos - Pepsi': 'Mercadeo - Pepsi',
        'Frentes - Pepsi': 'Mercadeo - Pepsi',
        'Frentes - Pepsi - SKU': 'Frentes - Pepsi',
        'Acomodo - Pepsi': 'Mercadeo - Pepsi',
        'Acomodo Scene - Pepsi': 'Acomodo - Pepsi',
        'Acomodo - Pepsi - SKU': 'Acomodo Scene - Pepsi',
        'Invasion - Pepsi': 'Mercadeo - Pepsi',
        'Surtido - Pepsi': 'Refrescos Pepsi',
        'Surtido - Pepsi - SKU': 'Surtido - Pepsi'}

    KPI_EMPTY = 'Huecos - Pepsi'
    KPI_FACINGS = 'Frentes - Pepsi - SKU'
    KPI_COUNTING = 'Frentes - Pepsi - SKU'
    KPI_INVASION = 'Invasion - Pepsi'
    KPI_POSITION = 'Acomodo - Pepsi - SKU'
    KPI_DISTRIBUTION = 'Surtido - Pepsi - SKU'

    KPI_SURTIDO = 'Surtido - Pepsi'
    KPI_MERCADEO = 'Mercadeo - Pepsi'
    KPI_REFRESCO = 'Refrescos Pepsi'

    KPI_WEIGHTS = {
        'Refrescos Pepsi': 4,
        'Mercadeo - Pepsi': 1.6,
        'Huecos - Pepsi': 0.272,
        'Frentes - Pepsi': 0.272,
        'Acomodo - Pepsi': 0.528,
        'Invasion - Pepsi': 0.528,
        'Surtido - Pepsi': 2.4, }
