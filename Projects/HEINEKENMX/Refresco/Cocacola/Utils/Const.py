import os

__author__ = 'Nicolas'


class Const(object):
    KPI_TEMPLATE = os.path.join(os.path.dirname(os.path.realpath(__file__)),
                                '..', 'Data', 'KPI Template - Heineken MX -feb18.xlsx')

    sheetname_Bebidas = 'Acomodo_Bebidas'
    sheetname_Invasion = 'Invasion'
    RELEVANT_SCENES_TYPES = ['COCA COLA', 'Coca Cola', 'coca cola']

    SOS_EXCLUDE_FILTERS = {'product_type': ['Irrelevant', 'Empty']}
    ALLOWED_FILTERS = {'product_type': ['Other', 'Empty']}
    EMPTY = 'Empty'
    COCACOLA = 'Coca Cola'
    IGN_STACKING = {"stacking_layer": 1}

    KPIS_HIERACHY = {
        'Refrescos Coca Cola': 'Refrescos',
        'Mercadeo - RCC': 'Refrescos Coca Cola',
        'Huecos - RCC': 'Mercadeo - RCC',
        'Frentes - RCC': 'Mercadeo - RCC',
        'Frentes - RCC - SKU': 'Frentes - RCC',
        'Acomodo - RCC': 'Mercadeo - RCC',
        'Acomodo Scene - RCC': 'Acomodo - RCC',
        'Acomodo - RCC - SKU': 'Acomodo Scene - RCC',
        'Invasion - RCC': 'Mercadeo - RCC',
        'Surtido - RCC': 'Refrescos Coca Cola',
        'Surtido - RCC - SKU': 'Surtido - RCC'}

    KPI_EMPTY = 'Huecos - RCC'
    KPI_FRENTES_SKU = 'Frentes - RCC - SKU'
    KPI_FRENTES = 'Frentes - RCC'
    KPI_INVASION = 'Invasion - RCC'

    KPI_DISTRIBUTION = 'Surtido - RCC - SKU'

    KPI_SURTIDO = 'Surtido - RCC'
    KPI_MERCADEO = 'Mercadeo - RCC'
    KPI_REFRESCO = 'Refrescos Coca Cola'

    KPI_ACAMODO = 'Acomodo - RCC'
    KPI_ACAMODO_SCENE = 'Acomodo Scene - RCC'
    KPI_ACAMODO_SKU = 'Acomodo - RCC - SKU'

    KPI_WEIGHTS = {
                   'Mercadeo - RCC': 40,
                   'Huecos - RCC': 17,
                   'Frentes - RCC': 17,
                   'Acomodo - RCC': 33,
                   'Invasion - RCC': 33,
                   'Surtido - RCC': 60, }


    KPI_POINTS = {'Refrescos': 7,
                   'Refrescos Coca Cola': 2,
                   'Mercadeo - RCC': 0.8,
                   'Huecos - RCC': 0.136,
                   'Frentes - RCC': 0.136,
                   'Acomodo - RCC': 0.264,
                   'Invasion - RCC': 0.264,
                   'Surtido - RCC': 1.2, }