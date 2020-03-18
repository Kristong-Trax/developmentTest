import os

__author__ = 'Nicolas'


class Const(object):
    KPI_TEMPLATE = os.path.join(os.path.dirname(os.path.realpath(__file__)),
                                '..', 'Data', 'KPI Template - Heineken MX -feb18.xlsx')

    sheetname_Bebidas = 'Acomodo_Botanas'
    sheetname_Invasion = 'Invasion'
    RELEVANT_SCENES_TYPES = ['Gondola', 'Barcel']

    SOS_EXCLUDE_FILTERS = {'product_type': ['Irrelevant', 'Empty']}
    ALLOWED_FILTERS = {'product_type': ['Other', 'Empty']}
    EMPTY = 'Empty'
    COCACOLA = 'Coca Cola'
    IGN_STACKING = {"stacking_layer": 1}

    KPIS_HIERACHY = {
        'Botana Barcel': 'Botanas',
        'Mercadeo - Botana Barcel': 'Botana Barcel',
        'Huecos - Botana Barcel': 'Mercadeo - Botana Barcel',
        'Frentes - Botana Barcel': 'Mercadeo - Botana Barcel',
        'Frentes - Botana Barcel - SKU': 'Frentes - Botana Barcel',
        'Acomodo - Botana Barcel': 'Mercadeo - Botana Barcel',
        'Acomodo Scene - Botana Barcel': 'Acomodo - Botana Barcel',
        'Acomodo - Botana Barcel - SKU': 'Acomodo Scene - Botana Barcel',
        'Invasion - Botana Barcel': 'Mercadeo - Botana Barcel',
        'Surtido - Botana Barcel': 'Refrescos Coca Cola',
        'Surtido - Botana Barcel - SKU': 'Surtido - Botana Barcel'}

    KPI_EMPTY = 'Huecos - Botana Barcel'
    KPI_FRENTES_SKU = 'Frentes - Botana Barcel - SKU'
    KPI_FRENTES = 'Frentes - Botana Barcel'
    KPI_INVASION = 'Invasion - Botana Barcel'

    KPI_DISTRIBUTION = 'Surtido - Botana Barcel - SKU'

    KPI_SURTIDO = 'Surtido - Botana Barcel'
    KPI_MERCADEO = 'Mercadeo - Botana Barcel'
    KPI_REFRESCO = 'Botana Barcel'



    KPI_WEIGHTS = {
                   'Mercadeo - Botana Barcel': 40,
                   'Huecos - Botana Barcel': 28,
                   'Frentes - Botana Barcel': 28,
                   'Invasion - Botana Barcel': 44,
                   'Surtido - Botana Barcel': 60, }


    KPI_POINTS = {
                   'Botana Barcel': 1,
                   'Mercadeo - Botana Barcel': 0.4,
                   'Huecos - Botana Barcel': 0.112,
                   'Frentes - Botana Barcel': 0.112,
                   'Invasion - Botana Barcel': 0.176,
                   'Surtido - Botana Barcel': 0.6 }