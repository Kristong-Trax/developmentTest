import os

__author__ = 'Nicolas'


class Const(object):
    KPI_TEMPLATE = os.path.join(os.path.dirname(os.path.realpath(__file__)),
                                '..', 'Data', 'KPI Template - Heineken MX -marzo 18.xlsx')

    sheetname_Bebidas = 'Acomodo_Botanas'
    sheetname_Invasion = 'Invasion'
    RELEVANT_SCENES_TYPES = ['Gondola', 'Sabritas']
    RELEVANT_MANUFACTURER = 'PEPSI CO'

    SOS_EXCLUDE_FILTERS = {'product_type': ['Irrelevant', 'Empty']}
    ALLOWED_FILTERS = {'product_type': ['Other', 'Empty']}
    EMPTY = 'Empty'
    COCACOLA = 'Coca Cola'
    IGN_STACKING = {"stacking_layer": 1}

    KPIS_HIERACHY = {
        'Botana Sabritas': 'Botanas',
        'Mercadeo - Botana Sabritas': 'Botana Sabritas',
        'Huecos - Botana Sabritas': 'Mercadeo - Botana Sabritas',
        'Frentes - Botana Sabritas': 'Mercadeo - Botana Sabritas',
        'Frentes - Botana Sabritas - SKU': 'Frentes - Botana Sabritas',
        'Acomodo - Botana Sabritas': 'Mercadeo - Botana Sabritas',
        'Acomodo Scene - Botana Sabritas': 'Acomodo - Botana Sabritas',
        'Acomodo - Botana Sabritas - SKU': 'Acomodo Scene - Botana Sabritas',
        'Invasion - Botana Sabritas': 'Mercadeo - Botana Sabritas',
        'Surtido - Botana Sabritas': 'Botana Sabritas',
        'Surtido - Botana Sabritas - SKU': 'Surtido - Botana Sabritas'}

    KPI_EMPTY = 'Huecos - Botana Sabritas'
    KPI_FRENTES_SKU = 'Frentes - Botana Sabritas - SKU'
    KPI_FRENTES = 'Frentes - Botana Sabritas'
    KPI_INVASION = 'Invasion - Botana Sabritas'

    KPI_DISTRIBUTION = 'Surtido - Botana Sabritas - SKU'

    KPI_SURTIDO = 'Surtido - Botana Sabritas'
    KPI_MERCADEO = 'Mercadeo - Botana Sabritas'
    KPI_BOTANA= 'Botana Sabritas'



    KPI_WEIGHTS = {
                   'Mercadeo - Botana Sabritas': 40,
                   'Huecos - Botana Sabritas': 28,
                   'Frentes - Botana Sabritas': 28,
                   'Invasion - Botana Sabritas': 44,
                   'Surtido - Botana Sabritas': 60, }


    KPI_POINTS = {
                   'Botana Sabritas': 2,
                   'Mercadeo - Botana Sabritas': 0.8,
                   'Huecos - Botana Sabritas': 0.224,
                   'Frentes - Botana Sabritas': 0.224,
                   'Invasion - Botana Sabritas': 0.352,
                   'Surtido - Botana Sabritas': 1.2 }