import os
__author__ = 'Nicolas'


class Const(object):

    RELEVANT_SCENES_TYPES = ['Bebidas - Coca Cola 2 Puertas']
    SOS_EXCLUDE_FILTERS = {'product_type': ['Irrelevant', 'Empty']}
    ALLOWED_FILTERS = {'product_type': ['Other', 'Empty']}
    EMPTY = 'Empty'
    COCACOLA = 'COCA COLA'
    IGN_STACKING = {"stacking_layer": 1}


    KPIS_HIERACHY  = {'I.E. 6.0':'Refrescos Coca Cola',
            'Refrescos Coca Cola':'Mercadeo - RCC',
            'Mercadeo - RCC':'Huecos - RCC',
            'Mercadeo - RCC':'Frentes - RCC',
            'Frentes - RCC':'Frentes - RCC - SKU',
            'Mercadeo - RCC':'Acomodo - RCC',
            'Acomodo - RCC':'Acomodo Scene - RCC',
            'Acomodo Scene - RCC':'Acomodo - RCC - SKU',
            'Mercadeo - RCC':'Invasion - RCC',
            'Refrescos Coca Cola':'Surtido - RCC',
            'Surtido -  RCC':'Surtido - RCC - SKU'}

    KPI_TEMPLATE = os.path.join(os.path.dirname(os.path.realpath(__file__)),
                                                  '..', 'Data', 'Data/KPI Template - Heineken MX - v5_feb13.xlsx')



    KPI_EMPTY = ['Huecos - RCC']
    KPI_FACINGS = ['Frentes - RCC - SKU']
    KPI_COUNTING = ['Frentes - RCC - SKU']
    KPI_PRESENSE = ['Invasion - RCC']