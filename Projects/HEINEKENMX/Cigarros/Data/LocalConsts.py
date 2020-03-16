import os


class Consts(object):
    KPIS_HIERARCHY = {'Cigarros': 'I.E. 6.0',
                      'Mercadeo Cigarros': 'Cigarros',
                      'Huecos - Cigarros': 'Mercadeo Cigarros',
                      'Frentes - Cigarros': 'Mercadeo Cigarros',
                      'Frentes - Cigarros - SKU': 'Frentes - Cigarros',
                      'Acomodo - Cigarros': 'Mercadeo Cigarros',
                      'Acomodo - Scene - Cigarros': 'Acomodo - Cigarros',
                      'Colcado Correctamente - Cigarros': 'Acomodo - Scene - Cigarros',
                      'Colcado Correctamente - Cigarros - SKU': 'Colcado Correctamente - Cigarros',
                      'Colcado Incorrectamente - Cigarros': 'Acomodo - Scene - Cigarros',
                      'Colcado Incorrectamente - Cigarros - SKU': 'Colcado Incorrectamente - Cigarros',
                      'Extra - Cigarros': 'Acomodo - Scene - Cigarros',
                      'Extra - Cigarros - SKU': 'Extra - Cigarros',
                      'Invasion - Cigarros': 'Mercadeo Cigarros',
                      'Surtido - Cigarros': 'Cigarros',
                      'Calificador - Cigarros': 'Surtido - Cigarros',
                      'Calificador - Cigarros - SKU': 'Calificador - Cigarros',
                      'Prioritario - Cigarros': 'Surtido - Cigarros',
                      'Prioritario - Cigarros - SKU': 'Prioritario - Cigarros',
                      'Opcional - Cigarros': 'Surtido - Cigarros',
                      'Opcional - Cigarros - SKU': 'Opcional - Cigarros'
                      }

    IE_60 = 'I.E. 6.0'
    CIGARROS = 'Cigarros'
    MERCADEO = 'Mercadeo Cigarros'
    HUECOS = 'Huecos - Cigarros'
    FRENTES = 'Frentes - Cigarros'
    FRENTES_SKU = 'Frentes - Cigarros - SKU'
    ACOMODO = 'Acomodo - Cigarros'
    ACOMODO_SCENE = 'Acomodo - Scene - Cigarros'
    COLCADO_CORRECT = 'Colcado Correctamente - Cigarros'
    COLCADO_CORRECT_SKU = 'Colcado Correctamente - Cigarros - SKU'
    COLCADO_INCORRECT = 'Colcado Incorrectamente - Cigarros'
    COLCADO_INCORRECT_SKU = 'Colcado Incorrectamente - Cigarros - SKU'
    EXTRA = 'Extra - Cigarros'
    EXTRA_SKU = 'Extra - Cigarros - SKU'
    INVASION = 'Invasion - Cigarros'
    SURTIDO = 'Surtido - Cigarros'
    CALIFICADOR = 'Calificador - Cigarros'
    CALIFICADOR_SKU = 'Calificador - Cigarros - SKU'
    PRIORITARIO = 'Prioritario - Cigarros'
    PRIORITARIO_SKU = 'Prioritario - Cigarros - SKU'
    OPCIONAL = 'Opcional - Cigarros'
    OPCIONAL_SKU = 'Opcional - Cigarros - SKU'

    KPI_POINTS = {
        IE_60: 25.0,
        CIGARROS: 5.0,
        MERCADEO: 2.0,
        SURTIDO: 3.0,
        HUECOS: 0.2,
        FRENTES: 0.4,
        ACOMODO: 0.8,
        INVASION: 0.6,
        PRIORITARIO: 3,
        CALIFICADOR: 0,
        OPCIONAL: 0
    }

    KPI_WEIGHTS = {
        MERCADEO: 40,
        SURTIDO: 60,
        HUECOS: 10,
        FRENTES: 20,
        ACOMODO: 40,
        INVASION: 30,
        PRIORITARIO: 100,
        CALIFICADOR: 0,
        OPCIONAL: 0
    }

    TEMPLATE_PATH = os.path.join(os.path.dirname(os.path.realpath(__file__)),
                                 'KPI Template (External Targets) Feb18.xlsx')

    TEMPLATE_SCENE_TYPE = 'Nombre de Tarea'
    TEMPLATE_DOOR_ID = 'Puertas'
    TEMPLATE_SHELF_NUMBER = 'y'
    TEMPLATE_SEQUENCE_NUMBER = 'x'
    TEMPLATE_FACINGS_COUNT = 'Frentes'