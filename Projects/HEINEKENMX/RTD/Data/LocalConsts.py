import os


class Consts(object):
    KPIS_HIERARCHY = {'RTD': 'I.E. 6.0',
                      'Mercadeo RTD': 'RTD',
                      'Huecos - RTD': 'Mercadeo RTD',
                      'Frentes - RTD': 'Mercadeo RTD',
                      'Frentes - RTD - SKU': 'Frentes - RTD',
                      'Acomodo - RTD': 'Mercadeo RTD',
                      'Acomodo - Scene - RTD': 'Acomodo - RTD',
                      'Colcado Correctamente - RTD': 'Acomodo - Scene - RTD',
                      'Colcado Correctamente - RTD - SKU': 'Colcado Correctamente - RTD',
                      'Colcado Incorrectamente - RTD': 'Acomodo - Scene - RTD',
                      'Colcado Incorrectamente - RTD - SKU': 'Colcado Incorrectamente - RTD',
                      'Extra - RTD': 'Acomodo - Scene - RTD',
                      'Extra - RTD - SKU': 'Extra - RTD',
                      'Invasion - RTD': 'Mercadeo RTD',
                      'Surtido - RTD': 'RTD',
                      'Calificador - RTD': 'Surtido - RTD',
                      'Calificador - RTD - SKU': 'Calificador - RTD',
                      'Prioritario - RTD': 'Surtido - RTD',
                      'Prioritario - RTD - SKU': 'Prioritario - RTD',
                      'Opcional - RTD': 'Surtido - RTD',
                      'Opcional - RTD - SKU': 'Opcional - RTD'
                      }

    IE_60 = 'I.E. 6.0'
    RTD = 'RTD'
    MERCADEO = 'Mercadeo RTD'
    HUECOS = 'Huecos - RTD'
    FRENTES = 'Frentes - RTD'
    FRENTES_SKU = 'Frentes - RTD - SKU'
    ACOMODO = 'Acomodo - RTD'
    ACOMODO_SCENE = 'Acomodo - Scene - RTD'
    COLCADO_CORRECT = 'Colcado Correctamente - RTD'
    COLCADO_CORRECT_SKU = 'Colcado Correctamente - RTD - SKU'
    COLCADO_INCORRECT = 'Colcado Incorrectamente - RTD'
    COLCADO_INCORRECT_SKU = 'Colcado Incorrectamente - RTD - SKU'
    EXTRA = 'Extra - RTD'
    EXTRA_SKU = 'Extra - RTD - SKU'
    INVASION = 'Invasion - RTD'
    SURTIDO = 'Surtido - RTD'
    CALIFICADOR = 'Calificador - RTD'
    CALIFICADOR_SKU = 'Calificador - RTD - SKU'
    PRIORITARIO = 'Prioritario - RTD'
    PRIORITARIO_SKU = 'Prioritario - RTD - SKU'
    OPCIONAL = 'Opcional - RTD'
    OPCIONAL_SKU = 'Opcional - RTD - SKU'

    KPI_POINTS = {
        IE_60: 25.0,
        RTD: 4.0,
        MERCADEO: 1.6,
        SURTIDO: 2.4,
        HUECOS: 0.16,
        FRENTES: 0.96,
        ACOMODO: 0.32,
        INVASION: 0.16,
        PRIORITARIO: 2.4,
        CALIFICADOR: 0,
        OPCIONAL: 0
    }

    KPI_WEIGHTS = {
        MERCADEO: 40,
        SURTIDO: 60,
        HUECOS: 10,
        FRENTES: 60,
        ACOMODO: 20,
        INVASION: 10,
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