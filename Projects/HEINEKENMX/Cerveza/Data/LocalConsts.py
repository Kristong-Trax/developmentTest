import os


class Consts(object):
    KPIS_HIERARCHY = {'Cerveza': 'I.E. 6.0',
                      'Mercadeo Cerveza': 'Cerveza',
                      'Huecos - Cerveza': 'Mercadeo Cerveza',
                      'Frentes - Cerveza': 'Mercadeo Cerveza',
                      'Frentes - Cerveza - SKU': 'Frentes - Cerveza',
                      'Acomodo - Cerveza': 'Mercadeo Cerveza',
                      'Acomodo - Scene - Cerveza': 'Acomodo - Cerveza',
                      'Colcado Correctamente - Cerveza': 'Acomodo - Scene - Cerveza',
                      'Colcado Correctamente - Cerveza - SKU': 'Colcado Correctamente - Cerveza',
                      'Colcado Incorrectamente - Cerveza': 'Acomodo - Scene - Cerveza',
                      'Colcado Incorrectamente - Cerveza - SKU': 'Colcado Incorrectamente - Cerveza',
                      'Extra - Cerveza': 'Acomodo - Scene - Cerveza',
                      'Extra - Cerveza - SKU': 'Extra - Cerveza',
                      'Invasion - Cerveza': 'Mercadeo Cerveza',
                      'Surtido - Cerveza': 'Cerveza',
                      'Calificador - Cerveza': 'Surtido - Cerveza',
                      'Calificador - Cerveza - SKU': 'Calificador - Cerveza',
                      'Prioritario - Cerveza': 'Surtido - Cerveza',
                      'Prioritario - Cerveza - SKU': 'Prioritario - Cerveza',
                      'Opcional - Cerveza': 'Surtido - Cerveza',
                      'Opcional - Cerveza - SKU': 'Opcional - Cerveza'
                      }

    IE_60 = 'I.E. 6.0'
    CERVEZA = 'Cerveza'
    MERCADEO = 'Mercadeo Cerveza'
    HUECOS = 'Huecos - Cerveza'
    FRENTES = 'Frentes - Cerveza'
    FRENTES_SKU = 'Frentes - Cerveza - SKU'
    ACOMODO = 'Acomodo - Cerveza'
    ACOMODO_SCENE = 'Acomodo - Scene - Cerveza'
    COLCADO_CORRECT = 'Colcado Correctamente - Cerveza'
    COLCADO_CORRECT_SKU = 'Colcado Correctamente - Cerveza - SKU'
    COLCADO_INCORRECT = 'Colcado Incorrectamente - Cerveza'
    COLCADO_INCORRECT_SKU = 'Colcado Incorrectamente - Cerveza - SKU'
    EXTRA = 'Extra - Cerveza'
    EXTRA_SKU = 'Extra - Cerveza - SKU'
    INVASION = 'Invasion - Cerveza'
    SURTIDO = 'Surtido - Cerveza'
    CALIFICADOR = 'Calificador - Cerveza'
    CALIFICADOR_SKU = 'Calificador - Cerveza - SKU'
    PRIORITARIO = 'Prioritario - Cerveza'
    PRIORITARIO_SKU = 'Prioritario - Cerveza - SKU'
    OPCIONAL = 'Opcional - Cerveza'
    OPCIONAL_SKU = 'Opcional - Cerveza - SKU'

    KPI_POINTS = {
        IE_60: 25.0,
        CERVEZA: 6.0,
        MERCADEO: 3.0,
        SURTIDO: 3.0,
        HUECOS: 0.3,
        FRENTES: 1.8,
        ACOMODO: 0.6,
        INVASION: 0.3,
        PRIORITARIO: 1.17,
        CALIFICADOR: 1.5,
        OPCIONAL: 0.33
    }

    TEMPLATE_PATH = os.path.join(os.path.dirname(os.path.realpath(__file__)),
                                 'KPI Template (External Targets) Feb18.xlsx')

    TEMPLATE_SCENE_TYPE = 'Nombre de Tarea'
    TEMPLATE_DOOR_ID = 'Puertas'
    TEMPLATE_SHELF_NUMBER = 'y'
    TEMPLATE_SEQUENCE_NUMBER = 'x'
    TEMPLATE_FACINGS_COUNT = 'Frentes'