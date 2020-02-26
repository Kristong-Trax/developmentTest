import os


class Consts(object):

    TEMPLATE_PATH = os.path.join(os.path.dirname(os.path.realpath(__file__)),
                                 'KPI Template (External Targets) Feb18.xlsx')

    TEMPLATE_SCENE_TYPE = 'Nombre de Tarea'
    TEMPLATE_DOOR_ID = 'Puertas'
    TEMPLATE_SHELF_NUMBER = 'y'
    TEMPLATE_SEQUENCE_NUMBER = 'x'
    TEMPLATE_FACINGS_COUNT = 'Frentes'