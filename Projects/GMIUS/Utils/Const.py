
__author__ = 'Elyashiv'


class Const(object):
    ''' Holds Constants '''

    ''' Sheets '''
    KPIS = 'KPIs'
    AGGREGATION = 'Aggregation'

    SHEETS = [KPIS, AGGREGATION]

    ''' KPIs Columns '''
    KPI_NAME = 'KPI Name'
    TYPE = 'Type'
    PARENT = 'Parent'
    TEMPLATE_GROUP = 'Template Group'
    SCENE_TYPE = 'Scene Type'
    STORE_TYPE = 'Store Type'
    CHANNEL = 'Channel'
    SESSION_LEVEL = 'Session Level'

    ''' Aggregation Columns'''
    AGGREGATION_LEVELS = 'Aggregation Levels'
    SOS_TYPE = 'SOS Type'

    ''' Constants '''
    MM_TO_FT = 304.8
    COMMA_SPACE = ', '
    COMMA = ','
    SOS_EXCLUDE_FILTERS = {'product_type': ['Irrelevant', 'Empty']}
    SOS_COLUMN_DICT = {
                        'SOS': 'facings_ign_stack',
                        'Linear SOS': 'width_mm',
                        'AVERAGE': 'count',
                       }




