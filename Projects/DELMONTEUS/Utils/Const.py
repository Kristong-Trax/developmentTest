import os
__author__ = 'Sam'


class Const(object):
    ''' Holds Constants '''

    TMB_MAP_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.realpath(__file__))), 'Data',
                                   'TopMiddleBottom_Map.xlsx')

    ''' Sheets '''
    KPIS = 'KPIs'
    SHELF_PLACEMENT = 'Shelf Placement'
    SHELF_REGION = 'Shelf Region'
    ADJACENCY = 'Adjacency'
    BLOCKING = 'Blocking'
    MAX_BLOCK_ADJ = 'Max Blocking Adjacency'
    BLOCKED_TOGETHER = 'Blocked Together'
    BLOCKING_PERCENT = 'Blocking Percent'
    MULTI_BLOCK = 'Multi Block'
    ANCHOR = 'Anchor'
    SEQUENCE = 'Sequence'
    INTEGRATED = 'Integrated Core KPI'
    RELATIVE_POSTION = 'Relative Position'
    SAME_AISLE = 'Same Aisle'
    SOS = 'SOS Linear'

    RESULT = 'Result'

    ''' KPIs Columns '''
    KPI_NAME = 'KPI Name'
    TYPE = 'Type'
    PARENT = 'Parent'
    DEPENDENT = 'Dependent'
    DEPENDENT_RESULT = 'Dependent Result'
    SCENE_TYPE = 'Scene Type'

    ''' Columns '''
    EDGES = 'Edges'
    RESULT_TYPE = 'Result Type'

    ''' Constants '''
    COMMA_SPACE = ', '
    COMMA = ','
    KEEP = 'keep'
    SOS_EXCLUDE_FILTERS = {'product_type': ['Irrelevant', 'Empty']}
    ALLOWED_FILTERS = {'product_type': ['Other', 'Empty']}
    IGN_STACKING = {"stacking_layer": 1}
    ORIENTS = {'vertically', 'horizontally'}
    DIRECTIONS = ['left', 'right']
    REGIONS = ['left', 'center', 'right']
    NUM_REG = 3
    MM_FT = 304.8




