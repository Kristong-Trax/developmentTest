import os
__author__ = 'Sam'


class Const(object):
    ''' Holds Constants '''

    TMB_MAP_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.realpath(__file__))), 'Data',
                                'TopMiddleBottom_Map.xlsx')

    ''' Sheets '''
    KPIS = 'KPIS'

    SHARE_OF_FACINGS = 'Share of Facings'
    SHARE_OF_SHELF = 'Share of Shelf'
    ADJACENCY = 'Adjacency'
    DISTRIBUTION = 'Distribution'
    LINEAR_MEASUREMENT = 'Linear Measurement'
    OUT_OF_STOCK = 'Out of Stock'
    PACK_DISTRIBUTION = 'Pack Distribution'
    PURITY = 'Purity'
    NEGATIVE_DISTRIBUTION = 'Negative Distribution'
    ADJACENCY = 'Adjacency'
    BLOCKING = 'Blocking'
    SHELF_PLACEMENT = 'Shelf Placement'
    PRODUCT_SEQUENCE = 'Product Sequence'



    # Krishna Edits
    BAY_COUNT = "Bay Count"
    #End

    SHELF_PLACEMENT = 'Shelf Placement'
    SHELF_REGION = 'Shelf Region'
    BLOCKING = 'Blocking'
    MAX_BLOCK_ADJ = 'Max Block Adjacency'
    BLOCKING_PERCENT = 'Blocking Percent'
    BLOCK_ORIENTATION = 'Block Orientation'
    MULTI_BLOCK = 'Multi Block'
    ANCHOR = 'Anchor'
    SEQUENCE = 'Sequence'
    INTEGRATED = 'Integrated Core KPI'
    SERIAL = 'Serial Adj'
    RELATIVE_POSTION = 'Relative Position'
    SAME_AISLE = 'Same Aisle'
    SOS = 'SOS Linear'

    RESULT = 'Result'

    ''' KPIs Columns '''
    KPI_NAME = 'KPI Name'
    TYPE = 'KPI Type'
    PARENT = 'Parent'
    DEPENDENT = 'Dependent'
    DEPENDENT_RESULT = 'Dependent Result'
    SCENE_TYPE = 'Scene Type'
    HIERARCHY = 'Hierarchy'
    NUM = 'Numerator {}'
    DEN = 'Denominator {}'
    WIDTH_MM_ADV = 'width_mm_advance'
    FACE_COUNT = 'face_count'
    COUNT = 'count'

    ''' Columns '''
    EDGES = 'Edges'
    RESULT_TYPE = 'Result Type'

    ''' Constants '''
    COMMA_SPACE = ', '
    COMMA = ','
    KEEP = 'keep'
    SOS_EXCLUDE_FILTERS = {'product_type': ['Irrelevant']}
    ALLOWED_FILTERS = {'product_type': ['Other', 'Empty']}
    IGN_STACKING = {"stacking_layer": 1}
    ORIENTS = {'vertically', 'horizontally'}
    DIRECTIONS = ['left', 'right']
    REGIONS = ['left', 'center', 'right']
    NUM_REG = 3
    MM_FT = 304.8
    ALLOWED_FLAGS = ['unconnected', 'connected', 'encapsulated']
    ALL_SCENES_REQUIRED = 'All Scenes Required'

    COMP_COL_BASE = 'Store_Att_'


    LABEL_CONVERTERS = {'Segment': 'product_fk'}


