import os
__author__ = 'Sam'


class Const(object):
    ''' Holds Constants '''

    DICTIONARY_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.realpath(__file__))), 'Data',
                                   'Dictionary.xlsx')
    TMB_MAP_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.realpath(__file__))), 'Data',
                                   'TopMiddleBottom_Map.xlsx')

    ''' Sheets '''

    PRIMARY_LOCATION = 'Primary Location'
    MAX_BLOCK_ADJACENCY = 'Max Block Adjacency'



    KPIS = 'KPIs'
    TMB = 'Top-Middle-Bottom'
    COUNT = 'Count of'
    SET_COUNT = 'Set Count of'
    BLOCKING = 'Blocking'
    BASE_MEASURE = 'Base Measure'
    SURVEY = 'Survey Question'
    ORIENT = 'Product Orientation'
    PRESENCE = 'Presence'
    PRESENCE_WITHIN_BAY = 'Presence within Bay'
    PRESENCE_WITHIN_BAY_MEX = 'Presence on Same Bay Mex'
    COUNT_SHELVES = 'Count of Shelves'
    PERCENT = 'Percent'
    AGGREGATION = 'Aggregation'
    TMB_MAP = 'Top-Middle-Bottom Map'
    ADJACENCY = 'Adjacency'
    ADJACENCY_MIX = 'Adjacency Mix'
    IADJACENCY = 'IAdjacency'
    STOCKING = 'Stocking Location'
    YOGURT_MAP = 'Yogurt Location Map'
    ANCHOR = 'Anchor'
    ANCHOR_LIST = 'Anchor List'
    SURVEY_QUESTION = 'Survey Question'
    VARIETY_COUNT = 'Variety Count'
    RESULT = 'Result'
    SEQUENCE = 'Sequence'

    # SHEETS = [KPIS, TMB, COUNT, BLOCKING, BASE_MEASURE, SURVEY, ORIENT, PRESENCE, COUNT_SHELVES, PERCENT, AGGREGATION,
    #           ADJACENCY, TMB_MAP, STOCKING, YOGURT_MAP, ANCHOR, ANCHOR_LIST, SURVEY_QUESTION, VARIETY_COUNT, RESULT,
    #           SEQUENCE]

    RESULT = 'Result'

    ''' KPIs Columns '''


    KPI_NAME = 'KPI Name'
    TYPE = 'Type'
    PARENT = 'Parent'
    DEPENDENT = 'Dependent'
    DEPENDENT_RESULT = 'Dependent Result'
    TEMPLATE_GROUP = 'Template Group'
    SCENE_TYPE = 'Scene Type'
    STORE_TYPE = 'Store Type'
    CHANNEL = 'Channel'
    SESSION_LEVEL = 'Session Level'

    ''' Aggregation Columns'''
    AGGREGATION_LEVELS = 'Aggregation Levels'
    SOS_TYPE = 'SOS Type'

    ''' Columns '''
    EDGES = 'Edges'
    RESULT_TYPE = 'Result Type'

    ''' Constants '''
    PRIV_LABEL_SKU = 106529
    PRIV_LABEL_BRAND = 6943
    PRIV_LABEL_MAN = 5282
    PRIV_LABEL_NAME = 'Generic Private Label'
    MM_TO_FT = 304.8
    COMMA_SPACE = ', '
    COMMA = ','
    KEEP = 'keep'


    ALLOWED_FLAGS = ['unconnected', 'connected', 'encapsulated']


    INTEGRATED = 'Fully Integrated'
    ADJACENT = 'Adjacent Section'
    SAME_AISLE = 'Same Aisle, Not Adjacent'
    NO_CONNECTION = 'Not In The Same Aisle'
    SOS_EXCLUDE_FILTERS = {'product_type': ['Irrelevant', 'Empty']}
    ALLOWED_FILTERS = {'product_type': ['Other', 'Empty']}
    IGN_STACKING = {"stacking_layer": 1}
    SOS_COLUMN_DICT = {
                        'SOS': 'facings_ign_stack',
                        'Linear SOS': 'net_len_ign_stack',
                        'Average': 'count',
                       }
    REF_COLS = ['GMI_SEGMENT', 'Segment', 'GMI_SIZE', 'Package General Shape', 'Natural/ Organic', 'GMI_CATEGORY',
                 'GMI_AUDIENCE']
    TMB_VALUES = ['Top', 'Middle', 'Bottom']
    END_OF_CAT = 'END OF CATEGORY'
    PRIV_SCIF_COLS = ['facings', 'facings_ign_stack', 'net_len_add_stack', 'gross_len_add_stack', 'net_len_split_stack',
                      'gross_len_split_stack', 'net_len_ign_stack', 'gross_len_ign_stack', 'net_area_split_stack',
                      'gross_area_split_stack', 'net_area_ign_stack', 'gross_area_ign_stack']




