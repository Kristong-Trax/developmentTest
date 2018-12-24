
__author__ = 'Sam'


class Const(object):
    ''' Holds Constants '''

    ''' Sheets '''
    KPIS = 'KPIs'
    TMB = 'Top-Middle-Bottom'
    COUNT = 'Count of'
    BLOCKING = 'Blocking'
    BASE_MEASURE = 'Base Measure'
    SURVEY = 'Survey Question'
    ORIENT = 'Product Orientation'
    PRESENCE = 'Presence'
    COUNT_SHELVES = 'Count of Shelves'
    PERCENT = 'Percent'
    AGGREGATION = 'Aggregation'
    TMB_MAP = 'Top-Middle-Bottom Map'
    ADJACENCY = 'Adjacency'
    IADJACENCY = 'Integrated Adjacency'
    STOCKING = 'Stocking Location'
    YOGURT_MAP = 'Yogurt Location Map'
    ANCHOR = 'Anchor'
    ANCHOR_LIST = 'Anchor List'
    SURVEY_QUESTION = 'Survey Question'
    VARIETY_COUNT = 'Variety Count'
    RESULT = 'Result'

    SHEETS = [KPIS, TMB, COUNT, BLOCKING, BASE_MEASURE, SURVEY, ORIENT, PRESENCE, COUNT_SHELVES, PERCENT, AGGREGATION,
              ADJACENCY, TMB_MAP, STOCKING, YOGURT_MAP, ANCHOR, ANCHOR_LIST, SURVEY_QUESTION, VARIETY_COUNT, RESULT]

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
    MM_TO_FT = 304.8
    COMMA_SPACE = ', '
    COMMA = ','
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




