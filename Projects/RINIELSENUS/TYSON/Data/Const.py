__author__ = 'hunter'


class Const(object):

    # sheets
    KPIS = 'KPIs'
    ANCHOR = 'Anchor'
    BLOCK = 'Block'
    BLOCK_ADJACENCY = 'Block Adjacency'
    ADJACENCY = 'Adjacency'
    SHEETS = [KPIS, ANCHOR, BLOCK, BLOCK_ADJACENCY, ADJACENCY]

    # columns universal to all sheets
    KPI_NAME = 'KPI name'
    KPI_TYPE = 'KPI Type'
    TARGET = 'target'
    PARAM = 'param'
    VALUE = 'value'

    # columns of KPIs
    STORE_POLICY = 'Store Policy'
    STORE_LOCATION = 'Store Location'
    WRITE_NA = 'Write N/A'

    # columns of Block Adjacency
    ANCHOR_PARAM = 'anchor_param'
    ANCHOR_VALUE = 'anchor_value'
    TESTED_PARAM = 'tested_param'
    TESTED_VALUE = 'tested_value'
    LIST_ATTRIBUTE = 'list_attribute'


