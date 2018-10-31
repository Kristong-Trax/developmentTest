import os
__author__ = 'huntery'


class Const(object):
    # excel template paths and specifics

    TEMPLATE_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.realpath(__file__))), '..', 'DIAGEOCO',
                                 'Data', 'Diageo Colombia - Kpi Bank - 29102018.xlsx')
    TOUCH_POINT_SHEET_NAME = 'Touch Point'
    TOUCH_POINT_HEADER_ROW = 2


    # KPI names
    BRAND_BLOCKING_BRAND_FROM_CATEGORY = 'Brand Blocking'
    SECONDARY_DISPLAY = 'Secondary display'
    RELATIVE_POSITION = 'Relative Position'
    BRAND_POURING = 'Brand Pouring'
