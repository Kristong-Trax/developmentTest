import os
__author__ = 'huntery'


class Const(object):
    # excel template paths and specifics

    TEMPLATE_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.realpath(__file__))), '..', 'DIAGEOCO_SAND',
                                 'Data', 'Diageo Colombia - Kpi Bank - 29102018.xlsx')
    RELATIVE_POSITIONING_SHEET_NAME = '2.1 Relative Positioning'
    RELATIVE_POSITIONING_HEADER_ROW = 4
    BRAND_BLOCKING_SHEET_NAME = '2.2 Brand Blocking'
    BRAND_BLOCKING_HEADER_ROW = 6
    BRAND_POURING_SHEET_NAME = '5.1 Brand Pouring Status '
    BRAND_POURING_HEADER_ROW = 3
    TOUCH_POINT_SHEET_NAME = 'Touch Point'
    TOUCH_POINT_HEADER_ROW = 2


    # KPI names
    BRAND_BLOCKING_BRAND_FROM_CATEGORY = 'Brand Blocking'
    SECONDARY_DISPLAY = 'Secondary display'
    RELATIVE_POSITION = 'Relative Position'
