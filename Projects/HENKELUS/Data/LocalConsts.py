import os

__author__ = 'Nicolas Keeton'


class Consts(object):
    template_file_name = 'KPI Template - Working 5-11-2020 v4.xlsx'
    KPI_TEMPLATE_PATH = os.path.join(os.path.dirname(os.path.realpath(__file__)),
                                     template_file_name)

    FACING_COUNT_SHEET = 'Facings Count'
    SKU_COUNT_SHEET = 'SKU Count'
    SMART_TAG_SHEET = 'Smart Tag Presence'
    LINEAR_SHEET = 'Linear Measure'
    BASE_MEASURE_SHEET = 'Base Measure'
    VERTICAL_SHELF_SHEET = 'Vertical Shelf Position'
    HORIZONTAL_SHELF_SHEET = 'Horizontal Shelf Position'
    BLOCKING_SHEET = 'Blocking'
    SHELF_MAP_SHEET = 'Shelf Map'
    BLOCK_COMPOSITION_SHEET = 'Block Composition'

    relevant_kpi_sheets = [FACING_COUNT_SHEET, SKU_COUNT_SHEET, SMART_TAG_SHEET,
                           LINEAR_SHEET, BASE_MEASURE_SHEET,
                           VERTICAL_SHELF_SHEET, BLOCKING_SHEET, HORIZONTAL_SHELF_SHEET, SHELF_MAP_SHEET,
                           BLOCK_COMPOSITION_SHEET
                           ]

    OWN_MANUFACTURER_FK = 49

    VERTICAL_SHELF_POS_DICT = {
        'Eye': 0,
        'Top': 1,
        'Bottom': 2,
        'Middle': 3
    }

    HORIZONTAL_SHELF_POS_DICT = {
        'Left': 4,
        'Center': 5,
        'Right': 6
    }

    CUSTOM_RESULTS = {
        'Eye': 5,
        'Top': 4,
        'Bottom': 7,
        'Middle': 6,
        'Left': 14,
        'Center': 15,
        'Right': 16,
        'Yes': 12,
        'No': 13,
        'Not blocked': 11,
        'Blocked': 10,
        'Horizontal': 9,
        'Vertical': 8
    }
