import os

__author__ = 'hunter'


class Const(object):
    TEMPLATE_PATH = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'MSC Template v1.0.xlsx')

    # sheets
    KPIS = 'KPIs'
    AVAILABILITY = 'Availability'
    DOUBLE_AVAILABILITY = 'Double Availability'
    PRESENCE = 'Presence'

    SHEETS = [KPIS, AVAILABILITY, DOUBLE_AVAILABILITY, PRESENCE]

    # KPIs columns
    KPI_NAME = 'KPI Name'
    KPI_TYPE = 'Type'
    SCENE_TYPE = 'Scene Type'
    STORE_TYPE = 'store_type'

    # Availability columns
    MANUFACTURER = 'manufacturer'
    BRAND = 'brand'
    MINIMUM_FACINGS = 'Minimum facings'
    EXCLUDE = 'EXCLUDE'

    # Double Availability columns
    GROUP1_PARAM = 'Group 1 Param'
    GROUP1_VALUE = 'Group 1 Value'
    GROUP2_PARAM = 'Group 2 Param'
    GROUP2_VALUE = 'Group 2 Value'
    GROUP1_MINIMUM_FACINGS = 'Group 1 Minimum Facing'
    GROUP2_MINIMUM_FACINGS = 'Group 2 Minimum Facings'

    # Presence columns
    SIZE = 'size'
    NUMBER_OF_SUB_PACKAGES = 'number_of_sub_packages'
    MINIMUM_BRANDS = 'Minimum Brands'
    ATT4 = 'att4'

