import os

__author__ = 'hunter'


class Const(object):
    TEMPLATE_PATH = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'MSC Template v1.1.xlsx')

    # sheets
    KPIS = 'KPIs'
    AVAILABILITY = 'Availability'
    DOUBLE_AVAILABILITY = 'Double Availability'
    FACINGS = 'Facings'

    SHEETS = [KPIS, AVAILABILITY, DOUBLE_AVAILABILITY, FACINGS]

    # KPIs columns
    KPI_NAME = 'KPI Name'
    KPI_TYPE = 'Type'
    SCENE_TYPE = 'Scene Type'
    STORE_TYPE = 'store_type'

    # Availability columns
    MANUFACTURER = 'manufacturer'
    BRAND = 'brand'
    ATT1 = 'att1'
    ATT3 = 'att3'
    MINIMUM_FACINGS = 'Minimum facings'
    SIZE = 'size'
    SUB_PACKAGES = 'number_of_sub_packages'
    EXCLUDED_TYPE = 'excluded_type'
    EXCLUDED_VALUE = 'excluded_value'

    # Double Availability columns
    GROUP1_BRAND = 'Brand Group 1'
    GROUP2_BRAND = 'Brand Group 2'
    GROUP1_MINIMUM_FACINGS = 'Group 1 Minimum Facing'
    GROUP2_MINIMUM_FACINGS = 'Group 2 Minimum Facings'

    # Facings columns
    NUMERATOR_TYPE = 'numerator_types'
    NUMERATOR_VALUE = 'numerator_value'
    DENOMINATOR_TYPE = 'denominator_type'
    DENOMINATOR_VALUE = 'denominator_value'



    NUMERIC_VALUES_TYPES = ['size']

