import os

__author__ = 'hunter'


class Const(object):
    TEMPLATE_PATH = os.path.join(os.path.dirname(
        os.path.realpath(__file__)), 'MSC Template v8.xlsx')
    MSC = 'Market Street Challenge'
    MANUFACTURER_FK = 1

    # sheets
    KPIS = 'KPIs'
    AVAILABILITY = 'Availability'
    DOUBLE_AVAILABILITY = 'Double Availability'
    FACINGS = 'Facings'
    SHARE_OF_DISPLAYS = 'Share of Displays'
    DISPLAY_PRESENCE = 'Display Presence & Location'

    SHEETS = [KPIS, AVAILABILITY, DOUBLE_AVAILABILITY, FACINGS, SHARE_OF_DISPLAYS, DISPLAY_PRESENCE]

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
    GROUP2_MINIMUM_FACINGS = 'Group 2 Minimum Facing'

    # Facings columns
    NUMERATOR_TYPE = 'numerator_type'
    NUMERATOR_VALUE = 'numerator_value'
    DENOMINATOR_TYPE = 'denominator_type'
    DENOMINATOR_VALUE = 'denominator_value'

    # Share of Displays
    THRESHOLD = 'Threshold Value'

    # Display Presence & Location columns
    ACTIVATION_TYPE = 'activation_type'
    ACTIVATION_VALUE = 'activation_value'

    # KPI result values
    PASS = 'Pass'
    FAIL = 'Fail'

    NUMERIC_VALUES_TYPES = ['size']
