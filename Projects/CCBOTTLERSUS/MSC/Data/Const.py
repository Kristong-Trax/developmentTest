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
    PER_SCENE_AVAILABILITY = 'Per Scene Availability'
    DOUBLE_AVAILABILITY = 'Double Availability'
    FACINGS = 'Facings'
    SHARE_OF_SCENES = 'Share of Scenes'
    SHARE_OF_POCS = 'Share of POCs'

    SHEETS = [KPIS, AVAILABILITY, PER_SCENE_AVAILABILITY, DOUBLE_AVAILABILITY, FACINGS, SHARE_OF_SCENES, SHARE_OF_POCS]

    # KPIs columns
    KPI_NAME = 'KPI Name'
    KPI_TYPE = 'Type'
    SCENE_TYPE = 'Scene Type'
    STORE_TYPE = 'store_type'

    # Availability columns
    MINIMUM_SKUS = 'Minimum SKUs'
    MINIMUM_BRANDS = 'Minimum Brands'
    MINIMUM_PACKAGES = 'Minimum Packages'
    MINIMUM_FACINGS = 'Minimum Facings'

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

    # value types
    NUM_TYPE = 'numerator_type'
    DEN_TYPE = 'denominator_type'
    NUM_VALUE = 'numerator_value'
    DEN_VALUE = 'denominator_value'
    NUM_EXCLUDE_TYPE = 'numerator_exclude_type'
    NUM_EXCLUDE_VALUE = 'numerator_exclude_value'
    EXCLUDED_TYPE = 'excluded_type'
    EXCLUDED_VALUE = 'excluded_value'

