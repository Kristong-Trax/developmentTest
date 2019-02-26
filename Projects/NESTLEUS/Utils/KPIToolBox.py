
from Trax.Algo.Calculations.Core.DataProvider import Data
from Trax.Cloud.Services.Connector.Keys import DbUsers
from KPIUtils_v2.DB.PsProjectConnector import PSProjectConnector
from Trax.Utils.Logging.Logger import Log
import pandas as pd
import os

from KPIUtils_v2.DB.CommonV2 import Common
# from KPIUtils_v2.Calculations.AssortmentCalculations import Assortment
from KPIUtils_v2.Calculations.AvailabilityCalculations import Availability
# from KPIUtils_v2.Calculations.NumberOfScenesCalculations import NumberOfScenes
# from KPIUtils_v2.Calculations.PositionGraphsCalculations import PositionGraphs
from KPIUtils_v2.Calculations.SOSCalculations import SOS
# from KPIUtils_v2.Calculations.SequenceCalculations import Sequence
# from KPIUtils_v2.Calculations.SurveyCalculations import Survey

from KPIUtils_v2.Calculations.CalculationsUtils import GENERALToolBoxCalculations

__author__ = 'nicolaske'

KPI_RESULT = 'report.kpi_results'
KPK_RESULT = 'report.kpk_results'
KPS_RESULT = 'report.kps_results'


class NESTLEUSToolBox:
    LEVEL1 = 1
    LEVEL2 = 2
    LEVEL3 = 3

    EXCLUDE_FILTER = 0
    INCLUDE_FILTER = 1
    CONTAIN_FILTER = 2
    EXCLUDE_EMPTY = False
    INCLUDE_EMPTY = True

    STRICT_MODE = ALL = 1000
    ignore_stacking = False
    EMPTY = 'Empty'
    DEFAULT = 'Default'
    TOP = 'Top'
    BOTTOM = 'Bottom'

    def __init__(self, data_provider, output):
        self.output = output
        self.data_provider = data_provider
        self.common = Common(self.data_provider)
        self.project_name = self.data_provider.project_name
        self.session_uid = self.data_provider.session_uid
        self.products = self.data_provider[Data.PRODUCTS]
        self.all_products = self.data_provider[Data.ALL_PRODUCTS]
        self.match_product_in_scene = self.data_provider[Data.MATCHES]
        self.visit_date = self.data_provider[Data.VISIT_DATE]
        self.session_info = self.data_provider[Data.SESSION_INFO]
        self.scene_info = self.data_provider[Data.SCENES_INFO]
        self.store_id = self.data_provider[Data.STORE_FK]
        self.scif = self.data_provider[Data.SCENE_ITEM_FACTS]
        self.rds_conn = PSProjectConnector(self.project_name, DbUsers.CalculationEng)
        self.kpi_static_data = self.common.get_kpi_static_data()
        self.new_kpi_static_data = self.common.get_new_kpi_static_data()
        self.kpi_results_queries = []
        self.linear_calc = SOS(self.data_provider)
        self.availability = Availability(self.data_provider)
        self.ignore_stacking = False
        self.facings_field = 'facings' if not self.ignore_stacking else 'facings_ign_stack'
        self.MM_TO_FEET_CONVERSION = 0.0032808399

    def main_calculation(self, *args, **kwargs):
        """
        This function calculates the KPI results.
        """
        kpi_set_fk = kwargs['kpi_set_fk']
        self.calculate_facing_count_and_linear_feet(kpi_set_fk = kpi_set_fk)








    def calculate_facing_count_and_linear_feet(self, kpi_set_fk= None):
        kpi_name_facing_count = 'FACING_COUNT'
        kpi_name_linear_feet = 'LINEAR_FEET'

        kpi_fk_facing_count = self.common.get_kpi_fk_by_kpi_name(kpi_name_facing_count)
        kpi_fk_linear_feet = self.common.get_kpi_fk_by_kpi_name(kpi_name_linear_feet)
        if kpi_set_fk == kpi_fk_facing_count:

            product_fks = self.all_products['product_fk'][
                (self.all_products['category_fk'] == 32) | (self.all_products['category_fk'] == 5)]
            for product_fk in product_fks:

                sos_filter = {'product_fk': product_fk}


                facing_count = self.availability.calculate_availability(**sos_filter)

                if facing_count > 0:

                    self.common.write_to_db_result(fk=kpi_fk_facing_count, numerator_id=product_fk,
                                           numerator_result=facing_count,
                                           denominator_id=product_fk,
                                           result=facing_count, score=facing_count)

                    general_filter = {'category_fk': [32, 5]}

                    numerator_length = self.calculate_linear_share_of_shelf_with_numerator_denominator(
                        sos_filter, **general_filter)

                    numerator_length = int(round(numerator_length * self.MM_TO_FEET_CONVERSION))
                    if numerator_length > 0:

                        self.common.write_to_db_result(fk=kpi_fk_linear_feet, numerator_id=product_fk,
                                                       numerator_result=numerator_length,
                                                       denominator_id=product_fk,
                                                       result=numerator_length, score=numerator_length)


    def calculate_linear_share_of_shelf_with_numerator_denominator(self, sos_filters, include_empty=EXCLUDE_EMPTY,
                                                                   **general_filters):
        """
        :param sos_filters: These are the parameters on which ths SOS is calculated (out of the general DF).
        :param include_empty: This dictates whether Empty-typed SKUs are included in the calculation.
        :param general_filters: These are the parameters which the general data frame is filtered by.
        :return: The Linear SOS ratio.
        """
        if include_empty == self.EXCLUDE_EMPTY:
            general_filters['product_type'] = (self.EMPTY, self.EXCLUDE_FILTER)

        numerator_width = self.calculate_share_space_length(**dict(sos_filters, **general_filters))

        return  numerator_width

    def calculate_share_space_length(self, **filters):
        """
        :param filters: These are the parameters which the data frame is filtered by.
        :return: The total shelf width (in mm) the relevant facings occupy.
        """
        filtered_matches = \
            self.match_product_in_scene[self.get_filter_condition(self.match_product_in_scene, **filters)]
        space_length = filtered_matches['width_mm_advance'].sum()
        return space_length


    def get_filter_condition(self, df, **filters):
        """
        :param df: The data frame to be filters.
        :param filters: These are the parameters which the data frame is filtered by.
                       Every parameter would be a tuple of the value and an include/exclude flag.
                       INPUT EXAMPLE (1):   manufacturer_name = ('Diageo', DIAGEOAUGENERALToolBox.INCLUDE_FILTER)
                       INPUT EXAMPLE (2):   manufacturer_name = 'Diageo'
        :return: a filtered Scene Item Facts data frame.
        """
        if not filters:
            return df['pk'].apply(bool)
        if self.facings_field in df.keys():
            filter_condition = (df[self.facings_field] > 0)
        else:
            filter_condition = None
        for field in filters.keys():
            if field in df.keys():
                if isinstance(filters[field], tuple):
                    value, exclude_or_include = filters[field]
                else:
                    value, exclude_or_include = filters[field], self.INCLUDE_FILTER
                if not value:
                    continue
                if not isinstance(value, list):
                    value = [value]
                if exclude_or_include == self.INCLUDE_FILTER:
                    condition = (df[field].isin(value))
                elif exclude_or_include == self.EXCLUDE_FILTER:
                    condition = (~df[field].isin(value))
                elif exclude_or_include == self.CONTAIN_FILTER:
                    condition = (df[field].str.contains(value[0], regex=False))
                    for v in value[1:]:
                        condition |= df[field].str.contains(v, regex=False)
                else:
                    continue
                if filter_condition is None:
                    filter_condition = condition
                else:
                    filter_condition &= condition
            else:
                Log.warning('field {} is not in the Data Frame'.format(field))

        return filter_condition