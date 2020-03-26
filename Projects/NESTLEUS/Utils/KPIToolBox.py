from Trax.Algo.Calculations.Core.DataProvider import Data
from Trax.Cloud.Services.Connector.Keys import DbUsers
from KPIUtils_v2.DB.PsProjectConnector import PSProjectConnector
import numpy as np
from Trax.Utils.Logging.Logger import Log
import pandas as pd
import os

from KPIUtils_v2.DB.CommonV2 import Common
from KPIUtils_v2.DB.Common import Common as CommonV1
from KPIUtils_v2.Calculations.AssortmentCalculations import Assortment
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
        self.common_v1 = CommonV1(self.data_provider)
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
        self.match_product_in_scene = self.match_product_in_scene[self.match_product_in_scene['stacking_layer'] == 1]
        self.assortment = Assortment(self.data_provider, common=self.common_v1)

    def main_calculation(self, *args, **kwargs):
        """
        This function calculates the KPI results.
        """
        kpi_set_fk = kwargs['kpi_set_fk']
        self.calculate_facing_count_and_linear_feet(kpi_set_fk=kpi_set_fk)

    def get_numerator_denominator_ids(self, kpi_id):
        static = self.kpi_static_data
        static_kpi = static[static['pk'] == kpi_id]
        numerator_id = static_kpi['numerator_type_fk'].iloc[0]
        denominator_id = static_kpi['denominator_type_fk'].iloc[0]

        return (numerator_id, denominator_id)

    def calculate_facing_count_and_linear_feet(self, id_scene_type):
        fk_kpi_level_2 = {
            'facings': 909,
            'facings_ign_stack': 910,
            'net_len_add_stack': 911,
            'net_len_ign_stack': 912
        }

        df_scene = self.scif[self.scif['template_fk'] == id_scene_type]
        sums = {key: df_scene[key].sum() for key, _ in fk_kpi_level_2.items()}

        for row in df_scene.itertuples():
            for key, fk in fk_kpi_level_2.items():
                numerator = getattr(row, key)  # row[key] #row.get(key) # this seems awkward
                denominator = sums.get(key)
                result = numerator / denominator

                self.common.write_to_db_result(
                    fk=fk,
                    level=None,
                    score=result,
                    session_fk=row.session_id,
                    numerator_id=row.item_id,
                    numerator_result=numerator,
                    denominator_id=row.store_id,
                    denominator_result=denominator,
                    result=result
                )

    # def calculate_facing_count_and_linear_feet(self, kpi_set_fk=None):
    #     kpi_name_facing_count = 'FACING_COUNT'
    #     kpi_name_linear_feet = 'LINEAR_FEET'
    #
    #     kpi_fk_facing_count = self.common.get_kpi_fk_by_kpi_name(kpi_name_facing_count)
    #     kpi_fk_linear_feet = self.common.get_kpi_fk_by_kpi_name(kpi_name_linear_feet)
    #     if kpi_set_fk == kpi_fk_facing_count:
    #
    #         product_fks = self.all_products['product_fk'][
    #             (self.all_products['category_fk'] == 32) | (self.all_products['category_fk'] == 5)]
    #         for product_fk in product_fks:
    #
    #             sos_filter = {'product_fk': product_fk}
    #
    #             facing_count = self.availability.calculate_availability(**sos_filter)
    #
    #             if facing_count > 0:
    #
    #                 self.common.write_to_db_result(fk=kpi_fk_facing_count, numerator_id=product_fk,
    #                                                numerator_result=facing_count,
    #                                                denominator_id=product_fk,
    #                                                result=facing_count, score=facing_count)
    #
    #                 general_filter = {'category_fk': [32, 5]}
    #
    #                 numerator_length = self.calculate_linear_share_of_shelf_with_numerator_denominator(
    #                     sos_filter, **general_filter)
    #
    #                 numerator_length = int(np.ceil(numerator_length * self.MM_TO_FEET_CONVERSION))
    #                 if numerator_length > 0:
    #                     self.common.write_to_db_result(fk=kpi_fk_linear_feet, numerator_id=product_fk,
    #                                                    numerator_result=numerator_length,
    #                                                    denominator_id=product_fk,
    #                                                    result=numerator_length, score=numerator_length)

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

        return numerator_width

    def calculate_base_footage(self):
        water_aisle_base_footage_kpi_fk = 913

        numerator_id, denominator_id = self.get_numerator_denominator_ids(water_aisle_base_footage_kpi_fk)

        scif = self.scif
        water_aisle = scif[scif['template_fk'] == 2]
        water_aisle_ids = water_aisle['pk'].unique()

        mpis = self.match_product_in_scene
        bottom_shelves = mpis[mpis['shelf_number_from_bottom'] == 1]
        water_aisle_bottom_shelves = bottom_shelves[bottom_shelves['scene_fk'].isin(water_aisle_ids)]
        base_footage = bottom_shelves['width_mm_advance'].sum()

        self.common.write_to_db_result(
            fk=water_aisle_base_footage_kpi_fk,
            session_fk=self.session_uid,
            numerator_result=base_footage,
            numerator_id=numerator_id,
            denominator_result=1,
            denominator_id=denominator_id
        )


    def calculate_share_space_length(self, **filters):
        """
        :param filters: These are the parameters which the data frame is filtered by.
        :return: The total shelf width (in mm) the relevant facings occupy.
        """
        filtered_matches = \
            self.match_product_in_scene[self.get_filter_condition(self.match_product_in_scene, **filters)]
        space_length = filtered_matches['width_mm_advance'].sum()
        return space_length

    # a tad messy
    def calculate_facings_per_shelf_level(self):
        mpis = self.match_product_in_scene
        scif = self.scif

        mpis_scene_bays = mpis.groupby(by=['scene_fk', 'bay_number'])['shelf_number'].max().reset_index(name='total_number_of_shelves')
        # mpis_scene_bays = mpis.groupby(by=['scene_fk', 'bay_number'])['shelf_number'].value_counts().reset_index(name='total_number_of_shelves')

        num_shelves_by_bay = {(row.scene_fk, row.bay_number): row.total_number_of_shelves for row in mpis_scene_bays.itertuples()}
        shelf_position = {

        }

        mpis['number_of_shelves'] = mpis.apply(lambda row: num_shelves_by_bay.get((row.scene_fk, row.bay_number)), axis=1)

        shelf_map = self.get_shelf_map()
        shelf_position_labels = ["Bottom", "Middle", "Eye", "Top"]

        def get_shelf_position(row):
            shelf_number_from_bottom = int(row.shelf_number_from_bottom)
            number_of_shelves = int(row.number_of_shelves)
            shelf_position = shelf_map.get((shelf_number_from_bottom, number_of_shelves))
            shelf_position_id = shelf_position_labels.index(shelf_position)+1

            return shelf_position_id

        mpis['shelf_position'] = mpis.apply(get_shelf_position, axis=1)
        #mpis['shelf_position'] = mpis.apply(lambda row: shelf_position_labels.index(shelf_map.get((int(row.shelf_number_from_bottom), int(row.number_of_shelves))))+1, axis=1)

        # what am I doing again?
        huh = mpis[mpis['stacking_layer'] == 1]
        erm = huh.groupby(['product_fk', 'shelf_position'])
        # this is not quite it

        # for product in .itertuples():
        #     self.common.write_to_db_result(
        #         fk="",
        #         session_fk="",
        #         numerator_result="",
        #         numerator_id="",
        #         denominator_result=1,
        #         denominator_id=""
        #     )


    def calculate_assortment(self):
        # filter scif to get rid of scene types other than 'Waters'
        self.scif = self.scif[self.scif['template_name'] == 'Water Aisle']
        # if there are no Waters scenes, we don't need assortment
        if self.scif.empty:
            return

        self.assortment.main_assortment_calculation()

    def commit_assortment_results_without_delete(self):
        self.common_v1.commit_results_data_without_delete_version2()

    def commit_assortment_results(self):
        self.common_v1.commit_results_data_to_new_tables()

    # this could probably be moved elsewhere
    @staticmethod
    def get_shelf_map():
        mapping = """7,3,Middle
            9,1,Bottom
            6,6,Top
            9,8,Eye
            10,6,Eye
            5,4,Eye
            2,1,Bottom
            6,2,Middle
            9,4,Middle
            5,1,Bottom
            8,5,Eye
            7,2,Middle
            10,4,Middle
            10,8,Eye
            3,3,Eye
            8,1,Bottom
            10,3,Middle
            7,6,Eye
            4,4,Top
            6,3,Middle
            2,2,Eye
            10,1,Bottom
            8,6,Eye
            5,3,Middle
            4,1,Bottom
            10,9,Top
            9,7,Eye
            6,4,Eye
            3,2,Middle
            8,2,Middle
            7,1,Bottom
            9,3,Middle
            10,5,Middle
            7,7,Top
            7,5,Eye
            8,7,Eye
            4,2,Middle
            9,6,Eye
            6,5,Eye
            5,5,Top
            8,3,Middle
            10,10,Top
            9,2,Bottom
            6,1,Bottom
            3,1,Bottom
            9,9,Top
            7,4,Eye
            10,7,Eye
            8,8,Top
            4,3,Eye
            9,5,Middle
            10,2,Bottom
            5,2,Middle
            8,4,Middle
            """

        shelf_map = """Bottom
Bottom	Eye								
Bottom	Middle	Eye							
Bottom	Middle	Eye	Top						
Bottom	Middle	Middle	Eye	Top					
Bottom	Middle	Middle	Eye	Eye	Top				
Bottom	Middle	Middle	Eye	Eye	Eye	Top			
Bottom	Middle	Middle	Middle	Eye	Eye	Eye	Top		
Bottom	Bottom	Middle	Middle	Middle	Eye	Eye	Eye	Top	
Bottom	Bottom	Middle	Middle	Middle	Eye	Eye	Eye	Top	Top
"""

        shelf_map = shelf_map.split('\n')
        shelf_map = [shelf.split('\t') for shelf in shelf_map]
        shelf_map = [shelf for shelf in shelf_map if len(shelf) > 1]
        shelf_map = {(x+1, y+1): col for y, row in enumerate(shelf_map) for x, col in enumerate(row) if col}

        return shelf_map


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
                # Log.warning('field {} is not in the Data Frame'.format(field))
                pass

        return filter_condition
