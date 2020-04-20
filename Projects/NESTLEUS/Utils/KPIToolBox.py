from Trax.Algo.Calculations.Core.DataProvider import Data
from Trax.Cloud.Services.Connector.Keys import DbUsers
from KPIUtils_v2.DB.PsProjectConnector import PSProjectConnector
# import numpy as np
from Trax.Utils.Logging.Logger import Log
import pandas as pd
# import os
from Projects.NESTLEUS.Utils import Const

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
        self.own_manufacturer_fk = int(self.data_provider.own_manufacturer.param_value.values[0])

        self.categories = {category: self.get_category_fk(category) for category in Const.CATEGORIES}
        self.water_templates = {template: self.get_template_fk(template) for template in Const.WATER_TEMPLATES}

        self.mpis = self.match_product_in_scene \
            .merge(self.products, how="left", on="product_fk", suffixes=('', '_products')) \
            .merge(self.scene_info, how="left", on="scene_fk", suffixes=('', '_info'))
        self.mpis = self.mpis[self.mpis['product_name'] != Const.IRRELEVANT]
        self.mpis = self.mpis[self.mpis['category_fk'].isin(self.categories.values())]

    def main_calculation(self, *args, **kwargs):
        """
        This function calculates the KPI results.
        """

        # kpi_set_fk = kwargs['kpi_set_fk']
        # self.calculate_facing_count_and_linear_feet(kpi_set_fk=kpi_set_fk)

        if self.mpis[self.mpis['template_fk'].isin(self.water_templates.values())].empty:
            Log.info("Session: {} contains no water products.".format(self.session_uid))
            return

        self.calculate_facing_count_and_linear_feet()
        self.calculate_base_footage()
        self.calculate_facings_per_shelf_level()
        self.calculate_display_type(self.water_templates.get('Water Display'))
        self.calculate_display_type(self.water_templates.get('Water Display'), "NESTLE HOLDINGS INC")



    def calculate_facing_count_and_linear_feet(self):
        relevant_kpis = ['facings', 'facings_ign_stack', 'net_len_add_stack', 'net_len_ign_stack']
        relevant_kpis = {kpi: self.common.get_kpi_fk_by_kpi_name(Const.KPIs.get(kpi)['DB']) for kpi in relevant_kpis}

        water_scif = self.scif[self.scif['category_fk'].isin(self.categories.values())]
        water_scif = water_scif[water_scif['template_fk'].isin(self.water_templates.values())]
        water_scif = water_scif.groupby(by=['template_fk', 'product_fk'], as_index=False).sum()

        # calculates the sum of the values for each kpi for each template
        template_sums = water_scif.groupby(by=['template_fk']).sum()

        water_scif[[kpi+"_sum" for kpi in relevant_kpis.keys()]] = \
            water_scif['template_fk'].apply(
                lambda template: pd.Series([template_sums.get_value(template, kpi) for kpi in relevant_kpis.keys()])
            )

        for row in water_scif.itertuples():
            for kpi, fk in relevant_kpis.items():
                numerator = getattr(row, kpi)
                denominator = getattr(row, kpi+"_sum")

                if denominator < numerator:
                    Log.error("`denominator`: {} is not greater than `numerator`: {}".format(denominator, numerator))

                self.common.write_to_db_result(
                    fk=fk,
                    numerator_id=row.product_fk,
                    numerator_result=numerator,
                    denominator_id=row.template_fk,
                    denominator_result=denominator,
                    result=numerator / denominator
                )

    def calculate_base_footage(self):
        water_aisle_base_footage_kpi_fk = \
            self.common.get_kpi_fk_by_kpi_name(Const.KPIs.get('water_aisle_base_footage')['DB'])
        store_id = self.session_info.get_value(0, 'store_fk')

        water_category = self.mpis[self.mpis['category_fk'].isin(self.categories.values())]
        water_aisle = water_category[water_category['template_fk'] == self.water_templates.get('Water Aisle')]

        water_aisle_bottom_shelf = water_aisle[water_aisle['shelf_number_from_bottom'] == 1]
        water_aisle_bottom_shelf_ign_stacking = water_aisle_bottom_shelf[water_aisle_bottom_shelf['stacking_layer'] == 1]

        base_mm = water_aisle_bottom_shelf_ign_stacking['width_mm_advance'].sum()
        base_footage = self.mm_to_feet(base_mm)

        self.common.write_to_db_result(
            fk=water_aisle_base_footage_kpi_fk,
            numerator_result=base_footage,
            numerator_id=self.get_category_fk('Water'),
            denominator_result=1,
            denominator_id=store_id,
            result=base_footage
        )

    def calculate_facings_per_shelf_level(self):
        """

        """

        relevant_kpi = 'FACINGS_BY_SHELF_POSITION'
        relevant_kpi_fk = self.common.get_kpi_fk_by_kpi_name(relevant_kpi)

        # calculates the number of shelves in each bay in each scene
        mpis_scene_bays = self.mpis.groupby(by=['scene_fk', 'bay_number'])['shelf_number_from_bottom'].max() \
            .reset_index(name='total_number_of_shelves')

        self.mpis = self.mpis.merge(mpis_scene_bays, on=['scene_fk', 'bay_number'])

        shelf_map = self.get_shelf_map()
        shelf_position_labels = ["Bottom", "Middle", "Eye", "Top"]

        def get_shelf_position(row):
            shelf_number_from_bottom = int(row.shelf_number_from_bottom)
            number_of_shelves = int(row.total_number_of_shelves)

            shelf_position = shelf_map.get((shelf_number_from_bottom, number_of_shelves))
            shelf_position_id = shelf_position_labels.index(shelf_position)+1

            return shelf_position_id

        water_aisle = self.mpis[self.mpis['template_fk'] == self.get_template_fk('Water Aisle')]
        water_aisle['shelf_position'] = water_aisle.apply(get_shelf_position, axis=1)
        water_aisle_bottom_layer = water_aisle[water_aisle['stacking_layer'] == 1]

        num_product_facings_by_shelf_position = water_aisle_bottom_layer \
            .groupby(['product_fk', 'shelf_position'])['product_fk'] \
            .count().reset_index(name='product_count_per_shelf_position')

        for product in num_product_facings_by_shelf_position.itertuples():
            numerator = product.product_count_per_shelf_position

            self.common.write_to_db_result(
                fk=relevant_kpi_fk,
                numerator_result=numerator,
                numerator_id=product.product_fk,
                denominator_result=1,
                denominator_id=product.shelf_position,
                result=product.product_count_per_shelf_position
            )

    def calculate_display_type(self, display_type_id, manufacturer_name=None):
        """
        :param display_type_id: ID of template/display type
        :param manufacturer_name: Name of Manufacturer
        """
        store_id = self.session_info.get_value(0, 'store_fk')
        kpi_fk = 916 if manufacturer_name else 915

        display = self.scif[self.scif['template_fk'] == display_type_id]
        display = display[display['product_name'] != Const.IRRELEVANT]

        if manufacturer_name:
            display = display[display['manufacturer_local_name'] == manufacturer_name]

        count = len(display['scene_id'].unique())

        self.common.write_to_db_result(
            fk=kpi_fk,
            numerator_result=count,
            numerator_id=self.own_manufacturer_fk,
            denominator_result=1,
            denominator_id=store_id,
            result=count
        )

    def get_category_fk(self, category_name):
        try:
            return self.all_products[self.all_products['category'] == category_name]['category_fk'].iloc[0]
        except IndexError:
            return None

    def get_template_fk(self, template_name):
        try:
            return self.scif[self.scif['template_name'] == template_name]['template_fk'].iloc[0]
        except IndexError:
            return None

    @staticmethod
    def get_shelf_map():
        """
        :return A dict representing (shelf_number_from_bottom, number_of_shelves): shelf_position
        """
        with open(Const.SHELF_MAP_PATH) as f:
            shelf_map = pd.read_excel(f, header=None)

        shelf_map = {(x + 1, y + 1): col for y, row in shelf_map.iterrows() for x, col in enumerate(row) if
                     pd.notna(col)}

        return shelf_map

    @staticmethod
    def mm_to_feet(n):
        return n / 1000.0 * 3.28084

    # def calculate_share_space_length(self, **filters):
    #     """
    #     :param filters: These are the parameters which the data frame is filtered by.
    #     :return: The total shelf width (in mm) the relevant facings occupy.
    #     """
    #     filtered_matches = \
    #         self.match_product_in_scene[self.get_filter_condition(self.match_product_in_scene, **filters)]
    #     space_length = filtered_matches['width_mm_advance'].sum()
    #     return space_length

    # def calculate_linear_share_of_shelf_with_numerator_denominator(self, sos_filters, include_empty=EXCLUDE_EMPTY,
    #                                                                **general_filters):
    #     """
    #     :param sos_filters: These are the parameters on which ths SOS is calculated (out of the general DF).
    #     :param include_empty: This dictates whether Empty-typed SKUs are included in the calculation.
    #     :param general_filters: These are the parameters which the general data frame is filtered by.
    #     :return: The Linear SOS ratio.
    #     """
    #     if include_empty == self.EXCLUDE_EMPTY:
    #         general_filters['product_type'] = (self.EMPTY, self.EXCLUDE_FILTER)
    #
    #     numerator_width = self.calculate_share_space_length(**dict(sos_filters, **general_filters))
    #
    #     return numerator_width

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

    # def get_filter_condition(self, df, **filters):
    #     """
    #     :param df: The data frame to be filters.
    #     :param filters: These are the parameters which the data frame is filtered by.
    #                    Every parameter would be a tuple of the value and an include/exclude flag.
    #                    INPUT EXAMPLE (1):   manufacturer_name = ('Diageo', DIAGEOAUGENERALToolBox.INCLUDE_FILTER)
    #                    INPUT EXAMPLE (2):   manufacturer_name = 'Diageo'
    #     :return: a filtered Scene Item Facts data frame.
    #     """
    #     if not filters:
    #         return df['pk'].apply(bool)
    #     if self.facings_field in df.keys():
    #         filter_condition = (df[self.facings_field] > 0)
    #     else:
    #         filter_condition = None
    #     for field in filters.keys():
    #         if field in df.keys():
    #             if isinstance(filters[field], tuple):
    #                 value, exclude_or_include = filters[field]
    #             else:
    #                 value, exclude_or_include = filters[field], self.INCLUDE_FILTER
    #             if not value:
    #                 continue
    #             if not isinstance(value, list):
    #                 value = [value]
    #             if exclude_or_include == self.INCLUDE_FILTER:
    #                 condition = (df[field].isin(value))
    #             elif exclude_or_include == self.EXCLUDE_FILTER:
    #                 condition = (~df[field].isin(value))
    #             elif exclude_or_include == self.CONTAIN_FILTER:
    #                 condition = (df[field].str.contains(value[0], regex=False))
    #                 for v in value[1:]:
    #                     condition |= df[field].str.contains(v, regex=False)
    #             else:
    #                 continue
    #             if filter_condition is None:
    #                 filter_condition = condition
    #             else:
    #                 filter_condition &= condition
    #         else:
    #             # Log.warning('field {} is not in the Data Frame'.format(field))
    #             pass
    #
    #     return filter_condition
