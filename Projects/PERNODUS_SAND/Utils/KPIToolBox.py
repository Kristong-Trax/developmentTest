
from Trax.Algo.Calculations.Core.DataProvider import Data
from Trax.Cloud.Services.Connector.Keys import DbUsers
from Trax.Data.Projects.Connector import ProjectConnector
# from Trax.Utils.Logging.Logger import Log
import pandas as pd
from KPIUtils_v2.DB.CommonV2 import Common
from KPIUtils_v2.Calculations.AssortmentCalculations import Assortment
from KPIUtils_v2.Calculations.AvailabilityCalculations import Availability
from KPIUtils_v2.Calculations.NumberOfScenesCalculations import NumberOfScenes
from KPIUtils_v2.Calculations.PositionGraphsCalculations import PositionGraphs
from KPIUtils_v2.Calculations.SOSCalculations import SOS
from KPIUtils_v2.Calculations.SequenceCalculations import Sequence
from KPIUtils_v2.Calculations.SurveyCalculations import Survey

# from KPIUtils_v2.Calculations.CalculationsUtils import GENERALToolBoxCalculations

from Projects.PERNODUS_SAND.Utils.Const import Const

__author__ = 'nicolaske'

#adjacency look at png america
#anchor from png

class PERNODUSToolBox:
    LEVEL1 = 1
    LEVEL2 = 2
    LEVEL3 = 3

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
        self.rds_conn = ProjectConnector(self.project_name, DbUsers.CalculationEng)
        self.kpi_static_data = self.common.get_kpi_static_data()
        self.kpi_results_queries = []

    def main_calculation(self, *args, **kwargs):
        """
        This function calculates the KPI results.
        """
        score = 0
        return score

    def get_templates(self):

        for sheet in Const.SHEETS_MAIN:
            self.templates[sheet] = pd.read_excel(Const.TEMPLATE_PATH, sheetname=sheet,
                                                  keep_default_na=False)

    def calculate_presence(self):
        """
        Use facings to determine presence of specific UPCs, brands, or segments - INBEVBE
        """
        for i, row in self.templates[Const.PRESENCE].iterrows():
            param_type = row[Const.PARAM_TYPE]
            param_values = str(row[Const.PARAM_VALUES]).split(',')
            general_filters = {}
            general_filters[param_type] = param_values

            result = Availability.calculate_availability(general_filters) #
            if result == len(param_values):
                score = 1
            else:
                score = 0
            self.commonV2.write_to_db_result(fk = 2,
                                             numerator_id=999,
                                             numerator_result=result,
                                             result=score,
                                             score=score)

    def calculate_adjacency(self):
        score = result = threshold = 0

        params = self.adjacency_data[self.adjacency_data['fixed KPI name'] == kpi]

        group_a = {self.PRODUCT_EAN_CODE_FIELD: self._get_ean_codes_by_product_group_id(
            'Product Group Id;A', **params)}
        group_b = {self.PRODUCT_EAN_CODE_FIELD: self._get_ean_codes_by_product_group_id(
            'Product Group Id;B', **params)}

        allowed_filter = self._get_allowed_products(
            {'product_type': ['Irrelevant', 'Empty', 'Other']})
        allowed_filter_without_other = self._get_allowed_products(
            {'product_type': ['Irrelevant', 'Empty']})
        scene_filters = {'template_name': ''} #Get the templte names


        list = calculate_adjacency_list()
        pass


    def calculate_adjacency_list(self, filter_group_a, filter_group_b, scene_type_filter, allowed_filter,
                            allowed_filter_without_other, a_target, b_target, target):
        a_product_list = self.toolbox._get_group_product_list(filter_group_a)
        b_product_list = self.toolbox._get_group_product_list(filter_group_b)

        adjacency = self._check_groups_adjacency(a_product_list, b_product_list, scene_type_filter, allowed_filter,
                                                 allowed_filter_without_other, a_target, b_target, target)

    def _check_groups_adjacency(self, a_product_list, b_product_list, scene_type_filter, allowed_filter,
                                allowed_filter_without_other, a_target, b_target, target):
        a_b_union = list(set(a_product_list) | set(b_product_list))

        a_filter = {'product_fk': a_product_list}
        b_filter = {'product_fk': b_product_list}
        a_b_filter = {'product_fk': a_b_union}
        a_b_filter.update(scene_type_filter)

        matches = self.data_provider.matches
        relevant_scenes = matches[self.toolbox.get_filter_condition(matches, **a_b_filter)][
            'scene_fk'].unique().tolist()

        result = False
        for scene in relevant_scenes:
            a_filter_for_block = a_filter.copy()
            a_filter_for_block.update({'scene_fk': scene})
            b_filter_for_block = b_filter.copy()
            b_filter_for_block.update({'scene_fk': scene})
            try:
                a_products = self.toolbox.get_products_by_filters('product_fk', **a_filter_for_block)
                b_products = self.toolbox.get_products_by_filters('product_fk', **b_filter_for_block)
                if sorted(a_products.tolist()) == sorted(b_products.tolist()):
                    return False
            except:
                pass
            if a_target:
                brand_a_blocked = self.block.calculate_block_together(allowed_products_filters=allowed_filter,
                                                                      minimum_block_ratio=a_target,
                                                                      vertical=False, **a_filter_for_block)
                if not brand_a_blocked:
                    continue

            if b_target:
                brand_b_blocked = self.block.calculate_block_together(allowed_products_filters=allowed_filter,
                                                                      minimum_block_ratio=b_target,
                                                                      vertical=False, **b_filter_for_block)
                if not brand_b_blocked:
                    continue

            a_b_filter_for_block = a_b_filter.copy()
            a_b_filter_for_block.update({'scene_fk': scene})

            block = self.block.calculate_block_together(allowed_products_filters=allowed_filter_without_other,
                                                        minimum_block_ratio=target, block_of_blocks=True,
                                                        block_products1=a_filter, block_products2=b_filter,
                                                        **a_b_filter_for_block)
            if block:
                return True
        return result


    def calculate_category_space_length(self,  threshold=0.5,  **filters):
        """
        :param threshold: The ratio for a bay to be counted as part of a category.
        :param filters: These are the parameters which the data frame is filtered by.
        :return: The total shelf width (in mm) the relevant facings occupy.
        """

        try:
            #Remove this line of code when tagging is updated for femini hygiene.
            if any(item in filters['template_name'] for item in ESTIMATE_SPACE_BY_BAYS_TEMPLATE_NAMES):
                for k in filters.keys():
                    if k not in ['template_name','category']:

                        del filters[k]



            filtered_scif = self.scif[
                self.get_filter_condition(self.scif, **filters)]
            if self.EXCLUDE_EMPTY == True:
                    filtered_scif = filtered_scif[filtered_scif['product_type'] != 'Empty']

            space_length = 0
            bay_values = []
            max_linear_of_bays = 0
            product_fk_list = filtered_scif['product_fk'].unique().tolist()
            # space_length_DEBUG = 0
            for scene in filtered_scif['scene_fk'].unique().tolist():



                scene_matches = self.match_product_in_scene[self.match_product_in_scene['scene_fk'] == scene]
                scene_filters = filters
                scene_filters['scene_fk'] = scene
                scene_filters['product_fk'] = product_fk_list



                for bay in scene_matches['bay_number'].unique().tolist():
                    bay_total_linear = scene_matches.loc[(scene_matches['bay_number'] == bay) &
                                                         (scene_matches['stacking_layer'] == 1) &
                                                         (scene_matches['status'] == 1)]['width_mm_advance'].sum()
                    max_linear_of_bays += bay_total_linear
                    scene_filters['bay_number'] = bay
                    tested_group_linear = scene_matches[self.get_filter_condition(scene_matches, **scene_filters)]

                    tested_group_linear_value = tested_group_linear['width_mm_advance'].loc[tested_group_linear['stacking_layer'] == 1].sum()


                    if tested_group_linear_value:
                        bay_ratio = tested_group_linear_value / float(bay_total_linear)
                    else:
                        bay_ratio = 0



                    if bay_ratio >= threshold:
                        # bay_num_of_shelves = len(scene_matches.loc[(scene_matches['bay_number'] == bay) &
                        #                                            (scene_matches['stacking_layer'] == 1)][
                        #                              'shelf_number'].unique().tolist())
                        # if kpi_name not in self.average_shelf_values.keys():
                        #     self.average_shelf_values[kpi_name] = {'num_of_shelves': bay_num_of_shelves,
                        #                                            'num_of_bays': 1}
                        # else:
                        #     self.average_shelf_values[kpi_name]['num_of_shelves'] += bay_num_of_shelves
                        #     self.average_shelf_values[kpi_name]['num_of_bays'] += 1
                        # if bay_num_of_shelves:
                        #     bay_final_linear_value = tested_group_linear_value / float(bay_num_of_shelves)
                        # else:
                        #     bay_final_linear_value = 0


                        #  bay_final_linear_value * self.MM_TO_FEET_CONVERSION
                        #  space_length_DEBUG += bay_final_linear_value
                        bay_values.append(4)
                    else:

                        bay_values.append(0)
                if filtered_scif['template_name'].iloc[0] in ESTIMATE_SPACE_BY_BAYS_TEMPLATE_NAMES:
                    max_bays = len(scene_matches['bay_number'].unique().tolist())
                    space_length = max_bays * 4
                space_length = sum(bay_values)

        except Exception as e:
            Log.info('Linear Feet calculation failed due to {}'.format(e))
            space_length = 0

        return space_length