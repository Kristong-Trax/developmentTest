
from Trax.Algo.Calculations.Core.DataProvider import Data
from Trax.Cloud.Services.Connector.Keys import DbUsers
from KPIUtils_v2.DB.PsProjectConnector import PSProjectConnector
# from Trax.Utils.Logging.Logger import Log
import pandas as pd
import os

from KPIUtils_v2.DB.Common import Common
# from KPIUtils_v2.Calculations.AssortmentCalculations import Assortment
# from KPIUtils_v2.Calculations.AvailabilityCalculations import Availability
# from KPIUtils_v2.Calculations.NumberOfScenesCalculations import NumberOfScenes
# from KPIUtils_v2.Calculations.PositionGraphsCalculations import PositionGraphs
# from KPIUtils_v2.Calculations.SOSCalculations import SOS
# from KPIUtils_v2.Calculations.SequenceCalculations import Sequence
# from KPIUtils_v2.Calculations.SurveyCalculations import Survey

from KPIUtils_v2.Calculations.BlockCalculations import Block
from KPIUtils_v2.Calculations.AdjacencyCalculations import Adjancency

# from KPIUtils_v2.Calculations.CalculationsUtils import GENERALToolBoxCalculations

from Projects.MILLERCOORS_SAND.Data.Const import Const

__author__ = 'huntery'

KPI_RESULT = 'report.kpi_results'
KPK_RESULT = 'report.kpk_results'
KPS_RESULT = 'report.kps_results'

TEMPLATE_PATH = os.path.join(os.path.dirname(os.path.realpath(__file__)), '..', 'Data', 'Miller Coors KPI Template.xlsx')


class MILLERCOORSToolBox:
    LEVEL1 = 1
    LEVEL2 = 2
    LEVEL3 = 3

    EXCLUDE_FILTER = 0
    INCLUDE_FILTER = 1
    CONTAIN_FILTER = 2
    EXCLUDE_EMPTY = False
    INCLUDE_EMPTY = True

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
        self.kpi_results_queries = []
        self.templates = {}
        for sheet in Const.SHEETS:
            self.templates[sheet] = pd.read_excel(TEMPLATE_PATH, sheetname=sheet).fillna('')
        self.store_info = self.data_provider[Data.STORE_INFO]
        self.store_type = self.store_info['store_type'].iloc[0]
        # main_template = self.templates[Const.KPIS]
        # self.templates[Const.KPIS] = main_template[main_template[Const.STORE_TYPE] == self.store_type]
        self.block = Block(self.data_provider, self.output, common=self.common)
        self.adjacency = Adjancency(self.data_provider, self.output, common=self.common)
        self.ignore_stacking = False
        self.facings_field = 'facings' if not self.ignore_stacking else 'facings_ign_stack'

    def main_calculation(self, *args, **kwargs):
        """
        This function calculates the KPI results.
        """
        self.calculate_block_adjacency(None, None)
        # main_template = self.templates[Const.KPIS]
        # for i, main_line in main_template.iterrows():
        #     relevant_scif = self.scif[self.scif['product_type'] != 'Empty']
        #     kpi_name = main_line[Const.KPI_NAME]
        #     kpi_type = main_line[Const.KPI_TYPE]
        #     scene_types = self.does_exist(main_line, Const.STORE_LOCATION)
        #     if scene_types:
        #         relevant_scif = self.scif[self.scif['template_name'].isin(scene_types)]
        #
        #     relevant_template = self.templates[kpi_type]
        #     relevant_template = relevant_template[relevant_template[Const.KPI_NAME] == kpi_name]
        #     relevant_template = relevant_template.merge(main_template, how='left', left_on=Const.KPI_NAME, right_on=Const.KPI_NAME)
        #     kpi_function = self.get_kpi_function(kpi_type)
        #     for idx, kpi_line in relevant_template.iterrows():
        #         result_dict = kpi_function(kpi_line, relevant_scif)
        return

    def calculate_anchor(self, kpi_line, relevant_scif):

        filters = {
            kpi_line[Const.PARAM]: kpi_line[Const.VALUE],
            'template_name': kpi_line[Const.STORE_LOCATION]
        }

        result = self.calculate_products_on_edge(min_number_of_facings=1, min_number_of_shelves=1, **filters)
        return result

    def calculate_block(self, kpi_line, relevant_scif):
        filters = {
            kpi_line[Const.PARAM]: kpi_line[Const.VALUE],
            'template_name': kpi_line[Const.STORE_LOCATION]
        }
        result = self.block.calculate_block_together(self, allowed_products_filters=None, include_empty=True,
                                                   minimum_block_ratio=0.75, result_by_scene=False,
                                                   block_of_blocks=False,
                                                   block_products1=None, block_products2=None, vertical=False,
                                                   biggest_block=False,
                                                   n_cluster=None, min_facings_in_block=2, **filters)
        return result

    def calculate_block_adjacency(self, kpi_line, relevant_scif):

        filters = {
            'brand_name': 'Miller Lite',
        }
        location = {
            'template_name': 'Beer, Flavored Beer, Alcoholic Ciders and Alcoholic Malt Beverages - Cold Shelf (Open Deck Cooler)'
        }
        result = self.block.network_x_block_together(filters, location)
        return result


    def calculate_adjacency(self, kpi_line, relevant_scif):
        if self.does_exist(kpi_line, Const.LIST_ATTRIBUTE):
            pass
        else:
            filters = {
                'brand_name': 'Miller Lite',
                'template_name': ''
            }
            self.block.network_x_block_together()

    def calculate_products_on_edge(self, min_number_of_facings=1, min_number_of_shelves=1, **filters):
        """
        :param min_number_of_facings: Minimum number of edge facings for KPI to pass.
        :param min_number_of_shelves: Minimum number of different shelves with edge facings for KPI to pass.
        :param filters: This are the parameters which dictate the relevant SKUs for the edge calculation.
        :return: A tuple: (Number of scenes which pass, Total number of relevant scenes)
        """
        filters, relevant_scenes = self.separate_location_filters_from_product_filters(**filters)
        if len(relevant_scenes) == 0:
            return 0, 0
        number_of_edge_scenes = 0
        for scene in relevant_scenes:
            edge_facings = pd.DataFrame(columns=self.match_product_in_scene.columns)
            matches = self.match_product_in_scene[self.match_product_in_scene['scene_fk'] == scene]
            for shelf in matches['shelf_number'].unique():
                shelf_matches = matches[matches['shelf_number'] == shelf]
                if not shelf_matches.empty:
                    shelf_matches = shelf_matches.sort_values(by=['bay_number', 'facing_sequence_number'])
                    edge_facings = edge_facings.append(shelf_matches.iloc[0])
                    if len(edge_facings) > 1:
                        edge_facings = edge_facings.append(shelf_matches.iloc[-1])
            edge_facings = edge_facings[self.get_filter_condition(edge_facings, **filters)]
            if len(edge_facings) >= min_number_of_facings \
                    and len(edge_facings['shelf_number'].unique()) >= min_number_of_shelves:
                number_of_edge_scenes += 1
        return number_of_edge_scenes, len(relevant_scenes)

    def separate_location_filters_from_product_filters(self, **filters):
        """
        This function gets scene-item-facts filters of all kinds, extracts the relevant scenes by the location filters,
        and returns them along with the product filters only.
        """
        location_filters = {}
        for field in filters.keys():
            if field not in self.all_products.columns and field in self.scif.columns:
                location_filters[field] = filters.pop(field)
        relevant_scenes = self.scif[self.get_filter_condition(self.scif, **location_filters)]['scene_id'].unique()
        return filters, relevant_scenes

    def get_filter_condition(self, df, **filters):
        """
        :param df: The data frame to be filters.
        :param filters: These are the parameters which the data frame is filtered by.
                       Every parameter would be a tuple of the value and an include/exclude flag.
                       INPUT EXAMPLE (1):   manufacturer_name = ('Diageo', DIAGEOAUJTIUAGENERALToolBox.INCLUDE_FILTER)
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

    def get_kpi_function(self, kpi_type):
        """
        transfers every kpi to its own function
        :param kpi_type: value from "sheet" column in the main sheet
        :return: function
        """
        if kpi_type == Const.ANCHOR:
            return self.calculate_anchor
        elif kpi_type == Const.BLOCK:
            return self.calculate_block
        elif kpi_type == Const.BLOCK_ADJACENCY:
            return self.calculate_block_adjacency
        elif kpi_type == Const.ADJACENCY:
            return self.calculate_adjacency
        else:
            Log.warning("The value '{}' in column sheet in the template is not recognized".format(kpi_type))
            return None

    @staticmethod
    def does_exist(kpi_line, column_name):
        """
        checks if kpi_line has values in this column, and if it does - returns a list of these values
        :param kpi_line: line from template
        :param column_name: str
        :return: list of values if there are, otherwise None
        """
        if column_name in kpi_line.keys() and kpi_line[column_name] != "":
            cell = kpi_line[column_name]
            if type(cell) in [int, float]:
                return [cell]
            elif type(cell) in [unicode, str]:
                return cell.split(", ")
        return None