from Trax.Algo.Calculations.Core.DataProvider import Data
from Trax.Algo.Calculations.Core.AdjacencyGraph.Builders import AdjacencyGraphBuilder
from Trax.Algo.Geometry.Masking.MaskingResultsIO import retrieve_maskings_flat
from Trax.Algo.Geometry.Masking.Utils import transform_maskings_flat
from Trax.Utils.Logging.Logger import Log
from Trax.Cloud.Services.Connector.Keys import DbUsers
from KPIUtils_v2.GlobalDataProvider.PsDataProvider import PsDataProvider
from KPIUtils_v2.DB.CommonV2 import Common as CommonV2
from KPIUtils_v2.DB.PsProjectConnector import PSProjectConnector
from KPIUtils_v2.Utils.GlobalScripts.Scripts import GlobalSessionToolBox
from KPIUtils_v2.Utils.Parsers import ParseInputKPI
from KPIUtils_v2.Calculations.BlockCalculations_v2 import Block

from collections import OrderedDict
import pandas as pd
import simplejson
import os
import numpy as np
import re
import ast
from math import sqrt
from shapely import affinity
from shapely.geometry import box

from Projects.MONDELEZUSPS.Data.LocalConsts import Consts

# from KPIUtils_v2.Utils.Consts.DataProvider import 
# from KPIUtils_v2.Utils.Consts.DB import 
# from KPIUtils_v2.Utils.Consts.PS import 
# from KPIUtils_v2.Utils.Consts.GlobalConsts import 
# from KPIUtils_v2.Utils.Consts.Messages import 
# from KPIUtils_v2.Utils.Consts.Custom import 
# from KPIUtils_v2.Utils.Consts.OldDB import 

# from KPIUtils_v2.Calculations.AssortmentCalculations import Assortment
# from KPIUtils_v2.Calculations.AvailabilityCalculations import Availability
# from KPIUtils_v2.Calculations.NumberOfScenesCalculations import NumberOfScenes
# from KPIUtils_v2.Calculations.PositionGraphsCalculations import PositionGraphs
# from KPIUtils_v2.Calculations.SOSCalculations import SOS
# from KPIUtils_v2.Calculations.SequenceCalculations import Sequence
# from KPIUtils_v2.Calculations.SurveyCalculations import Survey

# from KPIUtils_v2.Calculations.CalculationsUtils import GENERALToolBoxCalculations

MATCH_PRODUCT_IN_PROBE_FK = 'match_product_in_probe_fk'
MATCH_PRODUCT_IN_PROBE_STATE_REPORTING_FK = 'match_product_in_probe_state_reporting_fk'
TEMPLATE_PATH = os.path.join(os.path.dirname(os.path.realpath(__file__)), '..', 'Data',
                             'shelf_position.xlsx')
SHEETS = [Consts.SHELF_MAP]

__author__ = 'krishnat'


class ToolBox(GlobalSessionToolBox):

    def __init__(self, data_provider, output):
        GlobalSessionToolBox.__init__(self, data_provider, output)
        self.ps_data_provider = PsDataProvider(self.data_provider, self.output)
        self.common_v2 = CommonV2(self.data_provider)
        self.rds_conn = PSProjectConnector(self.project_name, DbUsers.CalculationEng)
        self.templates = {}
        self.parse_template()
        self.shelf_number = self.templates[Consts.SHELF_MAP].set_index('Num Shelves')
        self.block = Block(data_provider)
        self.match_product_in_probe_state_reporting = self.ps_data_provider.get_match_product_in_probe_state_reporting()
        self.match_product_in_scene = self.data_provider[Data.MATCHES]
        self.merged_scif_mpis = self.match_product_in_scene.merge(self.scif, how='left',
                                                                  left_on=['scene_fk', 'product_fk'],
                                                                  right_on=['scene_fk', 'product_fk'])
        self.gold_zone_scene_location_kpi = ['Lobby/Entrance', 'Main Alley/Hot Zone', 'Gold Zone End Cap',
                                             'Lobby/Main Entrance']
        self.custom_entity_table = self.get_kpi_custom_entity_table()
        self.final_custom_entity_table = self.custom_entity_table.copy()
        self.store_area = self.get_store_area_df()
        # self.targets = self.ps_data_provider.get_kpi_external_targets()
        self.targets = self.ps_data_provider.get_kpi_external_targets(key_fields=['KPI Type', 'store_type', 'EAN Original', 'Assortment type'],
                                                                      data_fields=['Granular Group Name',
                                                                                   'Location: JSON',
                                                                                   'Config Params: JSON',
                                                                                   'Dataset 1: JSON'])
        self.results_df = pd.DataFrame(columns=['kpi_name', 'kpi_fk', 'numerator_id', 'numerator_result', 'context_id',
                                                'denominator_id', 'denominator_result', 'result', 'score'])

    def parse_template(self):
        for sheet in SHEETS:
            self.templates[sheet] = pd.read_excel(TEMPLATE_PATH, sheet_name=sheet)

    def save_results_to_db(self):
        self.results_df.drop(columns=['kpi_name'], inplace=True)
        self.results_df.rename(columns={'kpi_fk': 'fk'}, inplace=True)
        self.results_df[['result']].fillna(0, inplace=True)
        results = self.results_df.to_dict('records')
        for result in results:
            result = simplejson.loads(simplejson.dumps(result, ignore_nan=True))
            self.write_to_db(**result)

    def main_calculation(self):
        # Consts.SHARE_OF_SCENES, Consts.SCENE_LOCATION, Consts.SHELF_POSITION, Consts.BLOCKING, Consts.BAY_POSITION
        relevant_kpi_types = [Consts.SHARE_OF_SCENES, Consts.SHELF_POSITION, Consts.BLOCKING, Consts.BAY_POSITION, Consts.DISTRIBUTION, Consts.DIAMOND_POSITION]
        # relevant_kpi_types = [Consts.DISTRIBUTION]
        targets = self.targets[(self.targets[Consts.KPI_TYPE].isin(relevant_kpi_types)) & (
            self.targets[Consts.GRANULAR_GROUP_NAME].isnull())]

        self._calculate_kpis_from_template(targets)
        self.save_results_to_db()
        return

    def _calculate_kpis_from_template(self, template_df):
        for i, row in template_df.iterrows():
            calculation_function = self._get_calculation_function_by_kpi_type(row[Consts.KPI_TYPE])
            row = self.apply_json_parser(row)
            merged_scif_mpis = self._parse_json_filters_to_df(row)
            result_data = calculation_function(row, merged_scif_mpis)
            if result_data and isinstance(result_data, list):
                for result in result_data:
                    self.results_df.loc[len(self.results_df), result.keys()] = result
            elif result_data and isinstance(result_data, dict):
                self.results_df.loc[len(self.results_df), result_data.keys()] = result_data

    def _get_calculation_function_by_kpi_type(self, kpi_type):
        if kpi_type == Consts.SHARE_OF_SCENES:
            return self.calculate_share_of_scenes
        elif kpi_type == Consts.SCENE_LOCATION:
            return self.calculate_scene_location
        elif kpi_type == Consts.SHELF_POSITION:
            return self.calculate_shelf_position
        elif kpi_type == Consts.BLOCKING:
            return self.calculate_blocking
        elif kpi_type == Consts.DISTRIBUTION:
            return self.calculate_distribution
        elif kpi_type == Consts.BAY_POSITION:
            return self.calculate_bay_position
        elif kpi_type == Consts.DIAMOND_POSITION:
            return self.calculate_diamond_position

    def calculate_diamond_position(self, row, df):
        return_holder = self._get_kpi_name_and_fk(row)
        numerator_type, denominator_type = self._get_numerator_and_denominator_type(
            row['Config Params: JSON'], context_relevant=False)
        overlap_pct = float(row['Config Params: JSON']['overlap_pct'][0])
        population_pct = float(row['Config Params: JSON']['population_pct'][0])
        df.dropna(subset=[numerator_type], inplace=True)
        diamond_boarder_df = self._diamond_boarder(return_holder, df.scene_fk.unique(),
                                                   overlap_pct)  # need to write inside the diamond position
        result_dict_list = self._logic_diamond_position(df, diamond_boarder_df, return_holder, numerator_type,
                                                        denominator_type,
                                                        population_pct)

        return result_dict_list

    def _logic_diamond_position(self, df, diamond_boarder_df, return_holder, numerator_type, denominator_type,
                                population_pct):
        # self.mark_tags_in_explorer(nodes_in_diamond, relevant_holder[0])

        result_dict_list = []
        for unique_denominator_type in set(df[denominator_type]):
            denominator_filtered_df = self._filter_df(df, {denominator_type: unique_denominator_type})
            for unique_numerator_id in set(denominator_filtered_df[numerator_type]):
                filtered_numerator_df = self._filter_df(denominator_filtered_df, {numerator_type: unique_numerator_id})
                groupby_numerator_df = self._df_groupby_logic(filtered_numerator_df, ['scene_fk', numerator_type],
                                                              {'facings': 'count'})  # may cause issue
                scene_with_most_numerator_facings = \
                    groupby_numerator_df.agg(['max', 'idxmax']).loc['idxmax', 'facings'][0]
                scene_filtered_numerator_df = self._filter_df(filtered_numerator_df,
                                                              {'scene_fk': scene_with_most_numerator_facings})
                nodes_in_diamond = self._filter_df(diamond_boarder_df, {
                    'scene_fk': scene_with_most_numerator_facings}).nodes_in_diamond.iat[0]
                final_numerator_df = scene_filtered_numerator_df[
                    scene_filtered_numerator_df.scene_match_fk.isin(nodes_in_diamond)]
                # numerator_result = len(final_numerator_df)
                # denominator_result = len(scene_filtered_numerator_df)
                if not isinstance(unique_numerator_id, int):
                    unique_numerator_id = self._get_id_from_custom_entity_table(numerator_type, unique_numerator_id)
                result = 1 if float(len(final_numerator_df)) / len(scene_filtered_numerator_df) >= population_pct else 0
                result_dict = {'kpi_name': return_holder[0], 'kpi_fk': return_holder[1],
                               'numerator_id': unique_numerator_id, 'numerator_result': result,
                               'denominator_id': unique_denominator_type,'context_id':scene_with_most_numerator_facings, 'denominator_result': 1,
                               'result': result}
                # save tags into explorer
                self.mark_tags_in_explorer(final_numerator_df.probe_match_fk.values.tolist(), return_holder[0])
                result_dict_list.append(result_dict)
        return result_dict_list

    def _diamond_boarder(self, relevant_holder, relevant_scene_fks, overlap_pct):
        diamond_boarder_df = pd.DataFrame()
        for unique_scene_fk in relevant_scene_fks:
            ### diamond positon kpi ###
            mpis = self.match_product_in_scene[
                self.match_product_in_scene.scene_fk == unique_scene_fk]
            # mpis = df[df.scene_fk == scene_with_most_numerator_facings]
            mpis['pk'] = mpis['scene_match_fk']
            maskings_data = transform_maskings_flat(*retrieve_maskings_flat(self.project_name,
                                                                            [unique_scene_fk]))
            kwargs = {'use_masking_only': True, 'minimal_overlap_ratio': 0.4}
            adj_graph = AdjacencyGraphBuilder.initiate_graph_by_dataframe(
                mpis, maskings_data, additional_attributes=['scene_fk'], **kwargs)

            # condense the graph by scene_fk so we get a polygon with the bounds of the products on the extreme edges
            # this could be enhanced (if needed) to exclude irrelevant items to ensure the diamond is over the core product area
            g = AdjacencyGraphBuilder.condense_graph_by_level('scene_fk', adj_graph)
            # get the bounds of the scene polygon
            bounds = g.nodes[0]['polygon'].bounds
            # get the center of the scene polygon
            center = g.nodes[0]['polygon'].centroid.coords[0]
            # we need half the height of the scene so we can use pythagorean theorem to get length of diamond
            half_height = (bounds[3] - bounds[1]) / 2
            # pythagorean theorem to get the length of the side of the diamond
            side_len = sqrt(half_height ** 2 + half_height ** 2)
            # create a square box in the center of the scene with each side being the length determined earlier
            b = box(center[0] - side_len / 2, center[1] - side_len / 2, center[0] + side_len / 2,
                    center[1] + side_len / 2)
            # rotate the square box 45 degrees to make a diamond
            diamond = affinity.rotate(b, 45, 'center')

            nodes_in_diamond = []
            # iterate over all nodes in the base adj graph to see if they overlap the diamond
            for node, node_data in adj_graph.nodes(data=True):
                # get the bounds of the node
                bounds = node_data['polygon'].bounds
                # build a rectangle from the bounds
                polygon = box(bounds[0], bounds[1], bounds[2], bounds[3])
                # check to see if the product rectangle overlaps the diamond, and if so by what %
                overlap_ratio = polygon.intersection(diamond).area / polygon.area
                # this can be tuned to include more products by relaxing the required overlap ratio
                if overlap_ratio > overlap_pct:
                    # if the product overlaps, add it to the 'in diamond' list
                    nodes_in_diamond.append(node)

            # add tags in explorer to define boarder of the diamond
            if nodes_in_diamond:
                diamond_boarder_probematch_fk = self.match_product_in_scene[
                    self.match_product_in_scene.scene_match_fk.isin(nodes_in_diamond)].probe_match_fk.values.tolist()
                self.mark_tags_in_explorer(diamond_boarder_probematch_fk, 'diamond_boarder')
                diamond_boarder_df = diamond_boarder_df.append(
                    {'scene_fk': unique_scene_fk, 'nodes_in_diamond': nodes_in_diamond}, ignore_index=True)

        return diamond_boarder_df

    def calculate_bay_position(self, row, df):
        return_holder = self._get_kpi_name_and_fk(row)
        numerator_type, denominator_type = self._get_numerator_and_denominator_type(
            row['Config Params: JSON'], context_relevant=False)
        anchor_pct = float(row['Config Params: JSON']['anchor_pct'][0])
        df = df.dropna(subset=[numerator_type])
        result_dict_list = self._logic_for_bay_postion(df, return_holder, numerator_type, denominator_type, anchor_pct)
        return result_dict_list
        # df_without_na_in_numerator_type_column = df.dropna(subset=[numerator_type])
        # for unqiue_denominator_id in set(df_without_na_in_numerator_type_column[denominator_type]):
        #     denominator_filtered_df = self._filter_df(df_without_na_in_numerator_type_column,
        #                                               {denominator_type: unqiue_denominator_id})
        #     denomi_df_grouped_facings = self._df_groupby_logic(denominator_filtered_df, ['scene_fk', numerator_type],
        #                                                        {'facings': 'count'})
        #
        #     relevant_scene_with_most_facings = \
        #         denomi_df_grouped_facings.agg(['max', 'idxmax']).loc['idxmax', 'facings'][0]
        #
        #     scene_filtered_df = self._filter_df(denominator_filtered_df, {'scene_fk': relevant_scene_with_most_facings})
        #     count_of_bays_in_scene = scene_filtered_df.bay_number.max()
        #
        #     ## logic for calculating bay number##
        #     bay_df = self._df_groupby_logic(scene_filtered_df, ['bay_number', numerator_type], {'facings': 'count'})
        #     relevant_bay_df = self._filter_df(bay_df, {'facings': bay_df.facings.max()}).reset_index()
        #
        #     if len(relevant_bay_df) > 1:
        #         relevant_bay_number_container = relevant_bay_df.bay_number
        #
        #         def apply_tie_breaker_logic_for_bay_position(bay_number_container, count_of_bays_in_scene):
        #             bay_number_df = pd.DataFrame()
        #             bay_number_df['bay_number'] = bay_number_container
        #             bay_number_df['bay_number_score'] = [np.square((num - ((count_of_bays_in_scene + 1) / 2))) for num
        #                                                  in bay_number_container]
        #             final_bay_number_df = bay_number_df[
        #                 bay_number_df.bay_number_score == bay_number_df.bay_number_score.max()]
        #             # if there is a tie with the max score between bay, we get the smallest bay #, else the bay number with the highest score
        #             return_bay_number = final_bay_number_df.bay_number.min()
        #             return return_bay_number
        #
        #         bay_number = apply_tie_breaker_logic_for_bay_position(relevant_bay_number_container,
        #                                                               count_of_bays_in_scene)
        #         numerator_id = relevant_bay_df[relevant_bay_df.bay_number == bay_number][numerator_type].values[0]
        #
        #
        #     else:
        #         bay_number = relevant_bay_df.bay_number.values[0]
        #         numerator_id = relevant_bay_df[relevant_bay_df.bay_number == bay_number][numerator_type].values[0]
        #
        #     ## logic for calculating bay number##
        #
        #     #####logic for calculating bay result #######
        #     if bay_number == 1:
        #         bay_filtered_df = self._filter_df(scene_filtered_df, {'bay_number': bay_number})
        #         bay_filtered_df_with_null = self._filter_df(df, {'scene_fk': relevant_scene_with_most_facings,
        #                                                          'bay_number': bay_number})
        #         numerator_result = bay_filtered_df.drop_duplicates([numerator_type]).facings.sum()
        #         denominator_result = bay_filtered_df_with_null.drop_duplicates([numerator_type]).facings.sum()
        #         result = float(numerator_result) / denominator_result
        #         final_result = 'Left Anchor' if result >= anchor_pct else 'Not Anchor'
        #     elif bay_number == count_of_bays_in_scene:
        #         bay_filtered_df = self._filter_df(scene_filtered_df, {'bay_number': bay_number})
        #         bay_filtered_df_with_null = self._filter_df(df, {'scene_fk': relevant_scene_with_most_facings,
        #                                                          'bay_number': bay_number})
        #         numerator_result = bay_filtered_df.drop_duplicates([numerator_type]).facings.sum()
        #         denominator_result = bay_filtered_df_with_null.drop_duplicates([numerator_type]).facings.sum()
        #         result = float(numerator_result) / denominator_result
        #         final_result = 'Right Anchor' if result >= anchor_pct else 'Not Anchor'
        #     else:
        #         numerator_result = 0
        #         denominator_result = 0
        #         final_result = 'Not Anchor'
        #
        #     # wait on logic for result
        #     result_dict = {'kpi_name': return_holder[0], 'kpi_fk': return_holder[1],
        #                    'numerator_id': numerator_id, 'numerator_result': numerator_result,
        #                    'denominator_id': denominator_type, 'denominator_result': denominator_result,
        #                    'result': final_result}
        #     result_dict_list.append(result_dict)
        # return result_dict_list

    def _logic_for_bay_postion(self, df, return_holder, numerator_type, denominator_type, anchor_pct):
        result_dict_list = []
        key_dict = {'Left Anchor': 8, 'Right Anchor': 9, 'Not Anchor': 10}
        for unqiue_denominator_id in set(df[denominator_type]):
            denominator_filtered_df = self._filter_df(df,
                                                      {denominator_type: unqiue_denominator_id})
            for unique_numerator_id in set(denominator_filtered_df[numerator_type]):
                filtered_numerator_df = self._filter_df(denominator_filtered_df, {numerator_type: unique_numerator_id})
                relevant_scene = self._df_groupby_logic(filtered_numerator_df, ['scene_fk'], {'facings': 'count'}).agg(
                    ['max', 'idxmax']).loc['idxmax']['facings']
                scene_filtered_df = self._filter_df(filtered_numerator_df, {'scene_fk': relevant_scene})
                # count_of_bays_in_scene = self.match_product_in_scene[self.match_product_in_scene.scene_fk == relevant_scene].bay_number.max()
                count_of_bays_in_scene = scene_filtered_df.bay_number.max()
                bay_number = self._get_bay_number_for_bay_positon(numerator_type, count_of_bays_in_scene,
                                                                  scene_filtered_df)
                final_result = self._get_result_for_bay_postion(df,
                                                                  numerator_type,
                                                                  anchor_pct,
                                                                  scene_filtered_df,
                                                                  relevant_scene,
                                                                  count_of_bays_in_scene,
                                                                  bay_number)
                if not isinstance(unique_numerator_id, int):
                    unique_numerator_id = self._get_id_from_custom_entity_table(numerator_type, unique_numerator_id)
                final_result = key_dict.get(final_result)
                result_dict = {'kpi_name': return_holder[0], 'kpi_fk': return_holder[1],
                               'numerator_id': unique_numerator_id, 'numerator_result': bay_number,
                               'denominator_id': unqiue_denominator_id, 'denominator_result': count_of_bays_in_scene, 'context_id':relevant_scene,
                               'result': final_result}
                result_dict_list.append(result_dict)
        return result_dict_list

        # denomi_df_grouped_facings = self._df_groupby_logic(denominator_filtered_df, ['scene_fk', numerator_type],
        #                                                    {'facings': 'count'})
        #
        # relevant_scene_with_most_facings = \
        #     denomi_df_grouped_facings.agg(['max', 'idxmax']).loc['idxmax', 'facings'][0]

        # scene_filtered_df = self._filter_df(denominator_filtered_df, {'scene_fk': relevant_scene_with_most_facings})
        # count_of_bays_in_scene = scene_filtered_df.bay_number.max()
        #
        # bay_number, numerator_id = self._get_bay_number_for_bay_positon(numerator_type,
        #                                                                 count_of_bays_in_scene, scene_filtered_df)
        #
        # numerator_result, denominator_result, final_result = self._get_result_for_bay_postion(df, numerator_type,
        #                                                                                       anchor_pct,
        #                                                                                       scene_filtered_df,
        #                                                                                       relevant_scene_with_most_facings,
        #                                                                                       count_of_bays_in_scene,
        #                                                                                       bay_number)
        # final_result = key_dict.get(final_result)
        #
        # result_dict = {'kpi_name': return_holder[0], 'kpi_fk': return_holder[1],
        #                'numerator_id': numerator_id, 'numerator_result': numerator_result,
        #                'denominator_id': denominator_type, 'denominator_result': denominator_result,
        #                'result': final_result}
        # result_dict_list.append(result_dict)

    def _get_bay_number_for_bay_positon(self, numerator_type, count_of_bays_in_scene, scene_filtered_df):
        bay_df = self._df_groupby_logic(scene_filtered_df, ['bay_number', numerator_type], {'facings': 'count'})
        relevant_bay_df = self._filter_df(bay_df, {'facings': bay_df.facings.max()}).reset_index()

        if len(relevant_bay_df) > 1:
            relevant_bay_number_container = relevant_bay_df.bay_number
            bay_number = self.apply_tie_breaker_logic_for_bay_position(relevant_bay_number_container,
                                                                       count_of_bays_in_scene)
            # numerator_id = relevant_bay_df[relevant_bay_df.bay_number == bay_number][numerator_type].values[0]
        else:
            bay_number = relevant_bay_df.bay_number.values[0]
            # numerator_id = relevant_bay_df[relevant_bay_df.bay_number == bay_number][numerator_type].values[0]
        return bay_number

    def _get_result_for_bay_postion(self, df, numerator_type, anchor_pct, scene_filtered_df,
                                    relevant_scene_with_most_facings, count_of_bays_in_scene, bay_number):
        if bay_number == 1:
            bay_filtered_df = self._filter_df(scene_filtered_df, {'bay_number': bay_number})
            bay_filtered_df_with_null = self._filter_df(df, {'scene_fk': relevant_scene_with_most_facings,
                                                             'bay_number': bay_number})
            numerator_result = bay_filtered_df.drop_duplicates([numerator_type])[Consts.FINAL_FACINGS].sum()
            denominator_result = bay_filtered_df_with_null.drop_duplicates([numerator_type])[Consts.FINAL_FACINGS].sum()
            result = float(numerator_result) / denominator_result
            final_result = 'Left Anchor' if result >= anchor_pct else 'Not Anchor'
        elif bay_number == count_of_bays_in_scene:
            bay_filtered_df = self._filter_df(scene_filtered_df, {'bay_number': bay_number})
            bay_filtered_df_with_null = self._filter_df(df, {'scene_fk': relevant_scene_with_most_facings,
                                                             'bay_number': bay_number})
            numerator_result = bay_filtered_df.drop_duplicates([numerator_type])[Consts.FINAL_FACINGS].sum()
            denominator_result = bay_filtered_df_with_null.drop_duplicates([numerator_type])[Consts.FINAL_FACINGS].sum()
            result = float(numerator_result) / denominator_result
            final_result = 'Right Anchor' if result >= anchor_pct else 'Not Anchor'
        else:
            # numerator_result = 0
            # denominator_result = 0
            final_result = 'Not Anchor'
        return final_result

    @staticmethod
    def apply_tie_breaker_logic_for_bay_position(bay_number_container, count_of_bays_in_scene):
        bay_number_df = pd.DataFrame()
        bay_number_df['bay_number'] = bay_number_container
        bay_number_df['bay_number_score'] = [np.square((num - ((count_of_bays_in_scene + 1) / 2))) for num
                                             in bay_number_container]
        final_bay_number_df = bay_number_df[
            bay_number_df.bay_number_score == bay_number_df.bay_number_score.max()]
        # if there is a tie with the max score between bay, we get the smallest bay #, else the bay number with the highest score
        return_bay_number = final_bay_number_df.bay_number.min()
        return return_bay_number

    def calculate_distribution(self, row, df):
        return_holder = self._get_kpi_name_and_fk(row)
        child_sku = self._get_sku_name_and_fk(return_holder[0])
        result_dict_list = []

        relevant_template_for_distribution = self.targets[
            (~ self.targets[Consts.GRANULAR_GROUP_NAME].isnull()) & (self.targets[Consts.ASSORTMENT_TYPE] == return_holder[0])]

        if relevant_template_for_distribution.empty:
            return {'kpi_name': return_holder[0], 'kpi_fk': return_holder[1],'result':0}

        relevant_template_for_distribution.dropna(subset=[Consts.EAN_CODE], inplace=True)
        relevant_template_for_distribution[Consts.EAN_CODE] = relevant_template_for_distribution[
            Consts.EAN_CODE].astype(str) #fails if issue in the original ean code


        product_df = self.all_products[['product_ean_code', 'product_fk', 'category_fk']].dropna()
        product_df.product_ean_code = product_df.product_ean_code.astype(str)

        df.dropna(subset=['product_ean_code'], inplace=True)
        df.drop_duplicates(subset=['product_ean_code'], keep='first', inplace=True)
        df['product_ean_code'] = df['product_ean_code'].astype(str)
        df = df[df.product_ean_code.isin(relevant_template_for_distribution[Consts.EAN_CODE])]
        final_df = df[['product_ean_code', 'product_fk', Consts.FINAL_FACINGS]]

        if final_df.empty:
            final_result = self.logic_for_failed_result_for_distribution(return_holder, child_sku, relevant_template_for_distribution)
            return final_result

        for unique_assortment in relevant_template_for_distribution[Consts.GRANULAR_GROUP_NAME].unique():
            assortment_relevant_template = relevant_template_for_distribution[
                relevant_template_for_distribution[Consts.GRANULAR_GROUP_NAME] == unique_assortment]
            if not isinstance(unique_assortment, str):
                assortment_id = self._get_id_from_custom_entity_table('assortment', unique_assortment)

            categorical_product_df = product_df[
                product_df.product_ean_code.isin(assortment_relevant_template[Consts.EAN_CODE])]

            for unique_categorical_fk in set(categorical_product_df.category_fk):
                final_categorical_df = categorical_product_df[
                    categorical_product_df.category_fk.isin([unique_categorical_fk])]
                product_results = []

                for unique_product_fk in final_categorical_df.product_fk:
                    result = 1 if unique_product_fk in final_df.product_fk.values else 0
                    numerator_result = final_df.loc[final_df.product_fk == unique_product_fk, 'final_facings'].iat[
                        0] if result else 0
                    numerator_id = unique_product_fk

                    result_dict = {'kpi_name': child_sku[0], 'kpi_fk': child_sku[1],
                                   'numerator_id': numerator_id,
                                   'numerator_result': numerator_result,
                                   'denominator_id': assortment_id, 'denominator_result': 1,
                                   'result': result}

                    product_results.append(result)
                    result_dict_list.append(result_dict)

            parent_numerator_id = assortment_id
            parent_denominator_id = self.store_id
            parent_numerator_result = sum(product_results)
            parent_denominator_result = len(product_results)
            parent_result = float(parent_numerator_result) / parent_denominator_result

            result_dict = {'kpi_name': return_holder[0], 'kpi_fk': return_holder[1],
                           'numerator_id': parent_numerator_id,
                           'numerator_result': parent_numerator_result,
                           'denominator_id': parent_denominator_id, 'denominator_result': parent_denominator_result,
                           'result': parent_result}
            result_dict_list.append(result_dict)

        return result_dict_list

    def logic_for_failed_result_for_distribution(self,return_holder, child_sku, relevant_template_for_distribution):
        result_list = [{'kpi_name': return_holder[0], 'kpi_fk': return_holder[1], 'result':0}]

        for unique_assortment in relevant_template_for_distribution['Granular Group Name'].unique():
            assortment_id = self._get_id_from_custom_entity_table('assortment', unique_assortment)
            result_dict = {'kpi_name': child_sku[0], 'kpi_fk': child_sku[1],
                           'denominator_id': assortment_id,
                           'result': 0}
            result_list.append(result_dict)

        return result_list

    def calculate_shelf_position(self, row, df):
        return_holder = self._get_kpi_name_and_fk(row)
        numerator_type, denominator_type = self._get_numerator_and_denominator_type(
            row['Config Params: JSON'], context_relevant=False)
        df.dropna(subset=[numerator_type, denominator_type], inplace=True)
        result_dict_list = self._logic_for_shelf_position(df, return_holder, numerator_type, denominator_type)
        return result_dict_list

    def _logic_for_shelf_position(self, df, return_holder, numerator_type, denominator_type):
        '''
        "For each [numerator] within each [denominator] population
        Find the scene that contains the most [numerator] facings and consider only bays in that scene with [numerator] product
        Determine the maximum number of shelves in those relevant bays, which we'll call [shelves]
        Now find the [shelf] within those bays with the most [numerator] facings, which we'll call [shelf#]
        For determining shelf numbers, use the 'shelf number from bottom' method, where the bottom shelf is #1
        If multiple shelves tie here, use the highest shelf number (from bottom as mentioned above)
        Use the shelf map template [See Shelf Map tab] to determine the result value
        The first column lists possible total shelf figures, which correspond to [shelves]
        The remaining columns list what the value should be for each [shelf#] in the corresponding set
        So the intersection of the row where column A matches [shelves] and the column where row 3 matches [shelf#] holds the [KPI Result] value"
        '''
        result_dict_list = []
        key_dict = {'Bottom': 4, 'Middle': 5, 'Eye': 6, 'Top': 7}

        for unique_denominator_fk in set(df[denominator_type]):
            unique_template_scif_mpis = self._filter_df(df, {denominator_type: unique_denominator_fk})
            for unique_numerator_id in set(unique_template_scif_mpis[numerator_type]):
                filtered_numerator_df = self._filter_df(unique_template_scif_mpis, {numerator_type: unique_numerator_id})
                relevant_scene = self._df_groupby_logic(filtered_numerator_df, ['scene_fk'], {'facings': 'count'}).agg(
                    ['max', 'idxmax']).loc['idxmax']['facings']
                scene_filtered_df = self._filter_df(filtered_numerator_df, {'scene_fk': relevant_scene})
                relevant_bay_df = self._df_groupby_logic(scene_filtered_df, ['bay_number', 'shelf_number_from_bottom'],
                                                         {'facings': 'count'}).agg(
                    ['max', 'idxmax']).loc['idxmax']['facings']
                relevant_bay = relevant_bay_df[0]
                relevant_shelf = relevant_bay_df[1]
                # max_shelf_number_from_bottom = scene_filtered_df.shelf_number_from_bottom.max()
                max_shelf_number_from_bottom = (
                    self._filter_df(self.match_product_in_scene, {'scene_fk': relevant_scene,
                                                                  'bay_number': relevant_bay})).shelf_number_from_bottom.max()
                try:
                    result = self.shelf_number.loc[max_shelf_number_from_bottom, relevant_shelf]
                    result_by_id = key_dict.get(result)
                except:
                    continue
                if not isinstance(unique_numerator_id, int):
                    unique_numerator_id = self._get_id_from_custom_entity_table(numerator_type, unique_numerator_id)
                result_dict = {'kpi_name': return_holder[0], 'kpi_fk': return_holder[1],
                               'numerator_id': unique_numerator_id,
                               'numerator_result': relevant_shelf,
                               'denominator_id': unique_denominator_fk,
                               'denominator_result': max_shelf_number_from_bottom,'context_id':relevant_scene,
                               'result': result_by_id}
                result_dict_list.append(result_dict)
        return result_dict_list

        # df_with_max_facings_by_scene = self._df_groupby_logic(unique_template_scif_mpis,
        #                                                       ['scene_fk', numerator_type],
        #                                                       {'facings': 'count'})
        #
        # relevant_scene_with_most_numerator_facings = \
        #     df_with_max_facings_by_scene.agg(['max', 'idxmax']).loc['idxmax', 'facings'][0]
        #
        # relevant_scif_mpis_scene_with_most_facings = self._filter_df(unique_template_scif_mpis, {
        #     'scene_fk': relevant_scene_with_most_numerator_facings})
        #
        # df_with_max_facings_by_bay = self._df_groupby_logic(relevant_scif_mpis_scene_with_most_facings,
        #                                                     ['bay_number', numerator_type], {'facings': 'count'})
        # relevant_bay_with_most_numerator_facings = \
        #     df_with_max_facings_by_bay.agg(['max', 'idxmax']).loc['idxmax', 'facings'][0]
        # final_df = self._filter_df(relevant_scif_mpis_scene_with_most_facings,
        #                            {'bay_number': relevant_bay_with_most_numerator_facings})
        # container_with_shelf_number = self._df_groupby_logic(final_df, ['shelf_number_from_bottom', numerator_type],
        #                                                      {'facings': 'count'})
        # max_shelf = final_df.shelf_number_from_bottom.max()
        # shelf_number = container_with_shelf_number.agg(['max', 'idxmax']).loc['idxmax', 'facings'][
        #     0]  # shelf with the mose number of facings
        #
        # try:
        #     result = self.shelf_number.loc[max_shelf, shelf_number]
        #     result_by_id = key_dict.get(result)
        # except:
        #     continue
        #
        # numerator_id = container_with_shelf_number.agg(['max', 'idxmax']).loc['idxmax', 'facings'][
        #     1]  # numerator_id with most facings in the bay
        # if not isinstance(numerator_id, int):
        #     numerator_id = self._get_id_from_custom_entity_table(numerator_type, numerator_id)
        # result_dict = {'kpi_name': return_holder[0], 'kpi_fk': return_holder[1],
        #                'numerator_id': numerator_id,
        #                'numerator_result': shelf_number,
        #                'denominator_id': unique_denominator_fk, 'denominator_result': max_shelf,
        #                'result': result_by_id}
        # result_dict_list.append(result_dict)

    def calculate_scene_location(self, row, df):
        return_holder = self._get_kpi_name_and_fk(row)
        scene_store_area_df = self.store_area
        scene_store_area_df['result'] = scene_store_area_df.name.apply(
            lambda x: 1 if x in self.gold_zone_scene_location_kpi else 0)
        result_dict_list = []

        for store_area_row in scene_store_area_df.itertuples():
            result_dict = {'kpi_name': return_holder[0], 'kpi_fk': return_holder[1],
                           'numerator_id': store_area_row.pk,
                           'numerator_result': store_area_row.result,
                           'denominator_id': self.store_id, 'denominator_result': 1,
                           'result': store_area_row.result}
            result_dict_list.append(result_dict)
        return result_dict_list

    def calculate_blocking(self, row, df):
        return_holder = self._get_kpi_name_and_fk(row)
        numerator_type, denominator_type = self._get_numerator_and_denominator_type(
            row['Config Params: JSON'], context_relevant=False)
        df.dropna(subset=[numerator_type], inplace=True)
        result_dict_list = self._logic_for_blocking(return_holder, df, numerator_type, denominator_type)
        return result_dict_list

    def _logic_for_blocking(self, return_holder, df, numerator_type, denominator_type):
        result_dict_list = []
        for unique_denominator_id in set(df[denominator_type]):
            relevant_df = self._filter_df(df, {denominator_type: unique_denominator_id})
            for unique_scene_fk in set(relevant_df.scene_fk):
                scene_relevant_df = self._filter_df(relevant_df, {'scene_fk': unique_scene_fk})
                location = {Consts.SCENE_FK: unique_scene_fk}
                for unique_numerator_id in set(scene_relevant_df[numerator_type]):
                    relevant_filter = {numerator_type: [unique_numerator_id]}
                    block = self.block.network_x_block_together(population=relevant_filter, location=location,
                                                                additional={'calculate_all_scenes': False,
                                                                            'use_masking_only': True,
                                                                            'include_stacking': False})
                    passed_block = block[block.is_block.isin([True])]
                    if block.empty:
                        continue
                    elif passed_block.empty:
                        relevant_block = block.iloc[block.block_facings.astype(
                            int).idxmax()]
                        numerator_result = relevant_block.block_facings
                        denominator_result = relevant_block.total_facings
                        result = 0
                    else:
                        relevant_block = passed_block.iloc[passed_block.block_facings.astype(
                            int).idxmax()]
                        numerator_result = relevant_block.block_facings
                        denominator_result = relevant_block.total_facings
                        result = 1
                        probe_match_fks = [item for each in relevant_block.cluster.nodes.values() for item in
                                           each['probe_match_fk']]
                        self.mark_tags_in_explorer(probe_match_fks, return_holder[0])
                    if not isinstance(unique_numerator_id, int):
                        unique_numerator_id = self._get_id_from_custom_entity_table(numerator_type, unique_numerator_id)
                    result_dict = {'kpi_name': return_holder[0], 'kpi_fk': return_holder[1],
                                   'numerator_id': unique_numerator_id, 'numerator_result': numerator_result,
                                   'denominator_id': unique_denominator_id,
                                   'denominator_result': denominator_result,'context_id':unique_scene_fk,
                                   'result': result}
                    result_dict_list.append(result_dict)
        return result_dict_list

    def mark_tags_in_explorer(self, probe_match_fk_list, mpipsr_name):
        if not probe_match_fk_list:
            return
        try:
            match_type_fk = \
                self.match_product_in_probe_state_reporting[
                    self.match_product_in_probe_state_reporting['name'] == mpipsr_name][
                    'match_product_in_probe_state_reporting_fk'].values[0]
        except IndexError:
            Log.warning('Name not found in match_product_in_probe_state_reporting table: {}'.format(mpipsr_name))
            return

        match_product_in_probe_state_values_old = self.common.match_product_in_probe_state_values
        match_product_in_probe_state_values_new = pd.DataFrame(columns=[MATCH_PRODUCT_IN_PROBE_FK,
                                                                        MATCH_PRODUCT_IN_PROBE_STATE_REPORTING_FK])
        match_product_in_probe_state_values_new[MATCH_PRODUCT_IN_PROBE_FK] = probe_match_fk_list
        match_product_in_probe_state_values_new[MATCH_PRODUCT_IN_PROBE_STATE_REPORTING_FK] = match_type_fk

        # self.common.match_product_in_probe_state_values = pd.concat([match_product_in_probe_state_values_old,
        #                                                                 match_product_in_probe_state_values_new])
        self.common.match_product_in_probe_state_values = self.common.match_product_in_probe_state_values.append(
            match_product_in_probe_state_values_new)

        return

    def calculate_share_of_scenes(self, row, df):
        return_holder = self._get_kpi_name_and_fk(row)
        facings_threshold = int(row['Config Params: JSON'].get('facings_threshold')[0])
        numerator_type, denominator_type, context_type = self._get_numerator_and_denominator_type(
            row['Config Params: JSON'], context_relevant=True)
        list_holder = self.logic_of_sos(df, numerator_type, denominator_type,
                                        context_type, facings_threshold)
        if list_holder:
            df_holder = pd.DataFrame(list_holder)
            count_of_denominator_df = df_holder.groupby(['context_id', 'denominator_id']).agg(
                {'numerator_id': 'count'}).rename(columns={'numerator_id': 'count_of_denominator_id'}).reset_index()
            count_of_numerator_df = df_holder.groupby(['context_id', 'denominator_id', 'numerator_id']).agg(
                {'numerator_id': 'count'}).rename(columns={'numerator_id': 'count_of_numerator_id'}).reset_index()
            final_df = count_of_numerator_df.merge(count_of_denominator_df, how='inner',
                                                   on=['context_id', 'denominator_id'])
            final_df['result'] = np.true_divide(final_df.count_of_numerator_id, final_df.count_of_denominator_id)
            list_holder = []
            for item in final_df.itertuples():
                result_dict = {'kpi_name': return_holder[0], 'kpi_fk': return_holder[1],
                               'numerator_id': item.numerator_id, 'numerator_result': item.count_of_numerator_id,
                               'denominator_id': item.denominator_id,
                               'denominator_result': item.count_of_denominator_id, 'context_id': item.context_id,
                               'result': item.result}
                list_holder.append(result_dict)
        return list_holder

    @staticmethod
    def logic_of_sos(relevant_scif, numerator_type, denominator_type, context_type,
                     facings_threshold):
        return_list = []
        # result_container_df = pd.DataFrame(columns=['numerator_id','denominator_id','context_id','numerator_result', 'denominator_result'])
        for unique_context_fk in set(relevant_scif[context_type]):
            template_unique_scif = relevant_scif[relevant_scif[context_type].isin([unique_context_fk])]
            for unique_denominator_fk in set(template_unique_scif[denominator_type]):
                category_unique_scif = template_unique_scif[
                    template_unique_scif[denominator_type].isin([unique_denominator_fk])]
                for unique_scene in set(category_unique_scif.scene_fk):
                    scene_unique_scif = category_unique_scif[category_unique_scif.scene_fk.isin([unique_scene])]
                    for unique_numerator_fk in set(scene_unique_scif[numerator_type]):
                        manufacturer_unique_scif = scene_unique_scif[
                            scene_unique_scif[numerator_type].isin([unique_numerator_fk])]

                        if manufacturer_unique_scif.drop_duplicates(subset=['product_fk'])[
                            Consts.FINAL_FACINGS].sum() >= facings_threshold:
                            dict_holder = {'numerator_id': unique_numerator_fk, 'denominator_id': unique_denominator_fk,
                                           'context_id': unique_context_fk}
                            return_list.append(dict_holder)
        return return_list
        # if unique_numerator_fk == 494 or unique_denominator_fk == 25:
        #     a = 1
        #
        # if unique_denominator_fk in result_container_df.denominator_id.values :
        #     place_holder_denominator_result = result_container_df.loc[result_container_df.denominator_id == unique_denominator_fk,'denominator_result'] + 1
        #     if unique_numerator_fk in result_container_df.numerator_id.values:
        #         place_holder_numerator_result = result_container_df.loc[result_container_df.numerator_id == unique_numerator_fk,'numerator_result'] + 1
        #
        #     else:
        #         place_holder_numerator_result = 1
        #
        #
        #     result_container_df.loc[
        #         result_container_df.numerator_id == unique_numerator_fk, 'numerator_result'] = place_holder_numerator_result
        #     result_container_df.loc[
        #         result_container_df.denominator_id == unique_denominator_fk, 'denominator_result'] = place_holder_denominator_result
        # else:
        #     result_container_df.loc[len(result_container_df)] = [unique_numerator_fk,
        #                                                          unique_denominator_fk,
        #                                                          unique_context_fk, 1, 1]

        # TERRIABLE LOGIC: NEED TO CHANGE
        # if unique_numerator_fk in result_container_df.numerator_id.values:
        #     if unique_denominator_fk not in result_container_df.denominator_id.values:
        #         result_container_df.loc[len(result_container_df)] = [unique_numerator_fk, unique_denominator_fk, unique_context_fk, 1, 1]
        #
        #     else:
        #         result_container_df.loc[result_container_df.numerator_id == unique_numerator_fk,'numerator_result'] = result_container_df.loc[result_container_df.numerator_id == unique_numerator_fk,'numerator_result'] + 1
        #         result_container_df.loc[result_container_df.denominator_id == unique_denominator_fk,'denominator_result'] = result_container_df.loc[result_container_df.denominator_id == unique_denominator_fk,'denominator_result'] + 1
        # else:
        #     if unique_denominator_fk in result_container_df.denominator_id.values:
        #         denominator_result_placeholder = (result_container_df.loc[
        #              result_container_df.denominator_id == unique_denominator_fk, 'denominator_result'] + 1).iat[
        #             0]
        #         result_container_df.loc[
        #             result_container_df.denominator_id == unique_denominator_fk, 'denominator_result'] = denominator_result_placeholder
        #         result_container_df.loc[len(result_container_df)] = [unique_numerator_fk, unique_denominator_fk, unique_context_fk, 1, denominator_result_placeholder]
        #
        #     else:
        #         result_container_df.loc[len(result_container_df)] = [unique_numerator_fk, unique_denominator_fk, unique_context_fk, 1, 1]

    def get_store_area_df(self):
        query = """
                 select st.pk, sst.scene_fk, st.name, sc.session_uid 
                 from probedata.scene_store_task_area_group_items sst
                 join static.store_task_area_group_items st on st.pk=sst.store_task_area_group_item_fk
                 join probedata.scene sc on sc.pk=sst.scene_fk
                 where sc.delete_time is null and sc.session_uid = '{}';
                 """.format(self.session_uid)

        df = pd.read_sql_query(query, self.rds_conn.db)
        return df

    def get_kpi_entity_type_fk(self, numerator_type):
        query = """select pk, name, table_name from static.kpi_entity_type 
                    where name = '{}';""".format(numerator_type)
        df = pd.read_sql_query(query, self.rds_conn.db)
        return df

    def get_kpi_custom_entity_table(self):
        """
        :param entity_type: pk of entity from static.entity_type
        :return: the DF of the static.custom_entity of this entity_type
        """
        query = "SELECT pk, name, entity_type_fk FROM static.custom_entity;"
        df = pd.read_sql_query(query, self.rds_conn.db)
        return df

    def _parse_json_filters_to_df(self, row):
        JSON = row[row.index.str.contains('JSON') & (~ row.index.str.contains('Config Params'))]
        filter_JSON = JSON[~JSON.isnull()]

        filtered_scif_mpis = self.merged_scif_mpis
        for each_JSON in filter_JSON:
            final_JSON = {'population': each_JSON} if ('include' or 'exclude') in each_JSON else each_JSON
            filtered_scif_mpis = ParseInputKPI.filter_df(final_JSON, filtered_scif_mpis)
        if 'include_stacking' in row['Config Params: JSON'].keys():
            including_stacking = row['Config Params: JSON']['include_stacking'][0]
            filtered_scif_mpis[
                Consts.FINAL_FACINGS] = filtered_scif_mpis.facings if including_stacking == 'True' else filtered_scif_mpis.facings_ign_stack
            filtered_scif_mpis = filtered_scif_mpis[filtered_scif_mpis.stacking_layer == 1]
        return filtered_scif_mpis

    def apply_json_parser(self, row):
        json_relevent_rows_with_parse_logic = row[row.index.str.contains('JSON')].apply(self.parse_json_row)
        row = row[~ row.index.isin(json_relevent_rows_with_parse_logic.index)].append(
            json_relevent_rows_with_parse_logic)
        return row

    def parse_json_row(self, item):
        '''
        :param item: improper json value (formatted incorrectly)
        :return: properly formatted json dictionary

        The function will be in conjunction with apply. The function will applied on the row(pandas series). This is
            meant to convert the json comprised of improper format of strings and lists to a proper dictionary value.
        '''
        if item:
            container = self.prereq_parse_json_row(item)
        else:
            container = None
        return container

    @staticmethod
    def prereq_parse_json_row(item):
        '''
        primarly logic for formatting the value of the json
        '''

        if isinstance(item, list):
            container = OrderedDict()
            for it in item:
                # value = re.findall("[0-9a-zA-Z_]+", it)
                value = re.findall("'([^']*)'", it)
                if len(value) == 2:
                    for i in range(0, len(value), 2):
                        container[value[i]] = [value[i + 1]]
                else:
                    if len(container.items()) == 0:
                        print('issue')  # delete later
                        # raise error
                        # haven't encountered an this. So should raise an issue.
                        pass
                    else:
                        last_inserted_value_key = container.items()[-1][0]
                        container.get(last_inserted_value_key).append(value[0])
        else:
            container = ast.literal_eval(item)
        return container

    def _get_kpi_name_and_fk(self, row):
        kpi_name = row[Consts.KPI_NAME]
        kpi_fk = self.common.get_kpi_fk_by_kpi_type(kpi_name)
        output = [kpi_name, kpi_fk]
        return output

    def _get_sku_name_and_fk(self, kpi_name):
        final_kpi = kpi_name + " - SKU"
        final_kpi_fk = self.common.get_kpi_fk_by_kpi_type(final_kpi)
        return [final_kpi, final_kpi_fk]

    def _get_id_from_custom_entity_table(self, numerator_type, string_numerator_id):
        '''
        :param numerator_type: Numerator type from the config json file
        :param string_numerator_id: the name of the numerator if the numerator type is a string

        Use the class variable to reference the data in the custom entity table. The goal of the method is to save the
        custom entity to the table if it doesn't exist. If it does exists, retreive the pk.
        '''

        # performs a query on static.kpi_entity_type table and then gets the relevant custom entity fk
        relevant_entity_type_fk = self.get_kpi_entity_type_fk(numerator_type).loc[0, 'pk']
        if self.final_custom_entity_table.empty:
            final_numerator_id = 1
            self.final_custom_entity_table.loc[final_numerator_id, self.final_custom_entity_table.columns] = [
                final_numerator_id, string_numerator_id, relevant_entity_type_fk]
            self._save_into_custom_entity_table(final_numerator_id, string_numerator_id,
                                                relevant_entity_type_fk)
        elif string_numerator_id not in self.final_custom_entity_table.name.values:
            final_numerator_id = np.amax(self.final_custom_entity_table.pk) + 1
            self.final_custom_entity_table.loc[final_numerator_id, self.final_custom_entity_table.columns] = [
                final_numerator_id, string_numerator_id, relevant_entity_type_fk]
            self._save_into_custom_entity_table(final_numerator_id, string_numerator_id,
                                                relevant_entity_type_fk)
        else:
            final_numerator_id = \
                self.final_custom_entity_table[self.final_custom_entity_table.name.isin([string_numerator_id])].iloc[
                    0, 0]
        return final_numerator_id

    def _save_into_custom_entity_table(self, final_numerator_id, string_numerator_id,
                                       relevant_entity_type_fk):
        query = """INSERT INTO static.custom_entity (pk,name, entity_type_fk) 
                                              values ({},"{}", {});""".format(final_numerator_id, string_numerator_id,
                                                                              relevant_entity_type_fk)
        cur = self.rds_conn.db.cursor()
        cur.execute(query)
        self.rds_conn.db.commit()

    @staticmethod
    def _get_numerator_and_denominator_type(config_param, context_relevant=False):
        numerator_type = config_param['numerator_type'][0]
        denominator_type = config_param['denominator_type'][0]
        if context_relevant:
            context_type = config_param['context_type'][0]
            return numerator_type, denominator_type, context_type
        return numerator_type, denominator_type

    @staticmethod
    def _filter_df(df, filters, exclude=0):
        for key, val in filters.items():
            if not isinstance(val, list):
                val = [val]
            if exclude:
                df = df[~df[key].isin(val)]
            else:
                df = df[df[key].isin(val)]

        return df

    @staticmethod
    def _df_groupby_logic(df, grouby_columns, aggregation_dict):
        '''
        :param df: relevant dataframe
        :param grouby_columns: list of relevant columns that are grouped
        :param aggregation_dict: aggregation dictionary with relevant column and logic
                example: {'facings':'sum'}
        :return: returns dataframe with groupby logic applied
        '''

        if not isinstance(grouby_columns, list):
            grouby_columns = [grouby_columns]

        final_df = df.groupby(grouby_columns).agg(aggregation_dict)
        return final_df
