import pandas as pd
from plotly.offline import download_plotlyjs, init_notebook_mode, plot, iplot
from plotly.subplots import make_subplots
import Trax.Algo.Calculations.Core.AdjacencyGraph.Builders
import Trax.Algo.Calculations.Core.AdjacencyGraph.GraphDiffs
import Trax.Algo.Calculations.Core.AdjacencyGraph.Plots.MaskingPlot
import Trax.Algo.Calculations.Core.AdjacencyGraph.Plots.GraphPlot
from Trax.Algo.Calculations.Core.AdjacencyGraph.Builders import AdjacencyGraphBuilder, NodeAttribute
from Trax.Algo.Calculations.Core.AdjacencyGraph.Plots import GraphPlot, MaskingPlot
import Trax.Algo.Geometry.Masking.MaskingResultsIO
import Trax.Algo.Geometry.Masking.Utils
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Trax.Utils.Conventions.Log import Severities
from Projects.ALTRIAUS.Utils.AltriaDataProvider import AltriaDataProvider
from Trax.Utils.Logging.Logger import Log

from Trax.Algo.Geometry.Masking.MaskingResultsIO import retrieve_maskings_flat
from Trax.Algo.Geometry.Masking.Utils import transform_maskings_flat

import numpy as np
from shapely.geometry import MultiPolygon, box, Point
from shapely import affinity
import networkx as nx


class AltriaGraphBuilder(object):
    def __init__(self, data_provider, scene_id):
        self.data_provider = data_provider
        self.adp = AltriaDataProvider(self.data_provider)
        self.scene_id = scene_id
        self.matches = data_provider.matches

        self.probe_groups = None
        self.masking_data = None
        self.matches_df = None
        self.pos_matches_df = None
        self.pos_masking_data = None

        self.base_adj_graph = None
        self.condensed_adj_graph = None

        self._generate_graph()

    def create_masking_and_matches(self):
        try:
            self.masking_data = transform_maskings_flat(*retrieve_maskings_flat(self.data_provider.project_name,
                                                                self.data_provider.scenes_info['scene_fk'].to_list()))
        except Exception as err:
            Log.error('Could not retrieve masking, error: {}'.format(err))
            return pd.DataFrame()

        self.matches_df = self.matches.merge(self.data_provider.all_products, on='product_fk')
        self.matches_df['pk'] = self.matches_df['scene_match_fk']
        self.probe_groups = self.adp.get_probe_groups()
        self.matches_df = self.matches_df.merge(self.probe_groups, on='probe_match_fk', how='left')
        self.pos_matches_df = self.matches_df[self.matches_df['category'] == 'POS']
        self.pos_masking_data = self.adp.get_masking_data(self.matches_df, y_axis_threshold=35)

    def _generate_graph(self):
        self.create_masking_and_matches()
        combined_maskings = pd.concat([self.masking_data, self.pos_masking_data])
        self.matches_df.loc[self.matches_df['category'] == 'POS', 'stacking_layer'] = 0
        self.matches_df.loc[(self.matches_df['stacking_layer'] < 1) &
                            (self.matches_df['probe_match_fk'].isin(self.check_and_add_menu_board_pos_item())),
                            'stacking_layer'] = 1
        filtered_mpis = self.matches_df.loc[~self.matches_df['category'].isin(['General'])]

        self.base_adj_graph = AdjacencyGraphBuilder.initiate_graph_by_dataframe(filtered_mpis, combined_maskings,
                                                                      ['category', 'in_menu_board_area',
                                                                       'probe_group_id'], use_masking_only=True)
        self.base_adj_graph = self.remove_pos_to_pos_edges(self.base_adj_graph)

        self.condensed_adj_graph = AdjacencyGraphBuilder.condense_graph_by_level('category', self.base_adj_graph)
        self.condensed_adj_graph = self.condensed_adj_graph.to_undirected()
        self._remove_products_outside_core_rectangles()
        return

    def check_and_add_menu_board_pos_item(self):
        pos_probe_match_fks_to_keep = self.pos_masking_data.probe_match_fk.tolist()
        mdis = self.adp.get_match_display_in_scene()
        mdis = mdis[mdis['display_name'] == 'Menu POS']
        if mdis.empty:
            self.matches_df['in_menu_board_area'] = False
            return pos_probe_match_fks_to_keep

        rect_size = 150
        minx = mdis['x'].iloc[0] - rect_size / 2
        miny = mdis['y'].iloc[0] - rect_size / 2
        maxx = mdis['x'].iloc[0] + rect_size / 2
        maxy = mdis['y'].iloc[0] + rect_size / 2

        menu_box = box(minx, miny, maxx, maxy)

        self.matches_df['in_menu_board_area'] = self.matches_df.apply(
            lambda row: menu_box.intersects(self.get_box_by_probe_match_fk(row['probe_match_fk'])), axis=1)
        probe_match_fks_to_add = self.matches_df[(self.matches_df['in_menu_board_area']) & (self.matches_df['category'] == 'POS')][
            'probe_match_fk'].tolist()
        pos_probe_match_fks_to_keep = pos_probe_match_fks_to_keep + probe_match_fks_to_add
        print(len(pos_probe_match_fks_to_keep))
        return pos_probe_match_fks_to_keep

    def get_box_by_probe_match_fk(self, probe_match_fk):
        mask = self.masking_data[self.masking_data['probe_match_fk'] == probe_match_fk]
        if mask.empty:
            return box()
        minx = mask['x_top'].iloc[0]
        miny = mask['y_top'].iloc[0]
        maxx = mask['x_right'].iloc[0]
        maxy = mask['y_right'].iloc[0]
        item_box = box(minx, miny, maxx, maxy)
        return item_box

    @staticmethod
    def remove_pos_to_pos_edges(adj_graph):
        edges_to_remove = []
        for node1, node2 in adj_graph.edges():
            if adj_graph.nodes[node1]['category'].value == 'POS' and adj_graph.nodes[node2]['category'].value == 'POS':
                edges_to_remove.append((node1, node2))
        for node_pair in edges_to_remove:
            adj_graph.remove_edge(node_pair[0], node_pair[1])
        return adj_graph

    def _remove_products_outside_core_rectangles(self):
        # TODO implement logic to determine outlier MPIS items
        mpis_fks_to_remove = [402157194, 402157017, 402157016, 402157015, 402157068]
        if self.matches_df[self.matches_df['scene_match_fk'].isin(mpis_fks_to_remove)].empty:
            return self.condensed_adj_graph
        # mpis_fks_to_remove = [402157017, 402157016, 402157015, 402157068]
        polygons_to_remove = self._get_polygons_by_mpis_fks(self.base_adj_graph, mpis_fks_to_remove)
        self.condensed_adj_graph = self. _remove_polygons(self.condensed_adj_graph, polygons_to_remove)
        return

    @staticmethod
    def _get_polygons_by_mpis_fks(uncondensed_graph, mpis_fks):
        polygons = []
        for fk in mpis_fks:
            polygons.append(uncondensed_graph.nodes[fk]['polygon'])
        return polygons

    @staticmethod
    def _remove_polygons(adj_graph, polygons_to_remove):
        for node_fk, node_data in adj_graph.nodes(data=True):
            multi_polygon = adj_graph.nodes[node_fk]['polygon']
            adj_graph.nodes[node_fk]['polygon'] = MultiPolygon(
                [p for p in multi_polygon if p not in polygons_to_remove])
        return adj_graph



