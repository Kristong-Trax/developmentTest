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

MINIMUM_FACINGS_FOR_BLOCK = 3


class AltriaGraphBuilder(object):
    def __init__(self, data_provider, scene_id):
        self.data_provider = data_provider
        self.adp = AltriaDataProvider(self.data_provider)
        self.scene_id = scene_id
        self.matches = data_provider.matches
        self.minimum_facings_for_block = MINIMUM_FACINGS_FOR_BLOCK

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
        self._pair_signage_with_fixture_block()
        self._filter_pos_edges_to_closest()
        self._consume_fully_encapsulated_nodes()
        self._remove_nodes_with_less_facings_than(self.minimum_facings_for_block)
        self._calculate_widths_of_flip_signs()

        # create component graph (split graph into separate components by removing horizontal connections

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
        probe_match_fks_to_add = self.matches_df[(self.matches_df['in_menu_board_area']) &
                                                 (self.matches_df['category'] == 'POS')][
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

    def _calculate_widths_of_flip_signs(self):
        additional_skus_to_add = 1
        skus_above_flipsign_dict = {}
        for node, node_data in self.condensed_adj_graph.nodes(data=True):
            skus_above_flipsign = []
            try:
                if node_data['pos_type'].value != 'Flip-Sign':
                    continue
            except KeyError:
                continue

            match_fk = node_data['match_fk'].value

            for neighbor_match_fk in nx.all_neighbors(self.base_adj_graph, match_fk):
                neighbor_data = self.base_adj_graph.nodes[neighbor_match_fk]
                if self.polygon_above_other_polygon(neighbor_data['polygon'], node_data['polygon'],
                                                    required_overlap=0.10):
                    if neighbor_match_fk not in skus_above_flipsign:
                        skus_above_flipsign.append(neighbor_match_fk)

            skus_above_flipsign_dict[node] = skus_above_flipsign
            # print(skus_above_flipsign)

        for node, skus_above in skus_above_flipsign_dict.iteritems():
            self.condensed_adj_graph.nodes[node]['width_of_signage_in_facings'] = NodeAttribute(
                [len(skus_above) + additional_skus_to_add])

        return

    def _pair_signage_with_fixture_block(self):
        # requires graph to be undirected
        edges_to_remove = []
        flip_sign_nodes = []
        header_nodes = []
        for node_id, node_data in self.condensed_adj_graph.nodes(data=True):
            if node_data['category'].value != 'POS':
                continue
            parent_nodes = []
            shadow_parent_nodes = []
            for neighbor in self.condensed_adj_graph.neighbors(node_id):
                neighbor_data = self.condensed_adj_graph.nodes[neighbor]
                if self.polygon_inside_other_polygon(node_data['polygon'], neighbor_data['polygon']):
                    parent_nodes.append(neighbor)
                elif self.polygon_above_other_polygon(node_data['polygon'], neighbor_data['polygon']):
                    shadow_parent_nodes.append(neighbor)
                else:
                    edges_to_remove.append((node_id, neighbor))
            if parent_nodes and shadow_parent_nodes:
                for shadow_parent in shadow_parent_nodes:
                    edges_to_remove.append((node_id, shadow_parent))
            # if the POS item only exists in a shadow, and not inside any other blocks, it must be a header
            if shadow_parent_nodes and not parent_nodes:
                header_nodes.append(node_id)
            elif parent_nodes:
                flip_sign_nodes.append(node_id)

        for node in header_nodes:
            self.condensed_adj_graph.nodes[node]['pos_type'] = NodeAttribute(['Header'])
        for node in flip_sign_nodes:
            self.condensed_adj_graph.nodes[node]['pos_type'] = NodeAttribute(['Flip-Sign'])

        edges_to_keep = [edge for edge in self.condensed_adj_graph.edges() if
                         edge not in edges_to_remove and (edge[1], edge[0]) not in edges_to_remove]

        self.condensed_adj_graph = self.condensed_adj_graph.edge_subgraph(edges_to_keep)
        return edges_to_keep

    @staticmethod
    def cast_shadow_above(p, xoff, yoff):
        return p.union(affinity.translate(p, xoff=xoff, yoff=yoff)).envelope

    @staticmethod
    def polygon_inside_other_polygon(child_polygon, potential_parent_polygon):
        required_overlap = 0.80
        bounds = potential_parent_polygon.bounds
        parent_box = box(bounds[0], bounds[1], bounds[2], bounds[3])

        child_inside_parent = parent_box.intersection(child_polygon).area / child_polygon.area >= required_overlap
        return child_inside_parent

    def polygon_above_other_polygon(self, child_polygon, potential_parent_polygon, required_overlap=0.80):
        bounds = potential_parent_polygon.bounds
        parent_box = box(bounds[0], bounds[1], bounds[2], bounds[3])

        # cast shadow above the parent
        shadow_above_potential_parent = self.cast_shadow_above(parent_box, xoff=0, yoff=bounds[1] - bounds[3])
        child_above_parent = shadow_above_potential_parent.intersection(
            child_polygon).area / child_polygon.area >= required_overlap
        return child_above_parent

    def _consume_fully_encapsulated_nodes(self):
        nodes_to_remove = []
        node_attribute_dict = {}

        for node1_id, node2_id in self.condensed_adj_graph.edges():
            node1 = self.condensed_adj_graph.nodes[node1_id]
            node2 = self.condensed_adj_graph.nodes[node2_id]
            if node1['category'].value == 'POS' or node2['category'].value == 'POS':
                continue
            if self._node_a_contains_node_b(node1, node2):
                root_category_attribute = node1['category']
                # print((node1_id, node2_id))
                merged_attributes = self.get_merged_node_attributes_from_nodes([node1_id, node2_id],
                                                                               self.condensed_adj_graph)
                merged_attributes['category'] = root_category_attribute
                node_attribute_dict[node1_id] = merged_attributes
                nodes_to_remove.append(node2_id)
            elif self._node_a_contains_node_b(node2, node1):
                root_category_attribute = node2['category']
                # print((node2_id, node1_id))
                merged_attributes = self.get_merged_node_attributes_from_nodes([node1_id, node2_id],
                                                                               self.condensed_adj_graph)
                merged_attributes['category'] = root_category_attribute
                node_attribute_dict[node2_id] = merged_attributes
                nodes_to_remove.append(node1_id)

        for node, attributes in node_attribute_dict.iteritems():
            self.condensed_adj_graph.nodes[node].update(attributes)

        # unfreeze graph
        self.condensed_adj_graph = self.condensed_adj_graph.copy()

        for node in nodes_to_remove:
            self.condensed_adj_graph.remove_node(node)
        return

    @staticmethod
    def _node_a_contains_node_b(node_a, node_b):
        node_a_bounds = node_a['polygon'].bounds
        node_b_bounds = node_b['polygon'].bounds
        node_a_box = box(node_a_bounds[0], node_a_bounds[1], node_a_bounds[2], node_a_bounds[3])
        node_b_box = box(node_b_bounds[0], node_b_bounds[1], node_b_bounds[2], node_b_bounds[3])
        # return node_a_box.contains(node_b_box)
        return (node_a_box.intersection(node_b_box).area / node_b_box.area) > 0.95

    @staticmethod
    def get_merged_node_attributes_from_nodes(selected_nodes, graph):
        filtered_nodes = [n for i, n in graph.nodes(data=True) if i in selected_nodes]

        attributes_list = [attr for attr, value in graph.node[selected_nodes[0]].items() if
                           isinstance(value, NodeAttribute)]

        node_attributes = {}
        for attribute_name in attributes_list:
            node_attributes[attribute_name] = AdjacencyGraphBuilder._chain_attribute(attribute_name, filtered_nodes)

        # Total facing of all the products.
        total_facings = sum([n['facings'] for n in filtered_nodes])
        node_attributes.update({'facings': total_facings})

        return node_attributes

    def _filter_pos_edges_to_closest(self):
        edges_filter = []
        edges_to_remove = []
        for node_fk, node_data in self.condensed_adj_graph.nodes(data=True):
            edges = list(self.condensed_adj_graph.edges(node_fk))
            if node_data['category'].value != 'POS':
                edges_filter.extend(edges)
            elif len(edges) <= 1:
                edges_filter.extend(edges)
            else:
                shortest_edge = self._get_shortest_path(adj_g, edges)
                edges_to_remove.extend([edge for edge in edges if edge != shortest_edge])
        edges_filter = [edge for edge in edges_filter if
                        edge not in edges_to_remove and (edge[1], edge[0]) not in edges_to_remove]

        self.condensed_adj_graph = self.condensed_adj_graph.edge_subgraph(edges_filter)
        return

    def _get_shortest_path(self, adj_g, edges_to_check):
        """ This method gets a list of edge and returns the one with the minimum distance"""
        distance_per_edge = {edge: self._get_edge_distance(adj_g, edge) for edge in edges_to_check}
        shortest_edge = min(distance_per_edge, key=distance_per_edge.get)
        return shortest_edge

    def _get_edge_distance(self, adj_g, edge):
        """
        This method gets an edge and calculate it's length (the distance between it's nodes)
        """
        first_node_coordinate = np.array(self._get_node_display_coordinates(adj_g, edge[0]))
        second_node_coordinate = np.array(self._get_node_display_coordinates(adj_g, edge[1]))
        distance = np.sqrt(np.sum((first_node_coordinate - second_node_coordinate) ** 2))
        return distance

    @staticmethod
    def _get_node_display_coordinates(adj_g, node_fk):
        """
        This method gets a node and extract the Display coordinates (x and y).
        Those attributes were added to each node since this is the attributes we condensed the graph by
        """
        # return float(list(adj_g.nodes[node_fk]['rect_x'])[0]), float(list(adj_g.nodes[node_fk]['rect_y'])[0])
        centroid = adj_g.node[node_fk]['polygon'].centroid
        return centroid.coords[0][0], centroid.coords[0][1]

    def _remove_nodes_with_less_facings_than(self, minimum_facings):
        for node_fk, node_data in self.condensed_adj_graph.nodes(data=True):
            if node_data['category'].value == 'POS':
                continue
            if len(node_data['match_fk'].values) < minimum_facings:
                self.condensed_adj_graph.remove_node(node_fk)
        return

