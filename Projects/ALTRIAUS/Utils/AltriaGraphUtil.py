import pandas as pd
from plotly.offline import iplot
from Trax.Algo.Calculations.Core.AdjacencyGraph.Builders import AdjacencyGraphBuilder, NodeAttribute
from Trax.Algo.Calculations.Core.AdjacencyGraph.Plots import GraphPlot, MaskingPlot
from Projects.ALTRIAUS.Utils.AltriaDataProvider import AltriaDataProvider
from Trax.Utils.Logging.Logger import Log

from Trax.Algo.Geometry.Masking.MaskingResultsIO import retrieve_maskings_flat
from Trax.Algo.Geometry.Masking.Utils import transform_maskings_flat

import numpy as np
from shapely.geometry import MultiPolygon, box
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
        self.conversion_data = None

        self.base_adj_graph = None
        self.condensed_adj_graph = None
        self.adj_component_graph = None

        self._generate_graph()

    def create_masking_and_matches(self):
        try:
            self.masking_data = transform_maskings_flat(*retrieve_maskings_flat(self.data_provider.project_name,
                                                                self.data_provider.scenes_info['scene_fk'].to_list()))
        except Exception as err:
            Log.error('Could not retrieve masking, error: {}'.format(err))
            return pd.DataFrame()

        self.matches_df = self.matches.merge(self.data_provider.all_products, on='product_fk')
        self.matches_df = self.matches_df[self.matches_df['scene_fk'] == self.scene_id]

        # smart_attribute_data = \
        #     self.adp.get_match_product_in_probe_state_values(self.matches_df['probe_match_fk'].unique().tolist())
        #
        # self.matches_df = pd.merge(self.matches_df, smart_attribute_data, on='probe_match_fk', how='left')
        # self.matches_df['match_product_in_probe_state_fk'].fillna(0, inplace=True)

        self.matches_df['pk'] = self.matches_df['scene_match_fk']
        self.probe_groups = self.adp.get_probe_groups()
        self.matches_df = self.matches_df.merge(self.probe_groups, on='probe_match_fk', how='left')
        self.pos_matches_df = self.matches_df[self.matches_df['category'] == 'POS']
        self.pos_masking_data = self.adp.get_masking_data(self.matches_df, y_axis_threshold=35, x_axis_threshold=100)

    def get_graph(self):
        return self.adj_component_graph

    def _generate_graph(self):
        self.create_masking_and_matches()
        combined_maskings = pd.concat([self.masking_data, self.pos_masking_data])
        self.matches_df.loc[self.matches_df['category'] == 'POS', 'stacking_layer'] = 0
        self.matches_df.loc[(self.matches_df['stacking_layer'] < 1) &
                            (self.matches_df['probe_match_fk'].isin(self.check_and_add_menu_board_pos_item())),
                            'stacking_layer'] = 2
        filtered_mpis = self.matches_df.loc[~self.matches_df['category'].isin(['General'])]

        self.base_adj_graph = AdjacencyGraphBuilder.initiate_graph_by_dataframe(filtered_mpis, combined_maskings,
                                                                      ['category', 'in_menu_board_area',
                                                                       'probe_group_id'], use_masking_only=True,
                                                                                minimal_overlap_ratio=0.4)
        self.base_adj_graph = self.remove_pos_to_pos_edges(self.base_adj_graph)
        self.conversion_data = self.calculate_width_conversion()

        self.condensed_adj_graph = AdjacencyGraphBuilder.condense_graph_by_level('category', self.base_adj_graph)
        self.condensed_adj_graph = self.condensed_adj_graph.to_undirected()
        self._severe_connections_across_weakly_related_bays()
        self._pair_signage_with_fixture_block()
        self._filter_pos_edges_to_closest()
        self._consume_fully_encapsulated_nodes()
        self._remove_nodes_with_less_facings_than(self.minimum_facings_for_block)
        self._calculate_widths_of_flip_signs()

        # create component graph (split graph into separate components by removing horizontal connections
        non_horizontal_edges = self._filter_horizontal_edges_between_blocks(self.condensed_adj_graph)
        if non_horizontal_edges:
            self.adj_component_graph = self.condensed_adj_graph.edge_subgraph(non_horizontal_edges)
        else:
            self.adj_component_graph = self.condensed_adj_graph.copy()
        self._assign_fixture_and_block_numbers_to_product_clusters()
        self._assign_block_numbers_to_pos_items()
        self._assign_width_values_to_all_nodes()
        self._assign_no_header_attribute()
        self._assign_product_above_header_attribute()

        # self.create_graph_image()

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

    def _severe_connections_across_weakly_related_bays(self):
        edges_to_remove = []
        # for every condensed node, find all edges that span across bays
        for node, node_data in self.condensed_adj_graph.nodes(data=True):
            bay_numbers = node_data['bay_number'].values
            if len(bay_numbers) < 2:
                continue

            node_matches = self.matches_df[self.matches_df['scene_match_fk'].isin(node_data['members'])]
            bay_groups = node_matches.groupby(['bay_number', 'shelf_number'], as_index=False)['scene_match_fk'].count()
            # we will allow bays that have one extra facing since it's probably just a tag placement error
            bay_groups = bay_groups[bay_groups['scene_match_fk'] > 1]
            bay_groups = bay_groups.groupby(['bay_number'], as_index=False)['shelf_number'].count()
            max_shelves = bay_groups['shelf_number'].max()
            bay_groups['orphaned'] = \
                bay_groups['shelf_number'].apply(lambda x: True if x <= max_shelves * 0.5 else False)

            if len(bay_groups[bay_groups['orphaned']]) > 1:
                Log.error("Unable to severe bay connections for {} category. Too many bays would be orphaned".format(
                    node_data['category'].value))
                continue
            elif len(bay_groups[bay_groups['orphaned']]) == 0:
                continue

            orphaned_bay = bay_groups[bay_groups['orphaned']]['bay_number'].iloc[0]

            sub_graph = self.base_adj_graph.subgraph(node_data['members'])
            for edge in sub_graph.edges():
                node1_bay = sub_graph.nodes[edge[0]]['bay_number'].value
                node2_bay = sub_graph.nodes[edge[1]]['bay_number'].value
                if node1_bay == node2_bay:
                    continue
                elif node1_bay == orphaned_bay or node2_bay == orphaned_bay:
                    edges_to_remove.append(edge)

        if edges_to_remove:
            edges_to_keep = [edge for edge in self.base_adj_graph.edges() if
                             edge not in edges_to_remove and (edge[1], edge[0]) not in edges_to_remove]

            self.base_adj_graph = self.base_adj_graph.edge_subgraph(edges_to_keep)

            self.condensed_adj_graph = AdjacencyGraphBuilder.condense_graph_by_level('category', self.base_adj_graph)
            self.condensed_adj_graph = self.condensed_adj_graph.to_undirected()
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

    @staticmethod
    def _filter_horizontal_edges_between_blocks(condensed_adj_graph):
        edges_to_remove = []
        sub_graph = condensed_adj_graph.subgraph([node for node, node_data in condensed_adj_graph.nodes(data=True) if
                                                  node_data['category'].value != 'POS'])
        for node1, node2, edge_data in sub_graph.edges(data=True):
            if -50.0 < edge_data['degree'] < 50.0:
                edges_to_remove.extend([(node1, node2), (node2, node1)])
            elif 130.0 < edge_data['degree'] < 185.0:
                edges_to_remove.extend([(node1, node2), (node2, node1)])
            elif -185.0 < edge_data['degree'] < -130.0:
                edges_to_remove.extend([(node1, node2), (node2, node1)])

        edges_to_keep = [edge for edge in condensed_adj_graph.edges() if edge not in edges_to_remove]
        return edges_to_keep

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

        for node, skus_above in skus_above_flipsign_dict.iteritems():
            self.condensed_adj_graph.nodes[node]['width_of_signage_in_facings'] = NodeAttribute(
                [len(skus_above) + additional_skus_to_add])

        return

    def _pair_signage_with_fixture_block(self):
        # requires graph to be undirected
        edges_to_remove = []
        flip_sign_nodes = []
        header_nodes = []
        orphaned_header_nodes = []
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
            # if a POS item doesn't exist in a shadow or in a block, it must be a header
            elif not shadow_parent_nodes and not parent_nodes:
                orphaned_header_nodes.append(node_id)

        for node in header_nodes:
            self.condensed_adj_graph.nodes[node]['pos_type'] = NodeAttribute(['Header'])
        for node in flip_sign_nodes:
            self.condensed_adj_graph.nodes[node]['pos_type'] = NodeAttribute(['Flip-Sign'])
        for node in orphaned_header_nodes:
            self.condensed_adj_graph.nodes[node]['pos_type'] = NodeAttribute(['Header'])

        if orphaned_header_nodes:
            edges_for_most_likely_parents = \
                self._get_most_likely_parent_edges_for_orphaned_headers(orphaned_header_nodes)
            edges_to_remove = [edge for edge in edges_to_remove if edge not in edges_for_most_likely_parents]

        if edges_to_remove:
            edges_to_keep = [edge for edge in self.condensed_adj_graph.edges() if
                             edge not in edges_to_remove and (edge[1], edge[0]) not in edges_to_remove]
            self.condensed_adj_graph = self.condensed_adj_graph.edge_subgraph(edges_to_keep)
        return

    def _get_most_likely_parent_edges_for_orphaned_headers(self, orphaned_header_nodes):
        edges_for_most_likely_parents = []
        for orphan_node_id in orphaned_header_nodes:
            potential_parents = {}
            orphan_bounds = self.condensed_adj_graph.nodes[orphan_node_id]['polygon'].bounds
            orphan_box = box(orphan_bounds[0], orphan_bounds[3], orphan_bounds[2], 9999)

            for node, node_data in self.condensed_adj_graph.nodes(data=True):
                if node_data['category'] == 'POS':
                    continue
                if node == orphan_node_id:
                    continue
                node_bounds = node_data['polygon'].bounds
                node_box = box(node_bounds[0], node_bounds[1], node_bounds[2], node_bounds[3])

                if node_box.intersects(orphan_box):
                    potential_parents[node_box.centroid.coords[0][1]] = node

            if potential_parents:
                closest_parent = sorted(potential_parents.items())[0][1]
                edges_for_most_likely_parents.append((orphan_node_id, closest_parent))
                edges_for_most_likely_parents.append((closest_parent, orphan_node_id))

        return edges_for_most_likely_parents

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
                merged_attributes = self.get_merged_node_attributes_from_nodes([node1_id, node2_id],
                                                                               self.condensed_adj_graph)
                merged_attributes['category'] = root_category_attribute
                node_attribute_dict[node1_id] = merged_attributes
                nodes_to_remove.append(node2_id)
            elif self._node_a_contains_node_b(node2, node1):
                root_category_attribute = node2['category']
                merged_attributes = self.get_merged_node_attributes_from_nodes([node1_id, node2_id],
                                                                               self.condensed_adj_graph)
                merged_attributes['category'] = root_category_attribute
                node_attribute_dict[node2_id] = merged_attributes
                nodes_to_remove.append(node1_id)

        for node, attributes in node_attribute_dict.iteritems():
            self.condensed_adj_graph.nodes[node].update(attributes)

        # unfreeze graph
        self.condensed_adj_graph = self.condensed_adj_graph.copy()

        for node in [node for node in set(nodes_to_remove)]:
            self.condensed_adj_graph.remove_node(node)
        return

    @staticmethod
    def _node_a_contains_node_b(node_a, node_b):
        node_a_bounds = node_a['polygon'].bounds
        node_b_bounds = node_b['polygon'].bounds
        node_a_box = box(node_a_bounds[0], node_a_bounds[1], node_a_bounds[2], node_a_bounds[3])
        node_b_box = box(node_b_bounds[0], node_b_bounds[1], node_b_bounds[2], node_b_bounds[3])
        # return node_a_box.contains(node_b_box)
        return (node_a_box.intersection(node_b_box).area / node_b_box.area) > 0.90

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
                shortest_edge = self._get_shortest_path(self.condensed_adj_graph, edges)
                edges_to_remove.extend([edge for edge in edges if edge != shortest_edge])

        if edges_to_remove:
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

    def _assign_fixture_and_block_numbers_to_product_clusters(self):
        x_position_avg_per_component = {}
        components = [c for c in nx.connected_components(self.adj_component_graph)]
        for component in components:
            x_coord_list = []
            y_position_per_node = {}
            for node in component:
                node_data = self.adj_component_graph.nodes[node]
                x_coord = node_data['polygon'].centroid.coords[0][0]
                x_coord_list.append(x_coord)

                if node_data['category'].value == 'POS':
                    continue
                y_coord = node_data['polygon'].centroid.coords[0][1]
                y_position_per_node[y_coord] = node

            for i, pair in enumerate(sorted(y_position_per_node.items())):
                self.adj_component_graph.nodes[pair[1]].update({"block_number": NodeAttribute([i + 1])})

            x_position_avg_per_component[(sum(x_coord_list) / len(x_coord_list))] = component

        for i, pair in enumerate(sorted(x_position_avg_per_component.items())):
            for node in pair[1]:
                self.adj_component_graph.nodes[node].update({"fixture_number": NodeAttribute([i + 1])})

        return

    def _assign_block_numbers_to_pos_items(self):
        node_to_block_number = {}
        for node, node_data in self.adj_component_graph.nodes(data=True):
            if node_data['category'].value != 'POS':
                continue
            block_number = 0
            for neighbor in self.adj_component_graph.neighbors(node):
                if block_number == 0:
                    block_number = self.adj_component_graph.nodes[neighbor]['block_number'].value

            node_to_block_number[node] = block_number

        for node, block_number in node_to_block_number.items():
            self.adj_component_graph.nodes[node].update({"block_number": NodeAttribute([block_number])})

        return

    def _assign_width_values_to_all_nodes(self):
        node_to_width = {}
        for node, node_data in self.adj_component_graph.nodes(data=True):
            one_ft = self.get_width_conversion_by_probe_group(node_data['probe_group_id'].value, self.conversion_data)
            bounds = node_data['polygon'].bounds
            width = abs((bounds[0] - bounds[2]) / one_ft)
            node_to_width[node] = width

        for node, width in node_to_width.items():
            self.adj_component_graph.nodes[node].update({'calculated_width_ft': NodeAttribute([width])})
        return

    def calculate_width_conversion(self):
        conversion_data = pd.DataFrame(columns=['probe_group_id', 'category', 'width'])
        for node, node_data in self.base_adj_graph.nodes(data=True):
            bounds = node_data['polygon'].bounds
            width = abs(bounds[0] - bounds[2])
            data = [node_data['probe_group_id'].value, node_data['category'].value, width]
            conversion_data.loc[len(conversion_data), conversion_data.columns.tolist()] = data

        return conversion_data

    @staticmethod
    def get_width_conversion_by_probe_group(probe_group_id, conversion_data):
        relevant_data = conversion_data[conversion_data['probe_group_id'] == probe_group_id]
        cigarettes_median = relevant_data[relevant_data['category'] == 'Cigarettes']['width'].median()
        cigarettes_avg = relevant_data[relevant_data['category'] == 'Cigarettes']['width'].mean()
        smokeless_median = relevant_data[relevant_data['category'] == 'Smokeless']['width'].median()

        cigarettes_one_ft = cigarettes_median * 5
        smokeless_one_ft = smokeless_median * 4

        if pd.np.isnan(cigarettes_one_ft):
            return smokeless_one_ft
        elif pd.np.isnan(smokeless_one_ft):
            return cigarettes_one_ft

        return (cigarettes_one_ft + smokeless_one_ft) / 2

    def _assign_no_header_attribute(self):
        nodes_without_headers = []
        for node, node_data in self.adj_component_graph.nodes(data=True):
            header_found = False
            try:
                if node_data['category'].value == 'POS' or node_data['block_number'].value != 1:
                    continue
            except KeyError:
                continue

            for neighbor in self.adj_component_graph.neighbors(node):
                neighbor_data = self.adj_component_graph.nodes[neighbor]
                if neighbor_data['category'].value != 'POS':
                    continue
                if neighbor_data['pos_type'].value == 'Header':
                    header_found = True

            if header_found:
                continue
            else:
                nodes_without_headers.append(node)

        for headerless_node in nodes_without_headers:
            self.adj_component_graph.nodes[headerless_node].update({'no_header': NodeAttribute([True])})

        return

    def _assign_product_above_header_attribute(self):
        nodes_with_products_above = []
        for node, node_data in self.adj_component_graph.nodes(data=True):
            try:
                if node_data['pos_type'].value != 'Header':
                    continue
            except KeyError:
                continue

            bounds = node_data['polygon'].bounds
            products_above_header = self.matches_df[(self.matches_df['rect_y'] < bounds[1]) &
                                         (self.matches_df['rect_x'] > bounds[0]) &
                                         (self.matches_df['rect_x'] < bounds[2])]
            products_above_header = products_above_header[products_above_header['category'] != 'POS']
            if not products_above_header.empty:
                nodes_with_products_above.append(node)

        for header_node in nodes_with_products_above:
            self.adj_component_graph.nodes[header_node].update({'product_above_header': NodeAttribute([True])})
            for parent_node in self.adj_component_graph.neighbors(header_node):
                self.adj_component_graph.nodes[parent_node].update({'product_above_header': NodeAttribute([True])})

        return

    # Plot Utils used for debugging

    @staticmethod
    def add_polygons_to_condensed_graph(condensed_adj_graph, level_attribute, mpis, fig):
        level_attribute_color_dict = GraphPlot.generate_sku_color_dict(mpis[level_attribute].unique().tolist())

        layout = fig.layout

        for node, node_data in condensed_adj_graph.nodes(data=True):
            rectangle = node_data['polygon']

            layout['shapes'] += ({
                                     'type': 'rect',
                                     'x0': rectangle.bounds[0],
                                     'y0': rectangle.bounds[1],
                                     'x1': rectangle.bounds[2],
                                     'y1': rectangle.bounds[3],
                                     'line': {'color': 'rgba(0, 230, 64, 1)', 'width': 3},
                                     'fillcolor': level_attribute_color_dict[node_data[level_attribute].value],
                                     'opacity': 0.4
                                 },)

        fig.update_layout(layout)
        return fig

    def create_graph_image(self, graph=None):
        if not graph:
            graph = self.adj_component_graph

        filtered_figure = GraphPlot.plot_networkx_graph(graph, overlay_image=True,
                                                        scene_id=self.scene_id, project_name='altriaus')
        filtered_figure.update_layout(autosize=False, width=1000, height=800)

        filtered_figure = self.add_polygons_to_condensed_graph(graph, 'category', self.matches_df,
                                                               filtered_figure)

        fig = MaskingPlot.plot_maskings(mpis_with_masking_df=self.matches_df.merge(self.pos_masking_data,
                                                                                   on='probe_match_fk'),
                                        overlay_image=True, scene_id=self.scene_id, project_name='altriaus',
                                        fig=filtered_figure)
        fig.update_layout(autosize=False, width=1000, height=800, title=str(self.scene_id))
        iplot(fig)

