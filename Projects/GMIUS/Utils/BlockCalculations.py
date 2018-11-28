import networkx as nx
import numpy as np
import pandas as pd

from KPIUtils_v2.Calculations.PositionGraphsCalculations import PositionGraphs
from KPIUtils_v2.Calculations.BaseCalculations import BaseCalculation
import KPIUtils_v2.Calculations.CalculationsUtils.CalculationUtils as CalculationUtils
from Trax.Algo.Calculations.Core.GraphicalModel.AdjacencyGraphs import AdjacencyGraph
from Trax.Utils.Logging.Logger import Log


# class Block(object):
class Block(BaseCalculation):
    EXCLUDE_FILTER = 0
    INCLUDE_FILTER = 1
    CONTAIN_FILTER = 2
    EXCLUDE_EMPTY = False
    INCLUDE_EMPTY = True

    STRICT_MODE = ALL = 1000
    MM_TO_FEET_CONVERSION = 0.0032808399
    PRODUCT_FK_GRAPH = 'product_fk_list'

    EMPTY = 'Empty'
    DEFAULT = 'Default'
    TOP = 'Top'
    BOTTOM = 'Bottom'

    def __init__(self, data_provider, output=None, ps_data_provider=None, common=None, rds_conn=None, front_facing=False, **kwargs):
        super(Block, self).__init__(data_provider, output, ps_data_provider, common, rds_conn, **kwargs)
        self._position_graphs = PositionGraphs(self.data_provider)
        self.horizontal_bucket_size = self.MM_TO_FEET_CONVERSION
        self.outliers_threshold = 0.4
        self.check_vertical_horizontal = False
        self.include_stacking = False
        self.front_facing = front_facing

    @property
    def position_graphs(self):
        if not hasattr(self, '_position_graphs'):
            self._position_graphs = PositionGraphs(self.data_provider, rds_conn=self.rds_conn)
        return self._position_graphs

    def calculate_block_edges(self, minimum_block_ratio=0.01, allowed_products_filters=None,
                              include_empty=EXCLUDE_EMPTY, biggest_block = False, **filters):
        """
        :param minimum_block_ratio:
        :param number_of_allowed_others: Number of allowed irrelevant facings between two cluster of relevant facings.
        :param filters: The relevant facings of the block.
        :return: This function calculates the number of 'flexible blocks' per scene, meaning, blocks which are allowed
                 to have a given number of irrelevant facings between actual chunks of relevant facings.
        """
        edges = []
        filters, relevant_scenes = self.toolbox.separate_location_filters_from_product_filters(**filters)
        product_list = self.toolbox._get_group_product_list(filters)

        if len(relevant_scenes) == 0:
            Log.debug('Block Together: No relevant SKUs were found for these filters {}'.format(product_list))
        for scene in relevant_scenes:

            scene_graph = self.position_graphs.get(scene).copy()
            clusters, scene_graph = self.get_scene_blocks(scene_graph,
                                                          allowed_products_filters=allowed_products_filters,
                                                          include_empty=include_empty, **{'product_fk': product_list})
            new_relevant_vertices = self.filter_vertices_from_graph(scene_graph, **{'product_fk': product_list})

            for cluster in clusters:
                relevant_vertices_in_cluster = set(cluster).intersection(new_relevant_vertices)
                if len(new_relevant_vertices) > 0:
                    cluster_ratio = len(relevant_vertices_in_cluster) / float(len(new_relevant_vertices))
                else:
                    cluster_ratio = 0
                if cluster_ratio >= minimum_block_ratio:
                    cluster.sort(reverse=True)
                    edges = self.get_block_edges(scene_graph.copy().vs[cluster])
                    if biggest_block:
                        minimum_block_ratio = cluster_ratio
                    else:
                        break
        return edges

    def get_block_edges(self, graph):
        """
        This function receives one or more vertex data of a block's graph, and returns the range of its edges -
        The far most top, bottom, left and right pixels of its facings.
        """
        top = right = bottom = left = None

        top = graph.get_attribute_values('y_mm')
        top_index = max(xrange(len(top)), key=top.__getitem__)
        top_height = graph.get_attribute_values('height_mm_advance')[top_index]
        top = graph.get_attribute_values('y_mm')[top_index]
        top += top_height / 2

        bottom = graph.get_attribute_values('y_mm')
        bottom_index = min(xrange(len(bottom)), key=bottom.__getitem__)
        bottom_height = graph.get_attribute_values('height_mm_advance')[bottom_index]
        bottom = graph.get_attribute_values('y_mm')[bottom_index]
        bottom -= bottom_height / 2

        left = graph.get_attribute_values('x_mm')
        left_index = min(xrange(len(left)), key=left.__getitem__)
        left_height = graph.get_attribute_values('width_mm_advance')[left_index]
        left = graph.get_attribute_values('x_mm')[left_index]
        left -= left_height / 2

        right = graph.get_attribute_values('x_mm')
        right_index = max(xrange(len(right)), key=right.__getitem__)
        right_width = graph.get_attribute_values('width_mm_advance')[right_index]
        right = graph.get_attribute_values('x_mm')[right_index]
        right += right_width / 2

        # top = min(graph.get_attribute_values(self.position_graphs.RECT_Y))
        # right = max(graph.get_attribute_values(self.position_graphs.RECT_X))
        # bottom = max(graph.get_attribute_values(self.position_graphs.RECT_Y))
        # left = min(graph.get_attribute_values(self.position_graphs.RECT_X))
        result = {'visual': {'top': top, 'right': right, 'bottom': bottom, 'left': left}}
        result.update({'shelfs': list(set(graph.get_attribute_values('shelf_number')))})
        return result

    def get_scene_blocks(self, graph, allowed_products_filters=None, include_empty=EXCLUDE_EMPTY, **filters):
        """
        This function is a sub-function for Block Together. It receives a graph and filters and returns a list of
        clusters.
        """
        relevant_vertices = set(self.filter_vertices_from_graph(graph, **filters))
        if allowed_products_filters:
            allowed_vertices = self.filter_vertices_from_graph(graph, **allowed_products_filters)
        else:
            allowed_vertices = set()

        if include_empty == self.EXCLUDE_EMPTY:
            empty_vertices = {v.index for v in graph.vs.select(product_type='Empty')}
            allowed_vertices = set(allowed_vertices).union(empty_vertices)

        all_vertices = {v.index for v in graph.vs}
        vertices_to_remove = all_vertices.difference(relevant_vertices.union(allowed_vertices))
        # saving identifier to allowed vertices before delete of graph
        allowed_list_ids = {graph.vs[i]['scene_match_fk'] for i in allowed_vertices}
        graph.delete_vertices(vertices_to_remove)
        # saving the new vertices id's after delete of vertices
        new_allowed_vertices = {v.index for v in graph.vs if v['scene_match_fk'] in allowed_list_ids}
        # removing clusters including 'allowed' SKUs only
        blocks = [block for block in graph.clusters() if set(block).difference(new_allowed_vertices)]
        return blocks, graph

    def filter_vertices_from_graph(self, graph, **filters):
        """
        This function is given a graph and returns a set of vertices calculated by a given set of filters.
        """
        vertices_indexes = None
        for field in filters.keys():
            field_vertices = set()
            values = filters[field] if isinstance(filters[field], (list, tuple)) else [filters[field]]
            for value in values:
                vertices = [v.index for v in graph.vs.select(**{field: value})]
                field_vertices = field_vertices.union(vertices)
            if vertices_indexes is None:
                vertices_indexes = field_vertices
            else:
                vertices_indexes = vertices_indexes.intersection(field_vertices)
        vertices_indexes = vertices_indexes if vertices_indexes is not None else [v.index for v in graph.vs]
        if self.front_facing:
            front_facing_vertices = [v.index for v in graph.vs.select(front_facing='Y')]
            vertices_indexes = set(vertices_indexes).intersection(front_facing_vertices)
        return list(vertices_indexes)

    def calculate_block_together(self, allowed_products_filters=None, include_empty=EXCLUDE_EMPTY,
                                 minimum_block_ratio=0.9, result_by_scene=False, block_of_blocks=False,
                                 block_products1=None, block_products2=None, vertical=False, biggest_block=False,
                                 n_cluster=None, min_facings_in_block=None, **filters):
        """
        :param biggest_block:
        :param block_products1:
        :param block_products2:
        :param block_of_blocks:
        :param vertical: if needed to check vertical block by average shelf
        :param allowed_products_filters: These are the parameters which are allowed to corrupt the block without failing it.
        :param include_empty: This parameter dictates whether or not to discard Empty-typed products.
        :param minimum_block_ratio: The minimum (block number of facings / total number of relevant facings) ratio
                                    in order for KPI to pass (if ratio=1, then only one block is allowed).
        :param result_by_scene: True - The result is a tuple of (number of passed scenes, total relevant scenes);
                                False - The result is True if at least one scene has a block, False - otherwise.
        :param filters: These are the parameters which the blocks are checked for.
        :return: see 'result_by_scene' above.
        """
        filters, relevant_scenes = self.toolbox.separate_location_filters_from_product_filters(**filters)
        if len(relevant_scenes) == 0:
            if result_by_scene:
                return 0, 0
            elif vertical:
                return False, 0
            else:
                Log.debug('Block Together: No relevant SKUs were found for these filters {}'.format(filters))
                return False
        number_of_blocked_scenes = 0
        cluster_ratios = []
        for scene in relevant_scenes:
            scene_graph = self.position_graphs.get(scene).copy()
            clusters, scene_graph = self.get_scene_blocks(scene_graph,
                                                          allowed_products_filters=allowed_products_filters,
                                                          include_empty=include_empty, **filters)

            if block_of_blocks:
                new_relevant_vertices1 = self.filter_vertices_from_graph(scene_graph, **block_products1)
                new_relevant_vertices2 = self.filter_vertices_from_graph(scene_graph, **block_products2)
            else:
                new_relevant_vertices = self.filter_vertices_from_graph(scene_graph, **filters)
            for cluster in clusters:
                if block_of_blocks:
                    relevant_vertices_in_cluster1 = set(cluster).intersection(new_relevant_vertices1)
                    if len(new_relevant_vertices1) > 0:
                        cluster_ratio1 = len(relevant_vertices_in_cluster1) / float(len(new_relevant_vertices1))
                    else:
                        cluster_ratio1 = 0
                    relevant_vertices_in_cluster2 = set(cluster).intersection(new_relevant_vertices2)
                    if len(new_relevant_vertices2) > 0:
                        cluster_ratio2 = len(relevant_vertices_in_cluster2) / float(len(new_relevant_vertices2))
                    else:
                        cluster_ratio2 = 0
                    if cluster_ratio1 >= minimum_block_ratio and cluster_ratio2 >= minimum_block_ratio:
                        if min_facings_in_block:
                            if len(relevant_vertices_in_cluster1) >= min_facings_in_block \
                                    and len(relevant_vertices_in_cluster2) >= min_facings_in_block:
                                return True
                        else:
                            return True
                else:
                    relevant_vertices_in_cluster = set(cluster).intersection(new_relevant_vertices)
                    if len(new_relevant_vertices) > 0:
                        cluster_ratio = len(relevant_vertices_in_cluster) / float(len(new_relevant_vertices))
                    else:
                        cluster_ratio = 0
                    cluster_ratios.append(cluster_ratio)
                    if biggest_block:
                        continue
                    if cluster_ratio >= minimum_block_ratio:
                        if min_facings_in_block:
                            if len(relevant_vertices_in_cluster) >= min_facings_in_block:
                                if result_by_scene:
                                    number_of_blocked_scenes += 1
                                    break
                                else:
                                    all_vertices = {v.index for v in scene_graph.vs}
                                    non_cluster_vertices = all_vertices.difference(list(relevant_vertices_in_cluster))
                                    scene_graph.delete_vertices(non_cluster_vertices)
                                    if vertical:
                                        return True, len(
                                            set(scene_graph.vs['shelf_number']))
                                    return True
                        else:
                            if result_by_scene:
                                number_of_blocked_scenes += 1
                                break
                            else:
                                all_vertices = {v.index for v in scene_graph.vs}
                                non_cluster_vertices = all_vertices.difference(list(relevant_vertices_in_cluster))
                                scene_graph.delete_vertices(non_cluster_vertices)
                                if vertical:
                                    return True, len(
                                        set(scene_graph.vs['shelf_number']))
                                return True

            if n_cluster is not None:
                copy_of_cluster_ratios = cluster_ratios[:]
                largest_cluster = max(copy_of_cluster_ratios)  # 39
                copy_of_cluster_ratios.remove(largest_cluster)
                if len(copy_of_cluster_ratios) > 0:
                    second_largest_integer = max(copy_of_cluster_ratios)
                else:
                    second_largest_integer = 0
                cluster_ratio = largest_cluster + second_largest_integer
                if cluster_ratio >= minimum_block_ratio:
                    if vertical:
                        return {'block': True}

            if biggest_block:
                max_ratio = max(cluster_ratios)
                biggest_cluster = clusters[cluster_ratios.index(max_ratio)]
                relevant_vertices_in_cluster = set(biggest_cluster).intersection(new_relevant_vertices)
                all_vertices = {v.index for v in scene_graph.vs}
                non_cluster_vertices = all_vertices.difference(list(relevant_vertices_in_cluster))
                scene_graph.delete_vertices(non_cluster_vertices)
                if min_facings_in_block:
                    if len(relevant_vertices_in_cluster) >= min_facings_in_block:
                        return {'block': True, 'shelf_numbers': set(scene_graph.vs['shelf_number'])}
                else:
                    return {'block': True, 'shelf_numbers': set(scene_graph.vs['shelf_number'])}

            if result_by_scene:
                return number_of_blocked_scenes, len(relevant_scenes)
            elif vertical:
                return False, 0
            else:
                return False

    def network_x_block_together(self, population, location=None, additional=None):

        """
        :param location: The location parameters which the blocks are checked for (scene_type for example).
        :param population: These are the parameters which the blocks are checked for.
        :param additional: Additional attributes for the blocks calculation:
        1. calculate_all_scenes: whether to check all the relevant scenes for a block or stop once a block was found.
                If True, the function will search for blocks in all scenes (which will affect the performance!!).
                If False, the function will stop running once a scene was found with the relevant block.
        2. horizontal_bucket_size: Used to calculate the number of products in order to determine whether a block
                is vertical or horizontal. Usually 1ft - the length of the bay
        3. outliers_threshold: The threshold for removing the outliers for the horizontal/vertical calculation.
        4. include_stacking: Whether the products can be stacked on the shelf or not
        5. minimum_facing_for_block: Minimum number of facings that needs to be in a block to consider it as one.
        6. adjacency_overlap_ratio: Minimal threshold the overlap between the products must exceeds to be considered
                as adjacent.
        7. allowed_products_filters: These are the parameters which are allowed to corrupt the block without failing it.
        8. minimum_block_ratio: The minimum (block number of facings / total number of relevant facings) ratio
                                    in order for KPI to pass (if ratio=1, then only one block is allowed).
        9. check_vertical_horizontal: True if the orientation (vertical or horizontal) of the block should be checked for.
        :return: df with the following fields;
                block (Graph), scene_fk, orientation (string - vertical or horizontal), facing_percentage.

        """

        block_parameters = {'minimum_block_ratio': 0.75,
                            'check_vertical_horizontal': self.check_vertical_horizontal,
                            'allowed_products_filters': None,
                            'adjacency_overlap_ratio': 0.4,
                            'minimum_facing_for_block': 1,
                            'include_stacking': self.include_stacking,
                            'horizontal_bucket_size': self.horizontal_bucket_size,
                            'outliers_threshold': self.outliers_threshold,
                            'calculate_all_scenes': True}

        block_parameters.update(additional)

        self.horizontal_bucket_size = block_parameters['horizontal_bucket_size']
        self.outliers_threshold = block_parameters['outliers_threshold']
        self.check_vertical_horizontal = block_parameters['check_vertical_horizontal']
        self.include_stacking = block_parameters['include_stacking']

        # Constructing the result_df that will be returned if calculate_all_scenes = True
        results_df = pd.DataFrame(columns=['block', 'scene_fk', 'orientation', 'facing_percentage'])

        # Separate the filters on products from the filters on scene and get the relevant scenes
        relevant_scenes = self.input_parser.filter_df_by_conditions(location, self.scif).scene_id.unique()
        if not relevant_scenes.any():
            Log.info('No scenes with the requested location filter.')
            return results_df

        # Constructing the product_name_df for the adj graph
        product_name_df_columns = set(
            ['product_fk', 'product_name', 'product_short_name', 'brand_name', 'product_type'] + population.keys())
        product_name_df = self.data_provider.all_products[list(product_name_df_columns)]
        product_name_df['index'] = range(len(product_name_df))

        # Creating unified filters from the block filters and the allowed filters
        unified_filters = population.copy()
        if block_parameters['allowed_products_filters']:
            for key in block_parameters['allowed_products_filters'].keys():
                unified_filters[key] = block_parameters['allowed_products_filters'][key]

        # For each relevant scene check if a block is exist
        for scene in relevant_scenes:
            try:
                # Get the specific scene data
                scene_matches = self.data_provider.matches[self.data_provider.matches.scene_fk == scene]
                product_data_df = self.data_provider.all_products[list(set(['product_fk'] + unified_filters.keys()))]
                relevant_matches_for_block = self.filter_graph_data(scene_matches, product_data_df, population)

                scene_data, matches_df = self.get_raw_data(scene_matches, product_data_df, unified_filters)

                if (matches_df is None and scene_data.empty) or (scene_data is None and matches_df.empty):
                    continue

                # Create the adjacency graph on the data we filtered above
                adj_g = AdjacencyGraph(matches_df, scene_data, product_name_df,
                                       product_attributes=['rect_x', 'rect_y'] + list(population.keys()),
                                       name=None, adjacency_overlap_ratio=block_parameters['adjacency_overlap_ratio'])

                # Creating the condensed graph based on the adjacency graph created above
                # condensed_graph_sku = adj_g.build_adjacency_graph_from_base_graph_by_level(population.keys()[0])

                # Transferring the graph to be undirected
                # condensed_graph_sku = condensed_graph_sku.to_undirected()

                # Calculating all the components in the graph
                # components = list(nx.connected_component_subgraphs(condensed_graph_sku))
                components = list(nx.connected_component_subgraphs(adj_g.base_adjacency_graph.to_undirected()))

                # Constructing the block_res df that will contain each block's data
                blocks_res = pd.DataFrame(columns=['component', 'sum_of_facings', 'num_of_shelves'])

                # For each component in the graph calculate the blocks data
                for component in components:
                    component_data = self.calculate_block_data(component, relevant_matches_for_block)

                    blocks_res = blocks_res.append(component_data)

                # total_facings = blocks_res['sum_of_facings'].sum()

                # For each block check if its valid or not
                for row in blocks_res.itertuples():

                    # Calculate facing percentage
                    facing_percentage = np.divide(float(row.sum_of_facings), float(total_facings))
                    if (facing_percentage >= block_parameters['minimum_block_ratio']) and \
                            (row.sum_of_facings >= block_parameters['minimum_facing_for_block']):

                        orientation = None

                        # If needed, calculate the block's orientation
                        if block_parameters['check_vertical_horizontal']:
                            orientation = self.handle_horizontal_and_vertical(row.component, row.num_of_shelves)

                        results_df = results_df.append(pd.DataFrame(columns=['block', 'scene_fk', 'orientation',
                                                                             'facing_percentage'],
                                                                    data=[[row.component, scene, orientation,
                                                                           facing_percentage]]))
                        if not block_parameters['calculate_all_scenes']:
                            return results_df

            except Exception as err:
                Log.info('{}'.format(err))
                return results_df
        return results_df

    def handle_horizontal_and_vertical(self, graph, num_of_shelves):
        """

        :param graph: graph of the block that is being calculated
        :param num_of_shelves: num of shelves the block is spread on.
        :return: The threshold for removing the outliers for the horizontal/vertical calculation.
        """
        num_of_shelves = int(num_of_shelves)
        x_values = sorted(self.get_matches_by_graph_att(graph, 'rect_x'))
        y_values = sorted(self.get_matches_by_graph_att(graph, 'rect_y'))
        max_x = max(x_values)
        min_x = min(x_values)
        max_y = max(y_values)
        min_y = min(y_values)
        if num_of_shelves:
            y_division_factor = (max_y - min_y) / float(num_of_shelves)
        else:
            return None
        x_width_feet = ((max_x - min_x) * self.horizontal_bucket_size)
        y_bins = []
        current_min_y = min_y
        for i in xrange(num_of_shelves):
            n_bin = [y_value for y_value in y_values if
                     y_value in range(current_min_y, current_min_y + int(y_division_factor))]
            y_bins.append(n_bin)
            current_min_y += int(y_division_factor)
            if current_min_y >= max_y:
                break
        average_vertices_y = sum([len(x) for x in y_bins]) / float(len([len(x) for x in y_bins]))
        updated_y_bins = self.handle_block_outliers(y_bins, average_vertices_y)
        x_bins = []
        current_min_x = min_x * self.horizontal_bucket_size
        for z in range(int(x_width_feet) + 1):
            n_bin = [x_value for x_value in x_values if current_min_x <= x_value * self.horizontal_bucket_size <
                     current_min_x + 1]
            x_bins.append(n_bin)
            current_min_x += 1
            if current_min_x >= max_x * self.horizontal_bucket_size:
                break
        average_vertices_x = sum([len(x) for x in x_bins]) / float(len([len(x) for x in x_bins]))
        updated_x_bins = self.handle_block_outliers(x_bins, average_vertices_x)
        if updated_x_bins and updated_y_bins:
            if max(updated_x_bins[-1]) - min(updated_x_bins[0]) > max(updated_y_bins[-1]) - min(updated_y_bins[0]):
                return 'HORIZONTAL'
            return 'VERTICAL'
        return None

    def get_matches_by_graph_att(self, graph, att):
        """

        :param graph: networkX graph
        :param att: attribute exist in the matches df
        :return: the attribute of matches df filtered by the graph nodes
        """
        matches_fks = []
        for i, n in graph.nodes(data=True):
            matches_fks += n['group_attributes']['match_fk_list']
        return self.data_provider.matches[
            self.data_provider.matches['scene_match_fk'].isin(matches_fks)][att].drop_duplicates().values

    def filter_graph_data(self, df1, df2, filters):
        """
        :param df1: df with all the scene dataF
        :param df2: additional df with products data
        :param filters: the keys and values the merged data will be filtered on
        :return: filtered df.
        """
        merged_df = pd.merge(df1, df2, how='inner', left_on=['product_fk'], right_on=['product_fk'])
        filtered_df = self.input_parser.filter_df_by_population(
            population={'include': filters, 'include_operator': 'or'},
            dataframe_to_filter=merged_df)
        return filtered_df

    def handle_block_outliers(self, bins, average_count):
        """

        :param bins: list of products divided to bins by location
        :param average_count: average num of products in bin
        :param outliers_threshold: The threshold for removing the outliers for the horizontal/vertical calculation.
        :return: the received list of bins without the products that exceed the outliers_threshold
        """
        for i, item in enumerate(bins):
            if len(item) < average_count*self.outliers_threshold:
                bins.pop(i)
            else:
                break
        for j, item in enumerate(reversed(bins)):
            if len(item) < average_count*self.outliers_threshold:
                bins.pop(j)
            else:
                break
        return bins

    def calculate_block_data(self, component, relevant_matches_for_block):
        block_facings = 0
        max_shelf = 1
        min_shelf = 1

        # For each node in the component check if the node is valid
        for i, n in component.nodes(data=True):

            # Check if the node is a valid product or allowed attribute
            if n['group_attributes'][self.PRODUCT_FK_GRAPH][0] in \
                    (relevant_matches_for_block.product_fk.unique()):

                # Calculate block facings
                block_facings += n['group_attributes']['facings']

                # Calculate the number of shelves in block
                if self.check_vertical_horizontal:
                    if max_shelf < max(n['group_attributes']['shelves']):
                        max_shelf = max(n['group_attributes']['shelves'])
                    if min_shelf > min(n['group_attributes']['shelves']):
                        min_shelf = min(n['group_attributes']['shelves'])

        blocks_data = pd.DataFrame(
            columns=['component', 'sum_of_facings', 'num_of_shelves'],
            data=[[component, block_facings, (max_shelf - min_shelf + 1)]])

        return blocks_data

    def get_raw_data(self, scene_matches, product_data_df, unified_filters):
        # Where we should look only on products with stacking_layer == 1
        if not self.include_stacking:
            scene_data = CalculationUtils.adjust_stacking(scene_matches)
            # Filtering the scene_data to contain only the data relevant for the block
            scene_data = self.filter_graph_data(scene_data, product_data_df, unified_filters)
            matches_df = None
        else:
            # Filtering the matches_df to contain only the data relevant for the block
            matches_df = self.filter_graph_data(scene_matches, product_data_df, unified_filters)
            scene_data = None
        return scene_data, matches_df

    def network_x_block_together2(self, population, location=None, additional=None):

        """
        :param location: The location parameters which the blocks are checked for (scene_type for example).
        :param population: These are the parameters which the blocks are checked for.
        :param additional: Additional attributes for the blocks calculation:
        1. calculate_all_scenes: whether to check all the relevant scenes for a block or stop once a block was found.
                If True, the function will search for blocks in all scenes (which will affect the performance!!).
                If False, the function will stop running once a scene was found with the relevant block.
        2. horizontal_bucket_size: Used to calculate the number of products in order to determine whether a block
                is vertical or horizontal. Usually 1ft - the length of the bay
        3. outliers_threshold: The threshold for removing the outliers for the horizontal/vertical calculation.
        4. include_stacking: Whether the products can be stacked on the shelf or not
        5. minimum_facing_for_block: Minimum number of facings that needs to be in a block to consider it as one.
        6. adjacency_overlap_ratio: Minimal threshold the overlap between the products must exceeds to be considered
                as adjacent.
        7. allowed_products_filters: These are the parameters which are allowed to corrupt the block without failing it.
        8. minimum_block_ratio: The minimum (block number of facings / total number of relevant facings) ratio
                                    in order for KPI to pass (if ratio=1, then only one block is allowed).
        9. check_vertical_horizontal: True if the orientation (vertical or horizontal) of the block should be checked for.
        :return: df with the following fields;
                block (Graph), scene_fk, orientation (string - vertical or horizontal), facing_percentage.

        """

        block_parameters = {'minimum_block_ratio': 0.75,
                            'check_vertical_horizontal': self.check_vertical_horizontal,
                            'allowed_products_filters': None,
                            'adjacency_overlap_ratio': 0.4,
                            'minimum_facing_for_block': 1,
                            'include_stacking': self.include_stacking,
                            'horizontal_bucket_size': self.horizontal_bucket_size,
                            'outliers_threshold': self.outliers_threshold,
                            'calculate_all_scenes': True}

        block_parameters.update(additional)


        self.horizontal_bucket_size = block_parameters['horizontal_bucket_size']
        self.outliers_threshold = block_parameters['outliers_threshold']
        self.check_vertical_horizontal = block_parameters['check_vertical_horizontal']
        self.include_stacking = block_parameters['include_stacking']

        # Constructing the result_df that will be returned if calculate_all_scenes = True
        results_df = pd.DataFrame(columns=['block', 'scene_fk', 'orientation', 'facing_percentage'])

        # Constructing the product_name_df for the adj graph
        product_name_df_columns = set(
            ['product_fk', 'product_name', 'product_short_name', 'brand_name', 'product_type'] + population.keys())
        product_name_df = self.data_provider.all_products[list(product_name_df_columns)]
        product_name_df['index'] = range(len(product_name_df))

        # Creating unified filters from the block filters and the allowed filters
        unified_filters = population.copy()
        if block_parameters['allowed_products_filters']:
            for key in block_parameters['allowed_products_filters'].keys():
                unified_filters[key] = block_parameters['allowed_products_filters'][key]

        try:
            # Get the specific scene data
            relevant_scenes = self.input_parser.filter_df_by_conditions(location, self.scif).scene_id.unique()
            scene_matches = self.data_provider.matches[self.data_provider.matches.scene_fk.isin(relevant_scenes)]
            product_data_df = self.data_provider.all_products[list(set(['product_fk'] + unified_filters.keys()))]
            relevant_matches_for_block = self.filter_graph_data(scene_matches, product_data_df, population)

            scene_data, matches_df = self.get_raw_data(scene_matches, product_data_df, unified_filters)

            # Create the adjacency graph on the data we filtered above
            adj_g = AdjacencyGraph(matches_df, scene_data, product_name_df,
                                   product_attributes=['rect_x', 'rect_y'] + list(population.keys()),
                                   name=None, adjacency_overlap_ratio=block_parameters['adjacency_overlap_ratio'])

            components = list(nx.connected_component_subgraphs(adj_g.base_adjacency_graph.to_undirected()))
        except:
            components = []

        return adj_g.base_adjacency_graph, components
