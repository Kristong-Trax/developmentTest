import networkx as nx
import numpy as np
import pandas as pd
import itertools

from KPIUtils_v2.Calculations.CalculationsUtils.Constants import AdditionalAttr, CalcConst, ColumnNames
from KPIUtils_v2.Calculations.PositionGraphsCalculations import PositionGraphs
from KPIUtils_v2.Calculations.BaseCalculations import BaseCalculation
import KPIUtils_v2.Calculations.CalculationsUtils.CalculationUtils as CalculationUtils
import KPIUtils_v2.Calculations.CalculationsUtils.DefaultValues as Default
# from Trax.Algo.Calculations.Core.GraphicalModel2.AdjacencyGraphs import AdjacencyGraphBuilder
from Projects.RINIELSENUS.Utils.AdjacencyGraphs import AdjacencyGraphBuilder
from Trax.Algo.Geometry.Masking.MaskingResultsIO import retrieve_maskings
# from Projects.RINIELSENUS.Utils.MaskingResultsIO_v2 import retrieve_maskings
from Trax.Algo.Geometry.Masking.Utils import transform_maskings
from Trax.Utils.Logging.Logger import Log


class Block(BaseCalculation):
    EXCLUDE_FILTER = 0
    INCLUDE_FILTER = 1
    CONTAIN_FILTER = 2
    EXCLUDE_EMPTY = False
    INCLUDE_EMPTY = True

    STRICT_MODE = ALL = 1000

    EMPTY = 'Empty'
    DEFAULT = 'Default'
    TOP = 'Top'
    BOTTOM = 'Bottom'

    BLOCK_KEY = 'block_key'

    UNCONNECTED = 'unconnected'
    CONNECTED = 'connected'
    ENCAPSULATED = 'encapsulated'

    def __init__(self, data_provider, output=None, ps_data_provider=None, common=None, rds_conn=None,
                 front_facing=False, custom_scif=None, custom_matches=None, **kwargs):
        super(Block, self).__init__(data_provider, output,
                                    ps_data_provider, common, rds_conn, **kwargs)
        self._position_graphs = PositionGraphs(self.data_provider)
        self.front_facing = front_facing
        self.outliers_threshold = Default.outliers_threshold
        self.check_vertical_horizontal = Default.check_vertical_horizontal
        self.include_stacking = Default.include_stacking
        self.ignore_empty = Default.ignore_empty
        self.allowed_edge_type = Default.allowed_edge_type
        self.scif = data_provider.scene_item_facts if custom_scif is None else custom_scif
        self.matches = data_provider.matches if custom_matches is None else custom_matches
        self.adj_graphs_by_scene = {}
        self.masking_data = transform_maskings(retrieve_maskings(self.data_provider.project_name,
                                                                 self.data_provider.scenes_info['scene_fk'].to_list()))
        self.masking_data = self.masking_data.merge(
            self.matches[['probe_match_fk', 'scene_fk']], on=['probe_match_fk'])
        self.matches_df = self.matches.merge(
            self.data_provider.all_products_including_deleted, on='product_fk')
        self.matches_df = self.matches_df[~(self.matches_df['product_type'].isin(['POS'])) &
                                           (self.matches_df['stacking_layer'] > 0)]
        self.matches_df[Block.BLOCK_KEY] = None

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
        10. allowed_edge_type: This determines what allowed products will actually be allowed. That is, allowed products
                in scene can be filtered based on their connections (or lack of) to relevant group products. Format is a
                Mix and Match list of -> [unconnected, connected, encapsulated].
                        unconnected-> allowed nodes with no connections to relevant products.
                        connected -> allowed nodes that connect to both relevant group and irrelevant group nodes
                        encapsulated -> allowed nodes that only connect to relevant group products
        :return: df with the following fields;
                block (Graph), scene_fk, orientation (string - vertical or horizontal), facing_percentage.

        """

        block_parameters = {AdditionalAttr.MINIMUM_BLOCK_RATIO: Default.minimum_block_ratio,
                            AdditionalAttr.CHECK_VERTICAL_HORIZONTAL: Default.check_vertical_horizontal,
                            AdditionalAttr.ALLOWED_PRODUCTS_FILTERS: None,
                            AdditionalAttr.ADJACENCY_OVERLAP_RATIO: Default.adjacency_overlap_ratio,
                            AdditionalAttr.MINIMUM_FACING_FOR_BLOCK: Default.minimum_facing_for_block,
                            AdditionalAttr.INCLUDE_STACKING: Default.include_stacking,
                            AdditionalAttr.IGNORE_EMPTY: Default.ignore_empty,
                            AdditionalAttr.OUTLIERS_THRESHOLD: Default.outliers_threshold,
                            AdditionalAttr.CALCULATE_ALL_SCENES: Default.calculate_all_scenes,
                            AdditionalAttr.ALLOWED_EDGE_TYPE: Default.allowed_edge_type,
                            AdditionalAttr.FILTER_OPERATOR: Default.filter_operator}

        if additional is not None:
            block_parameters.update(additional)

        self.outliers_threshold = block_parameters[AdditionalAttr.OUTLIERS_THRESHOLD]
        self.check_vertical_horizontal = block_parameters[AdditionalAttr.CHECK_VERTICAL_HORIZONTAL]
        self.include_stacking = block_parameters[AdditionalAttr.INCLUDE_STACKING]
        self.ignore_empty = block_parameters[AdditionalAttr.IGNORE_EMPTY]
        self.allowed_edge_type = block_parameters[AdditionalAttr.ALLOWED_EDGE_TYPE]
        operator = block_parameters[AdditionalAttr.FILTER_OPERATOR]

        # Constructing the result_df that will be returned if calculate_all_scenes = True
        results_df = pd.DataFrame(columns=[ColumnNames.CLUSTER, ColumnNames.SCENE_FK, ColumnNames.ORIENTATION,
                                           ColumnNames.FACING_PERCENTAGE, ColumnNames.IS_BLOCK])

        # Separate the filters on products from the filters on scene and get the relevant scenes
        if location is not None:
            conditions = {'location': location}
            relevant_scenes = self.input_parser.filter_df(conditions, self.scif).scene_id.unique()
        else:
            relevant_scenes = self.scif.scene_id.unique()
        if not relevant_scenes.any():
            Log.info('No scenes with the requested location filter.')
            return results_df

        # Identify relevant block values
        block_value = ['_'.join(str(x) for x in elem)
                       for elem in list(itertools.product(*population.values()))]

        # Creating unified filters from the block filters and the allowed filters
        allowed_product_filter = []
        if block_parameters[AdditionalAttr.ALLOWED_PRODUCTS_FILTERS]:
            allowed_product_filter = block_parameters[AdditionalAttr.ALLOWED_PRODUCTS_FILTERS].keys(
            )
        # For each relevant scene check if a block is exist
        rel_pop = {'stacking_layer': [1]}
        rel_pop.update(population)
        for scene in relevant_scenes:
            try:
                # filter masking and matches data by the scene
                scene_mask = self.masking_data[self.masking_data['scene_fk'] == scene].drop(
                    'scene_fk', axis=1)
                scene_matches = self.matches_df[self.matches_df['scene_fk'] == scene]
                relevant_matches_for_block = self.filter_graph_data(scene_matches, rel_pop, operator=operator,
                                                                    is_blocks_graph=False)
                scene_matches = scene_matches.drop('pk', axis=1).rename(
                    columns={'scene_match_fk': 'pk'})
                if relevant_matches_for_block.empty:
                    continue

                if not self.include_stacking:
                    scene_matches = self._get_no_stack_data(scene_matches)

                # check if the adj_g already exists for this scene, if not create it and save it
                if scene not in self.adj_graphs_by_scene:
                    # Create the adjacency graph on the data we filtered above
                    graph = AdjacencyGraphBuilder.initiate_graph_by_dataframe(scene_matches, scene_mask,
                                                                              additional_attributes=['rect_x', 'rect_y'] + allowed_product_filter +
                                                                              list(scene_matches.columns))
                    self.adj_graphs_by_scene[scene] = graph
                adj_g = self.adj_graphs_by_scene[scene].copy()

                # Update the block_key node attribute based on the population fields
                for i, n in adj_g.nodes(data=True):
                    n[Block.BLOCK_KEY].stored_values = set(
                        ['_'.join([str(*n[x]) for x in population.keys()])])

                if allowed_product_filter:
                    adj_g = self._set_allowed_nodes(adj_g, block_parameters[AdditionalAttr.ALLOWED_PRODUCTS_FILTERS],
                                                    block_value)

                # TODO: filter nodes to subgraph
                # Filter only relevant nodes for block out of the graph
                filtered_nodes = list(n for n, d in adj_g.nodes(data=True) if list(d[Block.BLOCK_KEY])[0] in
                                      block_value + self.allowed_edge_type)

                # Create a sub graph based on the adj_g containing only the filtered nodes
                adj_g = adj_g.subgraph(filtered_nodes)

                # Creating the condensed graph based on the adjacency graph created above
                if len(population.keys()) == 1 and population.keys()[0] != CalcConst.PRODUCT_FK:
                    # adj_g = AdjacencyGraphBuilder.build_adjacency_graph_by_level(Block.BLOCK_KEY, adj_g)
                    adj_g = AdjacencyGraphBuilder.condense_graph_by_level(Block.BLOCK_KEY, adj_g)

                # Transferring the graph to be undirected
                condensed_graph_sku = adj_g.to_undirected()

                # Calculating all the components in the graph
                components = list(nx.connected_component_subgraphs(condensed_graph_sku))

                # Constructing the block_res df that will contain each block's data
                blocks_res = pd.DataFrame(columns=['component', 'sum_of_facings', 'num_of_shelves'])

                # For each component in the graph calculate the blocks data
                for component in components:
                    component_data = self.calculate_block_data(
                        component, relevant_matches_for_block)

                    blocks_res = blocks_res.append(component_data)

                total_facings = blocks_res['sum_of_facings'].sum()

                # For each block check if its valid or not
                for row in blocks_res.itertuples():

                    if not row.sum_of_facings:
                        continue

                    orientation = None

                    # Calculate facing percentage
                    facing_percentage = np.divide(float(row.sum_of_facings), float(total_facings))
                    if (facing_percentage >= block_parameters[AdditionalAttr.MINIMUM_BLOCK_RATIO]) and \
                            (row.sum_of_facings >= block_parameters[AdditionalAttr.MINIMUM_FACING_FOR_BLOCK]):

                        # If needed, calculate the block's orientation
                        if block_parameters[AdditionalAttr.CHECK_VERTICAL_HORIZONTAL]:
                            orientation = self.handle_horizontal_and_vertical(
                                row.component, row.num_of_shelves)

                        results_df = results_df.append(pd.DataFrame(columns=[ColumnNames.CLUSTER, ColumnNames.SCENE_FK,
                                                                             ColumnNames.ORIENTATION,
                                                                             ColumnNames.FACING_PERCENTAGE,
                                                                             ColumnNames.IS_BLOCK],
                                                                    data=[[row.component, scene, orientation,
                                                                           facing_percentage, True]]))
                        if not block_parameters[AdditionalAttr.CALCULATE_ALL_SCENES]:
                            return results_df
                    else:
                        results_df = results_df.append(pd.DataFrame(columns=[ColumnNames.CLUSTER, ColumnNames.SCENE_FK,
                                                                             ColumnNames.ORIENTATION,
                                                                             ColumnNames.FACING_PERCENTAGE,
                                                                             ColumnNames.IS_BLOCK],
                                                                    data=[[row.component, scene, orientation,
                                                                           facing_percentage, False]]))

            except AttributeError as err:
                Log.error('{}'.format(err))
                continue
            except Exception as err:
                Log.error('{}'.format(err))
                return results_df
        return results_df

    def handle_horizontal_and_vertical(self, graph, num_of_shelves):
        """

        :param graph: graph of the block that is being calculated
        :param num_of_shelves: num of shelves the block is spread on.
        :return: The threshold for removing the outliers for the horizontal/vertical calculation.
        """
        # TODO - Optimize the function we should have some predefined calcs in pandas

        num_of_shelves = int(num_of_shelves)
        x_values = sorted(self.get_matches_by_graph_att(graph, 'rect_x'))
        y_values = sorted(self.get_matches_by_graph_att(graph, 'rect_y'))
        max_x = max(x_values)
        min_x = min(x_values)
        max_y = max(y_values)
        min_y = min(y_values)
        if num_of_shelves:
            # numerator max handles edge case where max_y == min_y
            y_division_factor = max((max_y - min_y), 1) / float(num_of_shelves)
        else:
            return None
        x_width_feet = ((max_x - min_x) * CalcConst.MM_TO_FEET_CONVERSION)
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
        current_min_x = min_x * CalcConst.MM_TO_FEET_CONVERSION
        for z in range(int(x_width_feet) + 1):
            n_bin = [x_value for x_value in x_values if current_min_x <= x_value * CalcConst.MM_TO_FEET_CONVERSION <
                     current_min_x + 1]
            x_bins.append(n_bin)
            current_min_x += 1
            if current_min_x >= max_x * CalcConst.MM_TO_FEET_CONVERSION:
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
        att_list = []
        for i, n in graph.nodes(att):
            att_list += n
        return att_list

    def filter_graph_data(self, df1, population_filters, allowed_filters=None, is_blocks_graph=True, operator='and'):
        """
        :param is_blocks_graph: is the filter used for the blocks graph
        :param allowed_filters: the population keys and values the merged data will be filtered on
        :param population_filters: the allowed keys and values the merged data will be filtered on
        :param df1: df with all the scene data
        :param df2: additional df with products data
        :return: filtered df.
        """
        # Assuming the population filters contain keys and values with 'and' operator between them.
        # Assuming the allowed filters contain keys and values with 'or' operator between them.
        exclude = {'product_type': ['Irrelevant']}
        filters = [population_filters]
        # if allowed_filters is not None:
        #     filters += [{k: v} for (k, v) in allowed_filters.iteritems()]
        if self.ignore_empty and is_blocks_graph:
            filters.append({'product_type': ['Empty']})
        if not is_blocks_graph:
            exclude['product_type'].append('Empty')
        filtered_df = self.input_parser.filter_df(
            conditions={'population': {'include': filters,
                                       'exclude': exclude, 'include_operator': operator}},
            data_frame_to_filter=df1)
        return filtered_df

    def handle_block_outliers(self, bins, average_count):
        """

        :param bins: list of products divided to bins by location
        :param average_count: average num of products in bin.
        :return: the received list of bins without the products that exceed the outliers_threshold
        """
        outliers_index = []
        for i in range(len(bins)):
            if len(bins[i]) < average_count*self.outliers_threshold:
                outliers_index.append(i)
            else:
                break
        for j in reversed(range((len(bins)))):
            if len(bins[j]) < average_count*self.outliers_threshold:
                outliers_index.append(j)
            else:
                break
        # TODO: Replace the loop on the bins array to be more efficient.
        return [i for j, i in enumerate(bins) if j not in outliers_index]

    def calculate_block_data(self, component, relevant_matches_for_block):
        max_shelf = 1
        min_shelf = 1

        # Check if the node is a valid product or allowed attribute
        relevant_matches = list(itertools.chain(
            *(d[CalcConst.MATCH_FK] for n, d in component.nodes(data=True))))
        block_facings = len(relevant_matches_for_block[(relevant_matches_for_block['scene_match_fk'].isin(
            relevant_matches)) & relevant_matches_for_block['product_type'].isin(['SKU', 'Other'])])

        # Calculate the number of shelves in block
        if self.check_vertical_horizontal:
            max_shelf = relevant_matches_for_block[relevant_matches_for_block['scene_match_fk'].isin(
                relevant_matches)]['shelf_number'].max()
            min_shelf = relevant_matches_for_block[relevant_matches_for_block['scene_match_fk'].isin(
                relevant_matches)]['shelf_number'].min()

        blocks_data = pd.DataFrame(
            columns=['component', 'sum_of_facings', 'num_of_shelves'],
            data=[[component, block_facings, (max_shelf - min_shelf + 1)]])

        return blocks_data

    def _get_no_stack_data(self, matches_df):
        return CalculationUtils.adjust_stacking(matches_df)

    def _apply_function(self, att_list):
        return '_'.join([str(elem) for elem in att_list])

    def _set_allowed_nodes(self, graph, allowed_filters, block_value):
        allowed_nodes = []
        for filter in allowed_filters.keys():
            allowed_nodes += list(itertools.chain(*(d[CalcConst.MATCH_FK] for n, d in graph.nodes(data=True)
                                                    if list(d[filter])[0] in allowed_filters[filter])))
        node_block_att = nx.get_node_attributes(graph, Block.BLOCK_KEY)
        for node in allowed_nodes:
            i = 0
            graph.nodes[node][Block.BLOCK_KEY].stored_values = set([Block.UNCONNECTED])
            for n in graph.neighbors(node):
                if list(node_block_att[n])[0] in block_value:
                    i += 1
                else:
                    break
            if i == len(list(graph.neighbors(node))):
                graph.nodes[node][Block.BLOCK_KEY].stored_values = set([Block.ENCAPSULATED])
            elif i >= 1:
                graph.nodes[node][Block.BLOCK_KEY].stored_values = set([Block.CONNECTED])

        return graph
