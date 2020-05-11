from KPIUtils_v2.Calculations.CalculationsUtils.Constants import AdditionalAttr, CalcConst, ColumnNames
from Trax.Algo.Calculations.Core.AdjacencyGraph.Builders import AdjacencyGraphBuilder
from KPIUtils_v2.Utils.Consts.DataProvider import MatchesConsts
import KPIUtils_v2.Calculations.CalculationsUtils.DefaultValues as Default
from KPIUtils_v2.Calculations.CalculationsUtils import CalculationUtils
from KPIUtils_v2.Calculations.BaseCalculations import BaseCalculation
from Trax.Utils.Logging.Logger import Log
import networkx as nx
import pandas as pd
import itertools
import numpy as np


# This sequence module created in order to support sequence calculation using NetworkX graph.
# Explanations and more example about the parameters can be found in the confluence page:
# https://confluence.trax-cloud.com/display/PS/Sequence+KPI


class Sequence(BaseCalculation):

    def __init__(self, data_provider, custom_matches=pd.DataFrame(), **kwargs):
        super(Sequence, self).__init__(data_provider, **kwargs)
        self.data_provider = data_provider
        self.custom_matches = custom_matches

    def calculate_sequence(self, population, location=None, additional=None):
        """
        :param location: The locations parameters which the sequences are checked for.
        E.g: {'template_group': 'Primary Shelf'}.
        :param population: These are the parameters which the sequences are checked for.
        E.g: {'product_fk': [1, 2, 3]}, {'brand_name': ['brand1', 'brand2', 'brand3']}.
        :param additional: Additional attributes for the sequence calculation:
        1. direction (str): LEFT/RIGHT/UP/DOWN - the direction of the sequence.
        2. exclude_filter (dict): In order to exclude data from the population
        3. check_all_sequences (boolean): Should we calculated all of the sequences or should we stop when one passed
        4. strict_mode (boolean): Should it be the the exact sequence or any permutation is valid.
        5. include_stacking (boolean): Should we consider stacked products or not
        6. allowed_products_filters (dict): These are the parameters that are allowed to corrupt the sequence
        without failing it. E.g: {ProductsConsts.PRODUCT_TYPE: [ProductTypeConsts.POS, ProductTypeConsts.EMPTY]}
        7. minimum_tags_per_entity (int): The number of straight facings for every entity in the sequence.
        8. adjacency_overlap_ratio (float): Minimal threshold the overlap between the products must exceeds to be considered
        as adjacent.
        E.g: If the population includes 2 products [1, 2] and minimum_tags_per_filters = 3 with default values:
        111222-pass! 1122-fail! 1112-fail!
        :return: A DataFrame with the following fields: cluster (Graph), scene_fk and direction
        """
        try:
            self._sequence_calculation(population, location, additional)
        except Exception as err:
            Log.error("Sequence calculation failed due to the following error: {}".format(err))
        finally:
            return self._results_df

    def _sequence_calculation(self, population, location, additional):
        """
        The full documentation can be found under sequence_calculation function or in the confluence page.
        """
        scenes_to_check, sequence_params = self._parse_sequence_arguments(location, additional)
        if not self._params_validation(scenes_to_check, sequence_params, population):
            return
        graph_key, graph_value = self._prepare_data_for_graph_creation(population, sequence_params)
        for scene in scenes_to_check:
            adj_g = self._create_adjacency_graph(scene, population, sequence_params, graph_key)
            result = self._find_sequence_per_scene(
                scene, adj_g, sequence_params, graph_key, graph_value)
            if result and not sequence_params[AdditionalAttr.CHECK_ALL_SEQUENCES]:
                break

    def _find_sequence_per_scene(self, scene_fk, adj_g, seq_params, graph_key, graph_value):
        """
        This method checks for the relevant sequence in the graph.
        If there's a sequence that stands in the sequence params it will be added to the results DataFrame.
        """
        result = 0
        if not adj_g:
            return result
        all_paths = self._get_all_paths_in_graph(adj_g, graph_value)
        node_entity_dict = nx.get_node_attributes(adj_g, graph_key)
        for path in all_paths:
            filtered_path = self._filter_allowed_product_filters_from_path(path, adj_g, seq_params)
            result = self._check_if_path_is_a_sequence(
                filtered_path, seq_params, graph_value, node_entity_dict, adj_g)
            if result:
                cluster = adj_g.subgraph(path)
                self._save_sequence_result(cluster, scene_fk, seq_params)
                if not seq_params[AdditionalAttr.CHECK_ALL_SEQUENCES]:
                    break
        return result

    @staticmethod
    def _filter_allowed_product_filters_from_path(path, adj_g, sequence_params):
        """
        This method filters the allowed product filters from the path in order to determine if there is a sequence
        of our relevant entities.
        """
        nodes_to_remove, allowed_filters = [], sequence_params[AdditionalAttr.ALLOWED_PRODUCTS_FILTERS]
        if allowed_filters is None:
            return path
        for key, values in allowed_filters.iteritems():
            values = [values] if not isinstance(values, list) else values
            filtered_nodes = list(node for node in path if adj_g.nodes[node][key].value in values)
            nodes_to_remove.extend(filtered_nodes)
        return [node for node in path if node not in nodes_to_remove]

    def _save_sequence_result(self, cluster, scene_fk, sequence_params):
        """This method responsible on maintaining the DataFrame
        that will be returned at the end of the calculation"""
        result = pd.DataFrame(columns=self._results_df.columns,
                              data=[[cluster, scene_fk, sequence_params[AdditionalAttr.DIRECTION]]])
        self._results_df = self._results_df.append(result)

    def _check_if_path_is_a_sequence(self, path, sequence_params, graph_value, node_entity_dict, adj_g):
        """
        :param path: A list of graph nodes.
        :param node_entity_dict: A dictionary that match every node_fk to the relevant sequence entity.
        :return: True in case this path is matching the relevant sequence, False otherwise.
        """
        path_by_seq_key = map(lambda x: list(node_entity_dict[x])[0], path)
        if sequence_params[AdditionalAttr.STRICT_MODE]:
            is_valid_sequence = path_by_seq_key == graph_value
        else:
            is_valid_sequence = len(path_by_seq_key) == len(
                graph_value) and set(path_by_seq_key) == set(graph_value)
        if is_valid_sequence:
            is_valid_sequence = self._check_if_path_is_a_sequence_vertical(path, path_by_seq_key, sequence_params) if \
                self._check_if_vertical(sequence_params) else \
                self._check_if_path_is_a_sequence_horizontal(adj_g, path, sequence_params)
        return is_valid_sequence

    def _check_if_path_is_a_sequence_vertical(self, path, path_by_seq, sequence_params):
        """
        If param 'min tags of entity' is bigger than 1 ,
        checks the tags of the same entity are in subsequent indexes
        """
        rept_prod = sequence_params[AdditionalAttr.MIN_TAGS_OF_ENTITY]
        is_valid_sequence = True
        if rept_prod > 1:
            for index in range(0, len(path), rept_prod):
                curr_node = path_by_seq[index:rept_prod]
                if len(set(curr_node)) > 1:
                    is_valid_sequence = False
                    break
        if is_valid_sequence:
            is_valid_sequence = self._validate_same_node(path)

        return is_valid_sequence

    def _check_if_path_is_a_sequence_horizontal(self, adj_g, path, sequence_params):
        """
        If param 'min tags of entity' is bigger than 1 ,
        validating if the path answer requirements.
        """
        is_valid_sequence = True
        if sequence_params[AdditionalAttr.MIN_TAGS_OF_ENTITY] > 1:
            min_tags_per_entity = sequence_params[AdditionalAttr.MIN_TAGS_OF_ENTITY]
            is_valid_sequence = self._validate_minimum_tags_of_entity(
                adj_g, path, min_tags_per_entity)

        return is_valid_sequence

    @staticmethod
    def _check_if_vertical(sequence_params):
        return sequence_params[AdditionalAttr.DIRECTION] in ['UP', 'DOWN']

    def _validate_same_node(self, path):
        """
           Making sure each sequence in simple path , 2 valid sequences can't go through the same node.
           If node has more than one exit point or one entry point ,the node repeat more than once
           it indicates the path  isn't 'simple'.
           node repetition dict contain key : node name , and value : direction
        """
        path_dict = {}
        for i in range(len(path)):
            curr_node = path[i]
            direction_node_in_past = self.nodes_repetition.get(curr_node)
            node_direction = self._translate_direction(i, path)
            if direction_node_in_past:
                if node_direction.issubset(direction_node_in_past) or direction_node_in_past.issubset(node_direction):
                    return False
            else:
                path_dict[curr_node] = node_direction
        self.nodes_repetition.update(path_dict)
        return True

    @staticmethod
    def _translate_direction(index, path):
        """
            This function determine for each node if it has in this path -> entry point / exit point / both

        """
        if index in [0]:
            return {'exit'}
        if index in [len(path) - 1]:
            return {'entry'}
        if index in range(1, len(path) - 1):
            return {'entry', 'exit'}

    @staticmethod
    def _validate_minimum_tags_of_entity(adj_g, path, min_tags_per_entity):
        """
        In case min_tags_per_entity > 1, this method calculates the validity of the current path.
        Every node must contain at least @param min_tags_per_entity number of facings in order to pass.
        """
        facings_per_node = {n: d['facings'] for n, d in adj_g.node(data=True) if n in path}
        return all([facings >= min_tags_per_entity for node, facings in facings_per_node.iteritems()])

    def _get_all_paths_in_graph(self, adj_g, sequence_values):
        """
        This method uses all_pairs_shortest_path algorithm (a.k.a as Floyd Warshall algorithm) in order to get all of
        the paths in the adjacency graph. Then it filters and flatten the output to be a list of lists.
        Every component in the list is a path and every integer in the path represents a node fk.
        """
        all_paths = list(nx.all_pairs_shortest_path(adj_g, len(sequence_values)))
        all_paths = self._filter_and_flatten_paths_list(all_paths, sequence_values)
        return all_paths

    @staticmethod
    def _filter_and_flatten_paths_list(all_paths, sequence_values):
        """
        This method gets the output of all_pairs_shortest_path algorithm, flatten it (to be a list of lists)
        and filters irrelevant paths.
        """
        relevant_paths = [path[1].values() for path in all_paths]
        all_paths = [path for path_list in relevant_paths for path in path_list if len(
            path) >= len(sequence_values)]
        return all_paths

    def _create_adjacency_graph(self, scene_fk, population, sequence_params, graph_key):
        """
        This method creates the Adjacency graph for the relevant scene.
        :param scene_fk: The relevant scene_fk.
        :param sequence_params: A dictionary with the relevant params of the sequence.
        :param graph_key: The relevant sequence entity.
        :return: Filtered adjacency graph.
        """
        filtered_matches = self._filter_graph_data(scene_fk, population, sequence_params)
        if filtered_matches.empty:
            return 0
        masking_df = AdjacencyGraphBuilder._load_maskings(self.data_provider.project_name, scene_fk)
        allowed_product_filters = sequence_params[AdditionalAttr.ALLOWED_PRODUCTS_FILTERS]
        graph_attr = self._get_additional_attribute_for_graph(graph_key, allowed_product_filters)
        kwargs = {'minimal_overlap_ratio': sequence_params[AdditionalAttr.ADJACENCY_OVERLAP_RATIO],
                  }  # AdditionalAttr.USE_MASKING_ONLY: True
        adj_g = AdjacencyGraphBuilder.initiate_graph_by_dataframe(
            filtered_matches, masking_df, graph_attr, **kwargs)
        adj_g = self._filter_adjacency_graph(adj_g, graph_key, sequence_params)
        return adj_g

    def _filter_adjacency_graph(self, adj_g, graph_key, sequence_params):
        """
        The method filters the relevant nodes for the sequence out of the graph.
        :param graph_key: The relevant sequence entity.
        :return: Filtered adjacency graph.
        """
        use_degrees = False
        direction = sequence_params[AdditionalAttr.DIRECTION]
        include_stacking = sequence_params[AdditionalAttr.INCLUDE_STACKING]
        if graph_key != CalcConst.PRODUCT_FK or sequence_params[AdditionalAttr.REPEATING_OCCURRENCES]:
            if sequence_params[AdditionalAttr.DIRECTION] in ['RIGHT', 'LEFT']:
                adj_g = AdjacencyGraphBuilder.condense_graph_by_level(ColumnNames.GRAPH_KEY, adj_g)
            use_degrees = True
        adj_g = self._filter_graph_by_edge_direction(
            adj_g, direction, include_stacking, use_degrees)
        return adj_g

    def _filter_graph_by_edge_direction(self, adj_g, direction_to_filter, include_stacking, use_degrees):
        """ This method creates a sub graph using only edges with the required direction"""
        if not use_degrees:
            valid_edges = [(u, v) for u, v, c in adj_g.edges.data(
                'direction') if c == direction_to_filter]
        else:
            valid_edges = self._filter_edges_by_degree(adj_g, direction_to_filter)
        if include_stacking and direction_to_filter in ['RIGHT', 'LEFT']:
            valid_edges = self._filter_edges_by_stacking_layer(adj_g, valid_edges)
        filtered_adj_g = adj_g.edge_subgraph(valid_edges)
        return filtered_adj_g

    @staticmethod
    def _filter_edges_by_stacking_layer(adj_g, edges_list):
        """
        This method gets a list of edges and keeps only the edges that have nodes in the same stacking layer.
        """
        valid_edges = [(u, v) for u, v in edges_list if
                       adj_g.nodes[u][MatchesConsts.STACKING_LAYER].value == adj_g.nodes[v][
                           MatchesConsts.STACKING_LAYER].value]
        return valid_edges

    @staticmethod
    def _filter_edges_by_degree(adj_g, requested_direction):
        """
        This method filters the edges by the relevant degree.
        :param requested_direction: 'RIGHT', 'LEFT', 'UP, 'BOTTOM'
        """
        degree_direction_dict = {'RIGHT': range(-45, 45), 'LEFT': range(-180, -134) + range(135, 181),
                                 'DOWN': range(-134, -44), 'UP': range(45, 136)}
        relevant_range = degree_direction_dict[requested_direction]
        valid_edges = [(u, v) for u, v, c in adj_g.edges.data('degree') if int(c) in relevant_range]

        return valid_edges

    @staticmethod
    def _get_additional_attribute_for_graph(population_key, allowed_product_filters):
        """
        This method returns the relevant graph attributes that will be exist in every node.
        """
        # todo handle better
        graph_attr = [population_key, ColumnNames.GRAPH_KEY, MatchesConsts.STACKING_LAYER]
        if allowed_product_filters is not None:
            graph_attr.extend(allowed_product_filters.keys())
        return graph_attr

    def _filter_graph_data(self, scene, population, sequence_params, operator=Default.filter_operator):
        """
        This method filters match product in scene DataFrame for the graph's creation.
        """
        filtered_matches = self._matches.loc[self._matches.scene_fk == scene]
        if not sequence_params[AdditionalAttr.INCLUDE_STACKING]:
            filtered_matches = CalculationUtils.adjust_stacking(filtered_matches)
        filters, excludes = [population], sequence_params[AdditionalAttr.EXCLUDE_FILTER]
        if sequence_params[AdditionalAttr.ALLOWED_PRODUCTS_FILTERS] is not None:
            filters.append(sequence_params[AdditionalAttr.ALLOWED_PRODUCTS_FILTERS])
        conditions = {self.input_parser.POPULATION: {self.input_parser.INCLUDE: filters,
                                                     self.input_parser.EXCLUDE: excludes,
                                                     'include_operator': operator}}
        filtered_df = self.input_parser.filter_df(conditions, filtered_matches)
        return filtered_df

    def _prepare_data_for_graph_creation(self, population, sequence_params):
        """
        This method handles the encoding and adjustments of the population JSON and match product in scene DataFrame.
        Plus, it returns the relevant block_key for graph creation: In case there is only 1 entity in the population
        it will be the graph_key, else, graph_key will be equal to "graph_key".
        """
        self._expand_matches_for_graph_creation(
            population, sequence_params[AdditionalAttr.ALLOWED_PRODUCTS_FILTERS])
        self._encode_matches_and_population(population)
        graph_key, graph_value = self._get_relevant_graph_key_and_value(population, sequence_params)
        return graph_key, graph_value

    def _get_relevant_graph_key_and_value(self, population, sequence_params):
        """
        This method extracts the relevant graph key and values. The graph key and values are basically the values
        that we will filter the graph by.This method adds graph_key in case we would need to condense
        the graph. In this case, the sequence calculation will be per shelf.
        """
        # Todo: handle better multi population
        if len(population.keys()) > 1:
            self._matches[ColumnNames.GRAPH_KEY] = self._matches[population.keys()].apply(
                self._concat_values, axis=1)
            graph_key = ColumnNames.GRAPH_KEY
            graph_value = ['_'.join(str(x) for x in elem)
                           for elem in list(itertools.product(*population.values()))]
        else:
            graph_key = population.keys()[0]
            graph_value = population[graph_key]
            graph_value = list(np.repeat(graph_value, sequence_params[AdditionalAttr.MIN_TAGS_OF_ENTITY])) \
                if sequence_params[AdditionalAttr.DIRECTION] in ['DOWN', 'UP'] else graph_value
        self._matches[ColumnNames.GRAPH_KEY] = self._matches.apply(
            lambda row: self._create_relevant_graph_key_column(row, graph_key), axis=1)
        return graph_key, graph_value

    @staticmethod
    def _create_relevant_graph_key_column(row, graph_key):
        """
        This method is generates the value for "graph key" column. In case include_stacking=False, it will
        be a combination of the population key and the shelf_number, in case include_stacking=True,
        the stacking layer will be a part of the combination as well.
        """
        relevant_values = [graph_key, MatchesConsts.SHELF_NUMBER, MatchesConsts.STACKING_LAYER]
        return '_'.join([str(row[value]) for value in relevant_values])

    @staticmethod
    def _concat_values(attr_list):
        """
        This method concatenates the population values in case of multiple entities.
        This is in order to support the graph creation.
        """
        return '_'.join([str(elem) for elem in attr_list])

    def _encode_matches_and_population(self, population):
        """
        This method encodes all of the population keys and match product in scene DataFrame in order to
        avoid the ASCII issues.
        """
        for key in population.keys():
            population_values = list(population[key]) if not isinstance(
                population[key], list) else population[key]
            population[key] = [self._encode_value_if_possible(value) for value in population_values]
            if key in self._matches.columns:
                self._matches[key] = self._matches[key].apply(
                    lambda val: self._encode_value_if_possible(val))

    @staticmethod
    def _encode_value_if_possible(value):
        return value.encode("utf-8") if isinstance(value, (str, unicode)) else value

    def _expand_matches_for_graph_creation(self, population, allowed_filters):
        """
        In order to support the Adjacency Graph we need to add relevant products' attributes into
        match_product_in_scene DataFrame and of course the products' attribute from the population filter.
        """
        allowed_filters_col = allowed_filters.keys() if allowed_filters is not None else []
        product_population_keys = set(population.keys()).intersection(
            self.all_products.columns.to_list())
        product_cols_to_add = set(list(product_population_keys) +
                                  allowed_filters_col + [CalcConst.PRODUCT_FK])
        product_attributes_df = self.data_provider.all_products[list(product_cols_to_add)]
        self._matches = self._matches.merge(
            product_attributes_df, how='inner', on=CalcConst.PRODUCT_FK)
        self._matches.drop(MatchesConsts.PK, axis=1, inplace=True)
        self._matches.rename(columns={MatchesConsts.SCENE_MATCH_FK: MatchesConsts.PK}, inplace=True)

    def _parse_sequence_arguments(self, location, additional):
        """
        This method handles the KPI parameters that were given and returns scene_fks list and relevant sequence params.
        """
        self._initialize_params()
        scenes_to_check = self._filter_scenes_by_location(location)
        sequence_params = self._get_default_sequence_parameters()
        if additional is not None:
            sequence_params.update(additional)
        return scenes_to_check, sequence_params

    def _initialize_params(self):
        """ This method resetting matches and results DataFrames in order to support multiple sequence calculations"""
        self._matches = self.data_provider.matches if self.custom_matches.empty else self.custom_matches
        self._results_df = self._get_results_df()
        self.nodes_repetition = dict()

    def _filter_scenes_by_location(self, location):
        """
        This method returns the relevant scene by location filters.
        :param location: A dictionary with location attributes like scene_id, template_fk, location_type and
        the requested values. E.g: {'template_name': ['great_template'], scene_id: [1, 2, 3]}
        """
        relevant_scenes = self.scif.scene_id.unique().tolist()
        if location is not None:
            conditions = {self.input_parser.LOCATION: location}
            try:
                relevant_scenes = self.input_parser.filter_df(
                    conditions, self.scif).scene_id.unique().tolist()
            except (AttributeError, KeyError):
                Log.debug("location parameter is not in the required structure.")
                relevant_scenes = []
        return relevant_scenes

    def _params_validation(self, scenes_to_check, sequence_params, population):
        """
        :param scenes_to_check: List of scenes that passed the location filters.
        :param sequence_params: The sequence additional attributes.
        :return: 1 in case of valid params, 0 otherwise.
        """
        if not scenes_to_check:
            Log.debug("There aren't any valid scenes for sequence calculation")
            return 0
        if self._matches.empty:
            Log.debug("Cannot calculate Sequence KPI since the match product in scene is empty!")
            return 0
        if not self._validate_population_structure(population):
            Log.debug("Wrong population structure! Exiting..")
            return 0
        if set(sequence_params.keys()).difference(self._get_default_sequence_parameters().keys()):
            Log.debug("Wrong attributes in Additional Attributes! Exiting..")
            return 0
        if not sequence_params[AdditionalAttr.REPEATING_OCCURRENCES] and \
                sequence_params[AdditionalAttr.MIN_TAGS_OF_ENTITY] > 1:
            Log.debug(
                "In case repeating occurrences is False, minimum tags of facings param must be equal to 1")
            return 0
        return 1

    def _validate_population_structure(self, population):
        """
        This method responsible on the population's json's validation.
        It checks for its keys and returns 1 if their are valid and 0 otherwise.
        """
        if population is None:
            return 0
        valid_population_keys = set(self.all_products.columns.tolist() +
                                    self._matches.columns.tolist())
        return set(population.keys()).issubset(valid_population_keys)

    @staticmethod
    def _get_default_sequence_parameters():
        sequence_parameters = {AdditionalAttr.DIRECTION: Default.sequence_direction,
                               AdditionalAttr.EXCLUDE_FILTER: None,
                               AdditionalAttr.CHECK_ALL_SEQUENCES: Default.check_all_sequences,
                               AdditionalAttr.STRICT_MODE: Default.strict_mode,
                               AdditionalAttr.REPEATING_OCCURRENCES: Default.repeating_occurrences,
                               AdditionalAttr.INCLUDE_STACKING: True,
                               AdditionalAttr.ALLOWED_PRODUCTS_FILTERS: None,
                               AdditionalAttr.MIN_TAGS_OF_ENTITY: Default.min_tags_of_entity,
                               AdditionalAttr.ADJACENCY_OVERLAP_RATIO: 0.1
                               }
        return sequence_parameters

    @staticmethod
    def _get_results_df():
        return pd.DataFrame(columns=[ColumnNames.CLUSTER, ColumnNames.SCENE_FK, AdditionalAttr.DIRECTION])


# For debugging:
# from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider
# from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
# from Trax.Utils.Conf.Configuration import Config

# if __name__ == '__main__':
#     LoggerInitializer.init('sequence calculations')
#     Config.init()
#     project_name = 'marsuae-sand'
#     test_data_provider = KEngineDataProvider(project_name)
#     session = '5fdd1d96-fa86-4567-98e9-80f6a4f6e056'
#     test_data_provider.load_session_data(session)
#     location = {MatchesConsts.SCENE_FK: 132238}
#     population = {'brand_fk': [16, 12]}
#     sequence_params = {AdditionalAttr.DIRECTION: 'DOWN',
#                        AdditionalAttr.EXCLUDE_FILTER: None,
#                        AdditionalAttr.CHECK_ALL_SEQUENCES: True,
#                        AdditionalAttr.STRICT_MODE: False,
#                        AdditionalAttr.REPEATING_OCCURRENCES: True,
#                        AdditionalAttr.INCLUDE_STACKING: True,
#                        AdditionalAttr.ALLOWED_PRODUCTS_FILTERS: {'brand_fk': 14},
#                        AdditionalAttr.MIN_TAGS_OF_ENTITY: 1}
#     sequence_res = Sequence(test_data_provider).calculate_sequence(population, location,
#                                                                    sequence_params)
#
#     # test allow products
#     print "Done"
