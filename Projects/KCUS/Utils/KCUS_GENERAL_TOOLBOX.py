
import xlrd
import json
import pandas as pd
from Trax.Algo.Calculations.Core.DataProvider import Data
from Trax.Algo.Calculations.Core.Shortcuts import BaseCalculationsGroup
from Trax.Utils.Logging.Logger import Log

from Projects.KCUS.Utils.PositionGraph import KCUSPositionGraphs

__author__ = 'Nimrod'
TITLE = {'Package Color': 'package_color',
         'Sub Brand': 'sub_brand', 'Package Size': 'package_size'}
SUB_BRAND = 'sub_brand'
SUB_CATEGORY = 'sub_category'
SUB = {'sub_brand':'sub_brand_x'}


class KCUSGENERALToolBox:

    EXCLUDE_FILTER = 0
    INCLUDE_FILTER = 1
    EXCLUDE_EMPTY = False
    INCLUDE_EMPTY = True

    STRICT_MODE = ALL = 1000

    EMPTY = 'Empty'
    DEFAULT = 'Default'
    TOP = 'Top'
    BOTTOM = 'Bottom'



    def __init__(self, data_provider, output, geometric_kpi_flag=False, **kwargs):
        self.k_engine = BaseCalculationsGroup(data_provider, output)
        self.data_provider = data_provider
        self.project_name = self.data_provider.project_name
        self.session_uid = self.data_provider.session_uid
        self.scif = self.data_provider[Data.SCENE_ITEM_FACTS]
        self.store_info = self.data_provider[Data.STORE_INFO]
        self.scif = self.scif.merge(self.data_provider[Data.STORE_INFO], how='left', left_on='store_id',
                                    right_on='store_fk')
        for sub in SUB:
            if sub in (SUB.keys()):
                self.scif = self.scif.rename(columns={sub: SUB.get(sub)})
        for title in TITLE:
            if title in (self.scif.columns.unique().tolist()):
                self.scif = self.scif.rename(columns={title: TITLE.get(title)})
        self.all_products = self.data_provider[Data.ALL_PRODUCTS]
        self.survey_response = self.data_provider[Data.SURVEY_RESPONSES]
        for data in kwargs.keys():
            setattr(self, data, kwargs[data])
        if geometric_kpi_flag:
            self.position_graph_data = KCUSPositionGraphs(self.data_provider)
            self.matches = self.position_graph_data.match_product_in_scene
            self.position_graph = self.position_graph_data.position_graphs
        else:
            self.position_graph_data = None
            self.matches = self.data_provider[Data.MATCHES]

    @property
    def position_graphs(self):
        if not hasattr(self, '_position_graphs'):
            self._position_graphs = KCUSPositionGraphs(self.data_provider)
        return self._position_graphs

    @property
    def match_product_in_scene(self):
        if not hasattr(self, '_match_product_in_scene'):
            self._match_product_in_scene = self.position_graph_data.match_product_in_scene
        return self._match_product_in_scene

    def check_survey_answer(self, survey_text, target_answer):
        """
        :param survey_text: The name of the survey in the DB.
        :param target_answer: The required answer/s for the KPI to pass.
        :return: True if the answer matches the target; otherwise - False.
        """
        if not isinstance(survey_text, (list, tuple)):
            entity = 'question_text'
            value = survey_text
        else:
            entity, value = survey_text
        value = [value] if not isinstance(value, list) else value
        survey_data = self.survey_response[self.survey_response[entity].isin(value)]
        if survey_data.empty:
            Log.warning('Survey with {} = {} doesn\'t exist'.format(entity, value))
            return None
        answer_field = 'selected_option_text' if not survey_data['selected_option_text'].empty else 'number_value'
        target_answers = [target_answer] if not isinstance(target_answer, (list, tuple)) else target_answer
        survey_answers = survey_data[answer_field].values.tolist()
        for answer in target_answers:
            if answer in survey_answers:
                return True
        return False

    def calculate_number_of_scenes(self, **filters):
        """
        :param filters: These are the parameters which the data frame is filtered by.
        :return: The number of scenes matching the filtered Scene Item Facts data frame.
        """
        number_of_scenes = self.calculate_assortment(assortment_entity='scene_id', **filters)
        return number_of_scenes

    def calculate_availability(self, **filters):
        """
        :param filters: These are the parameters which the data frame is filtered by.
        :return: Total number of SKUs facings appeared in the filtered Scene Item Facts data frame.
        """
        filtered_scif = self.scif[self.get_filter_condition(self.scif, **filters)]
        availability = filtered_scif['facings'].sum()
        return availability

    def calculate_assortment(self, assortment_entity='product_ean_code', minimum_assortment_for_entity=1, **filters):
        """
        :param assortment_entity: This is the entity on which the assortment is calculated.
        :param minimum_assortment_for_entity: This is the number of assortment per each unique entity in order for it
                                              to be counted in the final assortment result (default is 1).
        :param filters: These are the parameters which the data frame is filtered by.
        :return: Number of unique SKUs appeared in the filtered Scene Item Facts data frame.
        """
        if set(filters.keys()).difference(self.scif.keys()) or assortment_entity not in self.scif.keys():
            matches = self.match_product_in_scene.merge(self.scif, how='left',left_on='scene_fk', right_on='scene_id',
                                                        suffixes=['', '_1'])
            filtered_df = matches[self.get_filter_condition(matches, **filters)]
        else:
            filtered_df = self.scif[self.get_filter_condition(self.scif, **filters)]
        if minimum_assortment_for_entity == 1:
            assortment = len(filtered_df[assortment_entity].unique())
        else:
            assortment = 0
            for entity_id in filtered_df[assortment_entity].unique():
                assortment_for_entity = filtered_df[filtered_df[assortment_entity] == entity_id]
                if 'facings' in filtered_df.columns:
                    assortment_for_entity = assortment_for_entity['facings'].sum()
                else:
                    assortment_for_entity = len(assortment_for_entity)
                if assortment_for_entity >= minimum_assortment_for_entity:
                    assortment += 1
        return assortment

    def calculate_share_of_shelf(self, sos_filters=None, include_empty=EXCLUDE_EMPTY, **general_filters):
        """
        :param sos_filters: These are the parameters on which ths SOS is calculated (out of the general DF).
        :param include_empty: This dictates whether Empty-typed SKUs are included in the calculation.
        :param general_filters: These are the parameters which the general data frame is filtered by.
        :return: The ratio of the SOS.
        """
        if include_empty == self.EXCLUDE_EMPTY:
            general_filters['product_type'] = (self.EMPTY, self.EXCLUDE_FILTER)
        pop_filter = self.get_filter_condition(self.scif, **general_filters)
        subset_filter = self.get_filter_condition(self.scif, **sos_filters)

        try:
            ratio = self.k_engine.calculate_sos_by_facings(pop_filter=pop_filter, subset_filter=subset_filter)
        except:
            ratio = 0

        if not isinstance(ratio, (float, int)):
            ratio = 0
        return ratio

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
            if matches.empty:
                continue
            bays = matches['bay_number'].unique().tolist()
            first_bay = min(bays)
            last_bay = max(bays)
            for shelf in matches['shelf_number'].unique():
                shelf_matches = matches[matches['shelf_number'] == shelf]
                if not shelf_matches.empty:
                    shelf_matches = shelf_matches.sort_values(by=['bay_number', 'facing_sequence_number'])
                    if shelf_matches.iloc[0]['bay_number'] == first_bay:
                        edge_facings = edge_facings.append(shelf_matches.iloc[0])
                    if len(shelf_matches) > 1:
                        if shelf_matches.iloc[-1]['bay_number'] == last_bay:
                            edge_facings = edge_facings.append(shelf_matches.iloc[-1])
            edge_facings = edge_facings[self.get_filter_condition(edge_facings, **filters)]
            if len(edge_facings) >= min_number_of_facings \
                    and len(edge_facings['shelf_number'].unique()) >= min_number_of_shelves:
                number_of_edge_scenes += 1
        return number_of_edge_scenes, len(relevant_scenes)

    def calculate_shelf_level_assortment(self, shelves, from_top_or_bottom=TOP, **filters):
        """
        :param shelves: A shelf number (of type int or string), or a list of shelves (of type int or string).
        :param from_top_or_bottom: TOP for default shelf number (counted from top)
                                    or BOTTOM for shelf number counted from bottom.
        :param filters: These are the parameters which the data frame is filtered by.
        :return: Number of unique SKUs appeared in the filtered condition.
        """
        shelves = shelves if isinstance(shelves, list) else [shelves]
        shelves = [int(shelf) for shelf in shelves]
        if from_top_or_bottom == self.TOP:
            assortment = self.calculate_assortment(shelf_number=shelves, **filters)
        else:
            assortment = self.calculate_assortment(shelf_number_from_bottom=shelves, **filters)
        return assortment

    def calculate_eye_level_assortment(self, eye_level_configurations=DEFAULT, min_number_of_products=ALL, **filters):
        """
        :param eye_level_configurations: A data frame containing information about shelves to ignore (==not eye level)
                                         for every number of shelves in each bay.
        :param min_number_of_products: Minimum number of eye level unique SKUs for KPI to pass.
        :param filters: This are the parameters which dictate the relevant SKUs for the eye-level calculation.
        :return: A tuple: (Number of scenes which pass, Total number of relevant scenes)
        """
        filters, relevant_scenes = self.separate_location_filters_from_product_filters(**filters)
        if len(relevant_scenes) == 0:
            return 0, 0
        if eye_level_configurations == self.DEFAULT:
            if hasattr(self, 'eye_level_configurations'):
                eye_level_configurations = self.eye_level_configurations
            else:
                Log.error('Eye-level configurations are not set up')
                return False
        number_of_products = len(self.all_products[self.get_filter_condition(self.all_products, **filters)]['product_ean_code'])
        min_shelf, max_shelf, min_ignore, max_ignore = eye_level_configurations.columns
        number_of_eye_level_scenes = 0
        for scene in relevant_scenes:
            eye_level_facings = pd.DataFrame(columns=self.match_product_in_scene.columns)
            matches = self.match_product_in_scene[self.match_product_in_scene['scene_fk'] == scene]
            for bay in matches['bay_number'].unique():
                bay_matches = matches[matches['bay_number'] == bay]
                number_of_shelves = bay_matches['shelf_number'].max()
                configuration = eye_level_configurations[(eye_level_configurations[min_shelf] <= number_of_shelves) &
                                                         (eye_level_configurations[max_shelf] >= number_of_shelves)]
                if not configuration.empty:
                    configuration = configuration.iloc[0]
                else:
                    configuration = {min_ignore: 0, max_ignore: 0}
                min_include = configuration[min_ignore] + 1
                max_include = number_of_shelves - configuration[max_ignore]
                eye_level_shelves = bay_matches[bay_matches['shelf_number'].between(min_include, max_include)]
                eye_level_facings = eye_level_facings.append(eye_level_shelves)
            eye_level_assortment = len(eye_level_facings[
                                           self.get_filter_condition(eye_level_facings, **filters)]['product_ean_code'])
            if min_number_of_products == self.ALL:
                min_number_of_products = number_of_products
            if eye_level_assortment >= min_number_of_products:
                number_of_eye_level_scenes += 1
        return number_of_eye_level_scenes, len(relevant_scenes)

    def shelf_level_assortment(self, min_number_of_products ,shelf_target, strict=True, **filters):
        filters, relevant_scenes = self.separate_location_filters_from_product_filters(**filters)
        if len(relevant_scenes) == 0:
            relevant_scenes = self.scif['scene_id'].unique().tolist()
        number_of_products = \
            self.scif[self.get_filter_condition(self.scif, **filters)]['item_code'].unique().tolist()
        result = 0  # Default score is FALSE
        for scene in relevant_scenes:

            eye_level_facings = 0
            matches = pd.merge(self.match_product_in_scene[self.match_product_in_scene['scene_fk'] == scene],
                               self.all_products, on=['product_fk'])
            for bay in matches['bay_number'].unique():
                bay_matches = matches[matches['bay_number'] == bay]
                products_in_target_shelf = bay_matches[(bay_matches['shelf_number'].isin(shelf_target)) & (
                    bay_matches['item_code_y'].isin(number_of_products))]
                if not products_in_target_shelf.empty:
                    eye_level_facings += len(products_in_target_shelf)
            if eye_level_facings == 0:
                return result
            else:
                eye_level_assortment = eye_level_facings
                if eye_level_assortment >= min_number_of_products:
                    result = 1
        return result

    def calculate_product_sequence(self, sequence_filters, direction, empties_allowed=True, irrelevant_allowed=False,
                                   min_required_to_pass=STRICT_MODE, **general_filters):
        """
        :param sequence_filters: One of the following:
                        1- a list of dictionaries, each containing the filters values of an organ in the sequence.
                        2- a tuple of (entity_type, [value1, value2, value3...]) in case every organ in the sequence
                           is defined by only one filter (and of the same entity, such as brand_name, etc).
        :param direction: left/right/top/bottom - the direction of the sequence.
        :param empties_allowed: This dictates whether or not the sequence can be interrupted by Empty facings.
        :param irrelevant_allowed: This dictates whether or not the sequence can be interrupted by facings which are
                                   not in the sequence.
        :param min_required_to_pass: The number of sequences needed to exist in order for KPI to pass.
                                     If STRICT_MODE is activated, the KPI passes only if it has NO rejects.
        :param general_filters: These are the parameters which the general data frame is filtered by.
        :return: True if the KPI passes; otherwise False.
        """
        if isinstance(sequence_filters, (list, tuple)) and isinstance(sequence_filters[0], (str, unicode)):
            entity, sequence_filters = sequence_filters
        else:
            entity = None
        filtered_scif = self.scif[self.get_filter_condition(self.scif, **general_filters)]
        scenes = set(filtered_scif['scene_id'].unique())
        for filters in sequence_filters:
            if isinstance(filters, dict):
                scene_for_filters = filtered_scif[self.get_filter_condition(filtered_scif, **filters)]['scene_id'].unique()
            else:
                scene_for_filters = filtered_scif[filtered_scif[entity] == filters]['scene_id'].unique()
            scenes = scenes.intersection(scene_for_filters)
            if not scenes:
                Log.debug('None of the scenes include products from all types relevant for sequence')
                return True

        pass_counter = 0
        reject_counter = 0
        for scene in scenes:
            scene_graph = self.position_graph.get(scene)
            # removing unnecessary edges
            filtered_scene_graph = scene_graph.copy()
            edges_to_remove = filtered_scene_graph.es.select(direction_ne=direction)
            filtered_scene_graph.delete_edges([edge.index for edge in edges_to_remove])

            reversed_scene_graph = scene_graph.copy()
            edges_to_remove = reversed_scene_graph.es.select(direction_ne=self._reverse_direction(direction))
            reversed_scene_graph.delete_edges([edge.index for edge in edges_to_remove])

            vertices_list = []
            for filters in sequence_filters:
                if not isinstance(filters, dict):
                    filters = {entity: filters}
                vertices_list.append(self.filter_vertices_from_graph(scene_graph, **filters))
            tested_vertices, sequence_vertices = vertices_list[0], vertices_list[1:]
            vertices_list = reduce(lambda x, y: x + y, sequence_vertices)

            sequences = []
            for vertex in tested_vertices:
                previous_sequences = self.get_positions_by_direction(reversed_scene_graph, vertex)
                if previous_sequences and set(vertices_list).intersection(reduce(lambda x, y: x + y, previous_sequences)):
                    reject_counter += 1
                    if min_required_to_pass == self.STRICT_MODE:
                        return False
                    continue

                next_sequences = self.get_positions_by_direction(filtered_scene_graph, vertex)
                sequences.extend(next_sequences)

            sequences = self._filter_sequences(sequences)
            for sequence in sequences:
                all_products_appeared = True
                empties_found = False
                irrelevant_found = False
                full_sequence = False
                broken_sequence = False
                current_index = 0
                previous_vertices = list(tested_vertices)

                for vertices in sequence_vertices:
                    if not set(sequence).intersection(vertices):
                        all_products_appeared = False
                        break

                for vindex in sequence:
                    vertex = scene_graph.vs[vindex]
                    if vindex not in vertices_list and vindex not in tested_vertices:
                        if current_index < len(sequence_vertices):
                            if vertex['product_type'] == self.EMPTY:
                                empties_found = True
                            else:
                                irrelevant_found = True
                    elif vindex in previous_vertices:
                        pass
                    elif vindex in sequence_vertices[current_index]:
                        previous_vertices = list(sequence_vertices[current_index])
                        current_index += 1
                    else:
                        broken_sequence = True

                if current_index == len(sequence_vertices):
                    full_sequence = True

                if broken_sequence:
                    reject_counter += 1
                elif full_sequence:
                    if not empties_allowed and empties_found:
                        reject_counter += 1
                    elif not irrelevant_allowed and irrelevant_found:
                        reject_counter += 1
                    elif all_products_appeared:
                        pass_counter += 1

                if pass_counter >= min_required_to_pass:
                    return True
                elif min_required_to_pass == self.STRICT_MODE and reject_counter > 0:
                    return False

        if reject_counter == 0:
            return True
        else:
            return False

    @staticmethod
    def _reverse_direction(direction):
        """
        This function returns the opposite of a given direction.
        """
        if direction == 'top':
            new_direction = 'bottom'
        elif direction == 'bottom':
            new_direction = 'top'
        elif direction == 'left':
            new_direction = 'right'
        elif direction == 'right':
            new_direction = 'left'
        else:
            new_direction = direction
        return new_direction

    def get_positions_by_direction(self, graph, vertex_index):
        """
        This function gets a filtered graph (contains only edges of a relevant direction) and a Vertex index,
        and returns all sequences starting in it (until it gets to a dead end).
        """
        sequences = []
        edges = [graph.es[e] for e in graph.incident(vertex_index)]
        next_vertices = [edge.target for edge in edges]
        for vertex in next_vertices:
            next_sequences = self.get_positions_by_direction(graph, vertex)
            if not next_sequences:
                sequences.append([vertex])
            else:
                for sequence in next_sequences:
                    sequences.append([vertex] + sequence)
        return sequences

    @staticmethod
    def _filter_sequences(sequences):
        """
        This function receives a list of sequences (lists of indexes), and removes sequences which can be represented
        by a shorter sequence (which is also in the list).
        """
        if not sequences:
            return sequences
        sequences = sorted(sequences, key=lambda x: (x[-1], len(x)))
        filtered_sequences = [sequences[0]]
        for sequence in sequences[1:]:
            if sequence[-1] != filtered_sequences[-1][-1]:
                filtered_sequences.append(sequence)
        return filtered_sequences

    def calculate_non_proximity(self, tested_filters, anchor_filters, allowed_diagonal=False, **general_filters):
        """
        :param tested_filters: The tested SKUs' filters.
        :param anchor_filters: The anchor SKUs' filters.
        :param allowed_diagonal: True - a tested SKU can be in a direct diagonal from an anchor SKU in order
                                        for the KPI to pass;
                                 False - a diagonal proximity is NOT allowed.
        :param general_filters: These are the parameters which the general data frame is filtered by.
        :return:
        """
        direction_data = []
        if allowed_diagonal:
            direction_data.append({'top': (0, 1), 'bottom': (0, 1)})
            direction_data.append({'right': (0, 1), 'left': (0, 1)})
        else:
            direction_data.append({'top': (0, 1), 'bottom': (0, 1), 'right': (0, 1), 'left': (0, 1)})
        is_proximity = self.calculate_relative_position(tested_filters, anchor_filters, direction_data,
                                                        min_required_to_pass=1, **general_filters)
        return not is_proximity

    def calculate_relative_position(self, tested_filters, anchor_filters, direction_data, min_required_to_pass=1,
                                    **general_filters):
        """
        :param tested_filters: The tested SKUs' filters.
        :param anchor_filters: The anchor SKUs' filters.
        :param direction_data: The allowed distance between the tested and anchor SKUs.
                               In form: {'top': 4, 'bottom: 0, 'left': 100, 'right': 0}
                               Alternative form: {'top': (0, 1), 'bottom': (1, 1000), ...} - As range.
        :param min_required_to_pass: The number of appearances needed to be True for relative position in order for KPI
                                     to pass. If all appearances are required: ==a string or a big number.
        :param general_filters: These are the parameters which the general data frame is filtered by.
        :return: True if (at least) one pair of relevant SKUs fits the distance requirements; otherwise - returns False.
        """
        filtered_scif = self.scif[self.get_filter_condition(self.scif, **general_filters)]
        tested_scenes = filtered_scif[self.get_filter_condition(filtered_scif, **tested_filters)]['scene_id'].unique()
        anchor_scenes = filtered_scif[self.get_filter_condition(filtered_scif, **anchor_filters)]['scene_id'].unique()
        relevant_scenes = set(tested_scenes).intersection(anchor_scenes)

        if relevant_scenes:
            pass_counter = 0
            reject_counter = 0
            for scene in relevant_scenes:
                scene_graph = self.position_graph.get(scene)
                if not scene_graph:
                    continue
                tested_vertices = self.filter_vertices_from_graph(scene_graph, **tested_filters)
                anchor_vertices = self.filter_vertices_from_graph(scene_graph, **anchor_filters)
                for tested_vertex in tested_vertices:
                    for anchor_vertex in anchor_vertices:
                        moves = {'top': 0, 'bottom': 0, 'left': 0, 'right': 0}
                        path = scene_graph.get_shortest_paths(anchor_vertex, tested_vertex, output='epath')
                        if path:
                            path = path[0]
                            for edge in path:
                                moves[scene_graph.es[edge]['direction']] += 1
                            if self.validate_moves(moves, direction_data):
                                pass_counter += 1
                                if isinstance(min_required_to_pass, int) and pass_counter >= min_required_to_pass:
                                    return True
                            else:
                                reject_counter += 1
                        else:
                            Log.debug('Tested and Anchor have no direct path')
            if pass_counter > 0 and reject_counter == 0:
                return True
            else:
                return False
        else:
            Log.debug('None of the scenes contain both anchor and tested SKUs')
            return False

    @staticmethod
    def filter_vertices_from_graph(graph, **filters):
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
        return list(vertices_indexes)

    @staticmethod
    def validate_moves(moves, direction_data):
        """
        This function checks whether the distance between the anchor and the tested SKUs fits the requirements.
        """
        direction_data = direction_data if isinstance(direction_data, (list, tuple)) else [direction_data]
        validated = False
        for data in direction_data:
            data_validated = True
            for direction in moves.keys():
                allowed_moves = data.get(direction, (0, 0))
                min_move, max_move = allowed_moves if isinstance(allowed_moves, tuple) else (0, allowed_moves)
                if not min_move <= moves[direction] <= max_move:
                    data_validated = False
                    break
            if data_validated:
                validated = True
                break
        return validated
    #
    # def calculate_block_together(self, allowed_products_filters=None, include_empty=EXCLUDE_EMPTY,
    #                                  minimum_block_ratio=1, result_by_scene=False, **filters):
    #     """
    #     :param allowed_products_filters: These are the parameters which are allowed to corrupt the block without failing it.
    #     :param include_empty: This parameter dictates whether or not to discard Empty-typed products.
    #     :param minimum_block_ratio: The minimum (block number of facings / total number of relevant facings) ratio
    #                                 in order for KPI to pass (if ratio=1, then only one block is allowed).
    #     :param result_by_scene: True - The result is a tuple of (number of passed scenes, total relevant scenes);
    #                             False - The result is True if at least one scene has a block, False - otherwise.
    #     :param filters: These are the parameters which the blocks are checked for.
    #     :return: see 'result_by_scene' above.
    #     """
    #     filters, relevant_scenes = self.separate_location_filters_from_product_filters(**filters)
    #     if len(relevant_scenes) == 0:
    #         if result_by_scene:
    #             return 0, 0
    #         else:
    #             Log.debug('Block Together: No relevant SKUs were found for these filters {}'.format(filters))
    #             return True
    #     number_of_blocked_scenes = 0
    #     cluster_ratios = []
    #     for scene in relevant_scenes:
    #         scene_graph = self.position_graph.get(scene).copy()
    #
    #         relevant_vertices = set(self.filter_vertices_from_graph(scene_graph, **filters))
    #         if allowed_products_filters:
    #             allowed_vertices = self.filter_vertices_from_graph(scene_graph, **allowed_products_filters)
    #         else:
    #             allowed_vertices = set()
    #
    #         if include_empty == self.EXCLUDE_EMPTY:
    #             empty_vertices = {v.index for v in scene_graph.vs.select(product_type='Empty')}
    #             allowed_vertices = set(allowed_vertices).union(empty_vertices)
    #
    #         all_vertices = {v.index for v in scene_graph.vs}
    #         vertices_to_remove = all_vertices.difference(relevant_vertices.union(allowed_vertices))
    #         scene_graph.delete_vertices(vertices_to_remove)
    #         # removing clusters including 'allowed' SKUs only
    #         clusters = [cluster for cluster in scene_graph.clusters() if set(cluster).difference(allowed_vertices)]
    #         new_relevant_vertices = self.filter_vertices_from_graph(scene_graph, **filters)
    #         for cluster in clusters:
    #             relevant_vertices_in_cluster = set(cluster).intersection(new_relevant_vertices)
    #             cluster_ratio = len(relevant_vertices_in_cluster) / float(len(new_relevant_vertices))
    #             cluster_ratios.append(cluster_ratio)
    #             if cluster_ratio >= minimum_block_ratio:
    #                 if result_by_scene:
    #                     number_of_blocked_scenes += 1
    #                     break
    #                 else:
    #                     return True if minimum_block_ratio == 1 else cluster_ratio
    #     if result_by_scene:
    #         return number_of_blocked_scenes, len(relevant_scenes)
    #     else:
    #         return False if minimum_block_ratio == 1 else max(cluster_ratios)

    def calculate_block_together(self, allowed_products_filters=None, include_empty=EXCLUDE_EMPTY, **filters):
        """
        :param allowed_products_filters: These are the parameters which are allowed to corrupt the block without failing it.
        :param include_empty: This parameter dictates whether or not to discard Empty-typed products.
        :param filters: These are the parameters which the blocks are checked for.
        :return: True - if in (at least) one of the scenes all the relevant SKUs are grouped together in one block;
                 otherwise - returns False.
        """
        relevant_scenes = self.scif[self.get_filter_condition(self.scif, **filters)]['scene_id'].unique().tolist()
        for field in filters.keys():
            if field not in self.all_products.columns:
                filters.pop(field, None)
        if relevant_scenes:
            for scene in relevant_scenes:
                if self.position_graph.get(scene) is not None:
                    scene_graph = self.position_graph.get(scene).copy()
                    relevant_vertices = None
                    for field in filters.keys():
                        values = filters[field] if isinstance(filters[field], (list, float)) else [filters[field]]
                        vertices_for_field = set()
                        for value in values:
                            condition = {field: value}
                            vertices = {v.index for v in scene_graph.vs.select(**condition)}
                            vertices_for_field = vertices_for_field.union(vertices)
                        if relevant_vertices is None:
                            relevant_vertices = vertices_for_field
                        else:
                            relevant_vertices = relevant_vertices.intersection(vertices_for_field)

                    if allowed_products_filters:
                        allowed_vertices = None
                        for field in allowed_products_filters.keys():
                            values = allowed_products_filters[field] \
                                                            if isinstance(allowed_products_filters[field], (list, float)) \
                                                            else [allowed_products_filters[field]]
                            vertices_for_field = set()
                            for value in values:
                                condition = {field: value}
                                vertices = {v.index for v in scene_graph.vs.select(**condition)}
                                vertices_for_field = vertices_for_field.union(vertices)
                            if allowed_vertices is None:
                                allowed_vertices = vertices_for_field
                            else:
                                allowed_vertices = allowed_vertices.intersection(vertices_for_field)

                        if include_empty == self.EXCLUDE_EMPTY:
                            empty_vertices = {v.index for v in scene_graph.vs.select(product_type='Empty')}
                            allowed_vertices = allowed_vertices.union(empty_vertices)

                        relevant_vertices = relevant_vertices if relevant_vertices is not None else set()
                        allowed_vertices = allowed_vertices if allowed_vertices is not None else set()
                    else:
                        allowed_vertices = []

                    all_vertices = {v.index for v in scene_graph.vs}
                    vertices_to_remove = all_vertices.difference(relevant_vertices.union(allowed_vertices))
                    scene_graph.delete_vertices(vertices_to_remove)
                    # removing clusters including 'allowed' SKUs only
                    clusters = [cluster for cluster in scene_graph.clusters() if set(cluster).difference(allowed_vertices)]
                    if len(clusters) == 1:
                        return True
        else:
            Log.debug('None of the scenes contain relevant SKUs')
        return False

    def get_filter_condition(self, df, **filters):
        """
        :param df: The data frame to be filters.
        :param filters: These are the parameters which the data frame is filtered by.
                       Every parameter would be a tuple of the value and an include/exclude flag.
                       INPUT EXAMPLE (1):   manufacturer_name = ('Diageo', DIAGEOAUGENERALToolBox.INCLUDE_FILTER)
                       INPUT EXAMPLE (2):   manufacturer_name = 'Diageo'
        :return: a filtered Scene Item Facts data frame.
        """
        if 'facings' in df.keys():
            filter_condition = (df['facings'] > 0)
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
                else:
                    continue
                if filter_condition is None:
                    filter_condition = condition
                else:
                    filter_condition &= condition
            else:
                Log.warning('field {} is not in the Data Frame'.format(field))

        return filter_condition

    def separate_location_filters_from_product_filters(self, **filters):
        """
        This function gets scene-item-facts filters of all kinds, extracts the relevant scenes by the location filters,
        and returns them along with the product filters only.
        """
        location_filters = {}
        for field in filters.keys():
            if field not in self.all_products.columns:
                location_filters[field] = filters.pop(field)
        relevant_scenes = self.scif[self.get_filter_condition(self.scif, **location_filters)]['scene_id'].unique()
        return filters, relevant_scenes

    @staticmethod
    def get_json_data(file_path, sheet_name=None, skiprows=0):
        """
        This function gets a file's path and extract its content into a JSON.
        """
        data = {}
        if sheet_name:
            sheet_names = [sheet_name]
        else:
            sheet_names = xlrd.open_workbook(file_path).sheet_names()
        for sheet_name in sheet_names:
            try:
                output = pd.read_excel(file_path, sheetname=sheet_name, skiprows=skiprows)
            except xlrd.biffh.XLRDError:
                Log.warning('Sheet name {} doesn\'t exist'.format(sheet_name))
                return None
            output = output.to_json(orient='records')
            output = json.loads(output)
            for x in xrange(len(output)):
                for y in output[x].keys():
                    output[x][y] = unicode('' if output[x][y] is None else output[x][y]).strip()
                    if not output[x][y]:
                        output[x].pop(y, None)
            data[sheet_name] = output
        if sheet_name:
            data = data[sheet_name]
        elif len(data.keys()) == 1:
            data = data[data.keys()[0]]
        return data
