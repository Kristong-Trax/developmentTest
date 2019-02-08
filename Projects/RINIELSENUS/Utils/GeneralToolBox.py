
import xlrd
import json
import pandas as pd

from Projects.RINIELSENUS.Utils.PositionGraph import MarsUsPositionGraphs
from Trax.Algo.Calculations.Core.DataProvider import Data
from Trax.Algo.Calculations.Core.Shortcuts import BaseCalculationsGroup
from KPIUtils_v2.Calculations.BlockCalculations import Block as Block

from Trax.Utils.Logging.Logger import Log


__author__ = 'Nimrod'


class MarsUsGENERALToolBox:

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

    def __init__(self, data_provider, output, rds_conn=None, ignore_stacking=False, front_facing=False, **kwargs):
        self.k_engine = BaseCalculationsGroup(data_provider, output)
        self.rds_conn = rds_conn
        self.data_provider = data_provider
        self.project_name = self.data_provider.project_name
        self.session_uid = self.data_provider.session_uid
        self.scif = self.data_provider[Data.SCENE_ITEM_FACTS]
        self.all_products = self.data_provider[Data.ALL_PRODUCTS]
        self.survey_response = self.data_provider[Data.SURVEY_RESPONSES]
        self.scenes_info = self.data_provider[Data.SCENES_INFO].merge(self.data_provider[Data.ALL_TEMPLATES],
                                                                      how='left', on='template_fk', suffixes=['', '_y'])
        self.ignore_stacking = ignore_stacking
        self.facings_field = 'facings' if not self.ignore_stacking else 'facings_ign_stack'
        self.front_facing = front_facing
        for data in kwargs.keys():
            setattr(self, data, kwargs[data])
        if self.front_facing:
            self.scif = self.scif[self.scif['front_face_count'] == 1]

    @property
    def position_graphs(self):
        if not hasattr(self, '_position_graphs'):
            self._position_graphs = MarsUsPositionGraphs(self.data_provider, rds_conn=self.rds_conn)
        return self._position_graphs

    @property
    def match_product_in_scene(self):
        if not hasattr(self, '_match_product_in_scene'):
            self._match_product_in_scene = self.position_graphs.match_product_in_scene
            if self.front_facing:
                self._match_product_in_scene = self._match_product_in_scene[self._match_product_in_scene['front_facing'] == 'Y']
            if self.ignore_stacking:
                self._match_product_in_scene = self._match_product_in_scene[self._match_product_in_scene['stacking_layer'] == 1]
            # self._match_product_in_scene = pd.merge(self._match_product_in_scene, self.data_provider.probe_groups, on='probe_match_fk')
        return self._match_product_in_scene

    def get_survey_answer(self, survey_data, answer_field=None):
        """
        :param survey_data:     1) str - The name of the survey in the DB.
                                2) tuple - (The field name, the field value). For example: ('question_fk', 13)
        :param answer_field: The DB field from which the answer is extracted. Default is the usual hierarchy.
        :return: The required survey response.
        """
        if not isinstance(survey_data, (list, tuple)):
            entity = 'question_text'
            value = survey_data
        else:
            entity, value = survey_data
        survey = self.survey_response[self.survey_response[entity] == value]
        if survey.empty:
            return None
        survey = survey.iloc[0]
        if answer_field is None or answer_field not in survey.keys():
            answer_field = 'selected_option_text' if survey['selected_option_text'] else 'number_value'
        survey_answer = survey[answer_field]
        return survey_answer

    def check_survey_answer(self, survey_text, target_answer):
        """
        :param survey_text:     1) str - The name of the survey in the DB.
                                2) tuple - (The field name, the field value). For example: ('question_fk', 13)
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
        if filters:
            if set(filters.keys()).difference(self.scenes_info.keys()):
                scene_data = self.scif[self.get_filter_condition(self.scif, **filters)]
            else:
                scene_data = self.scenes_info[self.get_filter_condition(self.scenes_info, **filters)]
        else:
            scene_data = self.scenes_info
        number_of_scenes = len(scene_data['scene_fk'].unique().tolist())
        return number_of_scenes

    def calculate_availability(self, **filters):
        """
        :param filters: These are the parameters which the data frame is filtered by.
        :return: Total number of SKUs facings appeared in the filtered Scene Item Facts data frame.
        """
        if set(filters.keys()).difference(self.scif.keys()):
            filtered_df = self.match_product_in_scene[self.get_filter_condition(self.match_product_in_scene, **filters)]
        else:
            filtered_df = self.scif[self.get_filter_condition(self.scif, **filters)]
        if self.facings_field in filtered_df.columns:
            availability = filtered_df[self.facings_field].sum()
        else:
            availability = len(filtered_df)
        return availability

    def calculate_assortment(self, assortment_entity='product_ean_code', minimum_assortment_for_entity=1, **filters):
        """
        :param assortment_entity: This is the entity on which the assortment is calculated.
        :param minimum_assortment_for_entity: This is the number of assortment per each unique entity in order for it
                                              to be counted in the final assortment result (default is 1).
        :param filters: These are the parameters which the data frame is filtered by.
        :return: Number of unique SKUs appeared in the filtered Scene Item Facts data frame.
        """
        if set(filters.keys()).difference(self.scif.keys()):
            filtered_df = self.match_product_in_scene[self.get_filter_condition(self.match_product_in_scene, **filters)]
        else:
            filtered_df = self.scif[self.get_filter_condition(self.scif, **filters)]
        if minimum_assortment_for_entity == 1:
            assortment = len(filtered_df[assortment_entity].unique())
        else:
            assortment = 0
            for entity_id in filtered_df[assortment_entity].unique():
                assortment_for_entity = filtered_df[filtered_df[assortment_entity] == entity_id]
                if self.facings_field in filtered_df.columns:
                    assortment_for_entity = assortment_for_entity[self.facings_field].sum()
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
        :return: The ratio of the Facings SOS.
        """
        if include_empty == self.EXCLUDE_EMPTY:
            general_filters['product_type'] = (self.EMPTY, self.EXCLUDE_FILTER)

        numerator_facings = self.calculate_share_facings(**dict(sos_filters, **general_filters))
        denominator_facings = self.calculate_share_facings(**general_filters)

        if denominator_facings == 0:
            ratio = 0
        else:
            ratio = numerator_facings / float(denominator_facings)
        return ratio

    def calculate_share_facings(self, include_empty=EXCLUDE_EMPTY, **filters):
        """
        :param include_empty: This dictates whether Empty-typed SKUs are included in the calculation.
        :param filters: These are the parameters which the data frame is filtered by.
        :return: The total shelf width (in mm) the relevant facings occupy.
        """
        if include_empty == self.EXCLUDE_EMPTY:
            filters['product_type'] = (self.EMPTY, self.EXCLUDE_FILTER)
        filtered_matches = self.match_product_in_scene[self.get_filter_condition(self.match_product_in_scene, **filters)]
        return len(filtered_matches)

    def calculate_linear_share_of_shelf(self, sos_filters, include_empty=EXCLUDE_EMPTY, **general_filters):
        """
        :param sos_filters: These are the parameters on which ths SOS is calculated (out of the general DF).
        :param include_empty: This dictates whether Empty-typed SKUs are included in the calculation.
        :param general_filters: These are the parameters which the general data frame is filtered by.
        :return: The Linear SOS ratio.
        """
        if include_empty == self.EXCLUDE_EMPTY:
            general_filters['product_type'] = (self.EMPTY, self.EXCLUDE_FILTER)

        numerator_width = self.calculate_share_space_length(**dict(sos_filters, **general_filters))
        denominator_width = self.calculate_share_space_length(**general_filters)

        if denominator_width == 0:
            ratio = None
        else:
            ratio = numerator_width / float(denominator_width)
        return ratio

    def calculate_linear_share_of_shelf_numerator(self, sos_filters, include_empty=EXCLUDE_EMPTY, **general_filters):
        """
        :param sos_filters: These are the parameters on which ths SOS is calculated (out of the general DF).
        :param include_empty: This dictates whether Empty-typed SKUs are included in the calculation.
        :param general_filters: These are the parameters which the general data frame is filtered by.
        :return: The Linear SOS ratio.
        """
        if include_empty == self.EXCLUDE_EMPTY:
            general_filters['product_type'] = (self.EMPTY, self.EXCLUDE_FILTER)

        return self.calculate_share_space_length(**dict(sos_filters, **general_filters))

    def calculate_linear_share_of_shelf_denominator(self, sos_filters, include_empty=EXCLUDE_EMPTY, **general_filters):
        """
        :param sos_filters: These are the parameters on which ths SOS is calculated (out of the general DF).
        :param include_empty: This dictates whether Empty-typed SKUs are included in the calculation.
        :param general_filters: These are the parameters which the general data frame is filtered by.
        :return: The Linear SOS ratio.
        """
        if include_empty == self.EXCLUDE_EMPTY:
            general_filters['product_type'] = (self.EMPTY, self.EXCLUDE_FILTER)

        return self.calculate_share_space_length(**general_filters)

    def calculate_share_space_length(self, include_empty=EXCLUDE_EMPTY, **filters):
        """
        :param filters: These are the parameters which the data frame is filtered by.
        :return: The total shelf width (in mm) the relevant facings occupy.
        """
        if include_empty == self.EXCLUDE_EMPTY:
            filters['product_type'] = (self.EMPTY, self.EXCLUDE_FILTER)
        filtered_matches = self.match_product_in_scene[self.get_filter_condition(self.match_product_in_scene, **filters)]
        return filtered_matches['width_mm_advance'].fillna(filtered_matches['width_mm']).sum()

    def calculate_products_on_edge(self, min_number_of_facings=1, min_number_of_shelves=1, list_result=False,
                                   edge_population=None, category=None, position=None, **filters):
        """
        :param edge_population:
        :param position:
        :param category:
        :param list_result:
        :param min_number_of_facings: Minimum number of edge facings for KPI to pass.
        :param scene_filters: dict with params to filter the matches (ex-anchor of category...)
        :param min_number_of_shelves: Minimum number of different shelves with edge facings for KPI to pass.
        :param filters: This are the parameters which dictate the relevant SKUs for the edge calculation.
        :return: A tuple: (Number of scenes which pass, Total number of relevant scenes)
        """
        filters, relevant_scenes = self.separate_location_filters_from_product_filters(**filters)
        if len(relevant_scenes) == 0:
            return 0, 0
        number_of_edge_scenes = 0
        total_edge = pd.DataFrame(columns=self.match_product_in_scene.columns)
        for scene in relevant_scenes:
            edge_facings = pd.DataFrame(columns=self.match_product_in_scene.columns)
            matches = self.match_product_in_scene[self.match_product_in_scene['scene_fk'] == scene]
            bay_number_filter = filters.get('bay_number')
            if bay_number_filter:
                matches = matches[matches['bay_number'] == bay_number_filter]
            if category:
                matches = matches[matches['category'] == category]
            if edge_population:
                matches = matches[self.get_filter_condition(matches, **edge_population)]
            for shelf in matches['shelf_number'].unique():
                shelf_matches = matches[matches['shelf_number'] == shelf]
                if not shelf_matches.empty:
                    shelf_matches = shelf_matches.sort_values(by=['bay_number', 'facing_sequence_number'])
                    if position:
                        if position == 'left':
                            edge_facings = edge_facings.append(shelf_matches.iloc[0])
                        if position == 'right':
                            edge_facings = edge_facings.append(shelf_matches.iloc[-1])
                    else:
                        edge_facings = edge_facings.append(shelf_matches.iloc[0])
                        if len(edge_facings) > 1:
                            edge_facings = edge_facings.append(shelf_matches.iloc[-1])
            edge_facings = edge_facings[self.get_filter_condition(edge_facings, **filters)]
            total_edge = total_edge.append(edge_facings)
            if len(edge_facings) >= min_number_of_facings \
                    and len(edge_facings['shelf_number'].unique()) >= min_number_of_shelves:
                number_of_edge_scenes += 1
        if list_result:
            return total_edge
        else:
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
            relevant_scenes = self.scif['scene_fk'].unique().tolist()
        number_of_products = len(self.all_products[self.get_filter_condition(self.all_products, **filters)]
                                 ['product_ean_code'])
        result = 0  # Default score is FALSE
        for scene in relevant_scenes:
            eye_level_facings = pd.DataFrame(columns=self.match_product_in_scene.columns)
            matches = pd.merge(self.match_product_in_scene[self.match_product_in_scene['scene_fk'] == scene],
                               self.all_products, on=['product_fk'])
            for bay in matches['bay_number'].unique():
                bay_matches = matches[matches['bay_number'] == bay]
                products_in_target_shelf = bay_matches[(bay_matches['shelf_number'].isin(shelf_target)) & (
                    bay_matches['product_ean_code'].isin(number_of_products))]
                eye_level_facings = eye_level_facings.append(products_in_target_shelf)
            eye_level_assortment = len(eye_level_facings[
                                           self.get_filter_condition(eye_level_facings, **filters)][
                                           'product_ean_code'])
            if eye_level_assortment >= min_number_of_products:
                result = 1
        return result

    def calculate_product_sequence(self, sequence_filters, direction, empties_allowed=True, irrelevant_allowed=True,
                                   min_required_to_pass=STRICT_MODE, custom_graph=None, **general_filters):
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
        :param custom_graph: A filtered Positions graph - given in case only certain vertices need to be checked.
        :param general_filters: These are the parameters which the general data frame is filtered by.
        :return: True if the KPI passes; otherwise False.
        """
        if isinstance(sequence_filters, (list, tuple)) and isinstance(sequence_filters[0], (str, unicode)):
            sequence_filters = [{sequence_filters[0]: values} for values in sequence_filters[1]]

        pass_counter = 0
        reject_counter = 0

        if not custom_graph:

            filtered_scif = self.scif[self.get_filter_condition(self.scif, **general_filters)]
            scenes = set(filtered_scif['scene_id'].unique())
            for filters in sequence_filters:
                scene_for_filters = filtered_scif[self.get_filter_condition(filtered_scif, **filters)][
                    'scene_id'].unique()
                scenes = scenes.intersection(scene_for_filters)
                if not scenes:
                    Log.debug('None of the scenes include products from all types relevant for sequence')
                    return False

            for scene in scenes:
                scene_graph = self.position_graphs.get(scene)
                scene_passes, scene_rejects = self.calculate_sequence_for_graph(scene_graph, sequence_filters,
                                                                                direction,
                                                                                empties_allowed, irrelevant_allowed)
                pass_counter += scene_passes
                reject_counter += scene_rejects

                if pass_counter >= min_required_to_pass:
                    return True
                elif min_required_to_pass == self.STRICT_MODE and reject_counter > 0:
                    return False

        else:
            scene_passes, scene_rejects = self.calculate_sequence_for_graph(custom_graph, sequence_filters, direction,
                                                                            empties_allowed, irrelevant_allowed)
            pass_counter += scene_passes
            reject_counter += scene_rejects

        if pass_counter >= min_required_to_pass or reject_counter == 0:
            return True
        else:
            return False

    def calculate_sequence_for_graph(self, graph, sequence_filters, direction, empties_allowed, irrelevant_allowed):
        """
        This function checks for a sequence given a position graph (either a full scene graph or a customized one).
        """
        pass_counter = 0
        reject_counter = 0

        # removing unnecessary edges
        filtered_scene_graph = graph.copy()
        if len(filtered_scene_graph.es) == 0:
            return pass_counter, reject_counter
        edges_to_remove = filtered_scene_graph.es.select(direction_ne=direction)
        filtered_scene_graph.delete_edges([edge.index for edge in edges_to_remove])

        reversed_scene_graph = graph.copy()
        edges_to_remove = reversed_scene_graph.es.select(direction_ne=self._reverse_direction(direction))
        reversed_scene_graph.delete_edges([edge.index for edge in edges_to_remove])

        vertices_list = []
        for filters in sequence_filters:
            vertices_list.append(self.filter_vertices_from_graph(graph, **filters))
        tested_vertices, sequence_vertices = vertices_list[0], vertices_list[1:]
        vertices_list = reduce(lambda x, y: x + y, sequence_vertices)

        sequences = []
        for vertex in tested_vertices:
            previous_sequences = self.get_positions_by_direction(reversed_scene_graph, vertex)
            if previous_sequences and set(vertices_list).intersection(reduce(lambda x, y: x + y, previous_sequences)):
                reject_counter += 1
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
                vertex = graph.vs[vindex]
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
        return pass_counter, reject_counter

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
                scene_graph = self.position_graphs.get(scene)
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
        graph.delete_vertices(vertices_to_remove)
        # removing clusters including 'allowed' SKUs only
        blocks = [block for block in graph.clusters() if set(block).difference(allowed_vertices)]
        return blocks, graph

    def calculate_avg_shelf(self, **filters):
        matches = self.match_product_in_scene[self.get_filter_condition(self.match_product_in_scene, **filters)]
        matches = matches.groupby(['shelf_number_from_bottom']).size().reset_index(name='counts')
        avg_shelf = sum(matches.apply(lambda x: x['shelf_number_from_bottom'] * x['counts'], axis=1))
        products_count = sum(matches['counts'])
        return avg_shelf / products_count

    def make_net_x_block(self, scene, filters, allowed_products_filters):
        scene_filter = {'scene_fk': scene}
        result = self.block.network_x_block_together(filters, location=scene_filter,
                                                     additional={'allowed_products_filters': allowed_products_filters,
                                                                 'include_stacking': False,
                                                                 'check_vertical_horizontal': True})
        blocks = result[result['is_block'] == True]
        return blocks, result

    def calculate_block_together(self, allowed_products_filters=None, include_empty=EXCLUDE_EMPTY,
                                 minimum_block_ratio=1, result_by_scene=False, block_of_blocks=False,
                                 block_products1=None, block_products2=None, vertical=False, biggest_block=False,
                                 n_cluster=None, **filters):

        probe = float(filters.pop('probe_group_id')) if 'probe_group_id' in filters else None
        filters, relevant_scenes = self.separate_location_filters_from_product_filters(**filters)
        if len(relevant_scenes) == 0:
            if result_by_scene:
                return 0, 0
            else:
                Log.debug('Block Together: No relevant SKUs were found for these filters {}'.format(filters))
                return False
        number_of_blocked_scenes = 0
        cluster_ratios = []
        for scene in relevant_scenes:
            # print('~~~~~~~~~~~~~~ SCENE {} ~~~~~~~~~~~~'.format(scene))

            block, all_blocks = self.make_net_x_block(scene, filters, allowed_products_filters)

            if not block.empty:
                if biggest_block:
                    continue
                if result_by_scene:
                    number_of_blocked_scenes += 1
                    break  # This seems wrong....
                else:
                    all_vertices = {v.index for v in scene_graph.vs}
                    non_cluster_vertices = all_vertices.difference(list(relevant_vertices_in_cluster))
                    scene_graph.delete_vertices(non_cluster_vertices)
                    if vertical:
                        return {'block': True, 'shelves': len(
                            set(scene_graph.vs['shelf_number']))}
                    return cluster_ratio, scene_graph

            if block_of_blocks:
                new_relevant_vertices1 = self.filter_vertices_from_graph(scene_graph, **block_products1)
                new_relevant_vertices2 = self.filter_vertices_from_graph(scene_graph, **block_products2)
            else:
                new_relevant_vertices = self.filter_vertices_from_graph(scene_graph, **filters)
            for cluster in clusters:
                # print('\n\n')
                # sorted_clust = sorted(scene_graph.vs[cluster], key=lambda x: [x['bay_number'], x['shelf_number'],
                #                                                               x['shelf_px_left']])
                # for i in sorted_clust:
                #     print('bay:', i['bay_number'], 'shelf', i['shelf_number'], i['Sub-section'], i['Customer Brand'],
                #           i['Sub Brand'], i['Segment'], i['product_name'])
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
                return {'block': True, 'shelf_numbers': set(scene_graph.vs['shelf_number'])}
            if result_by_scene:
                return number_of_blocked_scenes, len(relevant_scenes)
            else:
                return False

    # def calculate_block_together(self, allowed_products_filters=None, include_empty=EXCLUDE_EMPTY,
    #                              minimum_block_ratio=1, result_by_scene=False, block_of_blocks=False,
    #                              block_products1=None, block_products2=None, vertical=False, biggest_block=False,
    #                              n_cluster=None, **filters):
    #     """
    #     :param biggest_block:
    #     :param block_products1:
    #     :param block_products2:
    #     :param block_of_blocks:
    #     :param vertical: if needed to check vertical block by average shelf
    #     :param allowed_products_filters: These are the parameters which are allowed to corrupt the block without failing it.
    #     :param include_empty: This parameter dictates whether or not to discard Empty-typed products.
    #     :param minimum_block_ratio: The minimum (block number of facings / total number of relevant facings) ratio
    #                                 in order for KPI to pass (if ratio=1, then only one block is allowed).
    #     :param result_by_scene: True - The result is a tuple of (number of passed scenes, total relevant scenes);
    #                             False - The result is True if at least one scene has a block, False - otherwise.
    #     :param filters: These are the parameters which the blocks are checked for.
    #     :return: see 'result_by_scene' above.
    #     """
    #     probe = float(filters.pop('probe_group_id')) if 'probe_group_id' in filters else None
    #     filters, relevant_scenes = self.separate_location_filters_from_product_filters(**filters)
    #     if len(relevant_scenes) == 0:
    #         if result_by_scene:
    #             return 0, 0
    #         else:
    #             Log.debug('Block Together: No relevant SKUs were found for these filters {}'.format(filters))
    #             return False
    #     number_of_blocked_scenes = 0
    #     cluster_ratios = []
    #     for scene in relevant_scenes:
    #         # print('~~~~~~~~~~~~~~ SCENE {} ~~~~~~~~~~~~'.format(scene))
    #         # scene_graph = self.position_graphs.get(scene).copy()
    #         scene_graph = self.position_graphs.get(scene, probe_id=probe).copy()
    #
    #         clusters, scene_graph = self.get_scene_blocks(scene_graph, allowed_products_filters=allowed_products_filters,
    #                                                       include_empty=include_empty, **filters)
    #
    #         if block_of_blocks:
    #             new_relevant_vertices1 = self.filter_vertices_from_graph(scene_graph, **block_products1)
    #             new_relevant_vertices2 = self.filter_vertices_from_graph(scene_graph, **block_products2)
    #         else:
    #             new_relevant_vertices = self.filter_vertices_from_graph(scene_graph, **filters)
    #         for cluster in clusters:
    #             # print('\n\n')
    #             # sorted_clust = sorted(scene_graph.vs[cluster], key=lambda x: [x['bay_number'], x['shelf_number'],
    #             #                                                               x['shelf_px_left']])
    #             # for i in sorted_clust:
    #             #     print('bay:', i['bay_number'], 'shelf', i['shelf_number'], i['Sub-section'], i['Customer Brand'],
    #             #           i['Sub Brand'], i['Segment'], i['product_name'])
    #             if block_of_blocks:
    #                 relevant_vertices_in_cluster1 = set(cluster).intersection(new_relevant_vertices1)
    #                 if len(new_relevant_vertices1) > 0:
    #                     cluster_ratio1 = len(relevant_vertices_in_cluster1) / float(len(new_relevant_vertices1))
    #                 else:
    #                     cluster_ratio1 = 0
    #                 relevant_vertices_in_cluster2 = set(cluster).intersection(new_relevant_vertices2)
    #                 if len(new_relevant_vertices2) > 0:
    #                     cluster_ratio2 = len(relevant_vertices_in_cluster2) / float(len(new_relevant_vertices2))
    #                 else:
    #                     cluster_ratio2 = 0
    #                 if cluster_ratio1 >= minimum_block_ratio and cluster_ratio2 >= minimum_block_ratio:
    #                     return True
    #             else:
    #                 relevant_vertices_in_cluster = set(cluster).intersection(new_relevant_vertices)
    #                 if len(new_relevant_vertices) > 0:
    #                     cluster_ratio = len(relevant_vertices_in_cluster) / float(len(new_relevant_vertices))
    #                 else:
    #                     cluster_ratio = 0
    #                 cluster_ratios.append(cluster_ratio)
    #                 if biggest_block:
    #                     continue
    #                 if cluster_ratio >= minimum_block_ratio:
    #                     if result_by_scene:
    #                         number_of_blocked_scenes += 1
    #                         break
    #                     else:
    #                         all_vertices = {v.index for v in scene_graph.vs}
    #                         non_cluster_vertices = all_vertices.difference(list(relevant_vertices_in_cluster))
    #                         scene_graph.delete_vertices(non_cluster_vertices)
    #                         if vertical:
    #                             return {'block': True, 'shelves': len(
    #                                 set(scene_graph.vs['shelf_number']))}
    #                         return cluster_ratio, scene_graph
    #         if n_cluster is not None:
    #             copy_of_cluster_ratios = cluster_ratios[:]
    #             largest_cluster = max(copy_of_cluster_ratios)  # 39
    #             copy_of_cluster_ratios.remove(largest_cluster)
    #             if len(copy_of_cluster_ratios) > 0:
    #                 second_largest_integer = max(copy_of_cluster_ratios)
    #             else:
    #                 second_largest_integer = 0
    #             cluster_ratio = largest_cluster + second_largest_integer
    #             if cluster_ratio >= minimum_block_ratio:
    #                 if vertical:
    #                     return {'block': True}
    #
    #         if biggest_block:
    #             max_ratio = max(cluster_ratios)
    #             biggest_cluster = clusters[cluster_ratios.index(max_ratio)]
    #             relevant_vertices_in_cluster = set(biggest_cluster).intersection(new_relevant_vertices)
    #             all_vertices = {v.index for v in scene_graph.vs}
    #             non_cluster_vertices = all_vertices.difference(list(relevant_vertices_in_cluster))
    #             scene_graph.delete_vertices(non_cluster_vertices)
    #             return {'block': True, 'shelf_numbers': set(scene_graph.vs['shelf_number'])}
    #         if result_by_scene:
    #             return number_of_blocked_scenes, len(relevant_scenes)
    #         else:
    #             return False

    def calculate_existence_of_blocks(self, conditions, include_empty=EXCLUDE_EMPTY, min_number_of_blocks=1, **filters):
        """
        :param conditions: A dictionary which contains assortment/availability conditions for filtering the blocks,
                           in the form of: {entity_type: (0 for assortment or 1 for availability,
                                                          a list of values =or None=,
                                                          minimum number of assortment/availability)}.
                           For example: {'product_ean_code': ('44545345434', 3)}
        :param include_empty: This parameter dictates whether or not to discard Empty-typed products.
        :param min_number_of_blocks: The number of blocks needed in order for the KPI to pass.
                                     If all appearances are required: == self.ALL.
        :param filters: These are the parameters which the blocks are checked for.
        :return: The number of blocks (from all scenes) which match the filters and conditions.
        """
        filters, relevant_scenes = self.separate_location_filters_from_product_filters(**filters)
        if len(relevant_scenes) == 0:
            Log.debug('Block Together: No relevant SKUs were found for these filters {}'.format(filters))
            return False

        number_of_blocks = 0
        for scene in relevant_scenes:
            scene_graph = self.position_graphs.get(scene).copy()
            blocks, scene_graph = self.get_scene_blocks(scene_graph, allowed_products_filters=None,
                                                        include_empty=include_empty, **filters)
            for block in blocks:
                entities_data = {entity: [] for entity in conditions.keys()}
                for vertex in block:
                    vertex_attributes = scene_graph.vs[vertex].attributes()
                    for entity in conditions.keys():
                        entities_data[entity].append(vertex_attributes[entity])

                block_successful = True
                for entity in conditions.keys():
                    assortment_or_availability, values, minimum_result = conditions[entity]
                    if assortment_or_availability == 0:
                        if values:
                            result = len(set(entities_data[entity]).intersection(values))
                        else:
                            result = len(set(entities_data[entity]))
                    elif assortment_or_availability == 1:
                        if values:
                            result = len([facing for facing in entities_data if facing in values])
                        else:
                            result = len(entities_data[entity])
                    else:
                        continue
                    if result < minimum_result:
                        block_successful = False
                        break
                if block_successful:
                    number_of_blocks += 1
                    if number_of_blocks >= min_number_of_blocks:
                        return True
                else:
                    if min_number_of_blocks == self.ALL:
                        return False

        if number_of_blocks >= min_number_of_blocks or min_number_of_blocks == self.ALL:
            return True
        return False

    def calculate_flexible_blocks(self, number_of_allowed_others=2, **filters):
        """
        :param number_of_allowed_others: Number of allowed irrelevant facings between two cluster of relevant facings.
        :param filters: The relevant facings of the block.
        :return: This function calculates the number of 'flexible blocks' per scene, meaning, blocks which are allowed
                 to have a given number of irrelevant facings between actual chunks of relevant facings.
        """
        results = {}
        filters, relevant_scenes = self.separate_location_filters_from_product_filters(**filters)
        if len(relevant_scenes) == 0:
            Log.debug('Block Together: No relevant SKUs were found for these filters {}'.format(filters))
            return results
        for scene in relevant_scenes:
            scene_graph = self.position_graphs.get(scene).copy()
            blocks, scene_graph = self.get_scene_blocks(scene_graph, **filters)
            blocks.sort(key=lambda x: len(x), reverse=True)
            blocks = [(0, self.get_block_edges(scene_graph.copy().vs[block])) for block in blocks]
            new_blocks = self.merge_blocks_into_flexible_blocks(filters, scene, number_of_allowed_others, list(blocks))
            while len(blocks) != len(new_blocks):
                blocks = list(new_blocks)
                new_blocks = self.merge_blocks_into_flexible_blocks(filters, scene, number_of_allowed_others, blocks)
            results[scene] = len(new_blocks)
        return results

    def merge_blocks_into_flexible_blocks(self, filters, scene_id, number_of_allowed_others, blocks):
        """
        This function receives blocks' ranges and tries to merge them based on an allowed number of irrelevant facings
        between them. If it manages to merge two blocks, it removes the original blocks and adds the merged block,
        and returns the new list immediately (merges at most one pair of blocks in one run).
        """
        for block1 in blocks:
            previous1, range1 = block1
            for block2 in blocks:
                previous2, range2 = block2

                if block1 != block2:
                    top = min(range1[0], range2[0])
                    right = max(range1[1], range2[1])
                    bottom = max(range1[2], range2[2])
                    left = min(range1[3], range2[3])

                    number_of_others = self.get_number_of_others_in_block_range(filters, scene_id, top, right, bottom, left)
                    previous_others = previous1 + previous2

                    if number_of_others <= number_of_allowed_others + previous_others:
                        blocks.insert(0, (previous_others + number_of_others, (top, right, bottom, left)))
                        blocks.remove(block1)
                        blocks.remove(block2)
                        return blocks
        return blocks

    def get_block_edges(self, *block_graphs):
        """
        This function receives one or more vertex data of a block's graph, and returns the range of its edges -
        The far most top, bottom, left and right pixels of its facings.
        """
        top = right = bottom = left = None
        for graph in block_graphs:
            max_top = min(graph.get_attribute_values(self.position_graphs.TOP))
            max_right = max(graph.get_attribute_values(self.position_graphs.RIGHT))
            max_bottom = max(graph.get_attribute_values(self.position_graphs.BOTTOM))
            max_left = min(graph.get_attribute_values(self.position_graphs.LEFT))
            if top is None or max_top < top:
                top = max_top
            if right is None or max_right > right:
                right = max_right
            if bottom is None or max_bottom > bottom:
                bottom = max_bottom
            if left is None or max_left < left:
                left = max_left
        return top, right, bottom, left

    def get_number_of_others_in_block_range(self, filters, scene_id, top, right, bottom, left):
        """
        This function gets a scene, a range (in pixels) and filters, and checks how many facings are in that range
         and are not part of the filters.
        """
        matches = self.match_product_in_scene[(self.match_product_in_scene['scene_fk'] == scene_id) &
                                              (~self.match_product_in_scene['product_type'].isin(['Empty']))]
        facings_in_range = matches[((matches[self.position_graphs.TOP].between(top, bottom-1)) |
                                    (matches[self.position_graphs.BOTTOM].between(top+1, bottom))) &
                                   ((matches[self.position_graphs.LEFT].between(left, right-1)) |
                                    (matches[self.position_graphs.RIGHT].between(left+1, right)))]
        relevant_facings_in_range = facings_in_range[self.get_filter_condition(facings_in_range, **filters)]
        other_facings_in_range = len(facings_in_range) - len(relevant_facings_in_range)
        return other_facings_in_range

    def get_product_unique_position_on_shelf(self, scene_id, shelf_number, include_empty=False, **filters):
        """
        :param scene_id: The scene ID.
        :param shelf_number: The number of shelf in question (from top).
        :param include_empty: This dictates whether or not to include empties as valid positions.
        :param filters: These are the parameters which the unique position is checked for.
        :return: The position of the first SKU (from the given filters) to appear in the specific shelf.
        """
        shelf_matches = self.match_product_in_scene[(self.match_product_in_scene['scene_fk'] == scene_id) &
                                                    (self.match_product_in_scene['shelf_number'] == shelf_number)]
        if not include_empty:
            filters['product_type'] = ('Empty', self.EXCLUDE_FILTER)
        if filters and shelf_matches[self.get_filter_condition(shelf_matches, **filters)].empty:
            Log.info("Products of '{}' are not tagged in shelf number {}".format(filters, shelf_number))
            return None
        shelf_matches = shelf_matches.sort_values(by=['bay_number', 'facing_sequence_number'])
        shelf_matches = shelf_matches.drop_duplicates(subset=['product_ean_code'])
        positions = []
        for m in xrange(len(shelf_matches)):
            match = shelf_matches.iloc[m]
            match_name = 'Empty' if match['product_type'] == 'Empty' else match['product_ean_code']
            if positions and positions[-1] == match_name:
                continue
            positions.append(match_name)
        return positions

    def get_filter_condition(self, df, **filters):
        """
        :param df: The data frame to be filters.
        :param filters: These are the parameters which the data frame is filtered by.
                       Every parameter would be a tuple of the value and an include/exclude flag.
                       INPUT EXAMPLE (1):   manufacturer_name = ('Diageo', DIAGEOAUGENERALToolBox.INCLUDE_FILTER)
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

    def separate_location_filters_from_product_filters(self, **filters):
        """
        This function gets scene-item-facts filters of all kinds, extracts the relevant scenes by the location filters,
        and returns them along with the product filters only.
        """
        filters_ = filters.copy()
        relevant_scenes = self.scif[self.get_filter_condition(self.scif, **filters_)]['scene_id'].unique()
        location_filters = {}
        for field in filters_.keys():
            if field not in self.all_products.columns and field in self.scif.columns:
                location_filters[field] = filters_.pop(field)
        return filters_, relevant_scenes

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

    @staticmethod
    def rename_filters_to_match_scif(filters, filters_dict):
        keys_to_replace = list(set(filters.keys()) & set(filters_dict.keys()))
        for key in keys_to_replace:
            filters[filters_dict[key]] = filters.pop(key)
        return filters
