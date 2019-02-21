
import xlrd
import json
import pandas as pd
from datetime import datetime, timedelta

from Trax.Cloud.Services.Connector.Keys import DbUsers
from KPIUtils_v2.DB.PsProjectConnector import PSProjectConnector

from Trax.Algo.Calculations.Core.DataProvider import Data
from Trax.Algo.Calculations.Core.Shortcuts import BaseCalculationsGroup
from Trax.Utils.Logging.Logger import Log
from Projects.BATRU.Utils.Fetcher import BATRUQueries
from openpyxl.utils import column_index_from_string, coordinate_from_string
from Trax.Data.Utils.MySQLservices import get_table_insertion_query as insert

from Projects.BATRU.Utils.PositionGraph import BATRUPositionGraphs

__author__ = 'Nimrod'

STORE_ASSORTMENT_TABLE = 'pservice.custom_osa'
OUTLET_ID = 'Outlet ID'
EAN_CODE = 'product_ean_code'

class BATRUGENERALToolBox:

    STORE_NUMBER = 'Store Number'
    PRODUCT_EAN_CODE = 'Product EAN'

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
    KPI_SETS_WITH_PERCENT_AS_SCORE = []

    def __init__(self, data_provider, output, rds_conn=None, ignore_stacking=False, front_facing=False, **kwargs):
        self.k_engine = BaseCalculationsGroup(data_provider, output)
        if rds_conn is not None:
            self._rds_conn = rds_conn
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
        self.stores = {}
        self.products = {}
        self.all_queries = []
        self.update_queries = []

    @property
    def rds_conn(self):
        if not hasattr(self, '_rds_conn'):
            self._rds_conn = PSProjectConnector(self.project_name, DbUsers.CalculationEng)
        try:
            pd.read_sql_query('select pk from probedata.session limit 1', self._rds_conn.db)
        except:
            self._rds_conn.disconnect_rds()
            self._rds_conn = PSProjectConnector(self.project_name, DbUsers.CalculationEng)
        return self._rds_conn

    @property
    def position_graphs(self):
        if not hasattr(self, '_position_graphs'):
            self._position_graphs = BATRUPositionGraphs(self.data_provider, rds_conn=self.rds_conn)
        return self._position_graphs

    @property
    def match_product_in_scene(self):
        if not hasattr(self, '_match_product_in_scene'):
            self._match_product_in_scene = self.position_graphs.match_product_in_scene
            if self.front_facing:
                self._match_product_in_scene = self._match_product_in_scene[self._match_product_in_scene['front_facing'] == 'Y']
            if self.ignore_stacking:
                self._match_product_in_scene = self._match_product_in_scene[self._match_product_in_scene['stacking_layer'] == 1]
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

    def calculate_posm_availability(self, **filters):
        """
        :param filters: These are the parameters which the data frame is filtered by.
        :return: Total number of SKUs facings appeared in the filtered Scene Item Facts data frame.
        """
        match_display_in_scene = self.get_match_display()
        posm_scif = self.scif.merge(match_display_in_scene, how='left', left_on='scene_id',
                                    right_on='scene_fk')
        if posm_scif.empty:
            Log.debug('scene item fact/ match display in scene is empty')
            return 0
        filtered_df = self.scif[self.get_filter_condition(posm_scif, **filters)]
        availability = len(filtered_df)
        return availability

    def get_posm_availability(self, **filters):
        """
        :param filters: These are the parameters which the data frame is filtered by.
        :return: Total number of SKUs facings appeared in the filtered Scene Item Facts data frame.
        """
        match_display_in_scene = self.get_match_display()
        posm_scif = self.scif.merge(match_display_in_scene, how='left', left_on='scene_id',
                                    right_on='scene_fk')
        if posm_scif.empty:
            Log.debug('scene item fact/ match display in scene is empty')
            return 0
        if filters:
            filtered_df = self.scif[self.get_filter_condition(posm_scif, **filters)]
        else:
            filtered_df = posm_scif
        return filtered_df

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
        if include_empty == self.EXCLUDE_EMPTY and 'product_type' not in sos_filters.keys() + general_filters.keys():
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
            ratio = 0
        else:
            ratio = numerator_width / float(denominator_width)
        return ratio

    def calculate_share_space_length(self, **filters):
        """
        :param filters: These are the parameters which the data frame is filtered by.
        :return: The total shelf width (in mm) the relevant facings occupy.
        """
        filtered_matches = self.match_product_in_scene[self.get_filter_condition(self.match_product_in_scene, **filters)]
        space_length = filtered_matches['width_mm_advance'].sum()
        return space_length

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

    def calculate_product_sequence(self, sequence_filters, direction, empties_allowed=True, irrelevant_allowed=False,
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
                scene_for_filters = filtered_scif[self.get_filter_condition(filtered_scif, **filters)]['scene_id'].unique()
                scenes = scenes.intersection(scene_for_filters)
                if not scenes:
                    Log.debug('None of the scenes include products from all types relevant for sequence')
                    return True

            for scene in scenes:
                scene_graph = self.position_graphs.get(scene)
                scene_passes, scene_rejects = self.calculate_sequence_for_graph(scene_graph, sequence_filters, direction,
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

    def calculate_block_together(self, allowed_products_filters=None, include_empty=EXCLUDE_EMPTY,
                                 minimum_block_ratio=1, result_by_scene=False, **filters):
        """
        :param allowed_products_filters: These are the parameters which are allowed to corrupt the block without failing it.
        :param include_empty: This parameter dictates whether or not to discard Empty-typed products.
        :param minimum_block_ratio: The minimum (block number of facings / total number of relevant facings) ratio
                                    in order for KPI to pass (if ratio=1, then only one block is allowed).
        :param result_by_scene: True - The result is a tuple of (number of passed scenes, total relevant scenes);
                                False - The result is True if at least one scene has a block, False - otherwise.
        :param filters: These are the parameters which the blocks are checked for.
        :return: see 'result_by_scene' above.
        """
        filters, relevant_scenes = self.separate_location_filters_from_product_filters(**filters)
        if len(relevant_scenes) == 0:
            if result_by_scene:
                return 0, 0
            else:
                Log.debug('Block Together: No relevant SKUs were found for these filters {}'.format(filters))
                return True
        number_of_blocked_scenes = 0
        cluster_ratios = []
        for scene in relevant_scenes:
            scene_graph = self.position_graphs.get(scene).copy()

            relevant_vertices = set(self.filter_vertices_from_graph(scene_graph, **filters))
            if allowed_products_filters:
                allowed_vertices = self.filter_vertices_from_graph(scene_graph, **allowed_products_filters)
            else:
                allowed_vertices = set()

            if include_empty == self.EXCLUDE_EMPTY:
                empty_vertices = {v.index for v in scene_graph.vs.select(product_type='Empty')}
                allowed_vertices = set(allowed_vertices).union(empty_vertices)

            all_vertices = {v.index for v in scene_graph.vs}
            vertices_to_remove = all_vertices.difference(relevant_vertices.union(allowed_vertices))
            scene_graph.delete_vertices(vertices_to_remove)
            # removing clusters including 'allowed' SKUs only
            clusters = [cluster for cluster in scene_graph.clusters() if set(cluster).difference(allowed_vertices)]
            new_relevant_vertices = self.filter_vertices_from_graph(scene_graph, **filters)
            for cluster in clusters:
                relevant_vertices_in_cluster = set(cluster).intersection(new_relevant_vertices)
                if len(new_relevant_vertices) > 0:
                    cluster_ratio = len(relevant_vertices_in_cluster) / float(len(new_relevant_vertices))
                else:
                    cluster_ratio = 0
                cluster_ratios.append(cluster_ratio)
                if cluster_ratio >= minimum_block_ratio:
                    if result_by_scene:
                        number_of_blocked_scenes += 1
                        break
                    else:
                        if minimum_block_ratio == 1:
                            return True
                        else:
                            all_vertices = {v.index for v in scene_graph.vs}
                            non_cluster_vertices = all_vertices.difference(cluster)
                            scene_graph.delete_vertices(non_cluster_vertices)
                            return cluster_ratio, scene_graph
        if result_by_scene:
            return number_of_blocked_scenes, len(relevant_scenes)
        else:
            if minimum_block_ratio == 1:
                return False
            elif cluster_ratios:
                return max(cluster_ratios)
            else:
                return None

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
            Log.debug("Products of '{}' are not tagged in shelf number {}".format(filters, shelf_number))
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
                       INPUT EXAMPLE (1):   manufacturer_name = ('Diageo', DIAGEOAUBATRUGENERALToolBox.INCLUDE_FILTER)
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
                Log.debug('field {} is not in the Data Frame'.format(field))

        return filter_condition

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
                Log.debug('Sheet name {} doesn\'t exist'.format(sheet_name))
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

    def get_match_display(self):
        """
        This function extracts the display matches data and saves it into one global data frame.
        The data is taken from probedata.match_display_in_scene.
        """
        query = BATRUQueries.get_match_display(self.session_uid)
        match_display = pd.read_sql_query(query, self.rds_conn.db)
        return match_display

    def p1_assortment_validator(self, file_path):
        """
        This function validates the store assortment template.
        It compares the OUTLET_ID (= store_number_1) and the products ean_code to the stores and products from the DB
        :param file_path: Store assortment template
        :return: False in case of an error and True in case of a valid template
        """
        raw_data = pd.read_csv(file_path, sep='\t')
        raw_data = raw_data.drop_duplicates(subset=raw_data.columns, keep='first')
        raw_data = raw_data.fillna('')
        stores = self.store_data
        valid_stores = stores.loc[stores['store_number'].isin(raw_data[OUTLET_ID])]
        if len(valid_stores) != len(raw_data[OUTLET_ID].unique()):
            print "Those stores don't exist in the DB: {}".format(list(set(raw_data[OUTLET_ID].unique()) -
                                                                         set(valid_stores['store_number'])))
            return False

        valid_product = self.all_products.loc[self.all_products[EAN_CODE].isin(raw_data[EAN_CODE])]
        if len(valid_product) != len(raw_data[EAN_CODE].unique()):
            print "Those products don't exist in the DB: {}".format(list(set(raw_data[EAN_CODE].unique()) -
                                                                           set(valid_product[EAN_CODE])))
            return False

        return True

    def upload_store_assortment_file(self, file_path):
        # raw_data = pd.read_excel(file_path)
        raw_data = pd.read_csv(file_path, sep='\t')
        raw_data = raw_data.drop_duplicates(subset=raw_data.columns, keep='first')
        raw_data = raw_data.fillna('')
        data = []
        for store in raw_data[OUTLET_ID].unique().tolist():
            store_data = {}
            store_products = raw_data.loc[raw_data[OUTLET_ID] == store][EAN_CODE].tolist()
            store_data[store] = store_products
            data.append(store_data)
        for store_data in data:
            self.update_db_from_json(store_data, immediate_change=True)
        queries = self.merge_insert_queries(self.all_queries)
        self.commit_results(queries)
        return data

    def update_db_from_json(self, data, immediate_change=False, discard_missing_products=False):
        products = set()
        missing_products = set()
        store_number = data.keys()[0]
        if store_number is None:
            Log.debug("'{}' is required in data".format(self.STORE_NUMBER))
            return
        store_fk = self.get_store_fk(store_number)
        if store_fk is None:
            Log.debug('Store {} does not exist. Exiting...'.format(store_number))
            return
        for key in data[store_number]:
            validation = False
            if isinstance(key, (float, int)):
                validation = True
            elif isinstance(key, (str, unicode)):
                validation = True
            if validation:
                product_ean_code = str(key).split(',')[-1]
                product_fk = self.get_product_fk(product_ean_code)
                if product_fk is None:
                    Log.debug('Product EAN {} does not exist'.format(product_ean_code))
                    missing_products.add(product_ean_code)
                    continue
                products.add(product_fk)
        if missing_products and not discard_missing_products:
            Log.debug('Some EANs do not exist: {}. Exiting...'.format('; '.join(missing_products)))
            return
        if products:
            current_date = datetime.now().date()
            if immediate_change:
                deactivate_date = current_date - timedelta(1)
                activate_date = current_date
            else:
                deactivate_date = current_date
                activate_date = current_date + timedelta(1)
            queries = []
            current_skus = self.current_top_skus[self.current_top_skus['store_fk'] == store_fk]['product_fk'].tolist()
            products_to_deactivate = set(current_skus).difference(products)
            products_to_activate = set(products).difference(current_skus)
            # for product_fk in products_to_deactivate:
            if products_to_deactivate:
                queries.append(self.get_deactivation_query(store_fk, tuple(products_to_deactivate), deactivate_date))
            for product_fk in products_to_activate:
                queries.append(self.get_activation_query(store_fk, product_fk, activate_date))
            self.all_queries.extend(queries)
            Log.debug('{} - Out of {} products, {} products were deactivated and {} products were activated'.format(
                store_number, len(products), len(products_to_deactivate), len(products_to_activate)))
        else:
            Log.debug('{} - No products are configured as Top SKUs'.format(store_number))

    @property
    def current_top_skus(self):
        if not hasattr(self, '_current_top_skus'):
            self._current_top_skus = self.get_current_top_skus()
        return self._current_top_skus

    def get_current_top_skus(self):
        query = """select store_fk, product_fk
                   from pservice.custom_osa
                   where end_date is null"""
        data = pd.read_sql_query(query, self.rds_conn.db)
        return data

    @property
    def store_data(self):
        if not hasattr(self, '_store_data'):
            query = "select pk as store_fk, store_number_1 as store_number from static.stores"
            self._store_data = pd.read_sql_query(query, self.rds_conn.db)
        return self._store_data

    @property
    def product_data(self):
        if not hasattr(self, '_product_data'):
            query = "select pk as product_fk, product_ean_code from static.product " \
                    "where delete_date is null"
            self._product_data = pd.read_sql_query(query, self.rds_conn.db)
        return self._product_data

    def get_store_fk(self, store_number):
        store_number = str(store_number)
        if store_number in self.stores:
            store_fk = self.stores[store_number]
        else:
            store_fk = self.store_data[self.store_data['store_number'] == store_number]
            if not store_fk.empty:
                store_fk = store_fk['store_fk'].values[0]
                self.stores[store_number] = store_fk
            else:
                store_fk = None
        return store_fk

    def get_product_fk(self, product_ean_code):
        product_ean_code = str(product_ean_code).strip()
        if product_ean_code in self.products:
            product_fk = self.products[product_ean_code]
        else:
            product_fk = self.product_data[self.product_data['product_ean_code'] == product_ean_code]
            if not product_fk.empty:
                product_fk = product_fk['product_fk'].values[0]
                self.products[product_ean_code] = product_fk
            else:
                product_fk = None
        return product_fk

    @staticmethod
    def get_deactivation_query(store_fk, product_fk, date):
        query = """update {} set end_date = '{}', is_current = '0'
                   where store_fk = {} and product_fk in {} and end_date is null""".format(STORE_ASSORTMENT_TABLE, date,
                                                                                           store_fk, product_fk)
        return query

    @staticmethod
    def get_activation_query(store_fk, product_fk, date):
        attributes = pd.DataFrame([(store_fk, product_fk, str(date), 1)],
                                  columns=['store_fk', 'product_fk', 'start_date', 'is_current'])
        query = insert(attributes.to_dict(), STORE_ASSORTMENT_TABLE)
        return query

    def merge_insert_queries(self, insert_queries):
        query_groups = {}
        for query in insert_queries:
            if 'update' in query:
                self.update_queries.append(query)
                continue
            static_data, inserted_data = query.split('VALUES ')
            if static_data not in query_groups:
                query_groups[static_data] = []
            query_groups[static_data].append(inserted_data)
        merged_queries = []
        for group in query_groups:
            for group_index in xrange(0, len(query_groups[group]), 10**4):
                merged_queries.append('{0} VALUES {1}'.format(group, ',\n'.join(query_groups[group]
                                                                                [group_index:group_index+10**4])))
        return merged_queries

    def commit_results(self, queries):
        self.rds_conn.disconnect_rds()
        rds_conn = PSProjectConnector('batru', DbUsers.CalculationEng)
        cur = rds_conn.db.cursor()
        for query in self.update_queries:
            try:
                cur.execute(query)
                print query
            except Exception as e:
                Log.debug('Inserting to DB failed due to: {}'.format(e))
                rds_conn.disconnect_rds()
                rds_conn = PSProjectConnector('batru', DbUsers.CalculationEng)
                cur = rds_conn.db.cursor()
                continue
        rds_conn.db.commit()
        rds_conn.disconnect_rds()
        rds_conn = PSProjectConnector('batru', DbUsers.CalculationEng)
        cur = rds_conn.db.cursor()
        for query in queries:
            try:
                cur.execute(query)
                print query
            except Exception as e:
                Log.debug('Inserting to DB failed due to: {}'.format(e))
                rds_conn.disconnect_rds()
                rds_conn = PSProjectConnector('batru', DbUsers.CalculationEng)
                cur = rds_conn.db.cursor()
                continue
        rds_conn.db.commit()

    def get_store_assortment_for_store(self, store_fk):
        query = """
                select ts.product_fk, p.product_ean_code
                from {} ts
                join static.product p on p.pk = ts.product_fk
                where ts.store_fk = {} and ts.end_date is null
                """.format(STORE_ASSORTMENT_TABLE, store_fk)
        data = pd.read_sql_query(query, self.rds_conn.db)
        return data.groupby('product_fk')['product_ean_code'].first().to_dict()