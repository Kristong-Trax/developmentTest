
import json
import pandas as pd

from Trax.Algo.Calculations.Core.DataProvider import Data
from Trax.Data.Orm.OrmCore import OrmSession
from Trax.Algo.Calculations.Core.Shortcuts import BaseCalculationsGroup
from Trax.Utils.Logging.Logger import Log

from Projects.DIAGEOKE.Utils.PositionGraph import DIAGEOKEPositionGraphs

__author__ = 'Nimrod'

BUCKET = 'traxuscalc'

KPI_NAME = 'KPI Level 2 Name'


class DIAGEOKEGENERALToolBox:
    """
    MOVED TO Trax.Data.ProfessionalServices.KPIUtils.GeneralToolBox
    """
    EXCLUDE_FILTER = 0
    INCLUDE_FILTER = 1
    EXCLUDE_EMPTY = 0
    INCLUDE_EMPTY = 1

    EMPTY = 'Empty'
    ASSORTMENT = 'assortment'
    AVAILABILITY = 'availability'

    def __init__(self, data_provider, output, kpi_static_data, geometric_kpi_flag=False):
        self.k_engine = BaseCalculationsGroup(data_provider, output)
        self.data_provider = data_provider
        self.project_name = self.data_provider.project_name
        self.session_uid = self.data_provider.session_uid
        self.scif = self.data_provider[Data.SCENE_ITEM_FACTS]
        self.all_products = self.data_provider[Data.ALL_PRODUCTS]
        self.survey_response = self.data_provider[Data.SURVEY_RESPONSES]
        self.kpi_static_data = kpi_static_data
        if geometric_kpi_flag:
            self.position_graphs = DIAGEOKEPositionGraphs(self.data_provider)
            self.matches = self.position_graphs.match_product_in_scene
        else:
            self.position_graphs = None
            self.matches = self.data_provider[Data.MATCHES]

    def check_survey_answer(self, survey_text, target_answer):
        """
        :param survey_text: The name of the survey in the DB.
        :param target_answer: The required answer/s for the KPI to pass.
        :return: True if the answer matches the target; otherwise - False.
        """
        survey_data = self.survey_response.loc[self.survey_response['question_text'] == survey_text]
        answer_field = 'selected_option_text' if not survey_data['selected_option_text'].empty else 'number_value'
        if target_answer in survey_data[answer_field].values.tolist():
            return True
        else:
            return False

    def calculate_number_of_scenes(self, **filters):
        """
        :param filters: These are the parameters which the data frame is filtered by.
        :return: The number of scenes matching the filtered Scene Item Facts data frame.
        """
        filtered_scif = self.scif[self.get_filter_condition(self.scif, **filters)]
        number_of_scenes = len(filtered_scif['scene_id'].unique())
        return number_of_scenes

    def calculate_availability(self, **filters):
        """
        :param filters: These are the parameters which the data frame is filtered by.
        :return: Total number of SKUs facings appeared in the filtered Scene Item Facts data frame.
        """
        filtered_scif = self.scif[self.get_filter_condition(self.scif, **filters)]
        availability = filtered_scif['facings'].sum()
        return availability

    def calculate_assortment(self, **filters):
        """
        :param filters: These are the parameters which the data frame is filtered by.
        :return: Number of unique SKUs appeared in the filtered Scene Item Facts data frame.
        """
        filtered_scif = self.scif[self.get_filter_condition(self.scif, **filters)]
        assortment = len(filtered_scif['product_ean_code'].unique())
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

    def calculate_relative_position(self, tested_filters, anchor_filters, direction_data, min_required_to_pass=1,
                                    **general_filters):
        """
        :param tested_filters: The tested SKUs' filters.
        :param anchor_filters: The anchor SKUs' filters.
        :param direction_data: The allowed distance between the tested and anchor SKUs.
                               In form: {'top': 4, 'bottom: 0, 'left': 100, 'right': 0}
                               Alternative form: {'top': (0, 1), 'bottom': (1, 1000), ...} - As range.
        :param min_required_to_pass: Number of appearances needed to be True for relative position in order for KPI
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
                scene_graph = self.position_graphs.position_graphs.get(scene)
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
        return vertices_indexes

    @staticmethod
    def validate_moves(moves, direction_data):
        """
        This function checks whether the distance between the anchor and the tested SKUs fits the requirements.
        """
        for direction in moves.keys():
            allowed_moves = direction_data[direction]
            min_move, max_move = allowed_moves if isinstance(allowed_moves, tuple) else (0, allowed_moves)
            if not min_move <= moves[direction] <= max_move:
                return False
        return True

    def calculate_block_together(self, allowed_products_filters=None, include_empty=EXCLUDE_EMPTY, **filters):
        """
        :param allowed_products_filters: These are the parameters which are allowed to corrupt the block without failing it.
        :param include_empty: This parameter dictates whether or not to discard Empty-typed products.
        :param filters: These are the parameters which the blocks are checked for.
        :return: True - if in (at least) one of the scenes all the relevant SKUs are grouped together in one block;
                 otherwise - returns False.
        """
        relevant_scenes = self.scif[self.get_filter_condition(self.scif, **filters)]['scene_id'].unique().tolist()
        for field in ['location_type', 'template_name']:
            filters.pop(field, None)
        if relevant_scenes:
            for scene in relevant_scenes:
                if scene not in self.position_graphs.position_graphs.keys():
                    Log.debug('Scene {} has not position graph'.format(scene))
                    continue
                scene_graph = self.position_graphs.position_graphs[scene].copy()
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
                       INPUT EXAMPLE (1):   manufacturer_name = ('Diageo', DIAGEOKEGENERALToolBox.INCLUDE_FILTER)
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

    @staticmethod
    def get_json_data(file_path, skiprows=0):
        """
        This function gets a file's path and extract its content into a JSON.
        """
        output = pd.read_excel(file_path, skiprows=skiprows)
        output = output.to_json(orient='records')
        output = json.loads(output)
        return output

    def add_new_kpi_to_static_tables(self, set_fk, new_kpi_list):
        """
        :param set_fk: The relevant KPI set FK.
        :param new_kpi_list: a list of all new KPI's parameters.
        This function adds new KPIs to the DB ('Static' table) - both to level2 (KPI) and level3 (Atomic KPI).
        """
        session = OrmSession(self.project_name, writable=True)
        with session.begin(subtransactions=True):
            for kpi in new_kpi_list:
                level2_query = """
                               INSERT INTO static.kpi (kpi_set_fk, display_text)
                               VALUES ('{0}', '{1}');""".format(set_fk, kpi.get(KPI_NAME))
                result = session.execute(level2_query)
                kpi_fk = result.lastrowid
                level3_query = """
                               INSERT INTO static.atomic_kpi (kpi_fk, name, description, display_text,
                                                              presentation_order, display)
                               VALUES ('{0}', '{1}', '{2}', '{3}', '{4}', '{5}');""".format(kpi_fk,
                                                                                            kpi.get(KPI_NAME),
                                                                                            kpi.get(KPI_NAME),
                                                                                            kpi.get(KPI_NAME),
                                                                                            1, 'Y')

                session.execute(level3_query)
        session.close()
        return

    def add_kpi_sets_to_static(self, set_names):
        """
        This function is to be ran at a beginning of a projects - and adds the constant KPI sets data to the DB.
        """
        session = OrmSession(self.project_name, writable=True)
        with session.begin(subtransactions=True):
            for set_name in set_names:

                level1_query = """
                               INSERT INTO static.kpi_set (name, missing_kpi_score, enable, normalize_weight,
                                                           expose_to_api, is_in_weekly_report)
                               VALUES ('{0}', '{1}', '{2}', '{3}', '{4}', '{5}');""".format(set_name, 'Bad', 'Y',
                                                                                            'N', 'N', 'N')
                session.execute(level1_query)
        session.close()
        return
