from Trax.Algo.Calculations.Core.KPI.UnifiedKPICalculation import UnifiedCalculationsScript
from Projects.DIAGEORU.KPIs.util import DiageoUtil
from KPIUtils_v2.Utils.Consts.DataProvider import ScifConsts, ProductsConsts
from KPIUtils.GlobalProjects.DIAGEO.Utils.Consts import DiageoKpiNames
from Projects.DIAGEORU.KPIs.util import DiageoConsts
from KPIUtils.GlobalProjects.DIAGEO.Utils.PositionGraph import PositionGraphs


class RelativePostionSKU(UnifiedCalculationsScript):

    def __init__(self, data_provider, config_params=None, **kwargs):
        super(RelativePostionSKU, self).__init__(data_provider, config_params=config_params, **kwargs)
        self.util = DiageoUtil(data_provider)

    def kpi_type(self):

        return DiageoKpiNames.RELATIVE_POSITION

    @staticmethod
    def _get_direction_for_relative_position(value):
        """
        This function converts direction data from the template (as string) to a number.
        """
        if value == DiageoConsts.UNLIMITED_DISTANCE:
            value = 1000
        elif not value or not str(value).isdigit():
            value = 0
        else:
            value = int(value)
        return value

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
        if self.util.front_facing:
            front_facing_vertices = [v.index for v in graph.vs.select(front_facing='Y')]
            vertices_indexes = set(vertices_indexes).intersection(front_facing_vertices)
        return list(vertices_indexes)


    def get_filter_condition(self, df, **filters):
        """
        :param df: The data frame to be filters.
        :param filters: These are the parameters which the data frame is filtered by.
                       Every parameter would be a tuple of the value and an include/exclude flag.
                       INPUT EXAMPLE (1):   manufacturer_name = ('Diageo', DIAGEOAUSANOFIAUGENERALToolBox.INCLUDE_FILTER)
                       INPUT EXAMPLE (2):   manufacturer_name = 'Diageo'
        :return: a filtered Scene Item Facts data frame.
        """
        if not filters:
            return df['pk'].apply(bool)
        if 'facings' in df.keys():
            filter_condition = (df['facings'] > 0)
        else:
            filter_condition = None
        for field in filters.keys():
            if field in df.keys():
                if isinstance(filters[field], tuple):
                    value, exclude_or_include = filters[field]
                else:
                    value, exclude_or_include = filters[field], DiageoConsts.INCLUDE_FILTER
                if not value:
                    continue
                if not isinstance(value, list):
                    value = [value]
                if exclude_or_include == DiageoConsts.INCLUDE_FILTER:
                    condition = (df[field].isin(value))
                elif exclude_or_include == DiageoConsts.EXCLUDE_FILTER:
                    condition = (~df[field].isin(value))
                elif exclude_or_include == DiageoConsts.CONTAIN_FILTER:
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
                self.util.Log.warning('field {} is not in the Data Frame'.format(field))

        return filter_condition

    @property
    def position_graphs(self):
        if not hasattr(self, '_position_graphs'):
            self._position_graphs = PositionGraphs(self.data_provider, rds_conn=self.util.rds_conn)
        return self._position_graphs

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
        filtered_scif = self.util.scif[self.get_filter_condition(self.util.scif, **general_filters)]
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
                            self.util.Log.debug('Tested and Anchor have no direct path')
            if pass_counter > 0 and reject_counter == 0:
                return True
            else:
                return False
        else:
            self.util.Log.debug('None of the scenes contain both anchor and tested SKUs')
            return False

    def get_relative_position_kpi_result(self, scif_tested_param, params, scif_anchor_param,
                                         location_type='template_name'):
        """
        this method does the actual calculation after extracting al necessary parameters
        :param location_type.
        :param scif_tested_param: type of tested param
        :param params: user template in a spcecific position
        :param scif_anchor_param: type of anchor param
        :return: result of this kpi
        """
        if scif_tested_param == ProductsConsts.PRODUCT_EAN_CODE:
            tested_filters = {scif_tested_param: params.get(DiageoConsts.TESTED)}
        else:
            tested_filters = {scif_tested_param: params.get(DiageoConsts.TESTED_PRODUCT_NAME)}
        if scif_anchor_param == ProductsConsts.PRODUCT_EAN_CODE:
            anchor_filters = {scif_anchor_param: params.get(DiageoConsts.ANCHOR)}
        else:
            anchor_filters = {scif_anchor_param: params.get(DiageoConsts.ANCHOR_PRODUCT_NAME)}
        direction_data = {'top': self._get_direction_for_relative_position(params.get(DiageoConsts.TOP_DISTANCE)),
                          'bottom': self._get_direction_for_relative_position(
                              params.get(DiageoConsts.BOTTOM_DISTANCE)),
                          'left': self._get_direction_for_relative_position(
                              params.get(DiageoConsts.LEFT_DISTANCE)),
                          'right': self._get_direction_for_relative_position(
                              params.get(DiageoConsts.RIGHT_DISTANCE))}
        if params.get(DiageoConsts.LOCATION, ''):
            general_filters = {location_type: params.get(DiageoConsts.LOCATION)}
        else:
            general_filters = {}
        result = self.calculate_relative_position(tested_filters, anchor_filters, direction_data,
                                                        **general_filters)
        return result

    def calculate(self):
        """
        this class calculates all the menu KPI's depending on the parameter given
        """
        # add template data to DB
        # filter the data accordingly
        # return results from network X functions
        res_list = []
        template_data = self.util.relative_position_template
        depend = self._config_params
        kpi_type = ''
        location_type = 'template_name'
        if depend:
            if 'type' in depend.keys():
                kpi_type = depend['type']
        for params in template_data:
            if (self.util.store_channel == params.get(DiageoConsts.CHANNEL, '').upper()) or \
                    (self.util.store_info.at[0, ScifConsts.ADDITIONAL_ATTRIBUTE_2] ==
                     params.get(ScifConsts.ADDITIONAL_ATTRIBUTE_2, 'Empty')):
                scif_anchor_param, scif_tested_param = self.util._get_params_types(params)
                kpi_name = self.util._get_relative_position_kpi_name(scif_tested_param, scif_anchor_param)
                if kpi_name == kpi_type:
                    result = self.get_relative_position_kpi_result(scif_tested_param, params, scif_anchor_param,
                                                                   location_type)
                    result = 1 if result else 0
                    tested_id, anchor_id = self.util._get_entities_ids(scif_anchor_param, scif_tested_param, params)
                    # if 'Atomic' in params.keys():
                    #     old_tables_kpi_name = params['Atomic']
                    #     self.util._write_results_to_old_tables(old_tables_kpi_name, result, 2)
                    template_fk = self.util._get_relevant_template_fk(params)
                    res_list.append(
                        self.util.build_dictionary_for_db_insert_v2(kpi_name=kpi_name, result=result,
                                                                    score=result,
                                                                    numerator_id=tested_id,
                                                                    denominator_id=anchor_id,
                                                                    context_id=template_fk,
                                                                    identifier_parent=DiageoConsts.RELATIVE_POSITION))
        for res in res_list:
            self.write_to_db_result(**res)
