import itertools
from copy import deepcopy
from functools import partial

import networkx as nx
import pandas as pd
from shapely import affinity
from shapely.geometry import box, MultiPolygon, LineString, Polygon
from shapely.strtree import STRtree

from Constants import Direction
from Trax.Algo.Geometry.Masking.MaskingResultsIO import retrieve_maskings
from Trax.Algo.Geometry.Masking.Utils import transform_maskings
from Trax.Algo.Calculations.Core.GraphicalModel2.Utils.PolygonUtils import add_neighbours_degree
from Trax.Data.Orm.OrmCore import OrmSession
from Trax.Utils.Logging.Logger import Log


class MaskingBasedAdjacency(object):

    def __init__(self, mpis, masking_information, **kwargs):
        self.mpis_with_masks = mpis.merge(masking_information, on='probe_match_fk')

        self._compute_maximal_castings()
        if kwargs.get('max_width'):
            self.max_width = kwargs.get('max_width')
        if kwargs.get('max_height'):
            self.max_height = kwargs.get('max_height')
        if kwargs.get('minify_scene_width'):
            self._minify_scene_width()

        self._init_polygon_meta_information()
        self._init_cache()
        self._init_direction_params()
        self.complex_stacking_maximal_overlap_height = 0.25
        self.complex_stacking_horizontal_overlap_ratio = 0.8
        self.minimal_overlap_ratio = 0.1
        # Pairs of stacking neighbour match ids (up down) in complex stacking positions
        self._complex_stacking_pairs = []

    def _minify_scene_width(self):
        right_side = self.mpis_with_masks.groupby(
            ["bay_number"])["x_right"].max().to_frame('bay_right')
        left_side = self.mpis_with_masks.groupby(["bay_number"])["x_top"].min().to_frame('bay_left')
        bay_width_df = left_side.join(right_side)
        bay_width_df['width'] = bay_width_df['bay_right'] - bay_width_df['bay_left']
        bay_offset_dict = {}
        for i in range(1, bay_width_df.index.max()):
            bay_distance = bay_width_df.loc[i + 1, 'bay_left'] - bay_width_df.loc[i, 'bay_right']
            if bay_distance > self.max_width:
                bay_offset_dict[i] = bay_width_df.loc[bay_width_df.index.max(), 'bay_right'] - bay_width_df.loc[
                    i, 'bay_right'] - \
                    (bay_width_df.loc[bay_width_df.index > i, 'width'].sum()
                     + 10 * (bay_width_df.index.max() - i))  # 10 pixel buffer between bays
        for col in ["x_top", "x_right"]:
            self.mpis_with_masks[col] = self.mpis_with_masks.apply(lambda x: x[col] + bay_offset_dict.get(
                x['bay_number'], 0), axis=1)

    def _compute_maximal_castings(self):
        # compute the maximal distance between two adjacent shelves
        shelf_y = self.mpis_with_masks.groupby(["bay_number", "shelf_number"])[
            "y_right"].max().reset_index()
        cross_shelfs = shelf_y.merge(shelf_y, on='bay_number')
        y_positions = cross_shelfs[(cross_shelfs["shelf_number_x"] -
                                    cross_shelfs["shelf_number_y"]).abs() == 1]
        max_shelf_distance = (y_positions["y_right_x"] - y_positions["y_right_y"]).abs().max()
        self.max_height = (self.mpis_with_masks["y_top"] - self.mpis_with_masks[
            "y_right"]).abs().max() + max_shelf_distance
        # Only one shelf exist in one of the bays - take max product height
        if (self.mpis_with_masks.groupby('bay_number')['shelf_number'].nunique().min() == 1) or y_positions.empty:
            self.max_height = (self.mpis_with_masks["y_top"] -
                               self.mpis_with_masks["y_right"]).abs().max()
        self.max_width = (self.mpis_with_masks["x_top"] -
                          self.mpis_with_masks["x_right"]).abs().max()

    def _init_cache(self):
        self.casting_dict = {}

    def _init_polygon_meta_information(self):
        polygons_meta_data = dict()
        match_fk_to_polygon = dict()
        polygon_shelf_coordinates = []
        for ind, row in self.mpis_with_masks.iterrows():
            poly = box(row["x_top"], row["y_top"], row["x_right"], row["y_right"])
            polygon_shelf_coordinates.append([poly, row['pk'], row['shelf_number'], row['bay_number'],
                                              row['facing_sequence_number'], row['stacking_layer']])
            match_fk_to_polygon[row["pk"]] = poly
            polygons_meta_data[poly.wkb] = row

        self.match_fk_to_polygon = match_fk_to_polygon
        self.polygons_meta_data = polygons_meta_data
        self.polygon_df = (pd.DataFrame(polygon_shelf_coordinates, columns=[
            'polygon', 'match_id', 'shelf_number', 'bay_number', 'facing_sequence_number', 'stacking_layer']).
            assign(is_last_facing_sequence_number=lambda df1: df1["facing_sequence_number"] ==
                   df1.groupby(['bay_number', 'shelf_number'])["facing_sequence_number"].transform(
                "max")).  # Add indicator for the last facing number in each shelf
            assign(is_top_stacking=lambda df2: df2["stacking_layer"] ==
                   df2.groupby(['bay_number', 'shelf_number',
                                'facing_sequence_number'])["stacking_layer"].
                   transform("max")).  # Add indicator for the last stacking layer in each shelf and sequence num
            set_index(['bay_number', 'shelf_number', 'facing_sequence_number', 'stacking_layer']).
            sort_index())

    def _init_direction_params(self):
        def candidate_order(pol, coord, sign):
            return sign * getattr(pol.centroid, coord)

        self.direction_params = {Direction.UP:
                                 {'yoff': -self.max_height,
                                  'xoff': 0,
                                  'order_func': partial(candidate_order, coord='y', sign=-1)},
                                 Direction.DOWN:
                                     {'yoff': self.max_height,
                                      'xoff': 0,
                                      'order_func': partial(candidate_order, coord='y', sign=1)},
                                 Direction.LEFT:
                                     {'yoff': 0,
                                      'xoff': -self.max_width,
                                      'order_func': partial(candidate_order, coord='x', sign=-1)},
                                 Direction.RIGHT:
                                     {'yoff': 0,
                                      'xoff': self.max_width,
                                      'order_func': partial(candidate_order, coord='x', sign=1)}
                                 }

    def get_left_right_candidates(self, bay_offset, polygon_meta):
        # Check if the polygon is not last or first in shelf and return its neighbouring sequence number
        if not ((bay_offset == -1 and polygon_meta['facing_sequence_number'] == 1) or
                (polygon_meta['is_last_facing_sequence_number'] and bay_offset == 1)):
            return self.polygon_df.loc[(polygon_meta['bay_number'], polygon_meta['shelf_number'],
                                        polygon_meta['facing_sequence_number'] + bay_offset)] \
                .polygon.tolist()
        else:
            # if product last on shelf, return neighboring bay candidates (only 1st or last sequences depending if its
            # left or right)
            if polygon_meta['bay_number'] + bay_offset in self.polygon_df.index.get_level_values(0):
                next_bay_products = self.polygon_df.loc[polygon_meta['bay_number'] + bay_offset]
                if bay_offset == 1:
                    return next_bay_products.loc[pd.IndexSlice[:, 1, :], :].polygon.tolist()
                else:
                    return next_bay_products[next_bay_products['is_last_facing_sequence_number']].polygon.tolist()
        return []

    def check_complex_stacking(self, polygon_meta, shelf_offset):
        """
        Handle this edge case:

        ---------------------------------
        -    Stacked                    -
        ---------------------------------
        --------- -----------------------
        - neig -- --not                 -
        - hbour-- --considered neighbour-
        --------- -----------------------
        :param polygon_meta: the stacked polygon data
        :param int shelf_offset: the shelf offset (should be 1 because only down cases are checked)
        :return:
        """

        def same_shelf_stacking_condition(p, horizontal_overlap_ratio, maximal_overlap_height):
            polygon = polygon_meta['polygon']
            polygon_height = polygon.bounds[3] - polygon.bounds[1]
            polygon_width_line = LineString([[polygon.bounds[0], 0], [polygon.bounds[2], 0]])
            candidate_width_line = LineString([[p.bounds[0], 0], [p.bounds[2], 0]])
            x_axis_intersection = polygon_width_line.intersection(candidate_width_line)
            intersection_area = p.intersection(polygon)
            if intersection_area.bounds:
                intersection_height = intersection_area.bounds[3] - intersection_area.bounds[1]
            else:
                intersection_height = 0
            if x_axis_intersection.bounds:
                intersection_width = x_axis_intersection.bounds[2] - x_axis_intersection.bounds[0]
            else:
                intersection_width = 0
            is_high_or_low_enough = intersection_height < maximal_overlap_height * polygon_height
            if not is_high_or_low_enough:
                return False
            # Check if polygon is covering at least <horizontal_overlap_ratio>
            # of candidate to check that it really is on top of the product
            is_covering_candidate = intersection_width > horizontal_overlap_ratio * \
                (p.bounds[2] - p.bounds[0])
            if not is_covering_candidate:
                return False
            return ((p.bounds[1] > polygon.centroid.y and shelf_offset == 1) or
                    (p.bounds[3] < polygon.centroid.y and shelf_offset == -1))

        sequence_numbers_to_check = [
            polygon_meta['facing_sequence_number'] + i for i in [-2, -1, 1, 2]]
        adjacent_products_in_same_bay_shelf = self.polygon_df.loc[pd.IndexSlice[
                                                                  polygon_meta['bay_number'],
                                                                  polygon_meta['shelf_number'],
                                                                  sequence_numbers_to_check, :]]
        return adjacent_products_in_same_bay_shelf.loc[
            adjacent_products_in_same_bay_shelf.polygon.apply(
                lambda x: same_shelf_stacking_condition(x, self.complex_stacking_horizontal_overlap_ratio,
                                                        self.complex_stacking_maximal_overlap_height))].polygon.tolist()

    def get_up_down_candidates(self, shelf_offset, polygon_meta):
        products_in_same_bay = self.polygon_df.loc[polygon_meta['bay_number']]
        # Check if higher / lower stacking products exist on the same sequence number
        if not ((shelf_offset == 1 and polygon_meta['stacking_layer'] == 1) or
                (polygon_meta['is_top_stacking'] and shelf_offset == -1)):
            same_shelf_sequence_skus = products_in_same_bay.loc[(polygon_meta['shelf_number'],
                                                                 polygon_meta['facing_sequence_number'])]
            # Get next stacking (and protect the case of no sequential stacking numbers)
            stacking_ind = (same_shelf_sequence_skus.index > polygon_meta['stacking_layer'] if shelf_offset == -1
                            else same_shelf_sequence_skus.index < polygon_meta['stacking_layer'])
            stacking_qry_ind = (same_shelf_sequence_skus.index[stacking_ind].min() if shelf_offset == -1 else
                                same_shelf_sequence_skus.index[stacking_ind].max())
            # Take minimal stacking layer that is above or below the current polygon stacking
            res = same_shelf_sequence_skus.loc[stacking_qry_ind].polygon
            if isinstance(res, Polygon):
                return [res]
            return res.tolist()

        else:
            # get all candidates from higher / lower shelf (only first or last stacking depending if its up or down)
            if polygon_meta['shelf_number'] + shelf_offset in products_in_same_bay.index.get_level_values(0):
                next_shelf_products = products_in_same_bay.loc[(
                    polygon_meta['shelf_number'] + shelf_offset)]
                if shelf_offset == -1:
                    return next_shelf_products.loc[pd.IndexSlice[:, 1], :].polygon.tolist()
                else:
                    return next_shelf_products[next_shelf_products['is_top_stacking']].polygon.tolist()
        return []

    @staticmethod
    def filter_candidates_using_tree(candidates, polygon_with_shadow, polygon):
        return [p for p in STRtree(candidates).query(polygon_with_shadow) if
                (p.intersection(polygon_with_shadow).area > 0) and not
                p.equals(polygon)]

    def get_candidates(self, polygon_row, direction):
        complex_stacking_polygons = []
        xoff, yoff, order_func = self.direction_params[direction]['xoff'], self.direction_params[direction]['yoff'], \
            self.direction_params[direction]['order_func']
        candidates_by_coordinates = []
        polygon_with_shadow = self.cast_shadow(polygon_row.polygon, xoff, yoff)
        if direction == Direction.UP:
            candidates_by_coordinates = self.get_up_down_candidates(-1, polygon_row)
        if direction == Direction.DOWN:
            candidates_by_coordinates = self.get_up_down_candidates(1, polygon_row)
            if polygon_row['stacking_layer'] > 1:
                # (polygon_row['match_id'], self.polygons_meta_data[p.wkb].pk)
                complex_stacking_polygons = [p for p in
                                             self.filter_candidates_using_tree(
                                                 self.check_complex_stacking(polygon_row, 1),
                                                 polygon_with_shadow, polygon_row.polygon)
                                             if p not in candidates_by_coordinates]
                candidates_by_coordinates += complex_stacking_polygons

        if direction == Direction.RIGHT:
            candidates_by_coordinates = self.get_left_right_candidates(1, polygon_row)
        if direction == Direction.LEFT:
            candidates_by_coordinates = self.get_left_right_candidates(-1, polygon_row)
        candidates = self.filter_candidates_using_tree(candidates_by_coordinates, polygon_with_shadow,
                                                       polygon_row.polygon)
        original_candidates = deepcopy(candidates)
        candidates = sorted(candidates, key=order_func)
        candidates_with_shadow = [self.cast_shadow(c, xoff, yoff) for c in candidates]

        return original_candidates, candidates, candidates_with_shadow, polygon_with_shadow, complex_stacking_polygons

    @staticmethod
    def get_side(pol, direction):
        bounds = pol.bounds
        if direction in [Direction.LEFT, Direction.RIGHT]:
            return abs(bounds[1] - bounds[3])
        return abs(bounds[2] - bounds[0])

    def cast_shadow(self, p, xoff, yoff):
        key = (p.wkb, xoff, yoff)
        if key in self.casting_dict:
            return self.casting_dict[key]

        ans = p.union(affinity.translate(p, xoff=xoff, yoff=yoff)).envelope
        self.casting_dict[key] = ans
        return ans

    def get_neighbours(self, polygon_row, direction, debug=False):

        if not self.direction_params.get(direction):
            raise Exception('Must use a valid direction')

        original_candidates, candidates, candidates_with_shadow, polygon_with_shadow, complex_stacking_polygons = \
            self.get_candidates(polygon_row, direction)

        if len(candidates) == 0:
            if debug:
                return [], original_candidates, polygon_with_shadow
            return []

        neighbours = []
        for idx, candidate_shadow in enumerate(candidates_with_shadow):
            if polygon_with_shadow.intersects(candidate_shadow):
                potential_overlap = min(MaskingBasedAdjacency.get_side(polygon_with_shadow, direction),
                                        MaskingBasedAdjacency.get_side(candidate_shadow, direction))
                intersect = polygon_with_shadow.intersection(candidate_shadow)
                actual_overlap = MaskingBasedAdjacency.get_side(intersect, direction)
                if actual_overlap > self.minimal_overlap_ratio * potential_overlap:
                    polygon_with_shadow = polygon_with_shadow.difference(candidate_shadow)
                    neighbours.append(candidates[idx])
                    if candidates[idx] in complex_stacking_polygons:
                        self._complex_stacking_pairs.append(
                            (polygon_row['match_id'], self.polygons_meta_data[candidates[idx].wkb].pk))
        if debug:
            return neighbours, original_candidates, polygon_with_shadow
        return neighbours

    def compute_neighbours(self):
        edges = []
        self._init_cache()
        for ind, polygon_row in self.polygon_df.reset_index().iterrows():
            polygon_pk = polygon_row['match_id']
            for direction in [x for x in Direction._member_map_.values()]:
                try:
                    neighbours = self.get_neighbours(polygon_row, direction)
                    edges += [(polygon_pk, self.polygons_meta_data[pol.wkb].pk, {"direction": direction.name}) for pol in
                              neighbours]
                except Exception as e:
                    pass
                    # Log.warning('{} scene_match_fk has no neighbor in direction {}'.format(polygon_pk, direction.name))
        for pair in self._complex_stacking_pairs:
            edges += [(pair[1], pair[0], {"direction": Direction.UP.name})]

        return edges


class NodeAttribute(object):
    def __init__(self, values):
        if type(values) is list:
            self.stored_values = set(values)
        elif type(values) is set:
            self.stored_values = values
        else:
            self.stored_values = {values}

    @property
    def values(self):
        return self.stored_values

    @property
    def value(self):
        lst = list(self.values)
        if len(lst) > 1:
            raise Exception("attribute has more than one unique value")
        return lst[0]

    def is_unique(self):
        lst = list(self.values)
        return len(lst) == 1

    def __contains__(self, key):
        return key in self.values

    def __iter__(self):
        return iter(self.values)

    def __str__(self):
        return str(self.values)

    def __repr__(self):
        return str(self.values)


class AdjacencyGraphBuilder(object):

    @staticmethod
    def initiate_graph_by_ref(project, scene_id, **kwargs):
        matches_df = AdjacencyGraphBuilder._load_mpis(project, scene_id)

        if matches_df.drop_duplicates('scene_fk').shape[0] > 1:
            raise NotImplementedError(
                'Match product in scene data cannot contain data from more than one scene')

        matches_df['product_fk'] = matches_df['product_fk'].astype(int)

        masking_df = AdjacencyGraphBuilder._load_maskings(project, matches_df.scene_fk.iloc[0])

        _base_adjacency_graph = AdjacencyGraphBuilder.build_base_adjacency_graph(
            matches_df, masking_df, **kwargs)
        return _base_adjacency_graph

    @staticmethod
    def initiate_graph_by_dataframe(matches_df, masking_df, additional_attributes=None, **kwargs):
        scene_matches_df = matches_df.copy()
        scene_masking_df = masking_df.copy()

        # Filter negative stacking --> POSM tags
        scene_matches_df = scene_matches_df[scene_matches_df['stacking_layer'] > 0]

        if scene_matches_df.drop_duplicates('scene_fk').shape[0] > 1:
            raise NotImplementedError(
                'Match product in scene data cannot contain data from more than one scene')

        mandatory_attributes = {"product_fk", "pk", "shelf_number", "bay_number"}
        found_from_mandatory = mandatory_attributes & set(scene_matches_df.columns)
        if len(found_from_mandatory) != len(mandatory_attributes):
            raise Exception("Missing mandatory attributes {}".format(found_from_mandatory))

        scene_matches_df['product_fk'] = scene_matches_df['product_fk'].astype(int)

        if additional_attributes:
            # verify all attributes are present in matches_df
            attributes_not_in_matches_df = set(
                additional_attributes) - set(scene_matches_df.columns)
            if len(attributes_not_in_matches_df) > 0:
                raise IndexError("The following attributes are missing from the matches dataframe {}".format(
                    attributes_not_in_matches_df))

            additional_attributes = additional_attributes
        else:
            additional_attributes = []

        _base_adjacency_graph = AdjacencyGraphBuilder.build_base_adjacency_graph(scene_matches_df, scene_masking_df,
                                                                                 additional_attributes, **kwargs)
        return _base_adjacency_graph

    @staticmethod
    def _load_mpis(project_name, scene_id, mpis_query=None):
        session = OrmSession(project_name)
        if not mpis_query:
            mpis_query = """
            select
                mpis.*,
                p.name as product_name,
                b.name as brand_name,
                m.name as manufacturer_name
            from 
                probedata.match_product_in_scene mpis
            inner join
                static_new.product p on p.pk = mpis.product_fk
            inner join
                static_new.brand b on b.pk = p.brand_fk
            inner join
                static_new.manufacturer m on m.pk = b.manufacturer_fk
            where mpis.scene_fk = {}  and stacking_layer > 0
            """
        mpis = pd.read_sql(mpis_query.format(scene_id), session.bind)
        return mpis

    @staticmethod
    def _load_maskings(project_name, scene_id):
        return transform_maskings(retrieve_maskings(project_name, [scene_id]))

    @staticmethod
    def build_base_adjacency_graph(matches_df, masking_df, additional_attributes=None, **kwargs):
        """
        Building a graph of a specific product where there edges define adjacency
        :return: product adjacency graph
        """

        if not additional_attributes:
            additional_attributes = []

        mba = MaskingBasedAdjacency(matches_df, masking_df, **kwargs)
        edges = mba.compute_neighbours()

        # Initializing the graph
        g = nx.DiGraph()

        # Adding all the nodes
        for idx, row in matches_df.iterrows():
            attr_dict = {'product_fk': NodeAttribute(row["product_fk"]),
                         'polygon': mba.match_fk_to_polygon[row['pk']],
                         'facings': 1,
                         'shelf_number': NodeAttribute(row["shelf_number"]),
                         'bay_number': NodeAttribute(row["bay_number"]),
                         'match_fk': NodeAttribute(row["pk"])}
            for _att in additional_attributes:
                attr_dict.update({_att: NodeAttribute(row.get(_att))})
            g.add_node(row["pk"], **attr_dict)

        g.add_edges_from(edges)
        add_neighbours_degree(g)
        return g

    @staticmethod
    def _chain_attribute(attribute_name, node_list):
        return NodeAttribute(list(itertools.chain(*[n[attribute_name].values for n in node_list])))

    @staticmethod
    def _validate_attribute_is_unique(graph, attribute_name):
        for _, node in graph.nodes(data=True):
            if not node[attribute_name].is_unique():
                raise Exception('used level {} has cases which is not unique in graph (e.g. {})'.format(attribute_name,
                                                                                                        node[
                                                                                                            attribute_name]))

    @staticmethod
    def get_merged_node_attributes_from_nodes(selected_nodes, graph):
        """
        Calculating a single merged node attributes from a connected component from the base graph.
        :param selected_nodes: connected graph of a specific product.
        :param graph: the base graph
        :return: a condensed node descriptor.
        """

        filtered_nodes = [n for i, n in graph.nodes(data=True) if i in selected_nodes]

        attributes_list = [attr for attr, value in graph.node[selected_nodes[0]
                                                              ].iteritems() if isinstance(value, NodeAttribute)]

        node_attributes = {}
        for attribute_name in attributes_list:
            node_attributes[attribute_name] = AdjacencyGraphBuilder._chain_attribute(
                attribute_name, filtered_nodes)

        # Total facing of all the products.
        total_facings = sum([n['facings'] for n in filtered_nodes])

        node_attributes.update({'facings': total_facings,
                                'polygon': MultiPolygon([n['polygon'] for n in filtered_nodes])})

        return node_attributes

    @staticmethod
    def condense_graph_by_level(level, graph):
        """
        Building a condensed adjacency graph from the base graph to the requested level.
        :param : level - the requested level for the condensed graph.
        """

        def get_relevant_nodes(graph, level_name, value):
            return [i for i, n in graph.nodes(data=True)
                    if value in n[level_name].values]

        AdjacencyGraphBuilder._validate_attribute_is_unique(graph, level)

        # A set of all the values of the relevant level.
        level_values = set(itertools.chain(
            *[node[level].values for idx, node in graph.nodes(data=True)]))

        # A list of all the nodes in a condensed node (group of nodes).
        node_groups = list()
        # A list of all the nodes attributes in a condensed node (group of nodes).
        node_group_attributes = dict()

        for level_value in level_values:

            # The list of the relevant nodes.
            relevant_nodes = tuple(get_relevant_nodes(
                graph=graph, level_name=level, value=level_value))

            if len(relevant_nodes) == 0:
                continue

            # Each connected component from the subgraph of the relevant level value will be condensed to a single node.
            for connected_component in nx.connected_component_subgraphs(graph.subgraph(relevant_nodes).to_undirected()):
                # Filtering the relevant nodes from the subgraph.
                subgraph_relevant_nodes = tuple(get_relevant_nodes(graph=connected_component,
                                                                   level_name=level,
                                                                   value=level_value))

                # Appending the group of nodes to the list.
                node_groups.append(subgraph_relevant_nodes)

                # The condensed node attributes.
                merged_node_attributes = AdjacencyGraphBuilder.get_merged_node_attributes_from_nodes(subgraph_relevant_nodes,
                                                                                                     connected_component)

                # Adding the condensed node attributes.
                node_group_attributes[subgraph_relevant_nodes] = merged_node_attributes

        # Building the condensed graph from the base graph.
        condensed_graph = nx.condensation(graph, node_groups)

        condensed_graph_attributes = dict()

        # A map from the condensed node index to the relevant nodes in the base graph.
        members_dict = {i: node['members'] for i, node in condensed_graph.nodes(data=True)}

        for node, members in members_dict.items():
            condensed_graph_attributes[node] = node_group_attributes[members]

        # Updating the condensed nodes with the relevant attributes.
        nx.set_node_attributes(condensed_graph, condensed_graph_attributes)

        add_neighbours_degree(condensed_graph)

        return condensed_graph

    @staticmethod
    def build_adjacency_matrix_from_graph(graph, attribute_name, all_nodes=None, distribution=False):
        """
        Building a adjacency matrix for a condensed graph.
        Args:
            graph: the condensed graph that is described by the produced adjacency matrix.
            attribute_name: the name of the attribute to use as names in the matrix
            all_nodes: list of nodes to be considered in the adjacency matrix,
                       should be according to the requested attribute.
            distribution: if false all the values are {0,1}, else the number number of adjacent nodes normalized.

        Returns: the adjacency matrix.

        """
        AdjacencyGraphBuilder._validate_attribute_is_unique(graph, attribute_name)

        graph = graph.copy()

        # The condensed nodes attributes.
        node_attribute_value = nx.get_node_attributes(graph, attribute_name)

        # The index / columns of the adjacency matrix will be returned with the attribute value.
        rename_dict = {k: v.value for k, v in node_attribute_value.items()}

        existing_nodes = rename_dict.values()

        if all_nodes is not None:
            for node in set(all_nodes) - set(existing_nodes):
                node_name = 'dummy {}'.format(node)
                graph.add_node(node_name, **{attribute_name: node})
                rename_dict[node_name] = node

        # The adjacency matrix of the condensed graph.
        condensed_graph_df = nx.to_pandas_adjacency(graph)

        # Renaming the index/columns to the appropriate name.
        condensed_graph_df = condensed_graph_df.rename(columns=rename_dict, index=rename_dict)

        # Condensing the matrix.
        condensed_graph_df = condensed_graph_df.groupby(condensed_graph_df.index).sum().T
        condensed_graph_df = condensed_graph_df.groupby(condensed_graph_df.index).sum()

        # If a distribution is desired then the number of adjacent nodes is normalized
        if distribution:
            condensed_graph_df = (condensed_graph_df / condensed_graph_df.sum()).fillna(0)
        # if not it is updated to {0/1}
        else:
            condensed_graph_df = (condensed_graph_df / condensed_graph_df).fillna(0)

        return condensed_graph_df
