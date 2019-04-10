import igraph
import datetime as dt
import pandas as pd

from Trax.Algo.Calculations.Core.DataProvider import Data
from Trax.Cloud.Services.Connector.Keys import DbUsers
from KPIUtils_v2.DB.PsProjectConnector import PSProjectConnector
from Trax.Utils.Logging.Logger import Log


__author__ = 'Nimrod'


VERTEX_FK_FIELD = 'scene_match_fk'


class MARSRU2_SANDPositionGraphs:

    TOP = 'shelf_px_top'
    BOTTOM = 'shelf_px_bottom'
    LEFT = 'shelf_px_left'
    RIGHT = 'shelf_px_right'

    FLEXIBLE_MODE = 'Flexible Mode'
    STRICT_MODE = 'Strict Mode'

    ATTRIBUTES_TO_SAVE = ['product_name', 'product_type', 'product_ean_code', 'sub_brand_name',
                          'brand_name', 'category', 'sub_category', 'manufacturer_name', 'front_facing', 'product_fk']

    def __init__(self, data_provider, flexibility=1, proximity_mode=FLEXIBLE_MODE, rds_conn=None):
        self.data_provider = data_provider
        self.flexibility = flexibility
        self.proximity_mode = proximity_mode
        if rds_conn is not None:
            self._rds_conn = rds_conn
        self.project_name = self.data_provider.project_name
        self.session_uid = self.data_provider.session_uid
        self.position_graphs = {}

    @property
    def rds_conn(self):
        if not hasattr(self, '_rds_conn'):
            self._rds_conn = PSProjectConnector(self.project_name, DbUsers.CalculationEng)
        return self._rds_conn

    @property
    def match_product_in_scene(self):
        if not hasattr(self, '_match_product_in_scene'):
            self._match_product_in_scene = self.get_filtered_matches()
        return self._match_product_in_scene

    def get(self, scene_id, horizontal_block_only=False):
        """
        This function returns a position graph for a given scene
        """
        if scene_id not in self.position_graphs.keys():
            self.create_position_graphs(scene_id, horizontal_block_only)
        return self.position_graphs.get(scene_id)

    def get_filtered_matches(self):
        matches = self.data_provider[Data.MATCHES]
        matches = matches.sort_values(by=['bay_number', 'shelf_number', 'facing_sequence_number'])
        matches = matches.merge(self.get_match_product_in_scene(), how='left',
                                on='scene_match_fk', suffixes=['', '_2'])
        matches = matches.merge(
            self.data_provider[Data.ALL_PRODUCTS], how='left', on='product_fk', suffixes=['', '_3'])
        matches = matches.merge(self.data_provider[Data.SCENE_ITEM_FACTS][['template_name', 'location_type',
                                                                           'scene_id', 'scene_fk']],
                                how='left', on='scene_fk', suffixes=['', '_4'])
        if set(self.ATTRIBUTES_TO_SAVE).difference(matches.keys()):
            missing_data = self.get_missing_data()
            matches = matches.merge(missing_data, on='product_fk', how='left', suffixes=['', '_5'])
        matches = matches[matches['status'] == 1]
        matches = matches.drop_duplicates(subset=[VERTEX_FK_FIELD])
        return matches

    def get_missing_data(self):
        query = """
                select p.pk as product_fk, p.product_name, p.product_type,
                       p.product_ean_code, p.sub_category, sb.name as sub_brand_name,
                       b.name as brand_name, c.name as category, m.name as manufacturer_name
                from static.product p
                join static.brand b on b.pk = p.brand_fk
                join static.sub_brand sb on sb.pk = p.sub_brand_fk
                join static.product_categories c on c.pk = b.category_fk
                join static.manufacturers m on m.pk = b.manufacturer_fk
                """
        data = pd.read_sql_query(query, self.rds_conn.db)
        return data

    def get_match_product_in_scene(self):
        self._rds_conn = PSProjectConnector(self.project_name, DbUsers.CalculationEng)
        query = """
                select ms.pk as scene_match_fk, ms.*
                from probedata.match_product_in_scene ms
                join probedata.scene s on s.pk = ms.scene_fk
                where s.session_uid = '{}'""".format(self.session_uid)
        matches = pd.read_sql_query(query, self.rds_conn.db)
        return matches

    def create_position_graphs(self, scene_id=None, horizontal_block_only=False):
        """
        This function creates a facings Graph for each scene of the given session.
        """
        calc_start_time = dt.datetime.utcnow()
        if scene_id:
            scenes = [scene_id]
        else:
            scenes = self.match_product_in_scene['scene_fk'].unique()
        for scene in scenes:
            matches = self.match_product_in_scene[(self.match_product_in_scene['scene_fk'] == scene) &
                                                  (self.match_product_in_scene['stacking_layer'] == 1)]
            matches['distance_from_end_of_shelf'] = matches['n_shelf_items'] - \
                matches['facing_sequence_number']
            scene_graph = igraph.Graph(directed=True)
            edges = []
            for f in xrange(len(matches)):
                facing = matches.iloc[f]
                facing_name = str(facing[VERTEX_FK_FIELD])
                scene_graph.add_vertex(facing_name)
                # adding attributes to vertex
                vertex = scene_graph.vs.find(facing_name)
                for attribute in self.ATTRIBUTES_TO_SAVE:
                    vertex[attribute] = facing[attribute]

                surrounding_products = self.get_surrounding_products(facing, matches, horizontal_block_only)
                for direction in surrounding_products.keys():
                    for pk in surrounding_products[direction]:
                        edge = dict(source=facing_name, target=str(pk), direction=direction)
                        edges.append(edge)
            for edge in edges:
                scene_graph.add_edge(**edge)

            self.position_graphs[scene] = scene_graph
        calc_finish_time = dt.datetime.utcnow()
        Log.debug('Creation of position graphs for scenes {} took {}'.format(
            scenes, calc_finish_time - calc_start_time))

    def get_surrounding_products(self, anchor, matches, horizontal_block_only=False):
        """
        :param anchor: The tested SKU.
        :param matches: The filtered match_product_in_scene data frame for the relevant scene.
        :return: The surrounding SKUs to the anchor (from all sides), as data frames.
        """
        anchor_top = int(anchor[self.TOP])
        anchor_bottom = int(anchor[self.BOTTOM])
        anchor_y = anchor_bottom - anchor_top
        height_flexibility = ((anchor_bottom - anchor_top) * (self.flexibility - 1)) / 2
        anchor_top -= height_flexibility
        anchor_bottom += height_flexibility

        anchor_left = int(anchor[self.LEFT])
        anchor_right = int(anchor[self.RIGHT])
        anchor_x = anchor_right - anchor_left
        width_flexibility = ((anchor_right - anchor_left) * (self.flexibility - 1)) / 2
        anchor_left -= width_flexibility
        anchor_right += width_flexibility

        anchor_shelf_number = int(anchor['shelf_number'])
        anchor_shelf_number_from_bottom = int(anchor['shelf_number_from_bottom'])
        anchor_bay_number = int(anchor['bay_number'])
        anchor_facing = int(anchor['facing_sequence_number'])
        anchor_shelf_items = int(anchor['n_shelf_items'])

        # checking top & bottom
        if self.proximity_mode == self.STRICT_MODE:
            filtered_matches = matches[(matches['bay_number'] == anchor_bay_number) &
                                       (matches[self.LEFT] < anchor_x) & (matches[self.RIGHT] > anchor_x)]
        else:
            filtered_matches = matches[(matches['bay_number'] == anchor_bay_number) &
                                       (matches[self.LEFT].between(anchor_left, anchor_right) |
                                        matches[self.RIGHT].between(anchor_left, anchor_right) |
                                        ((matches[self.LEFT] < anchor_left) & (anchor_left < matches[self.RIGHT])) |
                                        ((matches[self.LEFT] < anchor_right) & (anchor_right < matches[self.RIGHT])))]
        if anchor_shelf_number == 1 or horizontal_block_only:
            surrounding_top = []
        else:
            surrounding_top = filtered_matches[matches['shelf_number']
                                               == anchor_shelf_number - 1][VERTEX_FK_FIELD]
        if anchor_shelf_number_from_bottom == 1 or horizontal_block_only:
            surrounding_bottom = []
        else:
            surrounding_bottom = filtered_matches[matches['shelf_number']
                                                  == anchor_shelf_number + 1][VERTEX_FK_FIELD]

        # checking left & right
        filtered_matches = matches[(matches['shelf_number'] == anchor_shelf_number) &
                                   (matches['bay_number'] == anchor_bay_number)]
        if anchor_facing > 1:
            surrounding_left = filtered_matches[filtered_matches['facing_sequence_number'] ==
                                                anchor_facing - 1][VERTEX_FK_FIELD]
        elif anchor_bay_number == 1:
            surrounding_left = []
        else:
            left_bay = matches[(matches['bay_number'] == anchor_bay_number - 1) &
                               (matches['distance_from_end_of_shelf'] == 0)]

            if self.proximity_mode == self.STRICT_MODE:
                surrounding_left = left_bay[(left_bay[self.TOP] < anchor_y)
                                            & (left_bay[self.BOTTOM] > anchor_y)]
            else:
                surrounding_left = left_bay[(left_bay[self.TOP].between(anchor_top, anchor_bottom) |
                                             left_bay[self.BOTTOM].between(anchor_top, anchor_bottom) |
                                             ((left_bay[self.TOP] < anchor_top) &
                                              (anchor_top < left_bay[self.BOTTOM])) |
                                             ((left_bay[self.TOP] < anchor_bottom) &
                                              (anchor_bottom < left_bay[self.BOTTOM])))]
            surrounding_left = surrounding_left[VERTEX_FK_FIELD]

        if anchor_facing < anchor_shelf_items:
            surrounding_right = filtered_matches[filtered_matches['facing_sequence_number'] ==
                                                 anchor_facing + 1][VERTEX_FK_FIELD]
        else:
            right_bay = matches[(matches['bay_number'] == anchor_bay_number + 1) &
                                (matches['facing_sequence_number'] == 1)]
            if right_bay.empty:
                surrounding_right = []
            else:
                if self.proximity_mode == self.STRICT_MODE:
                    surrounding_right = right_bay[(right_bay[self.TOP] < anchor_y) & (
                        right_bay[self.BOTTOM] > anchor_y)]
                else:
                    surrounding_right = right_bay[(right_bay[self.TOP].between(anchor_top, anchor_bottom) |
                                                   right_bay[self.BOTTOM].between(anchor_top, anchor_bottom) |
                                                   ((right_bay[self.TOP] < anchor_top) &
                                                    (anchor_top < right_bay[self.BOTTOM])) |
                                                   ((right_bay[self.TOP] < anchor_bottom) &
                                                    (anchor_bottom < right_bay[self.BOTTOM])))]
                surrounding_right = surrounding_right[VERTEX_FK_FIELD]

        return dict(top=surrounding_top, bottom=surrounding_bottom,
                    left=surrounding_left, right=surrounding_right)

    def get_entity_matrix(self, scene_id, entity):
        """
        This function creates a list of lists:
        Each list represents a shelf in the scene - with the given entity for each facing, from left to right.
        """
        if entity not in self.ATTRIBUTES_TO_SAVE:
            Log.debug("Entity '{}' is not set as an attribute in the graph".format(entity))
            return None
        graph = self.get(scene_id).copy()
        edges_to_remove = graph.es.select(direction_ne='left')
        graph.delete_edges([edge.index for edge in edges_to_remove])

        incidents_dict = {}
        matrix = []
        for vertex in graph.vs:
            vertex_id = vertex.index
            incidents = graph.incident(vertex_id)
            if incidents:
                incidents_dict[graph.es[incidents[0]].target] = vertex_id
            else:
                matrix.append([vertex_id])
        for i, row in enumerate(matrix):
            current = row[0]
            while current in incidents_dict.keys():
                current = incidents_dict[current]
                row.append(current)
            for y, index in enumerate(row):
                row[y] = graph.vs[index][entity]
            matrix[i] = row
        return matrix
