from Trax.Data.ProfessionalServices.PsConsts.DataProvider import MatchesConsts, ScifConsts
from KPIUtils_v2.DB.PsProjectConnector import PSProjectConnector
from Trax.Cloud.Services.Connector.Keys import DbUsers
from Trax.Utils.Logging.Logger import Log
from Trax.Algo.Calculations.Core.DataProvider import Data
from Projects.PNGJP.Data.LocalConsts import Consts

import igraph
import datetime
import pandas as pd

__author__ = 'Nimrod'


class PNGJPPositionGraphs:

    def __init__(self, data_provider, flexibility=1, proximity_mode=Consts.FLEXIBLE_MODE, rds_conn=None):
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
            self.rds_conn.connect_rds()
            self._match_product_in_scene = self.get_filtered_matches()
        return self._match_product_in_scene

    def get(self, scene_id):
        """
        This function returns a position graph for a given scene
        """
        if scene_id not in self.position_graphs.keys():
            self.create_position_graphs(scene_id)
        return self.position_graphs.get(scene_id)

    def get_filtered_matches(self, include_stacking=False):
        matches = self.data_provider[Data.MATCHES]
        matches = matches.sort_values(by=[MatchesConsts.BAY_NUMBER, MatchesConsts.SHELF_NUMBER, MatchesConsts.FACING_SEQUENCE_NUMBER])
        matches = matches.merge(self.get_match_product_in_scene(), how='left', on=MatchesConsts.SCENE_MATCH_FK, suffixes=['', '_2'])
        matches = matches.merge(self.data_provider[Data.ALL_PRODUCTS], how='left', on=MatchesConsts.PRODUCT_FK, suffixes=['', '_3'])
        matches = matches.merge(self.data_provider[Data.SCENE_ITEM_FACTS][[ScifConsts.TEMPLATE_NAME, ScifConsts.LOCATION_TYPE,
                                                                           ScifConsts.SCENE_ID, ScifConsts.SCENE_FK]],
                                how='left', on=ScifConsts.SCENE_FK, suffixes=['', '_4'])
        if set(Consts.ATTRIBUTES_TO_SAVE).difference(matches.keys()):
            missing_data = self.get_missing_data()
            matches = matches.merge(missing_data, on=MatchesConsts.PRODUCT_FK, how='left', suffixes=['', '_5'])
        matches = matches.drop_duplicates(subset=[MatchesConsts.SCENE_MATCH_FK])
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
        query = """
                select ms.pk as scene_match_fk, ms.*
                from probedata.match_product_in_scene ms
                join probedata.scene s on s.pk = ms.scene_fk
                where s.session_uid = '{}'""".format(self.session_uid)
        matches = pd.read_sql_query(query, self.rds_conn.db)
        return matches

    def create_position_graphs(self, scene_id=None):
        """
        This function creates a facings Graph for each scene of the given session.
        """
        calc_start_time = datetime.datetime.utcnow()
        if scene_id:
            scenes = [scene_id]
        else:
            scenes = self.match_product_in_scene[MatchesConsts.SCENE_FK].unique()
        for scene in scenes:
            matches = self.match_product_in_scene[self.match_product_in_scene[MatchesConsts.SCENE_FK] == scene].copy()
            matches['distance_from_end_of_shelf'] = matches[MatchesConsts.N_SHELF_ITEMS] - matches[MatchesConsts.FACING_SEQUENCE_NUMBER]
            scene_graph = igraph.Graph(directed=True)
            edges = []
            for f in xrange(len(matches)):
                facing = matches.iloc[f]
                facing_name = str(facing[MatchesConsts.SCENE_MATCH_FK])
                scene_graph.add_vertex(facing_name)
                # adding attributes to vertex
                vertex = scene_graph.vs.find(facing_name)
                for attribute in Consts.ATTRIBUTES_TO_SAVE:
                    vertex[attribute] = facing[attribute]

                surrounding_products = self.get_surrounding_products(facing, matches)
                for direction in surrounding_products.keys():
                    for pk in surrounding_products[direction]:
                        edge = dict(source=facing_name, target=str(pk), direction=direction)
                        edges.append(edge)
            for edge in edges:
                scene_graph.add_edge(**edge)

            self.position_graphs[scene] = scene_graph
        calc_finish_time = datetime.datetime.utcnow()
        Log.info('Creation of position graphs for scenes {} took {}'.format(scenes, calc_finish_time - calc_start_time))

    def get_surrounding_products(self, anchor, matches):
        """
        :param anchor: The tested SKU.
        :param matches: The filtered match_product_in_scene data frame for the relevant scene.
        :return: The surrounding SKUs to the anchor (from all sides), as data frames.
        """
        anchor_top = int(anchor[MatchesConsts.SHELF_PX_TOP])
        anchor_bottom = int(anchor[MatchesConsts.SHELF_PX_BOTTOM])
        anchor_y = anchor_bottom - anchor_top
        height_flexibility = ((anchor_bottom - anchor_top) * (self.flexibility - 1)) / 2
        anchor_top -= height_flexibility
        anchor_bottom += height_flexibility

        anchor_left = int(anchor[MatchesConsts.SHELF_PX_LEFT])
        anchor_right = int(anchor[MatchesConsts.SHELF_PX_RIGHT])
        anchor_x = anchor_right - anchor_left
        width_flexibility = ((anchor_right - anchor_left) * (self.flexibility - 1)) / 2
        anchor_left -= width_flexibility
        anchor_right += width_flexibility

        anchor_shelf_number = int(anchor[MatchesConsts.SHELF_NUMBER])
        anchor_shelf_number_from_bottom = int(anchor[MatchesConsts.SHELF_NUMBER_FROM_BOTTOM])
        anchor_bay_number = int(anchor[MatchesConsts.BAY_NUMBER])
        anchor_facing = int(anchor[MatchesConsts.FACING_SEQUENCE_NUMBER])
        anchor_shelf_items = int(anchor[MatchesConsts.N_SHELF_ITEMS])

        # checking top & bottom
        if self.proximity_mode == Consts.STRICT_MODE:
            filtered_matches = matches[(matches[MatchesConsts.BAY_NUMBER] == anchor_bay_number) &
                                       (matches[MatchesConsts.SHELF_PX_LEFT] < anchor_x) & (matches[MatchesConsts.SHELF_PX_RIGHT] > anchor_x)]
        else:
            filtered_matches = matches[(matches[MatchesConsts.BAY_NUMBER] == anchor_bay_number) &
                                       (matches[MatchesConsts.SHELF_PX_LEFT].between(anchor_left, anchor_right) |
                                        matches[MatchesConsts.SHELF_PX_RIGHT].between(anchor_left, anchor_right) |
                                        ((matches[MatchesConsts.SHELF_PX_LEFT] < anchor_left) & (anchor_left < matches[MatchesConsts.SHELF_PX_RIGHT])) |
                                        ((matches[MatchesConsts.SHELF_PX_LEFT] < anchor_right) & (anchor_right < matches[MatchesConsts.SHELF_PX_RIGHT])))]
        if anchor_shelf_number == 1:
            surrounding_top = []
        else:
            surrounding_top = filtered_matches[matches[MatchesConsts.SHELF_NUMBER] == anchor_shelf_number - 1][MatchesConsts.SCENE_MATCH_FK]
        if anchor_shelf_number_from_bottom == 1:
            surrounding_bottom = []
        else:
            surrounding_bottom = filtered_matches[matches[MatchesConsts.SHELF_NUMBER] == anchor_shelf_number + 1][MatchesConsts.SCENE_MATCH_FK]

        # checking left & right
        filtered_matches = matches[(matches[MatchesConsts.SHELF_NUMBER] == anchor_shelf_number) &
                                   (matches[MatchesConsts.BAY_NUMBER] == anchor_bay_number)]
        if anchor_facing > 1:
            surrounding_left = filtered_matches[filtered_matches[MatchesConsts.FACING_SEQUENCE_NUMBER] ==
                                                anchor_facing - 1][MatchesConsts.SCENE_MATCH_FK]
        elif anchor_bay_number == 1:
            surrounding_left = []
        else:
            left_bay = matches[(matches[MatchesConsts.BAY_NUMBER] == anchor_bay_number - 1) &
                               (matches['distance_from_end_of_shelf'] == 0)]

            if self.proximity_mode == Consts.STRICT_MODE:
                surrounding_left = left_bay[(left_bay[MatchesConsts.SHELF_PX_TOP] < anchor_y) & (left_bay[MatchesConsts.SHELF_PX_BOTTOM] > anchor_y)]
            else:
                surrounding_left = left_bay[(left_bay[MatchesConsts.SHELF_PX_TOP].between(anchor_top, anchor_bottom) |
                                             left_bay[MatchesConsts.SHELF_PX_BOTTOM].between(anchor_top, anchor_bottom) |
                                             ((left_bay[MatchesConsts.SHELF_PX_TOP] < anchor_top) &
                                              (anchor_top < left_bay[MatchesConsts.SHELF_PX_BOTTOM])) |
                                             ((left_bay[MatchesConsts.SHELF_PX_TOP] < anchor_bottom) &
                                              (anchor_bottom < left_bay[MatchesConsts.SHELF_PX_BOTTOM])))]
            surrounding_left = surrounding_left[MatchesConsts.SCENE_MATCH_FK]

        if anchor_facing < anchor_shelf_items:
            surrounding_right = filtered_matches[filtered_matches[MatchesConsts.FACING_SEQUENCE_NUMBER] ==
                                                 anchor_facing + 1][MatchesConsts.SCENE_MATCH_FK]
        else:
            right_bay = matches[(matches[MatchesConsts.BAY_NUMBER] == anchor_bay_number + 1) &
                                (matches[MatchesConsts.FACING_SEQUENCE_NUMBER] == 1)]
            if right_bay.empty:
                surrounding_right = []
            else:
                if self.proximity_mode == Consts.STRICT_MODE:
                    surrounding_right = right_bay[(right_bay[MatchesConsts.SHELF_PX_TOP] < anchor_y) & (right_bay[MatchesConsts.SHELF_PX_BOTTOM] > anchor_y)]
                else:
                    surrounding_right = right_bay[(right_bay[MatchesConsts.SHELF_PX_TOP].between(anchor_top, anchor_bottom) |
                                                   right_bay[MatchesConsts.SHELF_PX_BOTTOM].between(anchor_top, anchor_bottom) |
                                                   ((right_bay[MatchesConsts.SHELF_PX_TOP] < anchor_top) &
                                                    (anchor_top < right_bay[MatchesConsts.SHELF_PX_BOTTOM])) |
                                                   ((right_bay[MatchesConsts.SHELF_PX_TOP] < anchor_bottom) &
                                                    (anchor_bottom < right_bay[MatchesConsts.SHELF_PX_BOTTOM])))]
                surrounding_right = surrounding_right[MatchesConsts.SCENE_MATCH_FK]

        return dict(top=surrounding_top, bottom=surrounding_bottom,
                    left=surrounding_left, right=surrounding_right)

    def get_entity_matrix(self, scene_id, entity):
        """
        This function creates a list of lists:
        Each list represents a shelf in the scene - with the given entity for each facing, from left to right.
        """
        if entity not in Consts.ATTRIBUTES_TO_SAVE:
            Log.warning("Entity '{}' is not set as an attribute in the graph".format(entity))
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

