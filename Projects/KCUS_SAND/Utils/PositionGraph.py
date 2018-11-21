
import igraph
import datetime
import pandas as pd

from Trax.Algo.Calculations.Core.DataProvider import Data
from Trax.Cloud.Services.Connector.Keys import DbUsers
from KPIUtils_v2.DB.PsProjectConnector import PSProjectConnector
from Trax.Utils.Logging.Logger import Log

__author__ = 'Nimrod'

VERTEX_FK_FIELD = 'scene_match_fk'
TITLE = {'Sub Category': 'sub_category', 'Package color': 'package_color',
         'Sub Brand': 'sub_brand', 'Package Size': 'package_size', 'Gender': 'Gender'}
SUB = {'sub_brand': 'sub_brand_x', 'sub_category': 'sub_category_x'}
EXTRA = ['package_size', 'package_color', 'quality_tier', 'Gender']
EXTRA_match = {'Package Size': 'package_size', 'Package color': 'package_color', 'Gender': 'Gender'}

class KCUS_SANDPositionGraphs:
    """
    MOVED TO Trax.Data.ProfessionalServices.KPIUtils.PositionGraph
    """
    TOP = 'shelf_px_top'
    BOTTOM = 'shelf_px_bottom'
    LEFT = 'shelf_px_left'
    RIGHT = 'shelf_px_right'
    ATTRIBUTES_TO_SAVE = ['product_name', 'product_type', 'product_ean_code', 'sub_brand_name',
                          'brand_name', 'category', 'manufacturer_name', 'package_size', 'package_color',
                          'quality_tier', 'sub_brand', 'sub_category', 'Gender']

    def __init__(self, data_provider, flexibility=1.4):
        self.data_provider = data_provider
        self.flexibility = flexibility
        self.project_name = self.data_provider.project_name
        self.session_uid = self.data_provider.session_uid
        self.rds_conn = PSProjectConnector(self.project_name, DbUsers.CalculationEng)
        self.scif = self.data_provider[Data.SCENE_ITEM_FACTS]
        for sub in SUB:
            if sub in (SUB.keys()):
                self.scif = self.scif.rename(columns={sub: SUB.get(sub)})
        for title in TITLE:
            if title in (self.scif.columns.unique().tolist()):
                self.scif = self.scif.rename(columns={title: TITLE.get(title)})
        self.match_product_in_scene = self.get_filtered_matches()
        self.position_graphs = self.create_position_graphs()

    def get_filtered_matches(self, include_stacking=False):
        matches = self.data_provider[Data.MATCHES]
        matches = matches.sort_values(by=['bay_number', 'shelf_number', 'facing_sequence_number'])
        matches = matches[matches['status'] == 1]
        if not include_stacking:
            matches = matches[matches['stacking_layer'] == 1]
        matches = matches.merge(self.get_match_product_in_scene(), how='left', on='scene_match_fk')
        matches = matches.merge(self.data_provider[Data.PRODUCTS], how='left', on='product_fk')
        matches = matches.merge(self.scif, how='left', on='product_fk',
                                suffixes=['_1', ''])
        for extra_match in EXTRA_match:
            if extra_match in (matches.columns.unique().tolist()):
                matches = matches.rename(columns={extra_match: TITLE.get(extra_match)})
        matches = matches.drop_duplicates(subset=[VERTEX_FK_FIELD])
        return matches

    def get_match_product_in_scene(self):
        query = """
                select ms.pk as scene_match_fk, ms.n_shelf_items, ms.{}, ms.{}, ms.{}, ms.{}
                from probedata.match_product_in_scene ms
                join probedata.scene s on s.pk = ms.scene_fk
                where s.session_uid = '{}'""".format(self.TOP, self.BOTTOM, self.LEFT, self.RIGHT, self.session_uid)
        matches = pd.read_sql_query(query, self.rds_conn.db)
        return matches

    def get(self, scene_id):
        """
        This function returns a position graph for a given scene
        """
        if scene_id not in self.position_graphs.keys():
            self.create_position_graphs(scene_id)
        return self.position_graphs.get(scene_id)

    def create_position_graphs(self,scene_id=None):
        """
        This function creates a facings Graph for each scene of the given session.
        """
        calc_start_time = datetime.datetime.utcnow()
        graphs = {}
        attributes_to_save = ['product_name', 'product_type', 'product_ean_code',
                              'brand_name', 'category', 'manufacturer_name', 'sub_category', 'sub_brand']
        for extra in EXTRA:
            if extra in self.scif.columns.unique().tolist():
                attributes_to_save.append(extra)
        calc_start_time = datetime.datetime.utcnow()
        if scene_id:
            scenes = [scene_id]
        else:
            scenes = self.match_product_in_scene['scene_fk'].unique()
        for scene in scenes:
            matches = self.match_product_in_scene[self.match_product_in_scene['scene_fk'] == scene]

            scene_graph = igraph.Graph(directed=True)
            edges = []
            for f in xrange(len(matches)):
                facing = matches.iloc[f]
                facing_name = str(facing[VERTEX_FK_FIELD])
                scene_graph.add_vertex(facing_name)
                # adding attributes to vertex
                vertex = scene_graph.vs.find(facing_name)
                for attribute in attributes_to_save:
                    if attribute in matches.columns:
                        vertex[attribute] = facing[attribute]
                    else:
                        vertex[attribute] = None
                surrounding_products = self.get_surrounding_products(facing, matches)
                for direction in surrounding_products.keys():
                    for pk in surrounding_products[direction]:
                        edge = (facing_name, str(pk), direction)
                        edges.append(edge)
            for edge in edges:
                source, target, direction = edge
                scene_graph.add_edge(source=source, target=target)
                edge_id = scene_graph.get_eid(source, target)
                scene_graph.es[edge_id]['direction'] = direction

            graphs[scene] = scene_graph
        calc_finish_time = datetime.datetime.utcnow()
        Log.info('Creation of position graphs took {}'.format(calc_finish_time - calc_start_time))
        return graphs

    def get_surrounding_products(self, anchor, matches):
        """
        :param anchor: The tested SKU.
        :param matches: The filtered match_product_in_scene data frame for the relevant scene.
        :return: The surrounding SKUs to the anchor (from all sides), as data frames.
        """
        anchor_top = int(anchor[self.TOP])
        anchor_bottom = int(anchor[self.BOTTOM])
        height_flexibility = ((anchor_bottom - anchor_top) * (self.flexibility - 1)) / 2
        anchor_top -= height_flexibility
        anchor_bottom += height_flexibility

        anchor_left = int(anchor[self.LEFT])
        anchor_right = int(anchor[self.RIGHT])
        width_flexibility = ((anchor_right - anchor_left) * (self.flexibility - 1)) / 2
        anchor_left -= width_flexibility
        anchor_right += width_flexibility

        anchor_shelf_number = int(anchor['shelf_number'])
        anchor_bay_number = int(anchor['bay_number'])
        anchor_facing = int(anchor['facing_sequence_number'])
        anchor_shelf_items = int(anchor['n_shelf_items'])

        # checking top & bottom
        filtered_matches = matches[(matches['bay_number'] == anchor_bay_number) &
                                   (matches[self.LEFT].between(anchor_left, anchor_right) |
                                    matches[self.RIGHT].between(anchor_left, anchor_right) |
                                    ((matches[self.LEFT] < anchor_left) & (anchor_left < matches[self.RIGHT])) |
                                    ((matches[self.LEFT] < anchor_right) & (anchor_right < matches[self.RIGHT])))]
        surrounding_top = filtered_matches[matches['shelf_number'] == anchor_shelf_number - 1]
        surrounding_bottom = filtered_matches[matches['shelf_number'] == anchor_shelf_number + 1]

        # checking left & right
        filtered_matches = matches[(matches['shelf_number'] == anchor_shelf_number) &
                                   (matches['bay_number'] == anchor_bay_number)]
        if anchor_facing > 1:
            surrounding_left = filtered_matches[filtered_matches['facing_sequence_number'] == anchor_facing - 1]
        else:
            surrounding_left = matches[(matches['bay_number'] == anchor_bay_number - 1)]
            if not surrounding_left.empty:
                left_shelf_items = surrounding_left['n_shelf_items'].values[0]
                surrounding_left = matches[(matches['facing_sequence_number'] == left_shelf_items) &
                                           (matches[self.TOP].between(anchor_top, anchor_bottom) |
                                            matches[self.BOTTOM].between(anchor_top, anchor_bottom) |
                                            ((matches[self.TOP] < anchor_top) & (anchor_top < matches[self.BOTTOM])) |
                                            ((matches[self.TOP] < anchor_bottom) & (anchor_bottom < matches[self.BOTTOM])))]
        if anchor_facing < anchor_shelf_items:
            surrounding_right = filtered_matches[filtered_matches['facing_sequence_number'] == anchor_facing + 1]
        else:
            surrounding_right = matches[(matches['bay_number'] == anchor_bay_number + 1)]
            if not surrounding_right.empty:
                surrounding_right = matches[(matches['facing_sequence_number'] == 1) &
                                            (matches[self.TOP].between(anchor_top, anchor_bottom) |
                                             matches[self.BOTTOM].between(anchor_top, anchor_bottom) |
                                             ((matches[self.TOP] < anchor_top) & (anchor_top < matches[self.BOTTOM])) |
                                             ((matches[self.TOP] < anchor_bottom) & (anchor_bottom < matches[self.BOTTOM])))]

        surrounding_products = {'top': surrounding_top[VERTEX_FK_FIELD].unique().tolist(),
                                'bottom': surrounding_bottom[VERTEX_FK_FIELD].unique().tolist(),
                                'left': surrounding_left[VERTEX_FK_FIELD].unique().tolist(),
                                'right': surrounding_right[VERTEX_FK_FIELD].unique().tolist()}
        return surrounding_products


    def get_entity_matrix(self, scene_id, entity, changes_matrix=False):
        """
        This function creates a list of lists:
        Each list represents a shelf in the scene - with the given entity for each facing, from left to right.
        """
        if entity not in self.ATTRIBUTES_TO_SAVE:
            Log.warning("Entity '{}' is not saved as an attribute in the graphs".format(entity))
        graph = self.get(scene_id).copy()
        edges_to_remove = graph.es.select(direction_ne='left')
        graph.delete_edges([edge.index for edge in edges_to_remove])

        incidents_dict = {}
        matrix = []
        filtered_row = []
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
            if changes_matrix:
                filtered_row = []
                s=0
                for y, index in enumerate(row):
                    if y == 0:
                        filtered_row.append(graph.vs[index][entity])
                    elif filtered_row[s] != graph.vs[index][entity]:
                        filtered_row.append(graph.vs[index][entity])
                        s += 1
            else:
                for y, index in enumerate(row):
                        row[y] = graph.vs[index][entity]
            if changes_matrix:
                matrix[i] = filtered_row
            else:
                matrix[i] = row
        return matrix
