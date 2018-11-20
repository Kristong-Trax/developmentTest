
import igraph
import datetime
import pandas as pd

from Trax.Algo.Calculations.Core.DataProvider import Data
from Trax.Cloud.Services.Connector.Keys import DbUsers
from KPIUtils_v2.DB.PsProjectConnector import PSProjectConnector
from Trax.Utils.Logging.Logger import Log

__author__ = 'Nimrod'

VERTEX_FK_FIELD = 'scene_match_fk'


class PositionGraphs:
    """
    MOVED TO Trax.Data.ProfessionalServices.KPIUtils.PositionGraph
    """
    TOP = 'shelf_px_top'
    BOTTOM = 'shelf_px_bottom'
    LEFT = 'shelf_px_left'
    RIGHT = 'shelf_px_right'

    def __init__(self, data_provider, flexibility=1):
        self.data_provider = data_provider
        self.flexibility = flexibility
        self.project_name = self.data_provider.project_name
        self.session_uid = self.data_provider.session_uid
        self.rds_conn = PSProjectConnector(self.project_name, DbUsers.CalculationEng)
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

    def create_position_graphs(self):
        """
        This function creates a facings Graph for each scene of the given session.
        """
        calc_start_time = datetime.datetime.utcnow()
        graphs = {}
        attributes_to_save = ['product_name', 'product_type', 'product_ean_code', 'sub_brand_name',
                              'brand_name', 'category', 'manufacturer_name']
        for scene in self.match_product_in_scene['scene_fk'].unique():
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
                    vertex[attribute] = facing[attribute]

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
