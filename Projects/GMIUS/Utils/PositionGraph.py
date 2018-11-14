import networkx as nx
import numpy as np
import pandas as pd
from Projects.GMIUS.ImageHTML.Image import ImageMaker
from Trax.Utils.Logging.Logger import Log
from Trax.Algo.Calculations.Core.GraphicalModel.AdjacencyGraphs import AdjacencyGraph
from Trax.Algo.Calculations.Core.DataProvider import Data


class Block(object):
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

    def __init__(self, data_provider, ignore_stacking=True, rds_conn=None, front_facing=False):
        self.data_provider = data_provider
        # self._position_graphs = PositionGraphs(self.data_provider)
        self.ignore_stacking = ignore_stacking
        self.scif = self.data_provider[Data.SCENE_ITEM_FACTS]
        self.match_product_in_scene = self.data_provider[Data.MATCHES]

        self.rds_conn = rds_conn
        self.scenes_info = self.data_provider[Data.SCENES_INFO]
        self.front_facing = front_facing
        # self.match_product_in_scene = self._position_graphs.match_product_in_scene
        # self.toolbox = GENERALToolBox(self.data_provider)

    def alt_block(self, mpis, raw, relevant_filter=None, allowed_filter=None):
        # mpis = mpis.drop(mpis.index[mpis['scene_match_fk'] == 554567292][0])


        img_maker = ImageMaker('rinielsenus', 342186, additional_attribs=['Segment'])
        html_x = float(img_maker.html_builder.size[1])
        html_y = float(img_maker.html_builder.size[0])
        mpis = mpis[mpis['status'] == 1]
        scenes = mpis.scene_fk.unique()
        scenes = [342186]
        nodes = []
        for scene in scenes:
            matches_df = mpis[mpis['scene_fk'] == scene]
            prod_df = raw[raw['scene_fk'] == scene]
            raw = raw[(raw['scene_fk'] == scene) &
                      (raw['status'] == 1)]
            if matches_df.empty:
                continue
            adj_g = AdjacencyGraph(matches_df, product_name_df=prod_df, product_attributes=['rect_x', 'rect_y'])
            mpis_indexed = mpis.set_index('scene_match_fk')
            adj_coords = adj_g.scene_data
            x_mm = max(adj_coords['right'] + (adj_coords['right'] - adj_coords['left']) / 2.0)
            y_mm = max(adj_coords['top'] + (adj_coords['top'] - adj_coords['bottom']) / 2.0)
            x_ratio = html_x / x_mm
            y_ratio = html_y / y_mm
            for i, connected_component in enumerate(nx.connected_component_subgraphs(
                                                    adj_g.base_adjacency_graph.to_undirected())):
                print('\n')
                img_maker.html_builder.add_cluster(group_num=i)
                for j, node in connected_component.nodes(data=True):
                    print(node['name'])
                    # attribs = self.gen_relevant_node_attribs(node, mpis_indexed, i)
                    mpis_fk = node['match_fk']
                    attribs = mpis_indexed.loc[mpis_fk].to_dict()
                    attribs['scene_match_fk'] = mpis_fk
                    attribs['left'] = node['p1'].x
                    attribs['top'] = node['p2'].y
                    attribs['width'] = node['p2'].x - node['p1'].x
                    attribs['height'] = node['p2'].y - node['p1'].y
                    attribs['w'] = attribs['shelf_px_right'] - attribs['shelf_px_left']
                    attribs['h'] = attribs['shelf_px_bottom'] - attribs['shelf_px_top']

                    ratio = attribs['w'] / float(attribs['width_mm'])

                    attribs['w'] = attribs['width'] * x_ratio
                    attribs['h'] = attribs['height'] * y_ratio
                    # break
                    attribs['rect_x'] = attribs['left'] * x_ratio
                    attribs['rect_y'] = (node['p1'].y) * y_ratio
                    attribs['cluster'] = i

                    img_maker.html_builder.add_product(attribs)
                    nodes.append(attribs['scene_match_fk'])

            img_maker.html_builder.add_cluster(group_num=-1)
            remaining = raw[~raw['scene_match_fk'].isin(nodes)]
            for i, node in remaining.iterrows():
                attribs = self.gen_irrelevant_node_attribs(node)
                img_maker.html_builder.add_product(attribs)

            img_maker.html_builder.products = img_maker.html_builder.products
            z = img_maker.html_builder.return_html()

    def gen_relevant_node_attribs(self, node, mpis, cluster):
        mpis_fk = node['match_fk']
        attribs = mpis.loc[mpis_fk].to_dict()
        attribs['scene_match_fk'] = mpis_fk
        attribs['left'] = node['p1'].x
        attribs['top'] = node['p2'].y
        attribs['width'] = node['p2'].x - node['p1'].x
        attribs['height'] = node['p2'].y - node['p1'].y
        attribs['w'] = attribs['shelf_px_right'] - attribs['shelf_px_left']
        attribs['h'] = attribs['shelf_px_bottom'] - attribs['shelf_px_top']

        ratio = attribs['w'] / float(attribs['width_mm'])

        attribs['w'] = attribs['width'] / ratio
        attribs['h'] = attribs['height'] / ratio

        attribs['rect_x'] = attribs['left'] / ratio
        attribs['rect_y'] = node['p1'].y / ratio
        attribs['cluster'] = cluster
        return attribs

    def gen_irrelevant_node_attribs(self, node):
        attribs = node.to_dict()
        # attribs['scene_match_fk'] = node['scene_match_fk']
        attribs['w'] = attribs['shelf_px_right'] - attribs['shelf_px_left']
        attribs['h'] = attribs['shelf_px_bottom'] - attribs['shelf_px_top']
        attribs['cluster'] = -1
        return attribs