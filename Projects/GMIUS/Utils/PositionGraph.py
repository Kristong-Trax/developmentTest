import networkx as nx
import numpy as np
from Projects.GMIUS.ImageHMTL import ImageMaker
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

    def network_x_block_together(self, sku_in_block, allowed):

        # TODO - remove product_name_df as input + add docstring
        sku_in_block = list(sku_in_block)
        product_name_df = self.match_product_in_scene
        # TODO - make the allowed dynamic
        if allowed:
            sku_in_block += list(product_name_df[product_name_df['product_type'] == allowed]['product_fk'].astype(np.int64))
        scenes = self.match_product_in_scene[(self.match_product_in_scene['product_fk'].isin(sku_in_block)) & (
                # self.match_product_in_scene.location_type == 'Primary Shelf')]['scene_fk'].drop_duplicates().values
                self.match_product_in_scene.stacking_layer == 1)]['scene_fk'].drop_duplicates().values
        if len(scenes) == 0:
            return False
        for scene in scenes:
            try:
                matches_df = self.data_provider.matches[self.data_provider.matches.scene_fk == scene]
                if matches_df.empty:
                    continue
                adj_g = AdjacencyGraph(matches_df, None, None, name=None)
                condenced_graph_sku = adj_g.build_adjacency_graph_from_base_graph_by_level('product_fk')
                # sku_adj_table = adj_g.build_adjacency_matrix_from_condensed_graph(condenced_graph_sku,
                #                                                                            all_nodes=sku_in_block)
                sub_nodes = [i for i, n in condenced_graph_sku.nodes(data=True) if n['group_attributes']['product_fk_list']
                             in sku_in_block]
                sku_sub_graph = condenced_graph_sku.subgraph(sub_nodes)

                # TODO - is better to use a directed graph
                sku_sub_graph = sku_sub_graph.to_undirected()
                components = list(nx.connected_component_subgraphs(sku_sub_graph))
                counter = 0
                res = True

                for component in components:
                    print('\n')
                    for n in component.nodes(data=True):
                        print(n[1]['product_short_name'])
                    #     if component.nodes(data=True)
                    # print (component)
                    # print (component.nodes(data=True))

                    # TODO - avoid looping over all nodes.

                    for i, n in component.nodes(data=True):
                        # print ('loop')
                        # print(i, n['group_attributes']['product_type_list'])
                        if n['group_attributes']['product_type_list'][0] == 'SKU':
                            counter += 1
                            # print(counter)
                            # print (i, n['group_attributes']['product_type_list'])

                            break
                    # print('counter: ', counter)
                    if counter > 1:
                        res = False
                        break
                # print(res)
                # sku_adj_table_undirected = adj_g.build_adjacency_matrix_from_condensed_graph(sku_sub_graph, all_nodes=sku_in_block)
                # sku_adj_table = sku_adj_table[sku_adj_table.index.isin(sku_in_block)][list(sku_in_block)]
                # res = nx.is_connected(sku_sub_graph)
            except Exception as err:
                Log.info('{}'.format(err))
                return False
            if res:
                return res
        return res

    def alt_block(self, mpis, relevant_filter=None, allowed_filter=None):
        img_maker = ImageMaker('rinielsenus', 342186)
        mpis = mpis[mpis['status'] == 1]
        scenes = mpis.scene_fk.unique()
        for scene in scenes:
            matches_df = mpis[mpis['scene_fk'] == scene]
            if matches_df.empty:
                continue
            adj_g = AdjacencyGraph(matches_df)
            for connected_component in nx.connected_component_subgraphs(adj_g.base_adjacency_graph.to_undirected()):
                print('\n')
                img_maker.html_builder
                for n in connected_component.nodes(data=True):
                    print(n[1]['product_short_name'])
