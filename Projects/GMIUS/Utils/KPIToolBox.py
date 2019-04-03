
import pandas as pd
import numpy as np
from functools import reduce
from collections import defaultdict, Counter

from Trax.Algo.Calculations.Core.DataProvider import Data
from Trax.Algo.Calculations.Core.Utils import Validation
from Trax.Utils.Logging.Logger import Log
from Projects.GMIUS.Utils.Const import Const
from KPIUtils_v2.GlobalDataProvider.PsDataProvider import PsDataProvider

from KPIUtils_v2.Calculations.BlockCalculations import Block
from Trax.Algo.Calculations.Core.GraphicalModel.AdjacencyGraphs import AdjacencyGraph

from shapely.strtree import STRtree
from shapely.geometry import Polygon, mapping


from networkx import nx



__author__ = 'Sam'
# if you're looking for template path check kpigenerator.find_template

class ToolBox:

    def __init__(self, data_provider, output, common):
        self.common = common
        self.output = output
        self.data_provider = data_provider
        self.block = Block(self.data_provider)
        self.project_name = self.data_provider.project_name
        self.session_uid = self.data_provider.session_uid
        self.templates = self.data_provider.all_templates
        self.ps_data_provider = PsDataProvider(self.data_provider, self.output)
        self.result_values_dict = self.make_result_values_dict()
        self.att_dict = self.make_att_dict()
        self.store_assortment = self.ps_data_provider.get_store_assortment()
        self.store_sos_policies = self.ps_data_provider.get_store_policies()
        self.labels = self.ps_data_provider.get_labels()
        self.entity_dict = {str(key).lower(): vals['pk'] for key, vals in
                            self.ps_data_provider.get_custom_entities(22).set_index('name').iterrows()}
        self.scene_info = self.data_provider[Data.SCENES_INFO]
        self.all_products = self.data_provider[Data.ALL_PRODUCTS]
        self.products = self.data_provider[Data.PRODUCTS]
        self.store_info = self.data_provider[Data.STORE_INFO]
        self.store_id = self.data_provider[Data.STORE_FK]
        self.match_product_in_scene = self.data_provider[Data.MATCHES]
        self.full_mpis = self.match_product_in_scene.merge(self.products, on='product_fk', suffixes=['', '_p'])\
                                                    .merge(self.scene_info, on='scene_fk', suffixes=['', '_s'])\
                                                    .merge(self.templates, on='template_fk', suffixes=['', '_t'])
        self.mpis = self.full_mpis[self.full_mpis['product_type'] != 'Irrelevant']
        self.mpis = self.filter_df(self.mpis, Const.SOS_EXCLUDE_FILTERS, exclude=1)
        self.visit_date = self.data_provider[Data.VISIT_DATE]
        self.session_info = self.data_provider[Data.SESSION_INFO]
        self.scenes = self.scene_info['scene_fk'].tolist()
        self.scif = self.data_provider[Data.SCENE_ITEM_FACTS]
        self.mpip = self.create_mpip()
        self.template = {}
        self.super_cat = ''
        self.res_dict = {}
        self.dependencies = {}
        self.dependency_lookup = {}
        self.base_measure = None

    # main functions:
    def main_calculation(self, template_path):
        """
            This function gets all the scene results from the SceneKPI, after that calculates every session's KPI,
            and in the end it calls "filter results" to choose every KPI and scene and write the results in DB.
        """

        self.template = pd.read_excel(template_path, sheetname=None)
        self.super_cat = template_path.split('/')[-1].split(' ')[0].upper()
        self.dependencies = {key: None for key in self.template[Const.KPIS][Const.KPI_NAME]}
        self.dependency_reorder()
        main_template = self.template[Const.KPIS]
        self.dependency_lookup = main_template.set_index(Const.KPI_NAME)[Const.DEPENDENT].to_dict()
        self.res_dict = self.template[Const.RESULT].set_index('Result Key').to_dict('index')

        for i, main_line in main_template.iterrows():
            self.calculate_main_kpi(main_line)

        # self.flag_failures()


    def calculate_main_kpi(self, main_line):
        kpi_name = main_line[Const.KPI_NAME]
        kpi_type = main_line[Const.TYPE]
        scene_types = self.read_cell_from_line(main_line, Const.SCENE_TYPE)
        general_filters = {}
        relevant_scif = self.filter_df(self.scif.copy(), Const.SOS_EXCLUDE_FILTERS, exclude=1)
        if scene_types:
            relevant_scif = relevant_scif[relevant_scif['template_name'].isin(scene_types)]
            general_filters['template_name'] = scene_types
        if relevant_scif.empty:
            return

        print(kpi_name)
        # if kpi_type == Const.AGGREGATION:
        # if kpi_type:
        # if kpi_type in[Const.SET_COUNT]: # Const.COUNT_SHELVES:
        if kpi_type in[Const.BASE_MEASURE, Const.BLOCKING, Const.AGGREGATION]: # Const.COUNT_SHELVES:
        # if kpi_type in[Const.PRESENCE_WITHIN_BAY]: # Const.COUNT_SHELVES:


            dependent_kpis = self.read_cell_from_line(main_line, Const.DEPENDENT)
            dependent_results = self.read_cell_from_line(main_line, Const.DEPENDENT_RESULT)
            if dependent_kpis:
                for dependent_kpi in dependent_kpis:
                    if self.dependencies[dependent_kpi] not in dependent_results:
                        if dependent_results or self.dependencies[dependent_kpi] is None:
                            return

            kpi_line = self.template[kpi_type].set_index(Const.KPI_NAME).loc[kpi_name]
            function = self.get_kpi_function(kpi_type, kpi_line[Const.RESULT])
            try:
               all_kwargs = function(kpi_name, kpi_line, relevant_scif, general_filters)
            except:
                Log.error('kpi "{}" failed to calculate in super category "{}"'.format(kpi_name, self.super_cat))
            if not isinstance(all_kwargs, list):
                all_kwargs = [all_kwargs]
                for kwargs in all_kwargs:
                    if not kwargs or kwargs['score'] is None:
                        kwargs = {'score': 0, 'result': 0, 'failed': 1}
                    self.write_to_db(kpi_name, **kwargs)
                    self.dependencies[kpi_name] = kwargs['result']

    def flag_failures(self):
        for kpi, val in self.dependencies.items():
            if val is None:
                Log.warning('Warning: KPI "{}" not run for session "{}"'.format(kpi, self.session_uid))

    def calculate_sos(self, kpi_name, kpi_line, relevant_scif, general_filters):
        super_cats = relevant_scif['Super Category'].unique().tolist()
        for super_cat in super_cats:
            if not super_cat:
                continue
            den_id = self.entity_dict[super_cat.lower()]
            levels = self.read_cell_from_line(kpi_line, Const.AGGREGATION_LEVELS)
            sos_types = self.read_cell_from_line(kpi_line, Const.SOS_TYPE)
            scif = self.filter_df(relevant_scif, Const.SOS_EXCLUDE_FILTERS, exclude=1)
            scif = self.filter_df(scif, {'Super Category': super_cat})
            scif = self.create_special_scif(scif, fake_cat=1)
            scif['count'] = Const.MM_TO_FT
            for level in levels:
                level_col = '{}_fk'.format(level).lower()
                groups = scif.groupby(level_col)
                for item_pk, df in groups:
                    for sos_type in sos_types:
                        if sos_type in Const.SOS_COLUMN_DICT:
                            sos_sum_col = Const.SOS_COLUMN_DICT[sos_type]
                        else:
                            Log.warning("SOS Type not found in Const.SOS_COLUMN_DICT in kpi {}".format(kpi_name))
                            return
                        den = scif[sos_sum_col].sum()
                        kpi_fk = self.common.get_kpi_fk_by_kpi_type('{} {} {}'.format(level, kpi_name, sos_type))
                        num = df[sos_sum_col].sum()
                        ratio, score = self.ratio_score(num, den)
                        self.common.write_to_db_result(fk=kpi_fk, score=score, result=ratio, numerator_id=item_pk,
                                                       numerator_result=num, denominator_result=den,
                                                       denominator_id=den_id)

    def calculate_topmiddlebottom(self, kpi_name, kpi_line, relevant_scif, general_filters):
        locations = set()
        map = self.template[Const.TMB_MAP].set_index('Num Shelves').to_dict('index')
        filters = self.get_kpi_line_filters(kpi_line)
        filters.update(general_filters)

        bay_max_shelf = self.filter_df(self.full_mpis, general_filters).set_index('bay_number')\
                                                                  .groupby(level=0)['shelf_number'].max().to_dict()
        mpis = self.filter_df(self.mpis, filters)
        if mpis.empty:
            return
        grouped_mpis = mpis.set_index('bay_number').groupby(level=0)

        for bay, shelves in grouped_mpis:
            sub_map = map[bay_max_shelf[bay]]
            shelf_with_most = shelves.groupby('shelf_number_from_bottom')[shelves.columns[0]].count()\
                .sort_values().index[-1]
            locations.add(sub_map[shelf_with_most])
            # for shelf in shelves:
            #     locations.add(sub_map[shelf])
            if len(locations) == 3:
                break

        locations = sorted(list(locations))[::-1]
        ordered_result = '-'.join(locations)
        # result_fk = self.result_values_dict[ordered_result]
        kwargs = {'score': 1, 'result': ordered_result, 'target': 0}
        return kwargs

    def calculate_new_integrated_adjacency(self, kpi_name, kpi_line, relevant_scif, general_filters):
        for scene in relevant_scif.scene_fk.unique():
            scene_filter = {'scene_fk': scene}
            mpis = self.filter_df(mpis, scene_filter)
            mpis = self.filter_df(mpis, Const.SOS_EXCLUDE_FILTERS, exclude=1)
            allowed = {'product_type': ['Other', 'Empty']}

            a_filter = {'sub_category_local_name': 'COOKIE DOUGH'}
            b_filter = {'sub_category_local_name': 'SWEET ROLL DOUGH'}
            a_items = set(self.filter_df(mpis, a_filter)['scene_match_fk'].values)
            b_items = set(self.filter_df(mpis, b_filter)['scene_match_fk'].values)


    def intersect(self):
        # The dictionary of nodes and adjacent candidate nodes.
        adjacent_nodes = dict()

        # A dictionary where the key is the index of the padded product and the value is a polygon of that product.
        polygons_dict = {i: Polygon(product.get_coordinates())
                         for i, product in self.dict_of_padded_products.items()}

        # A map from the polygon coordinates to the index.
        polygons_map = {mapping(polygon)['coordinates']: index for index, polygon in polygons_dict.items()}

        # Building the polygon STR tree.
        polygons_tree = STRtree(polygons_dict.values())

        # For each polygons save the intersecting polygons
        for index, polygon in polygons_dict.items():
            adjacent_nodes[index] = [polygons_map[mapping(p)['coordinates']] for p in polygons_tree.query(polygon)]

    def calculate_presence(self, kpi_name, kpi_line, relevant_scif, general_filters):
        general_filters.update({'Super Category': self.super_cat})
        filters = self.get_kpi_line_filters(kpi_line)
        filters.update(general_filters)
        full_mpis = self.filter_df(self.mpis, general_filters)
        mpis = self.filter_df(full_mpis, filters)
        # mpis = self.mpis.copy()
        sections = mpis.groupby(['scene_fk', 'bay_number']).count().sort_values('product_fk', ascending=False)
        section_filters = {sections.index.names[i]: lvl for i, lvl in enumerate(sections.index[0])}
        cat_skus = self.filter_df(full_mpis, section_filters).shape[0]
        rel_skus = self.filter_df(mpis, section_filters).shape[0]
        ratio, score = self.ratio_score(rel_skus, cat_skus, target=.95)
        if score:
            adj_bays = [section_filters['bay_number'] + 1, section_filters['bay_number'] - 1]
            adj_filters = {'bay_number': adj_bays}
            adj_filters.update(general_filters)
            adj_mpis = self.filter_df(full_mpis, adj_filters)
            if adj_mpis.empty:
                result = 'No'
                score = 0
            else:
                result = 'Yes'
        # result_fk = self.result_values_dict[result]
        kwargs = {'score': score, 'result': result, 'target': 1}
        return kwargs

    def calculate_presence_within_bay(self, kpi_name, kpi_line, relevant_scif, general_filters):
        filters = self.get_kpi_line_filters(kpi_line, 'excluded')
        num = self.filter_df(self.scif, general_filters)
        num = self.filter_df(num, filters, exclude=1)['facings_ign_stack'].sum()
        den = self.filter_df(self.scif, general_filters)['facings_ign_stack'].sum()
        ratio = num/float(den) * 100 if den else 0
        potential_results = self.get_results_value(kpi_line)
        result = self.inequality_results(ratio, potential_results, kpi_name)

        return {'score': ratio, 'result': result, 'numerator_result': num, 'denominator_result': den}

    def integrated_adjacency(self, kpi_name, kpi_line, relevant_scif, general_filters):
        ''' I think this should be a scene level kpi, i will need to move it to scene_kpi_toolbox '''
        block_thres = .75
        directional_diversity_max = .75
        mpis = self.mpis.copy()

        for scene in relevant_scif.scene_fk.unique():
            scene_filter = {'scene_fk': scene}
            mpis = self.filter_df(mpis, scene_filter)
            mpis = self.filter_df(mpis, Const.SOS_EXCLUDE_FILTERS, exclude=1)
            allowed = {'product_type': ['Other', 'Empty']}

            a_filter = {'sub_category_local_name': 'COOKIE DOUGH'}
            b_filter = {'sub_category_local_name': 'SWEET ROLL DOUGH'}
            a_items = set(self.filter_df(mpis, a_filter)['scene_match_fk'].values)
            b_items = set(self.filter_df(mpis, b_filter)['scene_match_fk'].values)

            integ = False
            adjac = False
            aisle = False

            if not (a_items and b_items):
                continue

            if a_items and b_items:
                aisle = True
            filters = self.filter_join([a_filter, b_filter])
            g, blocks = self.block.network_x_block_together2(filters, location=scene_filter,
                                                               additional={'allowed_products_filters': allowed,
                                                                           'include_stacking': False})
            for block in blocks:
                block = blocks[0] # for testing only, so I can pick which block stays alive in the local namespace
                nodes_dict = {i: n['match_fk'] for i, n in block.nodes(data=True)}
                def cond_1(x, y): return (nodes_dict[x] in a_items and nodes_dict[y] in b_items)
                def cond_2(x, y): return (nodes_dict[x] in b_items and nodes_dict[y] in a_items)
                nodes = set(nodes_dict.values())
                a_pass = len(nodes & a_items) / len(a_items) >= block_thres
                b_pass = len(nodes & b_items) / len(b_items) >= block_thres

                if a_pass and b_pass:
                    adjac = True
                    edges = [d['direction'] for x, y, d in block.to_undirected().edges(data=True) if cond_1(x, y) or
                                                                                                     cond_2(x, y)]
                    if not edges:
                        ''' This happens when the 2 groups are only connected by allowed products-
                            a decision on how to handle this is pending from elizabeth '''
                        continue
                    counts = Counter(edges)
                    if max([val / len(edges) for val in counts.values()]) <= directional_diversity_max:
                        integ = True
                    break  # there can only be one. block.

            if integ:
                result = Const.INTEGRATED
            elif adjac:
                result = Const.ADJACENT
            elif aisle:
                result = Const.SAME_AISLE
            else:
                result = Const.NO_CONNECTION

            # result_fk = self.result_values_dict[result]
            result_fk = result

    def base_adj_graph(self, scene, kpi_line, general_filters, use_allowed=0, gmi_only=0, super_cat_only=0,
                       additional_attributes=None):
        product_attributes = ['rect_x', 'rect_y']
        if additional_attributes is not None:
            product_attributes = product_attributes + additional_attributes
        filters = self.get_kpi_line_filters(kpi_line)
        filters.update(general_filters)
        mpis_filter = {'scene_fk': scene}
        if super_cat_only:
            mpis_filter.update({'Super Category': self.super_cat})
        if gmi_only:
            mpis_filter.update({'manufacturer_name': 'GENERAL MILLS'})
        mpis = self.filter_df(self.mpis, mpis_filter)
        items = self.filter_df(mpis, filters)
        if items.empty:
            return None, None, None, None
        items = set(items['scene_match_fk'].values)
        if use_allowed:
            allowed = Const.ALLOWED_FILTERS
            allowed_items = set(self.filter_df(mpis, allowed)['scene_match_fk'].values)
            items.update(allowed_items)
        all_graph = AdjacencyGraph(mpis, None, self.products,
                                   product_attributes=product_attributes + list(filters.keys()),
                                   name=None, adjacency_overlap_ratio=.4)
        return items, mpis, all_graph, filters

    def calculate_sequence(self, kpi_name, kpi_line, relevant_scif, general_filters):
        # this attribute should be pulled from the template once the template is updated
        sequence_attribute = 'sub_category'  # value for testing since GMI_Segment isn't coded :(

        # this might affect the max number of facings in each block, not sure - needs testing
        use_allowed = 1
        scenes = relevant_scif.scene_fk.unique()
        for scene in scenes:
            # create a master adjacency graph of all relevant products in the scene
            items, mpis, all_graph, filters = self.base_adj_graph(scene, kpi_line, general_filters,
                                                                  use_allowed=use_allowed, gmi_only=0,
                                                                  additional_attributes=[sequence_attribute])
            # no relevant items based on filters? go to the next scene
            if not items:
                continue

            # make a dataframe of matching (filtered) mpis data
            scene_items = self.filter_df(mpis, filters)

            # get a list of unique values for the sequence attribute
            # this should come from the template eventually, too
            sequence_values = scene_items[sequence_attribute].unique().tolist()

            # generate block components
            condensed_graph_sku = all_graph.build_adjacency_graph_from_base_graph_by_level(sequence_attribute)
            condensed_graph_sku = condensed_graph_sku.to_undirected()
            components = list(nx.connected_component_subgraphs(condensed_graph_sku))

            # create a dataframe to hold the block results
            blocks = pd.DataFrame(columns=[sequence_attribute, 'facings', 'x_coordinate',
                                           'y_coordinate', 'node_object'])

            # create blocks for every unique sequence attribute value
            for attribute_value in sequence_values:
                # get relevant product_fks for the current attribute_value
                relevant_items = self.filter_df(scene_items, {sequence_attribute: attribute_value})
                relevant_product_fks = relevant_items['product_fk'].unique().tolist()

                for component in components:
                    for i, n in component.nodes(data=True):

                        # check if the node is a valid product for the current attribute_value
                        if not set(n['group_attributes']['product_fk_list']).isdisjoint(relevant_product_fks):
                            # get facings
                            facings = n['group_attributes']['facings']
                            # get shelf(scene) position coordinates
                            center = n['group_attributes']['center']
                            # save block result
                            blocks = blocks.append(pd.DataFrame(columns=[sequence_attribute, 'facings', 'x_coordinate',
                                                                         'y_coordinate', 'node_object'],
                                                                data=[[attribute_value, facings, center.x, center.y, n]]
                                                                ))
            # get the max blocks (most facings) from each sequence attribute value in the passing block dataframe
            max_blocks = blocks.sort_values('facings', ascending=False).groupby(sequence_attribute, as_index=False).first()

            # order the max_block dataframe by x_coordinate and return an ordered list
            ordered_list = max_blocks.sort_values('x_coordinate', ascending=True).tolist()

            # to-do: need to compare the ordered_list to options from template and then return the atomic result

        return

    def base_adjacency(self, kpi_name, kpi_line, relevant_scif, general_filters, limit_potential=1, use_allowed=1,
                       col_list=Const.REF_COLS):
        allowed_edges = self.read_cell_from_line(kpi_line, Const.EDGES)
        scenes = relevant_scif.scene_fk.unique()
        if self.read_cell_from_line(kpi_line, 'MSL'):
            scenes = self.find_MSL(relevant_scif)
        all_results = {}
        for scene in scenes:
            items, mpis, all_graph, filters = self.base_adj_graph(scene, kpi_line, general_filters,
                                                                  use_allowed=use_allowed, gmi_only=0)
            if not items:
                continue
            for edge_dir in allowed_edges:
                g = self.prune_edges(all_graph.base_adjacency_graph.copy(), [edge_dir])

                match_to_node = {int(node['match_fk']): i for i, node in g.nodes(data=True)}
                node_to_match = {val: key for key, val in match_to_node.items()}
                edge_matches = set(sum([[node_to_match[i] for i in g[match_to_node[item]].keys()]
                                        for item in items], []))
                adjacent_items = edge_matches - items
                adj_mpis = mpis[(mpis['scene_match_fk'].isin(adjacent_items)) &
                                (~mpis['product_type'].isin(Const.SOS_EXCLUDE_FILTERS))]
                adjacent_sections = set(sum([list(adj_mpis[col].unique()) for col in col_list], []))
                if limit_potential:
                    potential_results = set(self.get_results_value(kpi_line))
                    adjacent_sections = list(adjacent_sections & potential_results)
                all_results[edge_dir] = [adjacent_sections, len(adjacent_items)/float(len(items))]
        return all_results

    def calculate_adjacency(self, kpi_name, kpi_line, relevant_scif, general_filters):
        res_col = self.read_cell_from_line(kpi_line, Const.RESULT_TYPE)
        if not res_col:
            res_col = Const.REF_COLS
        all_results = self.base_adjacency(kpi_name, kpi_line, relevant_scif, general_filters, col_list=res_col)
        ret_values = []
        for result in sum([x for x, y in all_results.values()], []):
            if not result and kpi_line[Const.TYPE] == Const.ANCHOR_LIST:
                result = Const.END_OF_CAT
            # result_fk = self.result_values_dict[result]
            ret_values.append({'denominator_id': result, 'score': 1, 'result': result, 'target': 0})
        return ret_values

    def calculate_anchor(self, kpi_name, kpi_line, relevant_scif, general_filters):
        scene = self.find_MSL(relevant_scif)[0]
        allowed_edges = self.read_cell_from_line(kpi_line, Const.EDGES)
        potential_ends = self.get_kpi_line_filters(kpi_line)
        potential_ends = [{key: val} for key, vals in potential_ends.items() for val in vals]
        sub_filters = dict(general_filters)
        for potential_end in potential_ends:
            sub_filters = dict(general_filters)
            sub_filters.update(potential_end)
            mod_kpi_line = kpi_line[[col for col in kpi_line.index if 'param' not in col.lower() and
                                            'value' not in col.lower()]]
            all_results = self.base_adjacency(kpi_name, mod_kpi_line, relevant_scif, sub_filters, use_allowed=0)
            res_dict = {}
            for _, ratio in all_results.values():
                pass_fail = 0
                if ratio <= .25:
                    pass_fail = 1
                res_dict[potential_end.values()[0]] = pass_fail

        ''' Here begins the truly awful- in order supply the client requested strings, I think
            this needs to be hardcoded.... :( If not here, then elsewhere'''
        if kpi_name == 'Do Kid AND ASH Both Anchor End of Category?':
            total = sum(res_dict.values())
            if total == 2:
                result = 'YES both Kid and ASH anchor'
            elif res_dict[[x for x in res_dict.keys() if 'ash' in x.lower()][0]] == 1:
                result = 'Only ASH Anchors'
            elif total == 1:
                result = 'Only Kid Anchors'
            else:
                result = 'Neither Kid or ASH Anchors'

            # result_fk = self.result_values_dict[result]
            kwargs = {'score': 1, 'result': result, 'target': 0}
            return kwargs


    # i think this should probably be removed now and replaced with calculate_vertical_block_adjacencies() - hunter
    def adjacency_by_block(self, kpi_name, kpi_line, relevant_scif, general_filters):
        ''' variant limited by block '''
        for scene in relevant_scif.scene_fk.unique():
            scene_filter = {'scene_fk': scene}
            mpis = self.filter_df(self.mpis, scene_filter)
            allowed = {'product_type': ['Other', 'Empty']}
            filter = {'sub_category_local_name': 'SWEET ROLL DOUGH'}
            items = set(self.filter_df(mpis, filter)['scene_match_fk'].values)
            allowed_items = set(self.filter_df(mpis, allowed)['scene_match_fk'].values)
            if not (items):
                return

            graph, blocks = self.block.network_x_block_together2(filter, location=scene_filter,
                                                                  additional={'allowed_products_filters': allowed,
                                                                              'include_stacking': False})
            all_graph = AdjacencyGraph(mpis, None, self.products,
                                       product_attributes=['rect_x', 'rect_y'],
                                       name=None, adjacency_overlap_ratio=.4)
            match_to_node = {int(node['match_fk']): 1 for i, node in all_graph.base_adjacency_graph.nodes(data=True)}
            all_nodes_dict = {i: n['match_fk'] for i, n in block.nodes(data=True)}
            for block in blocks:
                block = blocks[0]
                nodes_dict = {i: n['match_fk'] for i, n in block.nodes(data=True)}
                all_points = set()
                for match in nodes_dict.values():
                    all_points.update(all_graph.base_adjacency_graph[match_to_node[match]].keys())
                break

    def calculate_stocking_location(self, kpi_name, kpi_line, relevant_scif, general_filters):
        max_criteria = max([int(col.split(' ')[1]) for col in kpi_line.index if col != Const.KPI_NAME
                           and col != Const.RESULT])
        comp_dict = {}
        for ind, row in self.template[Const.YOGURT_MAP].set_index('Result').iterrows():
            comp_dict[tuple([1 if not pd.isnull(i) else 0 for i in row])] = ind
        results_list = [0 for i in xrange(max_criteria)]
        for i in xrange(1, max_criteria + 1):
            level = 'Criteria {}'.format(i)
            mod_kpi_line = kpi_line[[col for col in kpi_line.index if 'Type' not in col]]
            mod_kpi_line = mod_kpi_line[list([col for col in mod_kpi_line.index if level in col])
                                        + [Const.KPI_NAME, Const.RESULT]]
            mod_kpi_line.index = [col.replace(level, '').strip() for col in mod_kpi_line.index]
            sub_func = self.read_cell_from_line(kpi_line, '{} Type'.format(level))[0]

            if sub_func == Const.BLOCKING:
                score, _, _, _, _ = self.base_block(kpi_name, mod_kpi_line, relevant_scif, {})
            elif sub_func == 'Distribution':
                score = self.distribution(mod_kpi_line)

            results_list[i-1] = score
            if sum(results_list) > 0 and tuple(results_list) in comp_dict:
                break

        result = comp_dict[tuple(results_list)]
        # result_fk = self.result_values_dict[result]
        kwargs = {'score': 1, 'result': result, 'target': 0}
        return kwargs

    def distribution(self, kpi_line):
        filters = self.get_kpi_line_filters(kpi_line)
        filters.update(Const.IGN_STACKING)
        scenes = self.scif.scene_fk.unique()
        if self.read_cell_from_line(kpi_line, 'MSL'):
            scenes = self.find_MSL(self.scif)
        score = 0
        for scene in scenes:
            sub_filters = filters.copy().update({'scene_fk': scene})
            df = self.filter_df(self.scif, filters)
            if not df.empty:
                score = 1
        return score


    def base_block(self, kpi_name, kpi_line, relevant_scif, general_filters):
        score = 0
        blocks = pd.DataFrame()
        result = pd.DataFrame()
        orientation = 'Not Blocked'
        scenes = self.filter_df(self.scif, general_filters).scene_fk.unique()
        if 'template_name' in general_filters:
            del general_filters['template_name']
        mpis_dict = {}
        if self.read_cell_from_line(kpi_line, 'MSL'):
            scenes = self.find_MSL(relevant_scif)
        for scene in scenes:
            scene_filter = {'scene_fk': scene}
            filters = self.get_kpi_line_filters(kpi_line)
            filters.update(general_filters)
            # mpis is only here for debugging purposes
            mpis = self.filter_df(self.mpis, scene_filter)
            mpis = self.filter_df(mpis, filters)
            mpis_dict[scene] = mpis
            if mpis.empty:
                continue
            result = self.block.network_x_block_together(filters, location=scene_filter,
                                                         additional={
                                                                     # 'allowed_products_filters': Const.ALLOWED_FILTERS,
                                                                     'allowed_products_filters': {'product_type': 'Empty'},
                                                                     'include_stacking': False,
                                                                     'check_vertical_horizontal': True})
            blocks = result[result['is_block'] == True]
            if not blocks.empty:
                score = 1
                orientation = blocks.loc[0, 'orientation']
                break

        return score, orientation, mpis_dict, blocks, result

    def calculate_block(self, kpi_name, kpi_line, relevant_scif, general_filters):
        score, orientation, mpis_dict, _, _ = self.base_block(kpi_name, kpi_line, relevant_scif, general_filters)
        # result_fk = self.result_values_dict[orientation]
        kwargs = {'numerator_id': 999, 'numerator_result': score, 'score': score, 'result': orientation,
                  'target': 1}
        return kwargs

    def calculate_tub_block(self, kpi_name, kpi_line, relevant_scif, general_filters):
        score, orientation, mpis_dict, _, _ = self.base_block(kpi_name, kpi_line, relevant_scif, general_filters)
        potential_results = self.get_results_value(kpi_line)
        if score:
            result = [x for x in potential_results if x.lower() in orientation.lower()][0]
        else:
            msl_mpis = mpis_dict[self.find_MSL(relevant_scif)[0]]
            all_mpis = pd.concat(list(mpis_dict.values()))
            if not msl_mpis.empty:
                result = [x for x in potential_results if 'interspersed' in x.lower()][0]
            elif not all_mpis.empty:
                result = [x for x in potential_results if 'shelved' in x.lower()][0]
            else:
                result = [x for x in potential_results if 'distribution' in x.lower()][0]

        # result_fk = self.result_values_dict[orientation]
        kwargs = {'numerator_result': score, 'score': score, 'result': result, 'target': 1}
        return kwargs

    def calculate_yogurt_block(self, kpi_name, kpi_line, relevant_scif, general_filters):
        score, orientation, mpis_dict, blocks, _ = self.base_block(kpi_name, kpi_line, self.scif, general_filters)
        potential_results = self.get_results_value(kpi_line)
        if score:
            if self.find_MSL(relevant_scif)[0] in blocks['scene_fk'].values:
                result = 'Blocked'
            else:
                result = 'Not in MSL for Yogurt'
        elif score == 0:
            if not mpis_dict[self.find_MSL(relevant_scif)[0]].empty:
                result = 'Not Blocked'
            else:
                result = 'Not Blocked and Not in MSL for Yogurt'

        kwargs = {'numerator_result': score, 'score': score, 'result': result, 'target': 1}
        return kwargs

    def calculate_basic_block(self, kpi_name, kpi_line, relevant_scif, general_filters):
        score, _, _, _, _ = self.base_block(kpi_name, kpi_line, self.scif, general_filters)
        if score:
            result = 'Blocked'
        else:
            result = 'Not Blocked'

        kwargs = {'numerator_result': score, 'score': score, 'result': result, 'target': 1}
        return kwargs

    def calculate_vertical_block_adjacencies(self, kpi_name, kpi_line, relevant_scif, general_filters):
        # this could be updated to use base_block() if we don't need to respect unique scene results
        kpi_result = 0
        for scene in relevant_scif.scene_fk.unique():
            scene_filter = {'scene_fk': scene}
            location_filter = {'scene_id': scene}
            mpis = self.filter_df(self.mpis, scene_filter)
            # allowed = {'product_type': ['Other', 'Empty']}
            if kpi_line[Const.TESTED_PARAM] == kpi_line[Const.ANCHOR_PARAM]:
                filters = {kpi_line[Const.ANCHOR_PARAM]: [kpi_line[Const.ANCHOR_VALUE], kpi_line[Const.TESTED_VALUE]]}
            elif kpi_line[Const.TESTED_PARAM] == '':
                filters = {kpi_line[Const.ANCHOR_PARAM]: kpi_line[Const.ANCHOR_VALUE]}
            else:
                filters = {kpi_line[Const.ANCHOR_PARAM]: kpi_line[Const.ANCHOR_VALUE],
                           kpi_line[Const.TESTED_PARAM]: kpi_line[Const.TESTED_VALUE]}
            items = set(self.filter_df(mpis, filters)['scene_match_fk'].values)
            additional = {'minimum_facing_for_block': 2,
                          'check_vertical_horizontal': True}
            # allowed_items = set(self.filter_df(mpis, allowed)['scene_match_fk'].values)
            if not (items):
                break

            block_result = self.block.network_x_block_together(filters, location=location_filter, additional=additional)

            passed_blocks = block_result[block_result['is_block'] is True &
                                         block_result['orientation'] == 'Vertical'].cluster.tolist()

            if passed_blocks and kpi_line[Const.LIST_ATTRIBUTE]:
                match_fk_list = set(match for cluster in passed_blocks for node in cluster.nodes() for match in
                                    cluster.node[node]['group_attributes']['match_fk_list'])

                all_graph = AdjacencyGraph(mpis, None, self.products,
                                           product_attributes=['rect_x', 'rect_y'],
                                           name=None, adjacency_overlap_ratio=.4)
                # associate all nodes in the master graph to their associated match_fks
                match_to_node = {int(node['match_fk']): i for i, node in
                                 all_graph.base_adjacency_graph.nodes(data=True)}
                # create a dict of all match_fks to their corresponding nodes
                node_to_match = {val: key for key, val in match_to_node.items()}
                edge_matches = set(
                    sum([[node_to_match[i] for i in all_graph.base_adjacency_graph[match_to_node[match]].keys()]
                         for match in match_fk_list], []))
                adjacent_matches = edge_matches - match_fk_list
                adj_mpis = mpis[(mpis['scene_match_fk'].isin(adjacent_matches)) &
                                (~mpis['product_type'].isin(['Empty', 'Irrelevant', 'Other', 'POS']))]

                for value in adj_mpis[kpi_line[Const.LIST_ATTRIBUTE]].unique().tolist():
                    if kpi_line[Const.LIST_ATTRIBUTE] == 'brand_name':
                        numerator_fk = adj_mpis[adj_mpis['brand_name'] == value].brand_fk.values[0]
                    else:
                        if value is not None:
                            try:
                                numerator_fk = \
                                self.custom_entity_data[self.custom_entity_data['name'] == value].pk.values[0]
                            except IndexError:
                                Log.warning('Custom entity "{}" does not exist'.format(value))
                                continue
                        else:
                            continue

                    result_dict = self.build_dictionary_for_db_insert(kpi_name=kpi_line[Const.KPI_NAME],
                                                                      numerator_id=numerator_fk, numerator_result=1,
                                                                      result=1, denominator_id=scene,
                                                                      denominator_result=1)
                    self.common.write_to_db_result(**result_dict)
                return
            elif kpi_line[Const.LIST_ATTRIBUTE]:  # return if this is a list_attribute KPI with no passing blocks
                return
            if passed_blocks:  # exit loop if this isn't a list_attribute KPI, but has passing blocks
                kpi_result = 1
                break
        if kpi_line[Const.LIST_ATTRIBUTE]:  # handle cases where there are no relevant products,
            return                          # so we miss the other check above
        template_fk = relevant_scif['template_fk'].values[0]
        result_dict = self.build_dictionary_for_db_insert(kpi_name=kpi_line[Const.KPI_NAME],
                                                          numerator_id=999, numerator_result=kpi_result,
                                                          result=kpi_result, denominator_id=template_fk,
                                                          denominator_result=1)
        self.common.write_to_db_result(**result_dict)
        return

    def calculate_base_measure(self, kpi_name, kpi_line, relevant_scif, general_filters):
        mpis = self.make_mpis(kpi_line, general_filters)
        master_mpis = self.filter_df(self.full_mpis.copy(), Const.IGN_STACKING)
        if sum([1 if 'ignore' in x else 0 for x in kpi_line.index]):
            ign_filter = self.get_kpi_line_filters(kpi_line, 'ignore')
            mpis = self.filter_df(mpis, ign_filter, exclude=1)
            master_mpis = self.filter_df(master_mpis, ign_filter, exclude=1)
        mm_sum = 0
        if mpis.empty:
            return {"score": None}
        for scene in mpis.scene_fk.unique():
            smpis = self.filter_df(mpis, {'scene_fk': scene})
            master_smpis = self.filter_df(master_mpis, {'scene_fk': scene})

            for bay in smpis['bay_number'].unique().tolist():
                bmpis = self.filter_df(smpis, {'bay_number': bay})
                master_bmpis = self.filter_df(master_smpis, {'bay_number': bay})
                num_shelves = len(master_bmpis['shelf_number'].unique().tolist())
                linear_mm = float(bmpis['width_mm_advance'].sum())
                master_linear_mm = float(master_bmpis['width_mm_advance'].sum())
                # if linear_mm / master_linear_mm >= .5:
                #     mm_sum += linear_mm / num_shelves
                mm_sum += (linear_mm / num_shelves) * (linear_mm / master_linear_mm)
        ft_sum = mm_sum / Const.MM_TO_FT
        self.base_measure = ft_sum
        potential_results = self.get_results_value(kpi_line)
        cleaned_results = [(i, x.replace('Feet', '').replace('and', 'ft_sum').replace('>', '<').strip())
                           for i, x in enumerate(potential_results)
                           if '>' in x and '<=' in x]
        result = None
        for i, res in cleaned_results:
            if eval(res):
                result = potential_results[i]
                break
        if not result:
            if ft_sum <= int(cleaned_results[0][1].split(' ')[0]):
                result = potential_results[0]
            else:
                result = potential_results[-1]

        # result_fk = self.result_values_dict[result]
        kwargs = {'numerator_result': ft_sum, 'score': 1, 'result': result,
                  'target': None}
        return kwargs

    def calculate_count_of_shelves(self, kpi_name, kpi_line, relevant_scif, general_filters):
        mpis = self.filter_df(self.full_mpis, general_filters)
        num_shelves = int(len(mpis.groupby(['scene_fk', 'bay_number', 'shelf_number'])))
        potential_results = self.get_results_value(kpi_line)
        result = self.semi_numerical_results(num_shelves, potential_results, form='{} Shelves')
        # result_fk = self.result_values_dict[result]
        kwargs = {'numerator_result': num_shelves, 'score': 1, 'result': result,
                  'target': None}
        return kwargs

    def calculate_product_orientation(self, kpi_name, kpi_line, relevant_scif, general_filters):
        # filters = self.get_kpi_line_filters(kpi_line)
        filters = {}
        filters.update(general_filters)
        mpip = self.filter_df(self.mpip, filters)
        orients = [x for x in mpip['image_direction'].unique() if x is not None]
        result = 'Mix of orientation'
        if len(orients) == 1:
            orient = orients[0].lower()
            if self.is_in_string(['front', 'back'], orient):
                result = 'ALL Cans stocked on side'
            else:
                result = 'ALL Cans stocked on end'

        # base_ft = self.dependencies[self.dependency_lookup[kpi_name]]
        # num = [x for x in base_ft.split(' ') if self.is_int(x)][-1]
        potential_results = self.get_results_value(kpi_line)
        extracted_nums = {int(x.split(' ')[0]) for x in potential_results}
        # num = min_ft if num < min_ft else num
        match = self.nearest_number(self.base_measure, extracted_nums)
        base = '{} FT SET'.format(match)
        result = '{} - {}'.format(base, result)
        kwargs = {'score': 1, 'result': result}
        return kwargs

    @staticmethod
    def nearest_number(num, num_list):
        min_diff = float('inf')
        for i in num_list:
            diff = abs(i - num)
            if diff < min_diff:
                min_diff = diff
                match = i
        return match


    @staticmethod
    def is_int(x):
        try:
            int(x)
            return True
        except:
            return False

    @staticmethod
    def is_in_string(substrings, string):
        ret = False
        for substring in substrings:
            if substring in string:
                ret = True
                break
        return ret

    def base_count(self, kpi_name, kpi_line, relevant_scif, general_filters, min=0):
        filters = self.get_kpi_line_filters(kpi_line)
        filters.update(general_filters)
        count_col = self.read_cell_from_line(kpi_line, 'count_attribute')
        scif = self.filter_df(relevant_scif, filters)
        scif = scif[scif['facings_ign_stack'] >= min]
        if 'Allowed' in kpi_line.index:
            allowed = set(self.read_cell_from_line(kpi_line, 'Allowed'))
            scif = scif[scif[count_col].isin(allowed)]
        if scif.empty:
            return {}
        count = len(scif[count_col[0]].unique())
        return count

    def calculate_count_of(self, kpi_name, kpi_line, relevant_scif, general_filters):
        count = self.base_count(kpi_name, kpi_line, relevant_scif, general_filters)
        potential_results = self.get_results_value(kpi_line)
        # result = self.inequality_results(count, potential_results, kpi_name)
        result = self.semi_numerical_results(count, potential_results)
        kwargs = {'numerator_result': count, 'score': 1, 'result': result, 'target': 0}
        return kwargs

    def calculate_set_count(self, kpi_name, kpi_line, relevant_scif, general_filters):
        min = self.read_cell_from_line(kpi_line, 'Min')
        count = self.base_count(kpi_name, kpi_line, relevant_scif, general_filters, min=min)
        den = 0
        result = count
        if min != 0:
            potential_results = self.get_results_value(kpi_line)
            den = self.base_count(kpi_name, kpi_line, relevant_scif, general_filters, min=0)
            ratio, score = self.ratio_score(count, den)
            result = self.inequality_results(ratio, potential_results, kpi_name)
        kwargs = {'numerator_result': count, 'score': 1, 'result': result, 'target': 0, 'denominator_result': den}
        return kwargs

    def calculate_sos_percent(self, kpi_name, kpi_line, relevant_scif, general_filters):
        ratio, num, den = self.sos_with_num_and_dem(kpi_line, relevant_scif, general_filters, 'facings_ign_stack')
        if ratio is not None:
            potential_results = self.get_results_value(kpi_line)
            result = self.inequality_results(ratio, potential_results, kpi_name)
        kwargs = {'numerator_result': num, 'score': ratio, 'result': result, 'denominator_result': den, 'target': 0}
        return kwargs

    def graph(self, kpi_name, kpi_line, relevant_scif, general_filters):
        x = Block(self.data_provider)
        relevant_filter = {'Segment': 'Greek Yogurt'}
        allowed_filter = {'product_type': ['Empty', 'Other']}
        scene_filter = {'scene_fk': 78}
        res = x.network_x_block_together(relevant_filter, location=scene_filter,
                                         additional={'allowed_products_filters': allowed_filter, 'include_stacking': False})


    def semi_numerical_results(self, val, potential_results, form='{}'):
        min_cap, max_cap = self.find_caps(potential_results)
        if val < min_cap:
            result = potential_results[0]
        elif val > max_cap:
            result = potential_results[-1]
        else:
            result = form.format(val)
        return result

    def make_mpis(self, kpi_line, general_filters, ign_stacking=1, use_full_mpis=0):
        mpis = self.full_mpis if use_full_mpis else self.mpis
        filters = self.get_kpi_line_filters(kpi_line)
        filters.update(general_filters)
        if ign_stacking:
            filters.update(Const.IGN_STACKING)
        return self.filter_df(self.mpis, filters)

    def prune_edges(self, g, allowed_edges, keep_or_cut='keep'):
        for node in g.nodes():
            for edge_id, edge in g[node].items():
                for edge_dir in edge.values():
                    if keep_or_cut == 'keep':
                        if edge_dir not in allowed_edges:
                            del g[node][edge_id]
                    else:
                        if edge_dir in allowed_edges:
                            del g[node][edge_id]
        return g

    def find_caps(self, potential_results):
        ''' This function only handles bookended lists, eg ['not num', '1', '2', '3', 'not num']'''
        is_int = False
        max_cap = potential_results[-1]
        for i, res in enumerate(potential_results):
            prev = is_int
            try:
                int(res)
                is_int = True
            except:
                is_int = False

            if prev is False and is_int is True:
                min_cap = int(res)
            elif prev is True and is_int is False:
                max_cap = potential_results[i-1]
        return int(min_cap), int(max_cap)

    def inequality_results(self, result, potential_results, kpi, mid='-'):
        '''
        handles this sort of result list <25%, 25-50%, 50%-75%, >=75%
        result should be in percentage form, not decimal. eg 75 not .75
        '''
        inequality_results = []
        for res in potential_results:
            if mid in res:
                a, b = res.split(mid)
                inequality = '{} <= result < {}'.format(a, b)
            else:
                inequality = 'result {}'.format(res)
            if '%' in inequality:
                inequality = inequality.replace('%', '')
                result = result
            if eval(inequality):
                return res
        Log.error('Result "{}" not found in potential results "{}" in kpi "{}"'.format(res, potential_results, kpi))





    @staticmethod
    def filter_df(df, filters, exclude=0):
        cols = set(df.columns)
        for key, val in filters.items():
            if key not in cols:
                return pd.DataFrame()
            if not isinstance(val, list):
                val = [val]
            if exclude:
                df = df[~df[key].isin(val)]
            else:
                df = df[df[key].isin(val)]
        return df

    @staticmethod
    def filter_mask(df, filters, exclude=0):
        mask = []
        for key, val in filters.items():
            if not isinstance(val, list):
                val = [val]
            if exclude:
                mask.append(~df[key].isin(val))
            else:
                mask.append(df[key].isin(val))
        return reduce((lambda x, y: x & y), mask)

    @staticmethod
    def filter_join(filters):
        final_filter = defaultdict(list)
        filters = reduce((lambda x, y: x + y.items() if isinstance(x, list) else x.items() + y.items()), filters)
        for (key, val) in filters:
            final_filter[key].append(val)
        return final_filter

    @staticmethod
    def ratio_score(num, den, target=None):
        ratio = 0
        if den:
            ratio = round(num*100.0/den, 2)
        score = 1 if ratio >= target and target else 0
        return ratio, score

    @staticmethod
    def read_cell_from_line(line, col):
        try:
            val = line[col] if not pd.isnull(line[col]) else []
        except:
            val = []
        if val:
            if hasattr(val, 'split'):
                if ', ' in val:
                    val = val.split(', ')
                elif ',' in val:
                    val = val.split(',')
            if not isinstance(val, list):
                val = [val]

        return val

    def find_MSL(self, scif):
        scif = scif[scif['Super Category'] == self.super_cat]
        return [scif.groupby('scene_fk')['facings_ign_stack'].sum().sort_values().index[0]]

    def get_results_value(self, kpi_line):
        return self.splitter(self.res_dict[kpi_line[Const.RESULT]]['Results Value'],
                             self.res_dict[kpi_line[Const.RESULT]]['Delimiter'])

    def get_kpi_line_filters(self, kpi_orig, name=''):
        kpi_line = kpi_orig.copy()
        if name:
            name = name.lower() + ' '
        filters = defaultdict(list)
        attribs = [x.lower() for x in kpi_line.index]
        kpi_line.index = attribs
        c = 1
        while 1:
            if '{}param {}'.format(name, c) in attribs and kpi_line['{}param {}'.format(name, c)]\
                    and not pd.isnull(kpi_line['{}param {}'.format(name, c)]):
                filters[kpi_line['{}param {}'.format(name, c)]] += self.splitter(kpi_line['{}value {}'.format(name, c)])
            else:
                if c > 3:  # just in case someone inexplicably chose a nonlinear numbering format.
                    break
            c += 1
        return filters

    @staticmethod
    def splitter(text_str, delimiter=','):
        ret = [text_str]
        if hasattr(text_str, 'split'):
            ret = text_str.split(delimiter)
        return ret

    def sos_with_num_and_dem(self, kpi_line, relevant_scif, general_filters, facings_field):
        num_filters = self.get_kpi_line_filters(kpi_line, name='numerator')
        den_filters = self.get_kpi_line_filters(kpi_line, name='denominator')
        num_filters.update(general_filters)
        den_filters.update(general_filters)
        num_scif = self.filter_df(relevant_scif, num_filters)
        den_scif = self.filter_df(relevant_scif, den_filters)
        den = den_scif[facings_field].sum()
        num = num_scif[facings_field].sum()
        if den:
            ratio = round((num / float(den))*100, 2)
        else:
            ratio = 0

        return ratio, num, den

    def dependency_reorder(self):
        kpis = self.template[Const.KPIS].copy()
        name_to_index = kpis.reset_index().set_index(Const.KPI_NAME)['index'].to_dict()
        dependent_index = list(kpis[kpis[Const.DEPENDENT].notnull()].index)
        kpis_index = list(set(kpis.index) - set(dependent_index))
        set_index = set(kpis_index)
        c = 0
        while dependent_index:
            i = dependent_index.pop(0)
            kpi = kpis.loc[i, Const.KPI_NAME]
            dependencies = self.read_cell_from_line(kpis.loc[i, :], Const.DEPENDENT)
            met = True
            for dependency in dependencies:
                if name_to_index[dependency] not in set_index:
                    met = False
            if met:
                kpis_index.append(i)
                set_index.add(i)
                c = 0
            else:
                dependent_index.append(i)
                c += 1
                if c > kpis.shape[0] * 1.1:
                    Log.error('Circular Dependency Found: KPIs Affected {}'.format(
                        [kpis.loc[i, Const.KPI_NAME] for i in dependent_index]))
                    break
        self.template[Const.KPIS] = kpis.reindex(index=pd.Index(kpis_index)).reset_index(drop=True)

    def get_kpi_function(self, kpi_type, result):
        """
        transfers every kpi to its own function
        :param kpi_type: value from "sheet" column in the main sheet
        :return: function
        """
        if kpi_type == Const.AGGREGATION:
            return self.calculate_sos
        elif kpi_type == Const.TMB:
            return self.calculate_topmiddlebottom
        elif kpi_type == Const.ADJACENCY:
            return self.calculate_adjacency
        elif kpi_type == Const.ANCHOR_LIST:
            return self.calculate_adjacency
        elif kpi_type == Const.ANCHOR:
            return self.calculate_anchor
        elif kpi_type == Const.IADJACENCY:
            return self.calculate_new_integrated_adjacency
        elif kpi_type == Const.STOCKING:
            return self.calculate_stocking_location
        elif kpi_type == Const.BLOCKING:
            if result.lower() == kpi_type.lower():
                return self.calculate_block
            elif result.lower() == 'blocking tub':
                return self.calculate_tub_block
            elif result.lower() == 'blocking yogurt':
                return self.calculate_yogurt_block
            elif result.lower() == 'basic block':
                return self.calculate_basic_block
        elif kpi_type == Const.BASE_MEASURE:
            return self.calculate_base_measure
        elif kpi_type == Const.ORIENT:
            return self.calculate_product_orientation
        elif kpi_type == Const.COUNT_SHELVES:
            return self.calculate_count_of_shelves
        elif kpi_type == Const.COUNT:
            return self.calculate_count_of
        elif kpi_type == Const.SET_COUNT:
            return self.calculate_set_count
        elif kpi_type == Const.ORIENT:
            return self.calculate_orientation
        elif kpi_type == Const.PRESENCE:
            return self.calculate_presence
        elif kpi_type == Const.PRESENCE_WITHIN_BAY:
            return self.calculate_presence_within_bay
        elif kpi_type == Const.PERCENT:
            return self.calculate_sos_percent
        else:
            Log.warning("The value '{}' in column sheet in the template is not recognized".format(kpi_type))
            return None

    def make_result_values_dict(self):
        query = "SELECT * FROM static.kpi_result_value;"
        return pd.read_sql_query(query, self.ps_data_provider.rds_conn.db).set_index('value')['pk'].to_dict()

    def make_att_dict(self):
        df = pd.read_excel(Const.DICTIONARY_PATH)
        df = df[(df['unknown'] != 'Y') & (df['not_final'] != 'Y')].set_index('Name')
        params = {key: self.get_kpi_line_filters(row) for key, row in df.iterrows()}
        return params

    def create_special_scif(self, scif, fake_cat=0):
        priv = scif[scif['Private Label'] == 'Y']
        scif = scif[~scif.index.isin(priv.index)]
        priv = priv.groupby('scene_id')[Const.PRIV_SCIF_COLS + ['scene_id']].sum()
        priv['item_id'], priv['product_fk'] = [Const.PRIV_LABEL_SKU] * 2
        priv['brand_fk'] = Const.PRIV_LABEL_BRAND
        priv['manufacturer_fk'] = Const.PRIV_LABEL_MAN
        priv['product_name'], priv['manufacturer_name'], priv['brand_name'] = [Const.PRIV_LABEL_NAME] * 3
        if fake_cat:
            cats = scif[~np.isnan(scif['category_fk'])].reset_index()
            list_cats = list(cats['category_fk'].unique())
            if np.isnan(list_cats[0]) or list_cats[0] is None:
                Log.error('Category error in Session {} create_special_scif'.format(self.session_uid))
            else:
                priv['category'], priv['category_fk'] = cats.loc[0, 'category'], cats.loc[0, 'category_fk']
        return pd.concat([scif, priv]).reset_index()[scif.columns]

    def create_mpip(self):
        query = '''
                Select mpip.*, pi.image_direction, t.name as 'template_name'
                from probedata.match_product_in_probe mpip
                left join static_new.product_image pi on mpip.product_image_fk = pi.pk
                left join probedata.probe pr on mpip.probe_fk = pr.pk
                left join probedata.scene sc on pr.scene_fk = sc.pk
                left join static.template t on sc.template_fk = t.pk
                where sc.session_uid = '{}'
                '''.format(self.session_uid)
        return pd.read_sql_query(query, self.ps_data_provider.rds_conn.db)\
                                .merge(self.products, how='left', on='product_fk', suffixes=['', '_p'])


        # self.data_provider.add_resource_from_table('mpip', 'probedata', 'match_product_in_probe', '*',
        #                                            where='''pk in (select distinct p.pk
        #                                                     from probedata.probe p
        #                                                     left join probedata.scene s on p.scene_fk = s.pk
        #                                                     where s.session_uid = '{}')
        #                                                     '''.format(self.session_uid))
        # self.data_provider.add_resource_from_table('prod_img', 'static_new', 'product_image', ['pk', 'image_direction'])
        # mpip = self.data_provider['mpip'].drop('pk', axis=1).merge(self.data_provider['prod_img'],
        #                                                           left_on='product_image_fk', right_on='pk')
        return mpip


    def write_to_db(self, kpi_name, score=0, result=None, target=None, numerator_result=0,
                    denominator_result=None, numerator_id=999, denominator_id=999, failed=0):
        """
        writes result in the DB
        :param kpi_name: str
        :param score: float
        :param display_text: str
        :param result: str
        :param threshold: int
        """
        kpi_fk = self.common.get_kpi_fk_by_kpi_type(kpi_name)
        if not np.isnan(self.common.kpi_static_data[self.common.kpi_static_data['pk'] == kpi_fk]
                        ['kpi_result_type_fk'].iloc[0]):
            if not failed:
                result = self.result_values_dict[result]

        self.common.write_to_db_result(fk=kpi_fk, score=score, result=result, should_enter=True, target=target,
                                       numerator_result=numerator_result, denominator_result=denominator_result,
                                       numerator_id=numerator_id, denominator_id=denominator_id)
