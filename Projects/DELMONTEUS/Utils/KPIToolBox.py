
import pandas as pd
import numpy as np
from functools import reduce
from collections import defaultdict, Counter

from Trax.Algo.Calculations.Core.DataProvider import Data
from Trax.Algo.Calculations.Core.Utils import Validation
from Trax.Utils.Logging.Logger import Log
from Projects.DELMONTEUS.Utils.Const import Const
from KPIUtils_v2.GlobalDataProvider.PsDataProvider import PsDataProvider

from KPIUtils_v2.Calculations.BlockCalculations import Block
from Trax.Algo.Calculations.Core.GraphicalModel.AdjacencyGraphs import AdjacencyGraph


from networkx import nx



__author__ = 'Sam'
# if you're looking for template path check kpigenerator.find_template

class ToolBox:

    def __init__(self, data_provider, output, common):
        self.common = common
        self.output = output
        self.data_provider = data_provider

        # ----------- fix for nan types in dataprovider -----------
        all_products = self.data_provider._static_data_provider.all_products.where(
            (pd.notnull(self.data_provider._static_data_provider.all_products)), None)
        self.data_provider._set_all_products(all_products)
        self.data_provider._init_session_data(None, True)
        self.data_provider._init_report_data(self.data_provider.session_uid)
        self.data_provider._init_reporting_data(self.data_provider.session_id)
        # ----------- fix for nan types in dataprovider -----------

        self.block = Block(self.data_provider)
        self.project_name = self.data_provider.project_name
        self.session_uid = self.data_provider.session_uid
        self.templates = self.data_provider.all_templates
        self.ps_data_provider = PsDataProvider(self.data_provider, self.output)
        self.result_values_dict = self.make_result_values_dict()
        self.store_assortment = self.ps_data_provider.get_store_assortment()
        self.store_sos_policies = self.ps_data_provider.get_store_policies()
        self.labels = self.ps_data_provider.get_labels()
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
        self.tmb_map = pd.read_excel(Const.TMB_MAP_PATH).set_index('Num Shelves').to_dict('index')
        self.template = {}
        self.res_dict = {}
        self.dependencies = {}
        self.dependency_lookup = {}
        self.base_measure = None
        self.global_fail = 0

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
            self.global_fail = 0
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

        # print(kpi_name)
        # if kpi_name != 'Do Kid AND ASH Both Anchor End of Category?':
        # if kpi_name != 'In the MSL for Yogurt, which of the following is adjacent to Kite Hill?':
        # if kpi_name not in ('Where are Progresso RTS Light facings shelved?'):
        #     return

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
        except Exception as e:
            if self.global_fail:
                all_kwargs = [{'score': 0, 'result': "Not Applicable", 'failed': 0}]
                Log.warning('kpi "{}" failed to calculate in super category "{}"'.format(kpi_name, self.super_cat))

            else:
                all_kwargs = [{'score': 0, 'result': None, 'failed': 1}]
                Log.error('kpi "{}" failed in super category "{}," error: "{}"'.format(kpi_name, self.super_cat, e))

        finally:
            if not isinstance(all_kwargs, list) or not all_kwargs:
                all_kwargs = [all_kwargs]
            for kwargs in all_kwargs:
                if not kwargs or kwargs['score'] is None:
                    kwargs = {'score': 0, 'result': 'Not Applicable', 'failed': 0}
                self.write_to_db(kpi_name, **kwargs)
                self.dependencies[kpi_name] = kwargs['result']

    def flag_failures(self):
        for kpi, val in self.dependencies.items():
            if val is None:
                Log.warning('Warning: KPI "{}" not run for session "{}"'.format(kpi, self.session_uid))

    def calculate_sos(self, kpi_name, kpi_line, relevant_scif, general_filters):
        super_cats = relevant_scif['Super Category'].unique().tolist()
        for super_cat in super_cats:
            if not super_cat or pd.isnull(super_cat):
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
        filters = self.get_kpi_line_filters(kpi_line)
        relevant_scif = self.filter_df(relevant_scif, filters)
        relevant_scif = relevant_scif[relevant_scif['facings_ign_stack'] >= 1]
        if relevant_scif.empty:
            return
        filters.update(general_filters)
        for scene in relevant_scif.scene_id.unique():
            filters['scene_fk'] = scene
            general_filters['scene_fk'] = scene

            bay_shelf = self.filter_df(self.full_mpis, general_filters).set_index('bay_number')\
                                          .groupby(level=0)[['shelf_number', 'shelf_number_from_bottom']].max()
            bay_max_shelf = bay_shelf['shelf_number'].to_dict()
            bay_shelf['shelf_offset'] = bay_shelf['shelf_number_from_bottom'] - bay_shelf['shelf_number']
            shelf_offset = bay_shelf['shelf_offset'].to_dict()

            mpis = self.filter_df(self.mpis, filters)
            mpis = self.filter_df(mpis, {'stacking_layer': 1})
            if mpis.empty:
                continue
            grouped_mpis = mpis.set_index('bay_number').groupby(level=0)

            for bay, shelves in grouped_mpis:
                shelf_no = bay_max_shelf[bay] + shelf_offset[bay] if shelf_offset[bay] < 0 else bay_max_shelf[bay]
                if shelf_no not in self.tmb_map:
                    Log.warning('bay "{}" is a problem in kpi "{}" in session "{}"'.format(bay, kpi_name, self.session_uid))
                    continue
                sub_map = self.tmb_map[shelf_no]
                # shelf_with_most = shelves.groupby('shelf_number_from_bottom')[shelves.columns[0]].count()\
                #     .sort_values().index[-1]
                # locations.add(sub_map[shelf_with_most])
                for shelf in shelves['shelf_number_from_bottom'].unique():
                    shelf = shelf - shelf_offset[bay] if shelf_offset[bay] > 0 else shelf
                    locations.add(sub_map[shelf])
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

    def base_adj_graph(self, scene, kpi_line, general_filters, use_allowed=0, gmi_only=0, super_cat_only=0,
                       additional_attributes=None, item_filters={}):
        product_attributes = ['rect_x', 'rect_y']
        if additional_attributes is not None:
            product_attributes = product_attributes + additional_attributes
        filters = self.get_kpi_line_filters(kpi_line)
        filters.update(general_filters)
        mpis_filter = {'scene_fk': scene, 'stacking_layer': 1}
        if super_cat_only:
            mpis_filter.update({'Super Category': self.super_cat})
        if gmi_only:
            mpis_filter.update({'manufacturer_name': 'GENERAL MILLS'})
        mpis = self.filter_df(self.mpis, mpis_filter)
        items = self.filter_df(mpis, filters)
        items = self.filter_df(items, item_filters)
        if items.empty:
            return None, None, None, None
        items = set(items['scene_match_fk'].values)
        if use_allowed:
            # allowed = Const.ALLOWED_FILTERS
            allowed = {'product_type': ['Empty']}
            allowed_items = set(self.filter_df(mpis, allowed)['scene_match_fk'].values)
            items.update(allowed_items)
        all_graph = AdjacencyGraph(mpis, None, self.products,
                                   product_attributes=product_attributes + list(filters.keys()),
                                   name=None, adjacency_overlap_ratio=.4)
        return items, mpis, all_graph, filters

    def calculate_sequence(self, kpi_name, kpi_line, relevant_scif, general_filters):
        # this attribute should be pulled from the template once the template is updated
        sequence_attribute = 'GMI_SEGMENT'  # value for testing since GMI_Segment isn't coded :(

        # this might affect the max number of facings in each block, not sure - needs testing
        use_allowed = 1
        kwargs_list = []
        for scene in relevant_scif.scene_fk.unique():
            # create a master adjacency graph of all relevant products in the scene
            items, mpis, all_graph, filters = self.base_adj_graph(scene, kpi_line, general_filters,
                                                                  use_allowed=use_allowed, gmi_only=0,
                                                                  additional_attributes=[sequence_attribute])

            # make a dataframe of matching (filtered) mpis data
            if not items:
                continue
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
            ordered_list = max_blocks.sort_values('x_coordinate', ascending=True)[sequence_attribute].tolist()
            potential_results = self.get_results_value(kpi_line)
            result = ' --> '.join(ordered_list)
            if result not in potential_results:
                result = ' --> '.join(ordered_list[::-1])
                if result not in potential_results:
                    result = 'Other'

            kwargs_list.append({'result': result, 'score': 1})
        return kwargs_list

    def base_adjacency(self, kpi_name, kpi_line, relevant_scif, general_filters, limit_potential=1, use_allowed=1,
                       item_filters={}, col_list=['brand_name']):
        allowed_edges = self.read_cell_from_line(kpi_line, Const.EDGES)
        scenes = relevant_scif.scene_fk.unique()
        if self.read_cell_from_line(kpi_line, 'MSL'):
            scenes = self.find_MSL(relevant_scif)
        all_results = {}
        for scene in scenes:
            items, mpis, all_graph, filters = self.base_adj_graph(scene, kpi_line, general_filters,
                                                                  item_filters=item_filters, use_allowed=use_allowed,
                                                                  gmi_only=0)
            if not items:
                continue
            z_debug = mpis[mpis['scene_match_fk'].isin(items)]
            for edge_dir in allowed_edges:
                g = self.prune_edges(all_graph.base_adjacency_graph.copy(), [edge_dir])

                match_to_node = {int(node['match_fk']): i for i, node in g.nodes(data=True)}
                node_to_match = {val: key for key, val in match_to_node.items()}
                edge_matches = set(sum([[node_to_match[i] for i in g[match_to_node[item]].keys()]
                                        for item in items], []))
                adjacent_items = edge_matches - items
                adj_mpis = mpis[(mpis['scene_match_fk'].isin(adjacent_items))]
                adj_mpis = self.filter_df(adj_mpis, Const.SOS_EXCLUDE_FILTERS, exclude=1)
                adj_mpis = self.filter_df(adj_mpis, {'product_type': 'Empty'}, exclude=1)
                # adj_mpis = self.filter_df(adj_mpis, Const.ALLOWED_FILTERS, exclude=1)

                adjacent_sections = list(set(sum([list(adj_mpis[col].unique()) for col in col_list], [])))

                if limit_potential:
                    adjacent_sections = self.results_contorter(adj_mpis, kpi_line)
                #     potential_results = set(self.get_results_value(kpi_line))
                #     adjacent_sections = list(adjacent_sections & potential_results)
                all_results[edge_dir] = [adjacent_sections, len(adjacent_items)/float(len(items))]
        return all_results

    def results_contorter(self, df, kpi_line):
        found_results = []
        raw_results = self.get_results_value(kpi_line)
        for res in raw_results:
            if res == Const.END_OF_CAT:
                continue
            filters = self.att_dict[res.lower()]
            sub_df = self.filter_df(df, filters)
            if not sub_df.empty:
                found_results.append(res)
        return found_results

    def calculate_adjacency_list(self, kpi_name, kpi_line, relevant_scif, general_filters):
        max_block = self.read_cell_from_line(kpi_line, Const.MAX_BLOCK)
        item_filters = {}
        kwargs_list = []

        if max_block:
            _, _, _, _, blocks = self.base_block(kpi_name, kpi_line, relevant_scif, general_filters, check_orient=False)
            block = blocks.sort_values('facing_percentage').reset_index().iloc[-1, :]['cluster']
            ids = sum([node['group_attributes']['match_fk_list'] for i, node in block.node(data=True)], [])
            item_filters = {'scene_match_fk': ids}

        if Const.END_OF_CAT in self.get_results_value(kpi_line):
            anchor_filters = item_filters if item_filters else self.get_kpi_line_filters(kpi_line)
            anchor = self.anchor_base(general_filters, anchor_filters, relevant_scif['scene_fk'].unique(), 1)
            if sum(anchor.values()) > 0:
                kwargs_list.append({'score': 1, 'result': Const.END_OF_CAT, 'target': 1})

        all_results = self.base_adjacency(kpi_name, kpi_line, relevant_scif, general_filters, item_filters=item_filters)
        for result in sum([x for x, y in all_results.values()], []):
            # result_fk = self.result_values_dict[result]
            kwargs_list.append({'score': 1, 'result': result, 'target': 1})

        return kwargs_list

    def anchor_base(self, general_filters, potential_end, scenes, min_shelves):
        results = {}
        cat_filters = dict(general_filters)
        results['left'], results['right'] = 0, 0
        for scene in scenes:
            cat_filters['scene_fk'] = scene
            cat_mpis = self.filter_df(self.mpis, cat_filters)
            cat_mpis = self.filter_df(cat_mpis, Const.ALLOWED_FILTERS, exclude=1)
            cat_mpis = self.filter_df(cat_mpis, {'stacking_layer': 1})
            bays = {'left': cat_mpis['bay_number'].min(), 'right': cat_mpis['bay_number'].max()}
            for dir, bay in bays.items():
                bay_mpis = self.filter_df(cat_mpis, {'bay_number': bay})

                smpis = self.filter_df(bay_mpis, potential_end).groupby(['scene_fk', 'bay_number', 'shelf_number']) \
                    ['facing_sequence_number'].min()
                if smpis.empty:
                    continue
                rmpis = self.filter_df(bay_mpis, potential_end, exclude=1) \
                    .groupby(['scene_fk', 'bay_number', 'shelf_number'])['facing_sequence_number'].min()
                locs = pd.concat([smpis, rmpis], axis=1)
                locs.columns = ['A', 'B']
                locs.dropna(subset=['A'], inplace=True)
                if dir == 'left':
                    locs.fillna(float('inf'), inplace=True)
                    if sum(locs['A'] < locs['B']) >= min_shelves:
                        results[dir] = 1
                else:
                    locs.fillna(0, inplace=True)
                    if sum(locs['A'] > locs['B']) >= min_shelves:
                        results[dir] = 1
        return results

    def calculate_anchor(self, kpi_name, kpi_line, relevant_scif, general_filters):
        map = self.template['Anchor Map'].fillna(0).replace('Y', 1).set_index('Result').to_dict('index')
        potential_ends = self.get_kpi_line_filters(kpi_line)
        potential_ends = [{key: val} for key, vals in potential_ends.items() for val in vals]
        cat_filters = {'Super Category': self.super_cat}
        cat_filters.update(general_filters)
        scenes = relevant_scif['scene_fk'].unique()
        res_dict = {}
        for potential_end in potential_ends:
            results = self.anchor_base(general_filters, potential_end, scenes, 2)
            for dir, res in results.items():
                res_dict['{}_{}'.format(dir, potential_end.values()[0])] = res

        kwargs = None
        for result, conditions in map.items():
            is_true = 1
            for key, val in conditions.items():
                if val != res_dict[key]:
                    is_true = 0
            if is_true:
                kwargs = {'score': 1, 'result': result, 'target': 0}
                break

        return kwargs

    def base_block(self, kpi_name, kpi_line, relevant_scif, general_filters_base, check_orient=1, other=0, filters={},
                   multi=0):
        result = pd.DataFrame()
        general_filters = dict(general_filters_base)
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
            score = 0
            scene_filter = {'scene_fk': scene}
            if not filters:
                filters = self.get_kpi_line_filters(kpi_line)
                filters.update(general_filters)
            # mpis is only here for debugging purposes
            mpis = self.filter_df(self.mpis, scene_filter)
            mpis = self.filter_df(mpis, filters)
            mpis = self.filter_df(mpis, {'stacking_layer': 1})
            mpis_dict[scene] = mpis
            if mpis.empty:
                score = -1
                continue
            allowed_filter = Const.ALLOWED_FILTERS
            if not other:
                allowed_filter = {'product_type': 'Empty'}
            result = pd.concat([result, self.block.network_x_block_together(filters, location=scene_filter,
                                                         additional={
                                                                     'allowed_products_filters': allowed_filter,
                                                                     'include_stacking': False,
                                                                     'check_vertical_horizontal': check_orient,
                                                                     'minimum_facing_for_block': 1})])
            blocks = result[result['is_block'] == True]
            if not blocks.empty and not multi:
                score = 1
                orientation = blocks.loc[0, 'orientation']
                break
        if score == -1:
            self.global_fail = 1
            raise TypeError('No Data Found fo kpi "'.format(kpi_name))
        return score, orientation, mpis_dict, blocks, result

    def calculate_block(self, kpi_name, kpi_line, relevant_scif, general_filters):
        score, orientation, mpis_dict, _, _ = self.base_block(kpi_name, kpi_line, relevant_scif, general_filters)
        # result_fk = self.result_values_dict[orientation]
        kwargs = {'numerator_id': 999, 'numerator_result': score, 'score': score, 'result': orientation,
                  'target': 1}
        return kwargs

    def calculate_basic_block(self, kpi_name, kpi_line, relevant_scif, general_filters):
        score, _, _, _, _ = self.base_block(kpi_name, kpi_line, self.scif, general_filters)
        if score:
            result = 'Blocked'
        else:
            result = 'Not Blocked'

        kwargs = {'numerator_result': score, 'score': score, 'result': result, 'target': 1}
        return kwargs

    def calculate_multi_block(self, kpi_name, kpi_line, relevant_scif, general_filters):
        score, orientation, mpis_dict, blocks, results = self.base_block(kpi_name, kpi_line, self.scif, general_filters,
                                                                         multi=1)
        mpis = self.mpis[self.mpis['stacking_layer'] == 1]
        segs = self.get_kpi_line_filters(kpi_line)['GMI_SEGMENT']
        seg_count = {}
        seg_count = {seg: mpis[mpis['GMI_SEGMENT'] == seg].shape[0] for seg in segs}
        results['segments'] = [[] for i in range(results.shape[0])]
        for i, row in results.iterrows():
            block = row.cluster
            items = {seg: 0 for seg in seg_count.keys()}
            for i, node in block.nodes(data=True):
                if node['group_attributes']['group_name'] in segs:
                    items[node['group_attributes']['group_name']] += len(node['group_attributes']['match_fk_list'])
            row.segments += [seg for seg in segs if seg_count[seg] > 0 and float(items[seg]) / seg_count[seg] >= .75]
        results['seg_count'] = [len(stuff) if stuff else 0 for stuff in results.segments]
        together = results.sort_values('seg_count', ascending=False).reset_index().segments[0]
        result = 'None shelved together'
        if len(together) == 3:
            result = 'Taco, Enchilada Sauce and Cooking Sauce together'
        elif len(together) == 2:
            if 'TACO SAUCE/HOT SAUCE' not in together:
                result = 'Enchilada & Cooking Sauce together, not Taco Sauce'
            elif 'ENCHILADA SAUCE' not in together:
                result = 'Taco & Cooking Sauce together, not Enchilada Sauce'
            elif 'COOKING SAUCE/MARINADE' not in together:
                result = 'Taco & Enchilada Sauce together, not Cooking Sauce'
        kwargs = {'score': 1, 'result': result}
        return kwargs

    def calculate_count_of_shelves(self, kpi_name, kpi_line, relevant_scif, general_filters):
        filters = self.get_kpi_line_filters(kpi_line)
        filters.update(general_filters)
        filters.update({'stacking_layer': 1})
        mpis = self.filter_df(self.mpis, filters)
        full_mpis = self.filter_df(self.full_mpis, filters)

        cmpis = mpis.groupby(['scene_fk', 'bay_number'])['scene_match_fk'].count()
        cfull_mpis = full_mpis.groupby(['scene_fk', 'bay_number'])['scene_match_fk'].count()
        agg = pd.concat([cmpis, cfull_mpis], axis=1)
        agg.columns = ['A', 'B']
        agg['C'] = agg['A'] / agg['B']
        agg = agg[agg['C'] >= .9].reset_index().drop(['A', 'B', 'C'], axis=1)
        if agg.empty:
            return
        bay_filters = {'scene_fk': list(agg['scene_fk'].unique()), 'bay_number': list(agg['bay_number'].unique())}

        shelves = int(round(self.filter_df(self.full_mpis, bay_filters).groupby(['scene_fk', 'bay_number'])
                            ['shelf_number'].max().mean()))

        potential_results = [res.replace(' Shelves', '') for res in self.get_results_value(kpi_line)]
        result = self.semi_numerical_results(shelves, potential_results, form='{} Shelves')
        kwargs = {'numerator_result': shelves, 'score': 1, 'result': result,
                  'target': None}  
        return kwargs

    def base_count(self, kpi_name, kpi_line, relevant_scif, general_filters, min=0):
        filters = self.get_kpi_line_filters(kpi_line)
        filters.update(general_filters)
        count_col = self.read_cell_from_line(kpi_line, 'count_attribute')
        scif = self.filter_df(relevant_scif, filters)
        scif = scif[scif['facings_ign_stack'] >= min]
        if 'Allowed' in kpi_line.index:
            allowed = set(self.read_cell_from_line(kpi_line, 'Allowed'))
            if allowed:
                scif = scif[scif[count_col].isin(allowed)]
        if scif.empty:
            return 0
        count = len([x for x in scif[count_col[0]].unique() if x])
        return count

    def calculate_count_of(self, kpi_name, kpi_line, relevant_scif, general_filters):
        count = self.base_count(kpi_name, kpi_line, relevant_scif, general_filters)
        potential_results = self.get_results_value(kpi_line)
        # result = self.inequality_results(count, potential_results, kpi_name)
        result = self.semi_numerical_results(count, potential_results)
        kwargs = {'numerator_result': count, 'score': 1, 'result': result, 'target': 0}
        return kwargs

    def calculate_sos_percent(self, kpi_name, kpi_line, relevant_scif, general_filters):
        ratio, num, den = self.sos_with_num_and_dem(kpi_line, relevant_scif, general_filters, 'facings_ign_stack')
        if ratio is not None:
            potential_results = self.get_results_value(kpi_line)
            result = self.inequality_results(ratio, potential_results, kpi_name)
        kwargs = {'numerator_result': num, 'score': ratio, 'result': result, 'denominator_result': den, 'target': 0}
        return kwargs

    def make_mpis(self, kpi_line, general_filters, ign_stacking=1, use_full_mpis=0):
        mpis = self.full_mpis if use_full_mpis else self.mpis
        filters = self.get_kpi_line_filters(kpi_line)
        filters.update(general_filters)
        if ign_stacking:
            filters.update(Const.IGN_STACKING)
        return self.filter_df(self.mpis, filters)

    def prune_edges(self, g, action_edges, keep_or_cut='keep'):
        for node in g.nodes():
            for edge_id, edge in g[node].items():
                for edge_dir in edge.values():
                    if keep_or_cut == 'keep':
                        if edge_dir not in action_edges:
                            g.remove_edge(node, edge_id)
                    else:
                        if edge_dir in action_edges:
                            g.remove_edge(node, edge_id)
        return g

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

        if kpi_type == Const.TMB:
            return self.calculate_topmiddlebottom
        elif kpi_type == Const.ADJACENCY:
            return self.calculate_adjacency_list
        elif kpi_type == Const.ANCHOR:
            return self.calculate_anchor
        elif kpi_type == Const.BLOCKING:
            return self.calculate_block
        elif kpi_type == Const.BASE_MEASURE:
            return self.calculate_base_measure
        elif kpi_type == Const.COUNT:
            return self.calculate_count_of
        elif kpi_type == Const.VARIETY_COUNT:
            return self.calculate_count_of_variety
        elif kpi_type == Const.PERCENT:
            return self.calculate_sos_percent
        elif kpi_type == Const.SEQUENCE:
            return self.calculate_sequence
        else:
            Log.warning("The value '{}' in column sheet in the template is not recognized".format(kpi_type))
            return None

    def make_result_values_dict(self):
        query = "SELECT * FROM static.kpi_result_value;"
        return pd.read_sql_query(query, self.ps_data_provider.rds_conn.db).set_index('value')['pk'].to_dict()

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
                                       numerator_id=numerator_id, denominator_id=denominator_id,
                                       parent_fk=self.entity_dict[self.super_cat.lower()])
