
import pandas as pd
import operator as op
from functools import reduce
from collections import defaultdict, Counter, namedtuple

from Trax.Algo.Calculations.Core.DataProvider import Data
from Trax.Algo.Calculations.Core.Utils import Validation
from Trax.Utils.Logging.Logger import Log
from Projects.DELMONTEUS.Utils.Const import Const
from KPIUtils_v2.GlobalDataProvider.PsDataProvider import PsDataProvider

# from KPIUtils_v2.Calculations.BlockCalculations import Block
from Projects.DELMONTEUS.Utils.BlockCalculations_v3 import Block
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
        # all_products = self.data_provider._static_data_provider.all_products.where(
        #     (pd.notnull(self.data_provider._static_data_provider.all_products)), None)
        # self.data_provider._set_all_products(all_products)
        # self.data_provider._init_session_data(None, True)
        # self.data_provider._init_report_data(self.data_provider.session_uid)
        # self.data_provider._init_reporting_data(self.data_provider.session_id)
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
        self .blockchain = {}
        self.template = {}
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
        self.dependencies = {key: None for key in self.template[Const.KPIS][Const.KPI_NAME]}
        self.dependency_reorder()
        main_template = self.template[Const.KPIS]
        self.dependency_lookup = main_template.set_index(Const.KPI_NAME)[Const.DEPENDENT].to_dict()
        self.shun()

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


        # if kpi_name not in (
        #         'What is Apple Sauce Multi Serve linear footage?',
        #         'What is Apple Sauce Single Serve linear footage?',
        #         'What is Canned Fruit linear footage?',
        #         'What is Canned Veg category linear footage?',
        #         'What is Del Monte Canned Veg linear footage?',
        #         'What is PFC linear footage?',
        #         'What is Squeezers linear footage',
        #         'What is the COS Fruit category linear footage?',
        #
        # ):
        # # if kpi_name not in ('Are the majority of Green Giant Spec Veg blocked above Green Giant Core Veg'):
        # if kpi_name not in ('is multi serve pineapple shelved above Canned Fruit?'):
        # if kpi_name not in ('is multi serve pineapple shelved above Canned Fruit?'):
        # if kpi_type not in (Const.BLOCKING, Const.BLOCKING_PERCENT, Const.SOS, Const.ANCHOR, Const.MULTI_BLOCK,
        #                     Const.SAME_AISLE, Const.SHELF_REGION, Const.SHELF_PLACEMENT):
        #     return

        # print('~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~')
        # print(kpi_name)

        dependent_kpis = self.read_cell_from_line(main_line, Const.DEPENDENT)
        dependent_results = self.read_cell_from_line(main_line, Const.DEPENDENT_RESULT)
        if dependent_kpis:
            for dependent_kpi in dependent_kpis:
                if self.dependencies[dependent_kpi] not in dependent_results:
                    if dependent_results or self.dependencies[dependent_kpi] is None:
                        return

        kpi_line = self.template[kpi_type].set_index(Const.KPI_NAME).loc[kpi_name]
        function = self.get_kpi_function(kpi_type)
        try:
           all_kwargs = function(kpi_name, kpi_line, relevant_scif, general_filters)
        except Exception as e:
            # print(e)
            if self.global_fail:
                all_kwargs = [{'score': 0, 'result': None, 'failed': 0}]
                Log.warning('kpi "{}" failed to calculate'.format(kpi_name))

            else:
                all_kwargs = [{'score': 0, 'result': None, 'failed': 1}]
                Log.error('kpi "{}" failed error: "{}"'.format(kpi_name, e))

        finally:
            if not isinstance(all_kwargs, list) or not all_kwargs:
                all_kwargs = [all_kwargs]
            # print(all_kwargs)
            for kwargs in all_kwargs:
                if not kwargs or kwargs['score'] is None:
                    kwargs = {'score': 0, 'result': 0, 'failed': 0}
                self.write_to_db(kpi_name, **kwargs)
                self.dependencies[kpi_name] = kwargs['result']

    def flag_failures(self):
        for kpi, val in self.dependencies.items():
            if val is None:
                Log.warning('Warning: KPI "{}" not run for session "{}"'.format(
                    kpi, self.session_uid))

    def calculate_sos(self, kpi_name, kpi_line, relevant_scif, general_filters):
        num = self.filter_df(relevant_scif, self.get_kpi_line_filters(kpi_line))[
            'net_len_ign_stack'].sum() / 304.8
        return {'score': 1, 'result': num}

    def calculate_same_aisle(self, kpi_name, kpi_line, relevant_scif, general_filters):
        filters = self.get_kpi_line_filters(kpi_line)
        relevant_scif = self.filter_df(self.scif, filters)
        if relevant_scif.empty:
            return
        result = 0
        if len(relevant_scif.scene_fk.unique()) == 1:
            result = 1
        return {'score': 1, 'result': result}

    def calculate_shelf_placement(self, kpi_name, kpi_line, relevant_scif, general_filters):
        location = kpi_line['Shelf Placement'].lower()
        tmb_map = pd.read_excel(Const.TMB_MAP_PATH).melt(id_vars=['Num Shelves'], var_name=['Shelf'])\
                                                   .set_index(['Num Shelves', 'Shelf']).reset_index()
        tmb_map.columns = ['max_shelves', 'shelf_number_from_bottom', 'TMB']
        tmb_map['TMB'] = tmb_map['TMB'].str.lower()
        filters = self.get_kpi_line_filters(kpi_line)
        mpis = self.filter_df(self.mpis, filters)
        mpis = self.filter_df(mpis, {'stacking_layer': 1})
        if mpis.empty:
            return
        filters.update(general_filters)
        mpis = self.filter_df(mpis, {'scene_fk': list(relevant_scif.scene_id.unique())})

        bay_shelf = self.filter_df(self.full_mpis, general_filters).set_index(['scene_fk', 'bay_number'])\
            .groupby(level=[0, 1])[['shelf_number', 'shelf_number_from_bottom']].max()
        bay_max_shelf = bay_shelf['shelf_number'].to_dict()
        bay_shelf['shelf_offset'] = bay_shelf['shelf_number_from_bottom'] - \
            bay_shelf['shelf_number']
        bay_shelf = bay_shelf.drop('shelf_number_from_bottom', axis=1).rename(
            columns={'shelf_number': 'max_shelves'})
        mpis = mpis.merge(bay_shelf, on=['bay_number', 'scene_fk'])
        mpis['true_shelf'] = mpis['shelf_number_from_bottom'] + mpis['shelf_offset']
        mpis = mpis.merge(tmb_map, on=['max_shelves', 'shelf_number_from_bottom'])

        result = self.safe_divide(self.filter_df(mpis, {'TMB': location}).shape[0], mpis.shape[0])
        return {'score': 1, 'result': result}

    def calulate_shelf_region(self, kpi_name, kpi_line, relevant_scif, general_filters):
        base = self.get_base_name(kpi_name, Const.REGIONS)
        location = kpi_line['Shelf Placement'].lower()
        if base not in self.blockchain:
            num_filters = self.get_kpi_line_filters(kpi_line, 'numerator')
            den_filters = self.get_kpi_line_filters(kpi_line, 'denominator')
            mpis = self.filter_df(self.mpis, den_filters)
            reg_list = ['left', 'center', 'right']
            self.blockchain[base] = {reg: 0 for reg in reg_list}
            self.blockchain[base]['den'] = 0
            for scene in mpis.scene_fk.unique():
                smpis = self.filter_df(mpis, {'scene_fk': scene})
                num_df = self.filter_df(smpis, num_filters)
                bays = sorted(list(smpis.bay_number.unique()))
                size = len(bays) / Const.NUM_REG
                mod = len(bays) % Const.NUM_REG
                # find start ponts for center and right groups (left is always 0), this is bays var index
                center = size
                right = size * 2
                if mod == 1:
                    right += 1  # if there is one odd bay we expand center
                elif mod == 2:
                    center += 1  # If 2, we expand left and right by one
                    right += 1
                self.blockchain[base]['den'] += num_df.shape[0]
                regions = [0, center, right, len(bays)]
                for i, reg in enumerate(reg_list):
                    self.blockchain[base][reg] += self.filter_df(
                        num_df, {'bay_number': bays[regions[i]:regions[i+1]]}).shape[0]
        result = self.safe_divide(self.blockchain[base][location], self.blockchain[base]['den'])
        return {'score': 1, 'result': result}

    def calculate_sequence(self, kpi_name, kpi_line, relevant_scif, general_filters):
        # this attribute should be pulled from the template once the template is updated
        vector = kpi_line['Vector']
        orth = (set(['x', 'y']) - set(vector)).pop()
        Segment = namedtuple('Segment', 'seg position facings orth_min orth_max matches')
        segments = [i.strip() for i in self.splitter(kpi_line['Sequence'])]
        result = 0
        for scene in relevant_scif.scene_fk.unique():
            scene_scif = relevant_scif[relevant_scif['scene_fk'] == scene]
            seg_list = []
            for seg in segments:
                seg_filters = self.get_kpi_line_filters(kpi_line, seg)
                _, _, mpis_dict, _, results = self.base_block(kpi_name, kpi_line, scene_scif,
                                                              general_filters,
                                                              filters=seg_filters,
                                                              check_orient=0)
                cluster = results.sort_values('facing_percentage', ascending=False).iloc[0, 0]
                df = pd.DataFrame([(n['polygon'].centroid.x, n['polygon'].centroid.y, n['facings'],
                                  list(n['match_fk'].values)) + n['polygon'].bounds
                                  for i, n in cluster.nodes(data=True) if n['block_key'].value
                                  not in Const.ALLOWED_FLAGS],
                                  columns=['x', 'y', 'facings', 'matches', 'x_min', 'y_min', 'x_max', 'y_max'])
                facings = df.facings.sum()
                seg_list.append(Segment(seg=seg, position=(df[vector]*df['facings']).sum()/facings, facings=facings,
                                orth_min=mpis_dict[scene]['rect_{}'.format(orth)].min(),
                                orth_max=mpis_dict[scene]['rect_{}'.format(orth)].max(),
                                matches=df['matches'].sum()))

            order = [x.seg for x in sorted(seg_list, key=lambda x: x.position)]
            if '_'.join(order) == '_'.join(segments):
                flow_count = 1 # 1 is intentional, since loop is smaller than list by 1
                for i in range(1, len(order)):
                    if self.safe_divide(self.seq_axis_engulfs_df(i, seg_list, orth), seg_list[i].facings) >= .1 and\
                       self.safe_divide(self.seq_axis_engulfs_df(i, seg_list, orth, r=1), seg_list[i-1].facings) >= .1:
                        flow_count += 1
                if flow_count == len(order):
                    result = 1
        return {'result': result, 'score': 1}

    def seq_axis_engulfs_df(self, i, seg_list, orth, r=0):
        j = i - 1
        if r:
            i, j = j, i
        return self.mpis[(self.mpis['scene_match_fk'].isin(seg_list[i].matches)) &
                         (seg_list[j].orth_min <= self.mpis['rect_{}'.format(orth)]) &
                         (self.mpis['rect_{}'.format(orth)] <= seg_list[j].orth_max)].shape[0]

    def calculate_max_block_adj_base(self, kpi_name, kpi_line, relevant_scif, general_filters):
        allowed_edges = [x.upper() for x in self.read_cell_from_line(kpi_line, Const.EDGES)]
        d = {'A': {}, 'B': {}}
        for k, v in d.items():
            filters = self.get_kpi_line_filters(kpi_line, k)
            _, _, mpis_dict, _, results = self.base_block(kpi_name, kpi_line, relevant_scif,
                                                          general_filters,
                                                          filters=filters,
                                                          check_orient=0)
            v['row'] = results.sort_values('facing_percentage', ascending=False).iloc[0, :]
            v['items'] = sum([list(n['match_fk']) for i, n in v['row']['cluster'].nodes(data=True)
                              if n['block_key'].value not in Const.ALLOWED_FLAGS], [])
            scene_graph = self.block.adj_graphs_by_scene[d[k]['row']['scene_fk']]
            matches = [(edge, scene_graph[item][edge]['direction']) for item in v['items']
                       for edge in scene_graph[item].keys() if scene_graph[item][edge]['direction'] in allowed_edges]
            v['edge_matches'], v['directions'] = zip(*matches) if matches else ([], [])
        result = 0
        if set(d['A']['edge_matches']) & set(d['B']['items']):
            result = 1
        return {'score': 1, 'result': result}, set(d['A']['directions'])

    def calculate_max_block_adj(self, kpi_name, kpi_line, relevant_scif, general_filters):
        result, _ = self.calculate_max_block_adj_base(kpi_name, kpi_line, relevant_scif, general_filters)
        return result

    def calculate_integrated_core(self, kpi_name, kpi_line, relevant_scif, general_filters):
        result, dirs = self.calculate_max_block_adj_base(kpi_name, kpi_line, relevant_scif, general_filters)
        if len(dirs) < 2:
            result['result'] = 0
        return result

    def calculate_block_together(self, kpi_name, kpi_line, relevant_scif, general_filters):
        result, _ = self.calculate_max_block_adj_base(kpi_name, kpi_line, relevant_scif, general_filters)
        result['result'] = result['result'] ^ 1  # this kpi is reversed (is not blocked together?) so we xor
        return result

    def calculate_serial_adj(self, kpi_name, kpi_line, relevant_scif, general_filters):
        result = {'score': 0, 'result': 0}
        scif = self.filter_df(relevant_scif, self.get_kpi_line_filters(kpi_line, 'A'))
        sizes = self.get_kpi_line_filters(kpi_line, 'A')['DLM_ VEGSZ(C)']
        num_count_sizes = 0 if self.get_kpi_line_filters(kpi_line, 'A')['DLM_ VEGSZ(C)'] == [u'FAMILY LARGE'] else 1
        if scif.empty:
            return
        subsets = scif[kpi_line['Unit']].unique()
        tally = 0
        skip = 0
        for subset in subsets:
            size_pass = 0
            size_skip = 0
            for size in sizes:
                sub_kpi_line = kpi_line.copy()
                for i in sub_kpi_line.index:
                    if sub_kpi_line[i] == ','.join(sizes):
                        sub_kpi_line[i] == size
                general_filters[kpi_line['Unit']] = [subset]
                try:
                    result, _ = self.calculate_max_block_adj_base(kpi_name, sub_kpi_line, relevant_scif, general_filters)
                    tally += result['result']
                    size_pass += 1
                except TypeError:  # yeah, i really should define a custom error, but, another day
                    size_skip += 1  # we will ignore subsets that are missing either A group or B group
            if size_pass and not num_count_sizes: # Family large only needs to be next to one size, so we need to be careful how we increment skip
               skip += 0  # family passed, even if one size failed, so we don't increment skip
            if not size_pass and not num_count_sizes:
                skip += 1  # Family size failed so we increment by one
            else:
                skip += size_skip  # this is the mutipk rt.

        target = len(subsets)*len(sizes) - skip if num_count_sizes else len(subsets) - skip  #family only needs to pass one size, multipk both
        result['result'] = 0 if target else None
        if self.safe_divide(tally, target) > 75:
            result['result'] = 1
        return result

    def calculate_adjacency_list(self, kpi_name, kpi_line, relevant_scif, general_filters):
        max_block = self.read_cell_from_line(kpi_line, Const.MAX_BLOCK)
        item_filters = {}
        kwargs_list = []

        if max_block:
            _, _, _, _, blocks = self.base_block(
                kpi_name, kpi_line, relevant_scif, general_filters, check_orient=False)
            block = blocks.sort_values('facing_percentage').reset_index().iloc[-1, :]['cluster']
            ids = sum([node['group_attributes']['match_fk_list']
                       for i, node in block.node(data=True)], [])
            item_filters = {'scene_match_fk': ids}

        if Const.END_OF_CAT in self.get_results_value(kpi_line):
            anchor_filters = item_filters if item_filters else self.get_kpi_line_filters(kpi_line)
            anchor = self.anchor_base(general_filters, anchor_filters,
                                      relevant_scif['scene_fk'].unique(), 1)
            if sum(anchor.values()) > 0:
                kwargs_list.append({'score': 1, 'result': Const.END_OF_CAT, 'target': 1})

        all_results = self.base_adjacency(
            kpi_name, kpi_line, relevant_scif, general_filters, item_filters=item_filters)
        for result in sum([x for x, y in all_results.values()], []):
            # result_fk = self.result_values_dict[result]
            kwargs_list.append({'score': 1, 'result': result})

        return kwargs_list

    def anchor_base(self, general_filters, potential_end, scenes, min_shelves, ratio=False):
        results = {}
        cat_filters = dict(general_filters)
        func_dict = {'left': [min, op.lt, float('inf')], 'right': [max, op.gt, 0]}
        results['left'], results['right'] = 0, 0
        for scene in scenes:
            cat_filters['scene_fk'] = scene
            cat_mpis = self.filter_df(self.mpis, cat_filters)
            # cat_mpis = self.filter_df(cat_mpis, Const.ALLOWED_FILTERS, exclude=1)
            cat_mpis = self.filter_df(cat_mpis, {'product_type': 'Empty'}, exclude=1)
            cat_mpis = self.filter_df(cat_mpis, {'stacking_layer': 1})
            bays = {'left': cat_mpis['bay_number'].min(), 'right': cat_mpis['bay_number'].max()}
            for dir, bay in bays.items():
                agg_func, operator, fill_val = func_dict[dir]
                bay_mpis = self.filter_df(cat_mpis, {'bay_number': bay})
                smpis = self.filter_df(bay_mpis, potential_end).groupby(
                    ['scene_fk', 'bay_number', 'shelf_number'])['facing_sequence_number'].agg(agg_func)
                if smpis.empty:
                    continue
                rmpis = self.filter_df(bay_mpis, potential_end, exclude=1) \
                    .groupby(['scene_fk', 'bay_number', 'shelf_number'])['facing_sequence_number'].agg(agg_func)
                locs = pd.concat([smpis, rmpis], axis=1)
                locs.columns = ['A', 'B']
                locs.dropna(subset=['A'], inplace=True)
                if ratio:
                    min_shelves = max(self.filter_df(
                        self.mpis, {'scene_fk': scene, 'bay_number': bay})['shelf_number'])
                    min_shelves = round(min_shelves / 2.0)

                locs.fillna(fill_val, inplace=True)
                if sum(operator(locs['A'], locs['B'])) >= min_shelves:
                    results[dir] = 1
        return results

    def calculate_anchor(self, kpi_name, kpi_line, relevant_scif, general_filters):
        scenes = relevant_scif['scene_fk'].unique()
        potential_end = self.get_kpi_line_filters(kpi_line, 'numerator')
        general_filters.update(self.get_kpi_line_filters(kpi_line, 'denominator'))
        results = self.anchor_base(general_filters, potential_end, scenes, 0, ratio=True)
        edges = self.splitter(kpi_line[Const.EDGES].strip())
        result = 0
        for edge in edges:
            if results[edge]:
                result = 1
        return {'score': 1, 'result': result}

    def base_block(self, kpi_name, kpi_line, relevant_scif, general_filters_base, check_orient=1, other=1, filters={},
                   multi=0):
        result = pd.DataFrame()
        general_filters = dict(general_filters_base)
        blocks = pd.DataFrame()
        result = pd.DataFrame()
        orientation = 'Not Blocked'
        scenes = self.filter_df(relevant_scif, general_filters).scene_fk.unique()
        if 'template_name' in general_filters:
            del general_filters['template_name']
        if 'scene_fk' in general_filters:
            del general_filters['scene_fk']
        mpis_dict = {}
        valid_scene_found = 0
        for scene in scenes:
            score = 0
            empty_check = 0
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
                empty_check = -1
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
            valid_scene_found = 1
            if not blocks.empty and not multi:
                score = 1
                orientation = blocks.loc[0, 'orientation']
                break
        if empty_check == -1 and not valid_scene_found:
            self.global_fail = 1
            raise TypeError('No Data Found fo kpi "'.format(kpi_name))
        return score, orientation, mpis_dict, blocks, result

    def calculate_block(self, kpi_name, kpi_line, relevant_scif, general_filters):
        base = self.get_base_name(kpi_name, Const.ORIENTS)
        if base in self.blockchain:
            # Data exists. Get it.
            result, orientation, mpis_dict, blocks = self.blockchain[base]
        else:
            # Data doesn't exist, so create and add it
            result, orientation, mpis_dict, blocks, _ = self.base_block(
                kpi_name, kpi_line, relevant_scif, general_filters)
            self.blockchain[base] = result, orientation, mpis_dict, blocks  # result_fk = self.result_values_dict[orientation]

        if kpi_line['AntiBlock']:
            result = result ^ 1
        kwargs = {'score': 1, 'result': result}
        return kwargs

    def calculate_block_orientation(self, kpi_name, kpi_line, relevant_scif, general_filters):
        allowed_orientation = kpi_line['Orientation'].strip()
        # Check if data for this kpi already exists
        base = self.get_base_name(kpi_name, Const.ORIENTS)
        if base in self.blockchain:
            # Data exists. Get it.
            result, orientation, mpis_dict, blocks = self.blockchain[base]
        else:
            # Data doesn't exist, so create and add it
            result, orientation, mpis_dict, blocks, _ = self.base_block(
                kpi_name, kpi_line, relevant_scif, general_filters)
            self.blockchain[base] = result, orientation, mpis_dict, blocks

        if allowed_orientation.upper() != orientation:
            result = 0
        return {'score': 1, 'result': result}

    # def calculate_block_percent(self, kpi_name, kpi_line, relevant_scif, general_filters):
    #
    #     def concater(a, b):
    #         return pd.concat([a, b])
    #
    #     allowed_orientation = kpi_line['Orientation'].strip()
    #     facings, score, den, result = 0, 0, 0, 0
    #     # Check if data for this kpi already exists
    #     base = self.get_base_name(kpi_name, Const.ORIENTS)
    #     if base in self.blockchain:
    #         # Data exists. Get it.
    #         score, orientation, mpis_dict, blocks = self.blockchain[base]
    #     else:
    #         # Data doesn't exist, so create and add it
    #         score, orientation, mpis_dict, blocks, _ = self.base_block(
    #             kpi_name, kpi_line, relevant_scif, general_filters)
    #         self.blockchain[base] = score, orientation, mpis_dict, blocks
    #
    #     den = reduce(concater, mpis_dict.values()).shape[0]
    #     if orientation.lower() == allowed_orientation:
    #         for row in blocks.itertuples():
    #             skus = sum([list(node['match_fk']) for i, node in row.cluster.nodes(data=True)], [])
    #             mpis = mpis_dict[row.scene_fk]
    #             facings = mpis[mpis['scene_match_fk'].isin(skus)].shape[0]
    #             score = 1
    #             result = self.safe_divide(facings, den)
    #     return {'numerator_result': facings, 'denominator_result': den, 'result': result, 'score': score}

    def calculate_multi_block(self, kpi_name, kpi_line, relevant_scif, general_filters):
        den_filter = self.get_kpi_line_filters(kpi_line, 'denominator')
        num_filter = self.get_kpi_line_filters(kpi_line, 'numerator')
        if kpi_line[Const.ALL_SCENES_REQUIRED] in ('Y', 'y'):  # get value for all scenes required
            all_scenes_required = True
        else:
            all_scenes_required = False
        groups = list(*num_filter.values())
        result = 0
        scenes = self.filter_df(relevant_scif, general_filters).scene_fk.unique()
        if 'template_name' in general_filters:
            del general_filters['template_name']
        for scene in scenes:  # check every scene
            groups_exempt = 0
            score = 0
            scene_general_filters = general_filters.copy()
            scene_general_filters.update({'scene_fk': scene})

            for group in groups:  # check all the groups in the current scene
                sub_filters = {num_filter.keys()[0]: [group]}
                sub_filters.update(den_filter)
                sub_score = 0
                try:
                    sub_score, _, _, _, _ = self.base_block(kpi_name, kpi_line, relevant_scif, scene_general_filters,
                                                            check_orient=0, filters=sub_filters)
                except TypeError as e:
                    if e[0] == 'No Data Found fo kpi "':  # no relevant products found, so this group is exempt
                        groups_exempt += 1
                    else:
                        raise e
                score += sub_score
            if score and score == len(groups) - groups_exempt:  # check to make sure all non-exempt groups were blocked
                result += 1
                if not all_scenes_required:  # we already found one passing scene so we don't need to continue
                    break

        if all_scenes_required:
            final_result = 1 if result == len(scenes) else 0  # make sure all scenes have a passing result
        else:
            final_result = 1 if result > 0 else 0

        return {'score': 1, 'result': final_result}

    def make_mpis(self, kpi_line, general_filters, ign_stacking=1, use_full_mpis=0):
        mpis = self.full_mpis if use_full_mpis else self.mpis
        filters = self.get_kpi_line_filters(kpi_line)
        filters.update(general_filters)
        if ign_stacking:
            filters.update(Const.IGN_STACKING)
        return self.filter_df(self.mpis, filters)

    def shun(self):
        exclude = self.template['Exclude']
        filters = {}
        for i, row in exclude.iterrows():
            filters.update(self.get_kpi_line_filters(row))
        self.mpis = self.filter_df(self.mpis, filters, exclude=1)
        self.full_mpis = self.filter_df(self.full_mpis, filters, exclude=1)
        self.scif = self.filter_df(self.scif, filters, exclude=1)

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
        filters = reduce((lambda x, y: x + y.items() if isinstance(x, list)
                          else x.items() + y.items()), filters)
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
                filters[kpi_line['{}param {}'.format(
                    name, c)]] += self.splitter(kpi_line['{}value {}'.format(name, c)])
            else:
                if c > 3:  # just in case someone inexplicably chose a nonlinear numbering format.
                    break
            c += 1
        return filters

    @staticmethod
    def splitter(text_str, delimiter=','):
        ret = [text_str]
        if hasattr(text_str, 'split'):
            ret = [x.strip() for x in text_str.split(delimiter)]
        return ret

    @staticmethod
    def get_base_name(kpi, group):
        base = kpi.lower()
        for obj in group:
            base = base.replace(obj, '').strip()
        return base

    @staticmethod
    def safe_divide(num, den):
        res = 0
        if den:
            res = num*100.0 / den
        return res

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

    def get_kpi_function(self, kpi_type):
        """
        transfers every kpi to its own function
        :param kpi_type: value from "sheet" column in the main sheet
        :return: function
        """

        if kpi_type == Const.SHELF_PLACEMENT:
            return self.calculate_shelf_placement
        elif kpi_type == Const.SHELF_REGION:
            return self.calulate_shelf_region
        elif kpi_type == Const.ADJACENCY:
            return self.calculate_adjacency_list
        elif kpi_type == Const.ANCHOR:
            return self.calculate_anchor
        elif kpi_type == Const.BLOCKING:
            return self.calculate_block
        elif kpi_type == Const.BLOCKING_PERCENT:
            return self.calculate_block_percent
        elif kpi_type == Const.BLOCK_ORIENTATION:
            return self.calculate_block_orientation
        elif kpi_type == Const.MULTI_BLOCK:
            return self.calculate_multi_block
        elif kpi_type == Const.MAX_BLOCK_ADJ:
            return self.calculate_max_block_adj
        elif kpi_type == Const.INTEGRATED:
            return self.calculate_integrated_core
        elif kpi_type == Const.BLOCKED_TOGETHER:
            return self.calculate_block_together
        elif kpi_type == Const.SERIAL:
            return self.calculate_serial_adj
        elif kpi_type == Const.SEQUENCE:
            return self.calculate_sequence
        elif kpi_type == Const.RELATIVE_POSTION:
            return self.calculate_sequence
        elif kpi_type == Const.SOS:
            return self.calculate_sos
        elif kpi_type == Const.SAME_AISLE:
            return self.calculate_same_aisle
        else:
            Log.warning(
                "The value '{}' in column sheet in the template is not recognized".format(kpi_type))
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
        self.common.write_to_db_result(fk=kpi_fk, score=score, result=result, target=target,
                                       numerator_result=numerator_result, denominator_result=denominator_result,
                                       numerator_id=numerator_id, denominator_id=denominator_id)
