
import pandas as pd
import numpy as np
from functools import reduce
import operator as op
from collections import defaultdict, Counter, namedtuple

from Trax.Algo.Calculations.Core.DataProvider import Data
from Trax.Algo.Calculations.Core.Utils import Validation
from Trax.Utils.Logging.Logger import Log
from Projects.GMIUS.Snacks.Utils.Const import Const
from Projects.GMIUS.Utils.CustomError import NoDataForBlockError
from Projects.GMIUS.Snacks.Utils.Deco import empty_scif_decorator
from KPIUtils_v2.GlobalDataProvider.PsDataProvider import PsDataProvider

from KPIUtils_v2.Calculations.BlockCalculations_v2 import Block
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
        self.mpis, self.full_mpis, self.mpip = pd.DataFrame(), pd.DataFrame(), pd.DataFrame()
        self.make_mpi()
        self.visit_date = self.data_provider[Data.VISIT_DATE]
        self.session_info = self.data_provider[Data.SESSION_INFO]
        self.scenes = self.scene_info['scene_fk'].tolist()
        self.scif = self.data_provider[Data.SCENE_ITEM_FACTS]
        self.template = {}
        self.super_cat = ''
        self.res_dict = {}
        self.dependencies = {}
        self.dependency_lookup = {}
        self.base_measure = None
        self.global_fail = 0
        self.MSL = {}

        # self.att_dict = self.make_att_dict()
        # self.tmb_map = pd.read_excel(Const.TMB_MAP_PATH).set_index('Num Shelves').to_dict('index')

    # main functions:
    def main_calculation(self, template_path):
        """
            This function gets all the scene results from the SceneKPI, after that calculates every session's KPI,
            and in the end it calls "filter results" to choose every KPI and scene and write the results in DB.
        """
        if self.scif.empty:  # indicates that there is no mpis data
            return

        self.template = pd.read_excel(template_path, sheetname=None)
        self.super_cat = template_path.split('/')[-1].split(' ')[0].upper()
        self.dependencies = {key: None for key in self.template[Const.KPIS][Const.KPI_NAME]}
        self.dependency_reorder()
        main_template = self.template[Const.KPIS]
        self.dependency_lookup = main_template.set_index(Const.KPI_NAME)[Const.DEPENDENT].to_dict()
        self.res_dict = self.template[Const.RESULT].set_index('Result Key').to_dict('index')

        for i, main_line in main_template.iterrows():
            self.global_fail = 0
            all_kwargs = self.calculate_main_kpi(main_line)
            if not isinstance(all_kwargs, list) or not all_kwargs:
                all_kwargs = [all_kwargs]
            for kwargs in all_kwargs:
                if not kwargs or kwargs['score'] is None:
                    kwargs = {'score': 0, 'result': 'Not Applicable', 'failed': 0}
                self.write_to_db(main_line['KPI Name'], **kwargs)
                self.dependencies[main_line['KPI Name']] = kwargs['result']

        # self.flag_failures()

    def calculate_main_kpi(self, main_line):
        kpi_name = main_line[Const.KPI_NAME]
        kpi_type = main_line[Const.TYPE]
        scene_types = self.read_cell_from_line(main_line, 'Found Scene')
        general_filters = {}
        relevant_scif = self.filter_df(self.scif.copy(), Const.SOS_EXCLUDE_FILTERS, exclude=1)
        if scene_types:
            relevant_scif = relevant_scif[relevant_scif['scene_fk'] == self.MSL[scene_types[0]]]
            general_filters['scene_fk'] = [self.MSL[scene_types[0]]]
        if relevant_scif.empty and main_line['Run on Empty SCIF'] != 'Y':
            return

        # print(kpi_name)
        # if kpi_name != 'When there is a Non-Integrated Bars set, what Nutrition segments are present in the Nutrition MSL?':
        #     return

        if (self.super_cat in ['SNACKS']) and \
                (kpi_type in[
                    Const.PRIMARY_LOCATION,
                    Const.MAX_BLOCK_ADJACENCY,
                    Const.MAX_BLOCK_COMPOSITION,
                    Const.EXISTS_IN_MAX_BLOCK,
                    Const.BLOCKING,
                    Const.BASE_MEASURE,
                    Const.BLOCK_ORIENTATION,
                    Const.BLOCK_PERCENT,
                    Const.ANCHOR,
                    Const.PRODUCT_SEQUENCE,
                    Const.MAX_BLOCK_ADJACENCY_SUBCAT,
                    Const.STOCKED_LOCATION,
                ]):

            dependent_kpis = self.read_cell_from_line(main_line, Const.DEPENDENT)
            dependent_results = self.read_cell_from_line(main_line, Const.DEPENDENT_RESULT)
            if dependent_kpis:
                for i, dependent_kpi in enumerate(dependent_kpis):
                    # if dependent_results[0][0] != '!':
                        if dependent_results[i]:
                            if self.dependencies[dependent_kpi] != dependent_results[i]:
                                if not pd.isna(main_line['Dependency Fail Result']):
                                    return {'score': 1, 'result': main_line['Dependency Fail Result']}
                                return
                        # else:
                        #     if self.dependencies[dependent_kpi] == dependent_results \
                        #             or self.dependencies[dependent_kpi] is None:
                        #         return

            kpi_line = self.template[kpi_type].set_index(Const.KPI_NAME).loc[kpi_name]
            function = self.get_kpi_function(kpi_type, kpi_line[Const.RESULT])
            try:
                all_kwargs = function(kpi_name, kpi_line, relevant_scif, general_filters)
            except Exception as e:
                if self.global_fail:
                    all_kwargs = [{'score': 0, 'result': "Not Applicable", 'failed': 0}]
                    Log.warning('kpi "{}" failed to calculate in super category "{}"'.format(
                        kpi_name, self.super_cat))

                else:
                    all_kwargs = [{'score': 0, 'result': None, 'failed': 1}]
                    Log.error('kpi "{}" failed in super category "{}," error: "{}"'.format(
                        kpi_name, self.super_cat, e))

            finally:
                # print(all_kwargs)
                # print('\n')
                return all_kwargs

    def flag_failures(self):
        for kpi, val in self.dependencies.items():
            if val is None:
                Log.warning('Warning: KPI "{}" not run for session "{}"'.format(
                    kpi, self.session_uid))

    def calculate_primary_location(self, kpi_name, kpi_line, relevant_scif, general_filters):
        self.MSL[self.read_cell_from_line(kpi_line, 'Short Name')[0]] = 'Not Found'
        filters = self.get_kpi_line_filters(kpi_line)
        scif = self.filter_df(relevant_scif, filters)
        if scif.empty:
            return
        scene = scif.groupby(['scene_fk'])['facings'].sum().sort_values(ascending=False).index[0]
        potential_results = self.get_results_value(kpi_line)
        scores = {}
        for result in potential_results:
            scores[result] = 0
            substrs = result.split(' ')
            for word in self.filter_df(scif, {'scene_fk': scene})['template_name'].iloc[0].split(' '):
                if word in substrs:
                    scores[result] += 1
        top = sorted(scores.items(), key=lambda x: x[1])[-1][0]
        self.MSL[self.read_cell_from_line(kpi_line, 'Short Name')[0]] = scene
        return {'score': 1, 'result': top, 'denominator_id': scene}

    def calculate_base_measure(self, kpi_name, kpi_line, relevant_scif, general_filters):
        unit = kpi_line['Unit']
        max_res = kpi_line['Max']
        filters = self.get_kpi_line_filters(kpi_line)
        filters.update(general_filters)
        filters.update({'stacking_layer': 1})
        shelves = self.filter_df(self.full_mpis, general_filters).groupby('bay_number')['shelf_number'].max().to_dict()
        bays = self.filter_df(self.mpis, filters).groupby('bay_number')['width_mm_advance']
        total = 0
        for i, bay in bays:
            total += bay.sum() / shelves[i]
        total = total / Const.MM_TO_FT
        unit_base = total // unit
        ft = int(unit_base * unit)
        if total < unit:
            result = 'LESS THAN OR EQUAL TO {} FT'.format(unit)
        elif total > max_res:
            result = 'GREATER THAN {} FT'.format(max_res)
        elif total % unit == 0:
            result = '{} FT > AND <= {} FT'.format(ft - unit, ft)
        else:
            result = '{} FT > AND <= {} FT'.format(ft, ft + unit)

        return {'numerator_result': total, 'score': 1, 'result': result}

    def calculate_max_block_adj(self, kpi_name, kpi_line, relevant_scif, general_filters):
        result = 'Could not determine'
        a_req = self.get_kpi_line_filters(kpi_line, 'A Required')
        b_req = self.get_kpi_line_filters(kpi_line, 'B Required')
        a_filters = self.get_kpi_line_filters(kpi_line, 'A')
        thresh = {'A': self.read_cell_from_line(kpi_line, 'A Threshold')[0]}
        mpis = self.filter_df(self.mpis, general_filters)
        a_all_mpis = self.filter_df(self.mpis, a_req) #  Larabar can be anywhere
        a_mpis = self.filter_df(mpis, a_req)
        b_mpis = self.filter_df(mpis, b_req)
        if a_all_mpis.empty or b_mpis.empty:
            return {'score': 1, 'result': result}
        if a_mpis.empty:
            return {'score': 1, 'result': 'Non-Integrated Bars Set Present'}
        req = {'A': set(a_mpis.scene_match_fk.unique()), 'B': set(b_mpis.scene_match_fk.unique())}
        adj = self.calculate_max_block_adj_base(kpi_name, kpi_line, relevant_scif, general_filters, require=req,
                                                thresh=thresh)
        if adj:
            result = 'Integrated Bars Set Present'
        else:
            result = 'Non-Integrated Bars Set Present'
        return {'score': 1, 'result': result}

    @empty_scif_decorator
    def calculate_max_block_composition(self, kpi_name, kpi_line, relevant_scif, general_filters):
        filters = self.get_kpi_line_filters(kpi_line)
        segment = kpi_line['Segment']
        _, _, mpis_dict, _, results = self.base_block(kpi_name, kpi_line, relevant_scif, general_filters,
                                                      filters=filters, check_orient=0, add_cols=segment)
        maxblock = results.sort_values('facing_percentage', ascending=False).iloc[0, :].cluster
        subcats = set(sum([list(n[segment].values) for i, n in maxblock.nodes(data=True)], []))
        potential_results = self.get_results_value(kpi_line)
        components = set(y.strip().upper() for x in potential_results for y in x.split(','))
        found = [comp for cat in subcats if cat for comp in components if comp in cat]
        if not found:
            found = [comp for cat in subcats-components if cat for comp in components if cat in comp]
        if not found:
            result = 'COULD NOT DETERMINE'
        else:
            result = self.results_matching(found, potential_results)
        return {'score': 1, 'result': result}


    def results_matching(self, found, possible):
        return sorted(Counter((res for cat in found if cat for res in possible
                               if cat in res.upper() and len(res.split(',')) == len(found))).items(),
                       key=lambda x: x[1])[-1][0]

    def calculate_exists_in_max_block(self, kpi_name, kpi_line, relevant_scif, general_filters):
        num_filters = self.get_kpi_line_filters(kpi_line, 'Numerator')
        den_filters = self.get_kpi_line_filters(kpi_line, 'Denominator')
        num_filters.update(den_filters)
        num_filters.update(general_filters)
        _, _, _, _, results = self.base_block(kpi_name, kpi_line, relevant_scif, general_filters,
                                              filters=den_filters, check_orient=0)
        maxblock = results.sort_values('block_facings', ascending=False).iloc[0, :].cluster
        found = set(sum([list(n['scene_match_fk'].values) for i, n in maxblock.nodes(data=True)], []))
        num = set(self.filter_df(self.mpis, num_filters).scene_match_fk.values)
        result = 'Not Stocked with Kids Grain'
        if num & found:
            result = 'Stocked with Kids Grain'
        return {'score': 1, 'result': result}

    def calculate_max_block_adjacency_subcat(self, kpi_name, kpi_line, relevant_scif, general_filters):
        a_filters = self.get_kpi_line_filters(kpi_line, 'A')
        check_filters = self.get_kpi_line_filters(kpi_line, 'B')
        result = 'Other'
        for subcat in check_filters.values()[0]:
            b_filters = {check_filters.keys()[0]: [subcat]}
            mpis = self.filter_df(self.mpis, general_filters)
            a_mpis = self.filter_df(mpis, a_filters)
            b_mpis = self.filter_df(mpis, b_filters)
            if a_mpis.empty or b_mpis.empty:
                continue
            adj = self.calculate_max_block_adj_base(kpi_name, kpi_line, relevant_scif, general_filters)
            if adj:
                result = ' '.join([x.capitalize() for x in subcat.replace('GRAIN', '').split(' ') if x])
                break
        return {'score': 1, 'result': result}

    def calculate_max_block_adj_base(self, kpi_name, kpi_line, relevant_scif, general_filters, comp_filter={},
                                     thresh={}, require={}):
        allowed_edges = [x.upper() for x in self.read_cell_from_line(kpi_line, Const.EDGES)]
        d = {'A': {}, 'B': {}}
        for k, v in d.items():
            if comp_filter:
                filters = comp_filter[k]
            else:
                filters = self.get_kpi_line_filters(kpi_line, k)
            _, _, _, _, results = self.base_block(kpi_name, kpi_line, relevant_scif, general_filters, filters=filters,
                                                  check_orient=0)
            v['df'] = results.sort_values('facing_percentage', ascending=False)
            if self.read_cell_from_line(kpi_line, 'Max Block')[0] == 'Y':
                v['df'] = pd.DataFrame(v['df'].iloc[0, :]).T
            if k in thresh:
                if v['df']['total_facings'].iloc[0] / float(self.filter_df(self.mpis, filters).shape[0]) <= thresh[k]:
                    return 0
            v['items'] = sum([list(n['match_fk']) for j, row in v['df'].iterrows()
                              for i, n in row['cluster'].nodes(data=True)
                              if n['block_key'].value not in Const.ALLOWED_FLAGS], [])
            if require:
                if not require[k] & set(v['items']):
                    return 0
            matches = []
            for scene in d[k]['df']['scene_fk'].values:
                scene_key = [key for key in self.block.adj_graphs_by_scene.keys() if str(scene) in key][0]
                scene_graph = self.block.adj_graphs_by_scene[scene_key]
                matches += [(edge, scene_graph[item][edge]['direction']) for item in v['items'] if item in scene_graph
                            for edge in scene_graph[item].keys() if scene_graph[item][edge]['direction'] in allowed_edges]
            v['edge_matches'], v['directions'] = zip(*matches) if matches else ([], [])
        result = 0
        if set(d['A']['edge_matches']) & set(d['B']['items']):
            result = 1
        return result

    @empty_scif_decorator
    def calculate_blocking(self, kpi_name, kpi_line, relevant_scif, general_filters):
        _, _, _, blocks, _, = self.base_block(kpi_name, kpi_line, relevant_scif, general_filters)
        result = 'Not Blocked'
        if not blocks.empty:
            result = 'Blocked'
        return {'score': 1, 'result': result}

    @empty_scif_decorator
    def calculate_blocking_percent(self, kpi_name, kpi_line, relevant_scif, general_filters):
        shelves = self.filter_df(self.full_mpis, general_filters).groupby('bay_number')['shelf_number'].max().to_dict()
        _, _, mpis_dict, blocks, _, = self.base_block(kpi_name, kpi_line, relevant_scif, general_filters)
        result = 'Not Blocked'
        if not blocks.empty:
            block = blocks.cluster.iloc[0]
            items = sum([list(n['scene_match_fk'].values) for i, n in block.nodes(data=True)], [])
            block_mpis = self.filter_df(self.mpis, {'scene_match_fk': items})
            per_bay_mpis = block_mpis.groupby('bay_number')['scene_match_fk'].count()
            exclude_bays = [bay for bay in per_bay_mpis.index if per_bay_mpis[bay] / float(per_bay_mpis.sum()) < .10]
            block_mpis = self.filter_df(block_mpis, {'bay_number': exclude_bays}, exclude=1)
            block_mpis = pd.DataFrame(block_mpis.groupby('bay_number')['shelf_number'].nunique())
            block_mpis['real_shelf_number'] = [shelves[bay] for bay in block_mpis.index]
            block_mpis['ratio'] = block_mpis['shelf_number'] / block_mpis['real_shelf_number']
            block_mpis['passed'] = block_mpis.ratio.apply(lambda x: 1 if x >= .75 else 0)
            if block_mpis.passed.sum() / float(block_mpis.shape[0]) > .5:
                result = 'Vertical'
            else:
                result = 'Not Vertical'
        return {'score': 1, 'result': result}

    def calculate_blocking_orientation(self, kpi_name, kpi_line, relevant_scif, general_filters):
        children = self.generic_split(self.dependency_lookup[kpi_name])
        total = 0
        vertical = 0
        for child in children:
            total += 1
            skull_hat = self.dependencies[child]
            if skull_hat == 'Not Blocked':
                return {'score': 1, 'result': 'Not Blocked'}
            elif skull_hat == 'Vertical':
                vertical += 1
        result = 'Not Vertical'
        if vertical / total > .5:
            result = 'Vertical'
        return {'score': 1, 'result': result}

    def base_block(self, kpi_name, kpi_line, relevant_scif, general_filters_base, check_orient=1, other=1, filters={},
                   exclude={}, multi=0, add_cols=[]):
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
                                                                                'minimum_facing_for_block': 1,
                                                                                'exclude_filter': exclude,
                                                                                'additional_columns': add_cols})])
            blocks = result[result['is_block'] == True]
            valid_scene_found = 1
            if not blocks.empty and not multi:
                score = 1
                orientation = blocks.loc[0, 'orientation']
                # break
        if empty_check == -1 and not valid_scene_found:
            self.global_fail = 1
            raise NoDataForBlockError(Const.EMPTY_BLOCKS_ERROR.format(kpi_name))
        return score, orientation, mpis_dict, blocks, result

    def calculate_anchor(self, kpi_name, kpi_line, relevant_scif, general_filters):
        results = []
        edge = kpi_line['Edges']
        op = min if edge == 'left' else max
        scenes = relevant_scif['scene_fk'].unique()
        potential_end = self.get_kpi_line_filters(kpi_line)
        general_filters.update(potential_end)
        for scene in scenes:
            general_filters['scene_fk'] = scene
            mpis = self.filter_df(self.mpis, general_filters)
            if mpis.empty:
                continue
            bay = op(mpis['bay_number'])
            bmpis = self.filter_df(mpis, {'bay_number': bay})
            impis = bmpis.groupby('shelf_number').idxmin() if edge == 'left' else bmpis.groupby('shelf_number').idxmax()
            bmpis = bmpis.loc[impis['scene_match_fk']]
            if edge == 'right':
                bmpis['facing_sequence_number'] = bmpis['n_shelf_items'] - bmpis['facing_sequence_number']
            bmpis = bmpis[bmpis['facing_sequence_number'] == min(bmpis['facing_sequence_number'])]
            counts = bmpis.groupby(potential_end.keys()[0])['scene_match_fk'].count().sort_values(ascending=False)
            counts = counts[counts == max(counts)]
            for count in counts.index:
                result = ' '.join([sub.capitalize() for sub in count.split(' ')])
                results.append({'score': 1, 'result': result, 'denominator_id': scene, 'numerator_result': max(counts)})
        return results

    def calculate_product_sequence(self, kpi_name, kpi_line, relevant_scif, general_filters):
        # this attribute should be pulled from the template once the template is updated
        vector = kpi_line['Vector']
        orth = (set(['x', 'y']) - set(vector)).pop()
        filters = self.get_kpi_line_filters(kpi_line)
        scene = relevant_scif.scene_fk.iloc[0]
        Segment = namedtuple('Segment', 'seg position facings orth_min orth_max matches')
        segments = filters.values()[0]
        seg_list = []
        for seg in segments:
            seg_filters = {filters.keys()[0]: [seg]}
            _, _, mpis_dict, _, results = self.base_block(kpi_name, kpi_line, relevant_scif,
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
        result = ', '.join([' '.join([y.capitalize() for y in x.replace('GRAIN', '').split(' ') if y]) for x in order])
        return {'result': result, 'score': 1}

    def calculate_stocked_location(self, kpi_name, kpi_line, relevant_scif, general_filters):
        thresh = kpi_line['Threshold']
        filters = self.get_kpi_line_filters(kpi_line)
        num_df = self.filter_df(relevant_scif, filters)
        den_df = self.filter_df(self.scif, filters)
        if den_df.empty:
            return
        ratio, res = self.ratio_score(num_df.facings_ign_stack.sum(), den_df.facings_ign_stack.sum(), thresh)
        result = kpi_line['Fail']
        if res:
            result = kpi_line['Pass']
        return {'result': result, 'score': 1}

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

    def calculate_count_of_format(self, kpi_name, kpi_line, relevant_scif, general_filters):
        count = self.base_count(kpi_name, kpi_line, relevant_scif, general_filters)
        potential_results = self.get_results_value(kpi_line)
        num_results = [res.split('format(s)')[0].strip() for res in potential_results]
        base = potential_results[0].split('format(s)')[-1].strip()
        result = '{} {} {}'.format(self.semi_numerical_results(
            count, num_results), 'format(s)', base)
        kwargs = {'numerator_result': count, 'score': 1, 'result': result, 'target': 0}
        return kwargs

    def calculate_count_of_variety(self, kpi_name, kpi_line, relevant_scif, general_filters):
        ss_filters = self.get_kpi_line_filters(kpi_line, 'SS')
        mpk_filters = self.get_kpi_line_filters(kpi_line, 'MPK')
        ss_filters.update(general_filters)
        mpk_filters.update(general_filters)
        ss_count = self.base_count(kpi_name, kpi_line, relevant_scif, ss_filters)
        mpk_count = self.base_count(kpi_name, kpi_line, relevant_scif, mpk_filters)

        result = self.ss_mpk_results(ss_count, mpk_count, kpi_line)
        kwargs = {'numerator_result': ss_count,
                  'numerator_result': mpk_count, 'score': 1, 'result': result}
        return kwargs

    def ss_mpk_results(self, ss_count, mpk_count, kpi_line):
        ss_po_res, mpk_po_res = zip(*[res.split(', ') for res in self.get_results_value(kpi_line)])
        ss_po_res = [res.replace(' SS', '') for res in set(ss_po_res)]
        mpk_po_res = [res.replace(' MPK', '') for res in set(mpk_po_res)]
        ss_res = '{} {}'.format(self.ss_mpk_matching(ss_count, ss_po_res), 'SS')
        mpk_res = '{} {}'.format(self.ss_mpk_matching(mpk_count, mpk_po_res), 'MPK')
        return ', '.join([ss_res, mpk_res])

    def ss_mpk_matching(self, count, results):
        for res in results:
            if '-' in res:
                a, b = res.split('-')
                ineq = '{} <= {} <= {}'.format(a, count, b)
            elif '+' in res:
                a = res.replace('+', '')
                ineq = '{} >= {}'.format(count, a)
            else:
                try:
                    a = int(res)
                except:
                    a = 0
                finally:
                    ineq = '{} == {}'.format(count, a)
            if eval(ineq):
                break
        return res

    def calculate_set_count(self, kpi_name, kpi_line, relevant_scif, general_filters):
        min = self.read_cell_from_line(kpi_line, 'Min')
        if isinstance(min, list):
            min = min[0]
        count = self.base_count(kpi_name, kpi_line, relevant_scif, general_filters, min=min)
        den = 0
        result = count
        if min != 0:
            potential_results = self.get_results_value(kpi_line)
            den = self.base_count(kpi_name, kpi_line, relevant_scif, general_filters, min=0)
            ratio, score = self.ratio_score(count, den)
            result = self.inequality_results(ratio, potential_results, kpi_name)
        kwargs = {'numerator_result': count, 'score': 1,
                  'result': result, 'target': 0, 'denominator_result': den}
        return kwargs

    def calculate_sos_percent(self, kpi_name, kpi_line, relevant_scif, general_filters):
        ratio, num, den = self.sos_with_num_and_dem(
            kpi_line, relevant_scif, general_filters, 'facings_ign_stack')
        if ratio is not None:
            potential_results = self.get_results_value(kpi_line)
            result = self.inequality_results(ratio, potential_results, kpi_name)
        kwargs = {'numerator_result': num, 'score': ratio,
                  'result': result, 'denominator_result': den, 'target': 0}
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
        Log.error('Result "{}" not found in potential results "{}" in kpi "{}"'.format(
            res, potential_results, kpi))

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

    def read_cell_from_line(self, line, col):
        try:
            val = line[col] if not pd.isnull(line[col]) else []
        except:
            val = []
        if val:
            val = self.generic_split(val)

        return val

    @staticmethod
    def generic_split(val):
        if hasattr(val, 'split'):
            if ';' in val:
                val = [x.strip() for x in val.split(';')]
            elif ',' in val:
                val = [x.strip() for x in val.split(',')]
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
        if kpi_type == Const.PRIMARY_LOCATION:
            return self.calculate_primary_location
        elif kpi_type == Const.MAX_BLOCK_ADJACENCY:
            return self.calculate_max_block_adj
        elif kpi_type == Const.MAX_BLOCK_COMPOSITION:
            return self.calculate_max_block_composition
        elif kpi_type == Const.EXISTS_IN_MAX_BLOCK:
            return self.calculate_exists_in_max_block
        elif kpi_type == Const.BLOCKING:
            return self.calculate_blocking
        elif kpi_type == Const.BASE_MEASURE:
            return self.calculate_base_measure
        elif kpi_type == Const.BLOCK_PERCENT:
            return self.calculate_blocking_percent
        elif kpi_type == Const.BLOCK_ORIENTATION:
            return self.calculate_blocking_orientation
        elif kpi_type == Const.ANCHOR:
            return self.calculate_anchor
        elif kpi_type == Const.PRODUCT_SEQUENCE:
            return self.calculate_product_sequence
        elif kpi_type == Const.MAX_BLOCK_ADJACENCY_SUBCAT:
            return self.calculate_max_block_adjacency_subcat
        elif kpi_type == Const.STOCKED_LOCATION:
            return self.calculate_stocked_location
        """
        transfers every kpi to its own function
        :param kpi_type: value from "sheet" column in the main sheet
        :return: function
        """

        # elif kpi_type == Const.TMB:
        #     return self.calculate_topmiddlebottom
        # elif kpi_type == Const.ADJACENCY:
        #     if result.lower() == 'adj priv':
        #         return self.calculate_adjacency_both
        #     else:
        #         return self.calculate_adjacency_list
        # elif kpi_type == Const.ADJACENCY_MIX:
        #     return self.calculate_adjacency_mix
        # elif kpi_type == Const.ANCHOR_LIST:
        #     return self.calculate_adjacency_list

        # elif kpi_type == Const.IADJACENCY:
        #     return self.calculate_new_integrated_adjacency
        # elif kpi_type == Const.STOCKING:
        #     return self.calculate_stocking_location
        # elif kpi_type == Const.BLOCKING:
        #     if result.lower() == kpi_type.lower():
        #         return self.calculate_block
        #     elif result.lower() == 'blocking tub':
        #         return self.calculate_tub_block
        #     elif result.lower() == 'blocking yogurt':
        #         return self.calculate_yogurt_block
        #     elif result.lower() == 'basic block':
        #         return self.calculate_basic_block
        #     elif result.lower() == 'blocking covers':
        #         return self.calculate_blocking_all_shelves
        #     elif result.lower() == 'multi-block':
        #         return self.calculate_multi_block

        # elif kpi_type == Const.ORIENT:
        #     return self.calculate_product_orientation
        # elif kpi_type == Const.COUNT_SHELVES:
        #     return self.calculate_count_of_shelves
        # elif kpi_type == Const.COUNT:
        #     if result.lower() == 'count of':
        #         return self.calculate_count_of
        #     if result.lower() == 'format':
        #         return self.calculate_count_of_format
        # elif kpi_type == Const.VARIETY_COUNT:
        #     return self.calculate_count_of_variety
        # elif kpi_type == Const.SET_COUNT:
        #     return self.calculate_set_count
        # elif kpi_type == Const.ORIENT:
        #     return self.calculate_orientation
        # elif kpi_type == Const.PRESENCE:
        #     return self.calculate_presence
        # elif kpi_type == Const.PRESENCE_WITHIN_BAY:
        #     return self.calculate_presence_within_bay
        # elif kpi_type == Const.PRESENCE_WITHIN_BAY_MEX:
        #     return self.calculate_presence_within_bay_mex
        # elif kpi_type == Const.PERCENT:
        #     return self.calculate_sos_percent
        # elif kpi_type == Const.SEQUENCE:
        #     return self.calculate_sequence
        # else:
        #     Log.warning(
        #         "The value '{}' in column sheet in the template is not recognized".format(kpi_type))
        #     return None

    def make_result_values_dict(self):
        query = "SELECT * FROM static.kpi_result_value;"
        return pd.read_sql_query(query, self.ps_data_provider.rds_conn.db).set_index('value')['pk'].to_dict()

    def make_att_dict(self):
        df = pd.read_excel(Const.DICTIONARY_PATH)
        df = df[(df['unknown'] != 'Y') & (df['not_final'] != 'Y')].set_index('Name')
        params = {key.lower(): self.get_kpi_line_filters(row) for key, row in df.iterrows()}
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
                priv['category'], priv['category_fk'] = cats.loc[0,
                                                                 'category'], cats.loc[0, 'category_fk']
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

    def make_mpi(self):
        try:
            self.match_product_in_scene = self.data_provider[Data.MATCHES]
            self.full_mpis = self.match_product_in_scene.merge(self.products, on='product_fk', suffixes=['', '_p']) \
                                                        .merge(self.scene_info, on='scene_fk', suffixes=['', '_s']) \
                                                        .merge(self.templates, on='template_fk', suffixes=['', '_t'])
            self.mpis = self.full_mpis[self.full_mpis['product_type'] != 'Irrelevant']
            self.mpis = self.filter_df(self.mpis, Const.SOS_EXCLUDE_FILTERS, exclude=1)
            self.mpis = self.filter_df(self.mpis, {'stacking_layer': 1})
            self.mpip = self.create_mpip()
        except:
            Log.warning('No mpis data found for session {}'.format(self.session_uid))
            self.global_fail = 1

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
