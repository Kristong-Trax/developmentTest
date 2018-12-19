from datetime import datetime
import os
import pandas as pd
from functools import reduce
from collections import defaultdict, Counter

from Trax.Algo.Calculations.Core.DataProvider import Data
from Trax.Algo.Calculations.Core.Utils import Validation
from Trax.Utils.Logging.Logger import Log
from Projects.GMIUS.Utils.Const import Const
from KPIUtils_v2.GlobalDataProvider.PsDataProvider import PsDataProvider

from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider
from Projects.GMIUS.Utils.PositionGraph import Block
from KPIUtils_v2.Calculations.BlockCalculations import Block as Block
from Projects.GMIUS.Utils.BlockCalculations import Block as Block2
from Trax.Algo.Calculations.Core.GraphicalModel.AdjacencyGraphs import AdjacencyGraph


# from KPIUtils_v2.Calculations.BlockCalculations import Block
from Projects.GMIUS.ImageHTML.Image import ImageMaker



__author__ = 'Sam'

TEMPLATE_PATH = os.path.join(os.path.dirname(os.path.realpath(__file__)), '../Data', 'Yogurt GMI KPI Template v0.2.xlsx')


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
        self.match_product_in_scene = self.data_provider[Data.MATCHES]
        self.mpis = self.match_product_in_scene.merge(self.products, on='product_fk', suffixes=['', '_p'])\
                                               .merge(self.scene_info, on='scene_fk', suffixes=['', '_s'])\
                                               .merge(self.templates, on='template_fk', suffixes=['', '_t'])
        self.mpis = self.filter_df(self.mpis, Const.SOS_EXCLUDE_FILTERS, exclude=1)
        self.visit_date = self.data_provider[Data.VISIT_DATE]
        self.session_info = self.data_provider[Data.SESSION_INFO]
        self.scenes = self.scene_info['scene_fk'].tolist()
        self.scif = self.data_provider[Data.SCENE_ITEM_FACTS]
        self.mpip = self.create_mpip()
        self.template = {}
        for sheet in Const.SHEETS:
            try:
                self.template[sheet] = pd.read_excel(TEMPLATE_PATH, sheet)
            except:
                pass
        # self.hierarchy = self.template[Const.KPIS].set_index(Const.KPI_NAME)[Const.PARENT].to_dict()
        # self.template[Const.KPIS] = self.template[Const.KPIS][(self.template[Const.KPIS][Const.TYPE] != Const.PARENT) &
        #                                                       (~self.template[Const.KPIS][Const.TYPE].isnull())]
        self.dependencies = {key: None for key in self.template[Const.KPIS][Const.KPI_NAME]}
        self.dependency_reorder()

    # main functions:
    def main_calculation(self, *args, **kwargs):
        """
            This function gets all the scene results from the SceneKPI, after that calculates every session's KPI,
            and in the end it calls "filter results" to choose every KPI and scene and write the results in DB.
        """
        main_template = self.template[Const.KPIS]
        for i, main_line in main_template.iterrows():
            self.calculate_main_kpi(main_line)

    def calculate_main_kpi(self, main_line):
        kpi_name = main_line[Const.KPI_NAME]
        kpi_type = main_line[Const.TYPE]
        scene_types = self.read_cell_from_line(main_line, Const.SCENE_TYPE)
        print(kpi_name, kpi_type)
        general_filters = {}
        relevant_scif = self.filter_df(self.scif.copy(), Const.SOS_EXCLUDE_FILTERS, exclude=1)
        if scene_types:
            relevant_scif = relevant_scif[relevant_scif['template_name'].isin(scene_types)]
            general_filters['template_name'] = scene_types
        if relevant_scif.empty:
            return
        dependent_result = self.read_cell_from_line(main_line, Const.DEPENDENT_RESULT)
        if dependent_result and self.dependencies[kpi_name] not in dependent_result:
            return

        function = self.get_kpi_function(kpi_type)
        # function = self.integrated_adjacency
        # function = self.calculate_stocking_location
        # function = self.adjacency
        # function = self.graph
        # if kpi_name != 'What best describes the stocking location of Organic Yogurt?':
        #     return
        # if kpi_name != 'Aggregation':
        #     return
        if kpi_type != 'Base Measure':
            return
        # if kpi_name not in ['What best describes the stocking location of Organic Yogurt?',
        #                     'How is RTS Progresso blocked?',
        #                     'How is RTS Private Label blocked?',
        #                     'When RTS Progresso is vertically blocked, what is adjacent?',
        #                     'If not blocked, where is RTS Private Label stocked?',
        #                     ]:
        #     return
        for i, kpi_line in self.template[kpi_type].iterrows():
            kwargs = function(kpi_name, kpi_line, relevant_scif, general_filters)
            if kwargs['score'] is None:
                continue
            self.write_to_db(kpi_name, **kwargs)

    def calculate_sos(self, kpi_name, kpi_line, relevant_scif, general_filters):
        print('running sos')
        super_cats = relevant_scif['Super Category'].unique().tolist()
        for super_cat in super_cats:
            if not super_cat:
                continue
            den_id = self.entity_dict[super_cat.lower()]
            levels = self.read_cell_from_line(kpi_line, Const.AGGREGATION_LEVELS)
            sos_types = self.read_cell_from_line(kpi_line, Const.SOS_TYPE)
            scif = self.filter_df(relevant_scif, Const.SOS_EXCLUDE_FILTERS, exclude=1)
            scif = self.filter_df(scif, {'Super Category': super_cat})
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

        bay_max_shelf = self.filter_df(self.mpis, general_filters).set_index('bay_number')\
                                                                  .groupby(level=0)['shelf_number'].max().to_dict()
        mpis = self.filter_df(self.mpis, filters)
        if mpis.empty:
            return
        grouped_mpis = mpis.set_index('bay_number').groupby(level=0)

        for bay, shelves in grouped_mpis:
            sub_map = map[bay_max_shelf[bay]]
            shelf_with_most = shelves.groupby('shelf_number')[shelves.columns[0]].count().sort_values().index[-1]
            locations.add(sub_map[shelf_with_most])
            # for shelf in shelves:
            #     locations.add(sub_map[shelf])
            if len(locations) == 3:
                break

        locations = sorted(list(locations))[::-1]
        ordered_result = '-'.join(locations)
        result_fk = self.result_values_dict[ordered_result]

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

            result_fk = self.result_values_dict[result]

    def calculate_adjacency(self, kpi_name, kpi_line, relevant_scif, general_filters):
        for scene in relevant_scif.scene_fk.unique():
            scene_filter = {'scene_fk': scene}
            mpis = self.filter_df(self.mpis, scene_filter)
            allowed = {'product_type': ['Other', 'Empty']}
            filter = {'sub_category_local_name': 'SWEET ROLL DOUGH'}
            items = set(self.filter_df(mpis, filter)['scene_match_fk'].values)
            allowed_items = set(self.filter_df(mpis, allowed)['scene_match_fk'].values)
            items.update(allowed_items)
            if not (items):
                return

            all_graph = AdjacencyGraph(mpis, None, self.products,
                                       product_attributes=['rect_x', 'rect_y'],
                                       name=None, adjacency_overlap_ratio=.4)

            match_to_node = {int(node['match_fk']): i for i, node in all_graph.base_adjacency_graph.nodes(data=True)}
            node_to_match = {val: key for key, val in match_to_node.items()}
            edge_matches = set(sum([[node_to_match[i] for i in all_graph.base_adjacency_graph[match_to_node[item]].keys()]
                                    for item in items], []))
            adjacent_items = edge_matches - items
            adj_mpis = mpis[(mpis['scene_match_fk'].isin(adjacent_items)) &
                            (~mpis['product_type'].isin(Const.SOS_EXCLUDE_FILTERS))]
            # result_items = adj_mpis[desired_column].unique().to_list() # desired column to be read in from template
            # for item in result_items:
            #     pass # save each to DB

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
        max_criteria = max([int(col.split(' ')[1]) for col in kpi_line.index if col != Const.KPI_NAME])
        results_map = self.template[Const.YOGURT_MAP].set_index('Result').to_dict('index')
        for i in xrange(1, max_criteria + 1):
            level = 'Criteria {}'.format(i)
            filters = self.get_kpi_line_filters(kpi_line, name=level)
            sub_func = self.read_cell_from_line(kpi_line, '{} Type'.format(level))
            msl = self.read_cell_from_line(kpi_line, '{} MSL'.format(level))
            pass

    def calculate_block(self, kpi_name, kpi_line, relevant_scif, general_filters):
        score = 0
        orientation = 'Not Blocked'
        for scene in relevant_scif.scene_fk.unique():
            print(kpi_name, scene)
            scene_filter = {'scene_fk': scene}
            filters = self.get_kpi_line_filters(kpi_line)
            filters.update(general_filters)
            mpis = self.filter_df(self.mpis, scene_filter)
            mpis = self.filter_df(mpis, filters)
            if mpis.empty:
                continue
            result = self.block.network_x_block_together(filters, location=scene_filter,
                                                         additional={'allowed_products_filters': Const.ALLOWED_FILTERS,
                                                                     'include_stacking': False,
                                                                     'check_vertical_horizontal': True})
            blocks = result[result['is_block'] is True]

            if not blocks.empty:
                score = 1
                orientation = blocks.loc[0, 'orientation']
                break

        result_fk = self.result_values_dict[orientation]
        kwargs = {'numerator_id': result_fk, 'numerator_result': score, 'score': score, 'result': result_fk,
                  'target': 1}
        return kwargs

    def calculate_base_measure(self, kpi_name, kpi_line, relevant_scif, general_filters):
        filters = self.get_kpi_line_filters(kpi_line)
        filters.update(general_filters)
        scif = self.filter_df(relevant_scif, filters)
        if scif.empty:
            return {"score": None}
        for scene in scif.scene_fk.unique():
            print(kpi_name, scene)
            scene_filter = {'scene_fk': scene}
            scif = self.filter_df(relevant_scif, scene_filter)




    def graph(self, kpi_name, kpi_line, relevant_scif, general_filters):
        x = Block(self.data_provider)
        relevant_filter = {'Segment': 'BISCUIT DOUGH'}
        allowed_filter = {'product_type': ['Empty', 'Other']}
        scene_filter = {'scene_fk': 78}
        res = x.network_x_block_together(relevant_filter, location=scene_filter,
                                         additional={'allowed_products_filters': allowed_filter, 'include_stacking': False})

        for block in res['block']:
            print(sum([len(node[1]['members']) for node in block.nodes(data=True)]))


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
            if '{}param {}'.format(name, c) in attribs and kpi_line['{}param {}'.format(name, c)]:
                filters[kpi_line['{}param {}'.format(name, c)]] += self.splitter(kpi_line['{}value {}'.format(name, c)])
            else:
                if c > 3:  # just in case someone inexplicably chose a nonlinear numbering format.
                    break
            c += 1
        return filters

    @staticmethod
    def splitter(text_str, delimiter=','):
        ret = [text_str]
        if hasattr(ret, 'split'):
            ret = ret.split(delimiter)
        return ret

    @staticmethod
    def sos_with_num_and_dem(kpi_line, num_scif, den_scif, facings_field):

        try:
            Validation.is_empty_df(den_scif)
            Validation.df_columns_equality(den_scif, num_scif)
            Validation.is_subset(den_scif, num_scif)
        except Exception, e:
            msg = "Data verification failed: {}.".format(e)
            return None, None, None

        den = den_scif[facings_field].sum()
        try:
            Validation.is_empty_df(num_scif)
        except Exception as e:
            return (0, 0, den)

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
            print(dependent_index)
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
        if kpi_type == Const.AGGREGATION:
            return self.calculate_sos
        elif kpi_type == Const.TMB:
            return self.calculate_topmiddlebottom
        elif kpi_type == Const.ADJACENCY:
            return self.calculate_adjacency
        elif kpi_type == Const.IADJACENCY:
            return self.calculate_integrated_adjacency
        elif kpi_type == Const.STOCKING:
            return self.calculate_stocking_location
        elif kpi_type == Const.BLOCKING:
            return self.calculate_block
        elif kpi_type == Const.BASE_MEASURE:
            return self.calculate_base_measure()
        else:
            Log.warning("The value '{}' in column sheet in the template is not recognized".format(kpi_type))
            return None

    def make_result_values_dict(self):
        query = "SELECT * FROM static.kpi_result_value;"
        return pd.read_sql_query(query, self.ps_data_provider.rds_conn.db).set_index('value')['pk'].to_dict()

    def create_mpip(self):
        self.data_provider.add_resource_from_table('mpip', 'probedata', 'match_product_in_probe', '*',
                                                   where='''pk in (select distinct p.pk 
                                                            from probedata.probe p
                                                            left join probedata.scene s on p.scene_fk = s.pk
                                                            where s.session_uid = '{}')
                                                            '''.format(self.session_uid))
        self.data_provider.add_resource_from_table('prod_img', 'static_new', 'product_image', ['pk', 'image_direction'])
        mpip = self.data_provider['mpip'].drop('pk', axis=1).merge(self.data_provider['prod_img'],
                                                                  left_on='product_image_fk', right_on='pk')
        return mpip


    def write_to_db(self, kpi_name, score=0, result=None, threshold=None, num=None, den=None, num_id=999,
                    den_id=999):
        """
        writes result in the DB
        :param kpi_name: str
        :param score: float
        :param display_text: str
        :param result: str
        :param threshold: int
        """
        kpi_fk = self.common.get_kpi_fk_by_kpi_type(kpi_name)
        self.common.write_to_db_result(fk=kpi_fk, score=score, result=result, should_enter=True, target=threshold,
                                       numerator_result=num, denominator_result=den, numerator_id=num_id,
                                       denominator_id=den_id)


    def gen_html(self, scene, components, adj_g, mpis):
        img_maker = ImageMaker('gmius', scene, additional_attribs=['sub_category_local_name'])
        html_x = float(img_maker.html_builder.size[1])
        html_y = float(img_maker.html_builder.size[0])
        adj_coords = adj_g.scene_data
        x_mm = max(adj_coords['right'] + (adj_coords['right'] - adj_coords['left']) / 2.0)
        y_mm = max(adj_coords['top'] + (adj_coords['top'] - adj_coords['bottom']) / 2.0)
        x_ratio = html_x / x_mm
        y_ratio = html_y / y_mm
        nodes = []
        mpis_indexed = mpis.set_index('scene_match_fk')
        for i, component in enumerate(components):
            img_maker.html_builder.add_cluster(group_num=i)
            # print(len(component))
            # print('~~~~~~~')
            # print(len(component))
            for j, node in component.nodes(data=True):
                # attribs = self.gen_relevant_node_attribs(node, mpis_indexed, i)
                # node = node['group_attributes']
                # mpis_fk = node['match_fk_list']
                # print(len(mpis_fk))
                mpis_fk = node['match_fk']
                attribs = mpis_indexed.loc[mpis_fk].to_dict()
                attribs['scene_match_fk'] = mpis_fk
                attribs['left'] = node['p1'].x
                attribs['top'] = node['p2'].y
                attribs['width'] = node['p2'].x - node['p1'].x
                attribs['height'] = node['p2'].y - node['p1'].y
                attribs['w'] = attribs['shelf_px_right'] - attribs['shelf_px_left']
                attribs['h'] = attribs['shelf_px_bottom'] - attribs['shelf_px_top']

                # attribs['w'] = attribs['width'] * x_ratio
                # attribs['h'] = attribs['height'] * y_ratio
                # # break
                # attribs['rect_x'] = attribs['left'] * x_ratio
                # attribs['rect_y'] = (node['p1'].y) * y_ratio
                attribs['cluster'] = i

                img_maker.html_builder.add_product(attribs)
                nodes.append(attribs['scene_match_fk'])

        img_maker.html_builder.add_cluster(group_num=-1)
        remaining = mpis[~mpis['scene_match_fk'].isin(nodes)]
        for i, node in remaining.iterrows():
            attribs = self.gen_irrelevant_node_attribs(node)
            img_maker.html_builder.add_product(attribs)

        img_maker.html_builder.products = img_maker.html_builder.products
        return img_maker.html_builder.return_html()

    def gen_irrelevant_node_attribs(self, node):
        attribs = node.to_dict()
        # attribs['scene_match_fk'] = node['scene_match_fk']
        attribs['w'] = attribs['shelf_px_right'] - attribs['shelf_px_left']
        attribs['h'] = attribs['shelf_px_bottom'] - attribs['shelf_px_top']
        attribs['cluster'] = -1
        return attribs