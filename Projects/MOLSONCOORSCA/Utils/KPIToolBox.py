import pandas as pd
import operator as op
from datetime import datetime
from functools import reduce
from collections import defaultdict, namedtuple

from Trax.Algo.Calculations.Core.DataProvider import Data
from Trax.Algo.Calculations.Core.Utils import Validation
from Trax.Utils.Logging.Logger import Log
from Projects.MOLSONCOORSCA.Utils.Const import Const
from KPIUtils_v2.GlobalDataProvider.PsDataProvider import PsDataProvider

from Trax.Tools.ProfessionalServices.TemplatesLoaders.Assortment.AssortmentBase import DATA_QUERY
from KPIUtils_v2.Calculations.AssortmentCalculations import Assortment

from Trax.Tools.ProfessionalServices.TemplatesLoaders.Assortment.AssortmentTemplateLoader import \
    assortment_template_loader

# from KPIUtils_v2.Calculations.BlockCalculations import Block
# from Projects.MOLSONCOORSCA.Utils.BlockCalculations_v3 import Block
# from Trax.Algo.Calculations.Core.GraphicalModel.AdjacencyGraphs import AdjacencyGraph

from networkx import nx

__author__ = 'Sam'


# if you're looking for template path check kpigenerator.find_template


class ToolBox:

    def __init__(self, data_provider, output, common):
        self.global_fail = 0
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

        # self.block = Block(self.data_provider)
        self.project_name = self.data_provider.project_name
        self.session_uid = self.data_provider.session_uid
        self.templates = self.data_provider.all_templates
        self.ps_data_provider = PsDataProvider(self.data_provider, self.output)
        self.result_values_dict = self.make_result_values_dict()
        self.store_assortment = self.ps_data_provider.get_store_assortment()
        self.store_sos_policies = self.ps_data_provider.get_store_policies()
        self.states = self.load_state_data()
        self.labels = self.ps_data_provider.get_labels()
        self.scene_info = self.data_provider[Data.SCENES_INFO]
        self.all_products = self.data_provider[Data.ALL_PRODUCTS]
        self.products = self.data_provider[Data.PRODUCTS]
        self.store_info = self.data_provider[Data.STORE_INFO]
        self.store_id = self.data_provider[Data.STORE_FK]
        self.manufacturer_fk = int(self.data_provider[Data.OWN_MANUFACTURER].iloc[0, 1])
        self.match_product_in_scene = self.data_provider[Data.MATCHES]
        self.result_entities = self.make_result_values_dict()
        self.visit_date = self.data_provider[Data.VISIT_DATE]
        self.session_info = self.data_provider[Data.SESSION_INFO]
        self.scenes = self.scene_info['scene_fk'].tolist()
        self.scif = self.data_provider[Data.SCENE_ITEM_FACTS]
        self.scif['store_fk'] = self.scif['store_id']
        self.tmb_map = pd.read_excel(Const.TMB_MAP_PATH).set_index('Num Shelves').to_dict('index')
        self.res_dict = defaultdict(lambda: defaultdict(int))
        self.full_mpis = pd.DataFrame()
        self.mpis = pd.DataFrame()
        self.load_mpis()
        self.blockchain = {}
        self.template = {}
        self.competitive_brands = None
        self.adj_brands = None
        self.overview = None
        self.dependencies = {}
        self.dependency_lookup = {}
        self.base_measure = None
        self.circle_kpis = {}

        self.store_data = pd.read_sql_query(DATA_QUERY, self.common.rds_conn.db)
        self.rel_store_data = self.store_data[self.store_data['pk'] == self.store_id]

        self.assortment = Assortment(self.data_provider, self.output)
        self.prev_prods = self.load_prev_prods(self.store_id, self.session_info['visit_date'].iloc[0])

        self.mcc_cooler_fk = self.assortment.get_assortment_fk_by_name('MCC Cooler')




    # main functions:
    def main_calculation(self, template_path, comp_path, adj_path):
        """
            This function gets all the scene results from the SceneKPI, after that calculates every session's KPI,
            and in the end it calls "filter results" to choose every KPI and scene and write the results in DB.
        """
        if self.global_fail:
            return
        self.template = pd.read_excel(template_path, sheetname=None)
        self.competitive_brands = self.parse_comp_brands(self.competitive_brands, comp_path)
        self.adj_brands = self.parse_comp_brands(self.adj_brands, adj_path)
        # self.dependencies = {key: None for key in self.template[Const.KPIS][Const.KPI_NAME]}
        # self.dependency_reorder()
        main_template = self.template[Const.KPIS]
        # self.dependency_lookup = main_template.set_index(Const.KPI_NAME)[Const.DEPENDENT].to_dict()
        # self.shun()

        for i, main_line in main_template.iterrows():
            self.global_fail = 0
            self.calculate_base_data(main_line)
        for k, v in self.circle_kpis.items():
            self.write_to_db(**v)

        # self.flag_failures()

    def calculate_base_data(self, main_line):
        kpi_name = main_line[Const.KPI_NAME]
        kpi_type = main_line[Const.TYPE]
        scene_types = self.read_cell_from_line(main_line, Const.SCENE_TYPE)
        hierarchy = self.read_cell_from_line(main_line, Const.HIERARCHY)
        general_filters = {}
        relevant_scif = self.filter_df(self.scif.copy(), Const.SOS_EXCLUDE_FILTERS, exclude=1)
        if scene_types:
            relevant_scif = relevant_scif[relevant_scif['template_name'].isin(scene_types)]
            general_filters['template_name'] = scene_types
        if relevant_scif.empty:
            return

        if kpi_name not in [
            # 'Eye Level Availability'
            # 'Flanker Displays', 'Disruptor Displays'
            # 'Innovation Distribution',
            # 'Display by Location',
            # 'Display by Location'
            # 'Leading Main Section on Left',
            # 'Leading Cooler on Left',
            # 'Leading Cooler on Right',
            # 'Leading Main Section on Right',
            # 'Leading Cold Room on Left',
            # 'Leading Cold Room on Right',
            # 'Share of Segment Cooler Facings'
            # 'Share of Segment Warm Facings',
            # 'ABI Share of Display Space'
            # 'Sleeman Share of Display Space'
            # 'Share of Total Space'
            # 'Warm Base Measurement'
            # 'Warm Bays',
            # 'Dynamic Out of Stock',
            # 'Pack Distribution vs Competitors'
            'Molson Coors Cooler Compliance',
            'Molson Coors Cooler Purity'

        ]:
            return

        if kpi_type not in [
            # 'Share of Facings',
            # 'Share of Shelf',
            # 'Distribution',
            # 'Anchor',
            # 'Base Measurement',
            # 'Bay Count',
            # 'Out of Stock',
            # 'Pack Distribution'
            'Purity'
            ]:
            return

        if kpi_name in ['POP Seasonal Programs', 'Molson Coors Cooler Compliance']:
            return

        print('~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~')
        print('_____{}_____'.format(kpi_name))

        # dependent_kpis = self.read_cell_from_line(main_line, Const.DEPENDENT)
        # dependent_results = self.read_cell_from_line(main_line, Const.DEPENDENT_RESULT)
        # if dependent_kpis:
        #     for dependent_kpi in dependent_kpis:
        #         if self.dependencies[dependent_kpi] not in dependent_results:
        #             if dependent_results or self.dependencies[dependent_kpi] is None:
        #                 return

        kwargs = {'kpi_name': kpi_name, 'kpi_type': kpi_type, 'relevant_scif': relevant_scif,
                  'general_filters': general_filters, 'main_line': main_line, 'pappy': hierarchy[0]}

        for i, lvl in enumerate(hierarchy[::-1]):
            i += 1  # indexed from 1 in templated
            level = {'ind': i, 'lvl': lvl, 'num_col': Const.NUM.format(i), 'den_col': Const.DEN.format(i),
                     'kpi_lvl_name': self.lvl_name(kpi_name, lvl), 'end': len(hierarchy)}
            kwargs.update({'level': level, 'kpi_lvl_name': self.lvl_name(kpi_name, lvl)})  # KPI_level_name duplicated here and spent
            calc_lvl,  write_type = self.calculate_main_kpi(**kwargs)
            if calc_lvl >= len(hierarchy):  # Some kpis will handle all levels of hierarchy simultaneously instead of iteratively
                break
        if write_type:  # Kpi type is used to create the top level of the mobile drill down
            self.circle_kpis[kpi_type] = {'kpi_name': kpi_type, 'score': self.res_dict[kpi_type]['score'],
                                          'numerator_id': self.manufacturer_fk, 'numerator_result': 0,
                                          'result': self.res_dict[kpi_type]['score'],  # score used intentionally here, since only score propagates up the hierarchy
                                          'denominator_id': self.store_id, 'ident_result': kpi_type,
                                          }

    def calculate_main_kpi(self, kpi_name, kpi_lvl_name, kpi_type, **kwargs):
        kpi_line = self.template[kpi_type].set_index(Const.KPI_NAME).loc[kpi_name] if kpi_type in self.template else 0
        function = self.get_kpi_function(kpi_type)
        write_type = True
        try:
            level, all_results = function(kpi_name, kpi_line, **kwargs)
        except Exception as e:
            # print(e)
            # all_results = [{'score': None, 'result': None, 'failed': 0}]
            all_results = []
            level = -1
            write_type = False
            if self.global_fail:
                Log.warning('kpi "{}" did not to calculate'.format(kpi_name))

            else:
                # all_results[0]['failed'] = 1
                Log.error('kpi "{}" failed error: "{}"'.format(kpi_name, e))

        finally:
            if not isinstance(all_results, list) or not all_results:
                all_results = [all_results]
            # print(all_kwargs)
            for results in all_results:
                if not results:
                    write_type = False
                    results = {'score': 0, 'result': None, 'failed': 0}
                if 'kpi_name' not in results:
                    results['kpi_name'] = kpi_lvl_name
                if 'ident_parent' not in results:
                    results['ident_parent'] = kpi_type
                if write_type is True:
                    self.write_to_db(**results)
                    if kpi_line.get('Scored') == 'Y':
                        # self.res_dict[results['ident_parent']]['result'] += results['result']
                        self.res_dict[results['ident_parent']]['score'] += results['score']
                self.dependencies[kpi_name] = results['result']
                # print('-              {}'.format(results['result']))
        return level, write_type

    def flag_failures(self):
        for kpi, val in self.dependencies.items():
            if val is None:
                Log.warning('Warning: KPI "{}" not run for session "{}"'.format(
                    kpi, self.session_uid))

    def calculate_sos_facings(self, kpi_name, kpi_line, **kwargs):
        return self.calculate_sos(kpi_name, kpi_line, col=Const.FACE_COUNT, **kwargs)

    def calculate_sos_shelf(self, kpi_name, kpi_line, relevant_scif, general_filters, **kwargs):
        general_filters['stacking_layer'] = 1
        return self.calculate_sos(kpi_name, kpi_line, relevant_scif, general_filters, col=Const.WIDTH_MM_ADV, **kwargs)

    def calculate_sos(self, kpi_name, kpi_line, relevant_scif, general_filters, level, col, main_line, **kwargs):
        results = []
        items = [None]
        iter_col = None
        param_count = len([c for c in kpi_line.index if 'Value' in c]) + 1
        if kpi_line['Iterative'] and not pd.isna(kpi_line['Iterative']):
            iter_col = main_line[level[kpi_line['Iterative']]]
            if 'num' in kpi_line['Iterative']:
                relevant_scif = self.filter_df(relevant_scif, self.get_kpi_line_filters(kpi_line))
            items = relevant_scif[iter_col].unique()
        for item in items:
            if iter_col is not None:  # this handles iterative and non iterative kpis
                if item is None:  # skip Nones
                    continue
                if 'num' in kpi_line['Iterative']:
                    kpi_line['Param {}'.format(param_count)] = iter_col
                    kpi_line['Value {}'.format(param_count)] = item
                elif 'den' in kpi_line['Iterative']:
                    general_filters[iter_col] = item
            results.append(self.calculate_base_sos(kpi_name, kpi_line, general_filters, col, level, main_line,**kwargs))
        if len(items) == 0 and level['ind'] == level['end'] and level['ind'] > 1:
            df = self.filter_df(self.all_products, self.get_kpi_line_filters(kpi_line))
            res = self.calculate_base_sos(kpi_name, kpi_line, general_filters, col, level, main_line, **kwargs)
            res['numerator_id'] = self.get_fk(df, main_line[level['num_col']])
            results.append(res)

        return level['ind'], results

    def calculate_base_sos(self, kpi_name, kpi_line, general_filters, col, level,  main_line, **kwargs):
        den_df = self.filter_df(self.full_mpis, general_filters)
        if den_df.empty:
            return
        num_df = self.filter_df(den_df, self.get_kpi_line_filters(kpi_line))
        num_id = self.manufacturer_fk
        if not num_df.empty:  # num_df can only be empty on kpis that iterate around the den col, all of these use molson as the num
            num_id = self.get_fk(num_df, main_line[level['num_col']])
        den = den_df[col].sum()
        num = num_df[col].sum()
        if col == 'width_mm_advance':
            num /= Const.MM_FT
            den /= Const.MM_FT
        result = self.safe_divide(num, den)
        ret = {'score': 1, 'result': result, "numerator_result": num, "denominator_result": den,
               'numerator_id': num_id,
               'denominator_id': self.get_fk(den_df, main_line[level['den_col']]),
               'kpi_name': level['kpi_lvl_name']}
        if level['ind'] != level['end']:
            ret['ident_parent'] = kpi_name
        else:
            ret['ident_result'] = kpi_name
        return ret

    def calculate_bay_count(self, kpi_name, kpi_line, relevant_scif, general_filters, main_line, level, **kwargs):
        """
        :param kpi_name: Dataframe of the KPI name
        :param kpi_line: Dataframe of the KPI type
        :param relevant_scif:Filters through the self.scif by excluding Product Type: "Irrelevant" and "Empty"
        :param general_filters: Filters through self.full_mpis to output the nessesary dataframe
        :return: The sum of the unqiue bays (i.e: count of bay number one, count of bay number two) in every scene
        """
        df = self.filter_df(self.full_mpis, general_filters)
        result = df.groupby(['scene_fk', 'bay_number']).first().shape[0]
        results = {'score': 1, 'result': result, 'numerator_result': result,
                   'numerator_id': relevant_scif[main_line[level['num_col']]].iloc[0],
                   'denominator_id': relevant_scif[main_line[level['den_col']]].iloc[0],
                   'kpi_name': self.lvl_name(kpi_name, level['lvl'])}
        return level['end'], results

    def calculate_adjacency_init(self, kpi_name, kpi_line, relevant_scif, general_filters):
        results = []
        kpi_type = kpi_name.replace('Displays', '').strip()
        comps = self.competitive_brands[self.competitive_brands['Comparable Type'].str.contains(kpi_type)]
        for i, comp in comps.iterrows():
            if comp['Target Brand'] == comp['Comparable Brand']:  # Client has specified this as a specific case with it's own logic- check if any sub brands are adjacent
                sub_brands = set(relevant_scif[relevant_scif['brand_name'] == comp['Target Brand']].sub_brand.unique())
                if len(sub_brands) > 1:  # no point if only one sub_brand exists
                    for sb in sub_brands:
                        comp_filt = {'A': {'sub_brand': [sb]},
                                     'B': {'sub_brand': list(set(sub_brands) - set(sb))}}
                        results.append(self.calculate_max_block_adj_base(kpi_name, kpi_line, relevant_scif,
                                                                         general_filters, comp_filter=comp_filt))
            else:
                comp_filt = {'A': {'brand_name': self.splitter(comp['Target Brand'])},
                             'B': {'brand_name': self.splitter(comp['Comparable Brand'])}}
                results.append(self.calculate_max_block_adj_base(kpi_name, kpi_line, relevant_scif, general_filters,
                                                                 comp_filter=comp_filt))
        return results

    def calculate_distribution(self, kpi_name, kpi_line, relevant_scif, level, main_line, **kwargs):
        self.assortment.scif = relevant_scif
        res_2, res_3 = [], []
        lvl3_result = self.assortment.calculate_lvl3_assortment()
        if not lvl3_result.empty:
            lvl3_result = lvl3_result.merge(self.common.kpi_static_data[['pk', 'type', 'client_name']],
                                            left_on='kpi_fk_lvl2', right_on='pk', how='inner')
            lvl3_result = lvl3_result[lvl3_result['client_name'] == kpi_name]
            lvl3_result['target'] = 1
            lvl2_result = self.assortment.calculate_lvl2_assortment(lvl3_result)
            lvl3_result['score'] = lvl3_result['in_store']
            lvl3_result['in_store'] = ['Fail' if res == 0 else 'Pass' for res in lvl3_result['in_store']]
            lvl2_result['score'] = lvl2_result['passes']

            for df in [lvl3_result, lvl2_result]:
                df['store_fk'] = self.store_id

            lvl2_result['result'] = lvl2_result['passes']
            if kpi_line['Metric'] == 'Cap':
                lvl2_result['result'] = 'Pass' if lvl2_result['passes'].iloc[0] >= 1 else 'Fail'

            num_3 = main_line['Numerator 1']
            den_3 = main_line['Denominator 1']
            num_2 = main_line['Numerator 2']
            den_2 = main_line['Denominator 2']

            res_3 = self.parse_assortment_results(lvl3_result, 'in_store', 'score', 'kpi_fk_lvl3', num_3,
                                                  'score', den_3, 'target', None, 'kpi_fk_lvl2')
            res_2 = self.parse_assortment_results(lvl2_result, 'result', 'score', 'kpi_fk_lvl2', num_2,
                                                  'passes', den_2, 'total', 'kpi_fk_lvl2', None)
        return level['end'], res_3 + res_2

    def parse_assortment_results(self, df, result, score, kpi_col, num_id_col, num_col, den_id_col, den_col, self_id,
                                 parent):
        ret = []
        for i, row in df.iterrows():
            kpi_res = {'kpi_fk': row[kpi_col],
                       'numerator_id': row[num_id_col],
                       'numerator_result': row[num_col],
                       'denominator_id': row[den_id_col],
                       'denominator_result': row[den_col],
                       'score': row[score],
                       'result': row[result],  # self.safe_divide(row[num_col], row[den_col]),
                       'ident_result': row[self_id] if self_id else None,
                       }
            if parent is not None:
                kpi_res['ident_parent'] = row[parent]
            ret.append(kpi_res)
        return ret

    def anchor_base(self, general_filters, potential_end, scenes, min_shelves, ratio=False, edges=[]):
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
                if edges and dir not in edges:
                    continue
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

    def calculate_anchor(self, kpi_name, kpi_line, relevant_scif, general_filters, main_line, level, **kwargs):
        results = []
        scenes = relevant_scif['scene_fk'].unique()
        parent_filters = self.get_kpi_line_filters(kpi_line)
        num_col = main_line[level['num_col']]
        den_col = main_line[level['den_col']]
        dens = self.filter_df(relevant_scif, general_filters)[den_col].unique()
        for den in dens:
            total = 0
            score = 0
            result = 'Fail'
            general_filters[den_col] = den
            den_scif = self.filter_df(relevant_scif, general_filters)
            items = self.filter_df(den_scif, parent_filters)[num_col].unique()
            for item in items:
                num_filter = {'brand_fk': item}
                edges = self.splitter(kpi_line[Const.EDGES].strip())
                for edge in edges:
                    anchor = self.anchor_base(general_filters, num_filter, scenes, 1, ratio=False, edges=edges)
                    if anchor[edge]:
                        total += 1
                        result = 'Pass'
                        score = 1
                        results.append({'score': 1, 'result': 'Pass', 'numerator_result': 1,
                                        'ident_parent': '{}-{}'.format(den, kpi_name),
                                        'numerator_id': item, 'denominator_id': den,
                                        'kpi_name': self.lvl_name(kpi_name, 'Brand')})
            results.append({'score': score, 'result': result, 'numerator_result': total,
                            'ident_result': '{}-{}'.format(den, kpi_name),
                            'numerator_id': self.manufacturer_fk, 'denominator_id': den,
                            'kpi_name': self.lvl_name(kpi_name, 'Session')})
        return level['end'], results

    def calculate_base_measure(self, kpi_name, kpi_line, relevant_scif, general_filters, main_line, level, **kwargs):
        sum_col = self.read_cell_from_line(kpi_line, 'Sum Col')[0]
        general_filters.update({'stacking_layer': 1})
        mpis = self.filter_df(self.full_mpis, general_filters)

        # z = self.scif.merge(self.full_mpis.groupby(['scene_fk', 'product_fk'])['face_count'].sum(),
        #                     on=['scene_fk', 'product_fk'])
        # x = z[z.facings.astype(int) != z.face_count.astype(int)][['scene_fk', 'product_fk', 'facings', 'face_count']]
        # a = self.scif

        # result = relevant_scif[sum_col].sum() / Const.MM_FT
        result = mpis[sum_col].sum() / Const.MM_FT
        results = {'score': 1, 'result': result, 'numerator_result': result,
                   'numerator_id': relevant_scif[main_line[level['num_col']]].iloc[0],
                   'denominator_id': relevant_scif[main_line[level['den_col']]].iloc[0],
                   'kpi_name': self.lvl_name(kpi_name, level['lvl'])}
        return level['end'], results

    def calculate_dynamic_oos(self, kpi_name, kpi_line, relevant_scif, main_line, level, **kwargs):
        filters = self.get_kpi_line_filters(kpi_line)
        prev_prods = self.prev_prods['product_fk'].to_frame().merge(self.all_products, on='product_fk')
        prev_prods = self.filter_df(prev_prods, filters)
        matches = self.prev_prods['product_fk'].to_frame().merge(relevant_scif, on='product_fk')\
            .drop_duplicates(subset='product_fk')
        matches = self.filter_df(matches, filters)
        if prev_prods.empty or matches.empty:
            return None, None
        oos = set(prev_prods['product_fk'].values) - set(matches['product_fk'].values)
        ratio = self.safe_divide(len(oos), prev_prods.shape[0])
        results = []
        for sku in oos:
            results.append({'score': 1, 'result': 'OOS', 'ident_parent': self.lvl_name(kpi_name, 'Session'),
                            'numerator_id': sku, 'denominator_id': self.store_id,
                            'kpi_name': self.lvl_name(kpi_name, level['lvl'])})

        results.append({'score': 1, 'result': ratio, 'ident_result': self.lvl_name(kpi_name, 'Session'),
                        'numerator_id': filters['manufacturer_fk'], 'denominator_id': self.store_id,
                        'kpi_name': self.lvl_name(kpi_name, 'Session'), 'numerator_result': len(oos),
                        'denominator_result': prev_prods.shape[0]})

        return level['end'], results

    def calculate_pack_distribution(self, kpi_name, kpi_line, relevant_scif, main_line, level, **kwargs):
        results = []
        relevant_scif = self.filter_df(relevant_scif, {'product_type': 'SKU'})  # Only skus have a pack type
        relevant_scif['package'] = relevant_scif['form_factor'] + relevant_scif['size'].astype(str) +\
                                   relevant_scif['size_unit'] + relevant_scif['number_of_sub_packages'].astype(str)
        filters = self.get_kpi_line_filters(kpi_line)
        num_scif = self.filter_df(relevant_scif, filters)
        if num_scif.empty:
            return
        opposition = self.dictify_competitive_brands('Adjacencies', self.adj_brands)

        brands = [brand for brand in num_scif['brand_name'].unique() if brand in opposition.keys()]
        brand_dict = num_scif.set_index('brand_name')['brand_fk'].to_dict()

        total = {'num': 0, 'den': 0}
        for brand in brands:
            brand_fk = brand_dict[brand]
            adversary_df = self.filter_df(relevant_scif, {'brand_name': opposition[brand]})
            if adversary_df.empty:
                continue
            our_packs = set(num_scif['package'].values)
            enemy_packs = set(adversary_df['package'].values)
            missing = enemy_packs - our_packs
            num = len(enemy_packs) - len(missing)
            results.append({'score': 1, 'result': self.safe_divide(num, len(enemy_packs)), 'numerator_result': num,
                            'denominator_result': len(enemy_packs), 'ident_result': self.lvl_name(kpi_name, brand),
                            'numerator_id': brand_fk, 'kpi_name': self.lvl_name(kpi_name, 'Brand'),
                            'denominator_id': adversary_df.set_index('brand_name')['brand_fk'].iloc[0],
                            'ident_parent': self.lvl_name(kpi_name, self.manufacturer_fk)})

            missing_df = adversary_df[adversary_df['package'].isin(missing)].groupby('package').first()
            for i, row in missing_df.iterrows():
                results.append({'score': 1, 'result': 0, 'ident_parent': self.lvl_name(kpi_name, brand),
                                'numerator_id': row['product_fk'], 'denominator_id': brand_fk,
                                'kpi_name': self.lvl_name(kpi_name, 'SKU')})
            total['num'] += num
            total['den'] += len(enemy_packs)

        results.append({'score': 1, 'result': self.safe_divide(total['num'], total['den']),
                        'numerator_result': total['num'], 'denominator_result': total['den'],
                        'ident_result': self.lvl_name(kpi_name, self.manufacturer_fk),
                        'numerator_id': self.manufacturer_fk, 'denominator_id': self.store_id,
                        'kpi_name': self.lvl_name(kpi_name, 'Session')})
        return level['end'], results

    def calculate_purity(self, kpi_name, kpi_line, relevant_scif, main_line, level, **kwargs):
        remaining = self.get_relevant_prod(kpi_line, relevant_scif)
        result = 0
        if len(remaining) == 0:
            result = 100
        results = {'score': 1, 'result': result, 'numerator_result': len(remaining),
                   'numerator_id': relevant_scif[main_line[level['num_col']]].iloc[0],
                   'denominator_id': relevant_scif[main_line[level['den_col']]].iloc[0],
                   'kpi_name': self.lvl_name(kpi_name, level['lvl'])}
        return level['end'], results

    def calculate_negative_distribution(self, kpi_name, kpi_line, relevant_scif, main_line, level, **kwargs):
        results = []
        relevant_scif['assortment'] = self.mcc_cooler_fk
        remaining = self.get_relevant_prod(kpi_line, relevant_scif)
        results.append({'score': 1, 'result': len(remaining), 'numerator_result': len(remaining),
                        'numerator_id': relevant_scif[main_line['Numerator 2']].iloc[0],
                        'denominator_id': relevant_scif[main_line['Denominator 2']].iloc[0],
                        'kpi_name': self.lvl_name(kpi_name, 'Session'),
                        'ident_result': self.lvl_name(kpi_name, 'Session')})
        for prod in remaining:
            results.append({'score': 1, 'result': 0, 'numerator_result': 0, 'numerator_id': prod,
                            'denominator_id': relevant_scif[main_line[level['den_col']]].iloc[0],
                            'kpi_name': self.lvl_name(kpi_name, 'SKU'),
                            'ident_parent': self.lvl_name(kpi_name, 'SKU')})

    def get_relevant_prod(self, kpi_line, relevant_scif):
        filters = self.get_kpi_line_filters(kpi_line)
        if not filters:
            assortment = self.store_assortment.merge(self.common.kpi_static_data[['pk', 'type', 'client_name']],
                                                     left_on='kpi_fk_lvl2', right_on='pk', how='inner')
            relevant_prod = assortment[assortment['kpi_type'] == 'MCC Cooler']['product_fk']
        else:
            relevant_prod = self.filter_df(relevant_scif, filters)['product_fk']
        relevant_prod = set(relevant_prod.values())
        relevant_prod = self.get_relevant_prod(kpi_line, relevant_scif)
        total_prod = set(relevant_scif['product_fk'])
        return relevant_prod - total_prod
















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
        tmb_map = pd.read_excel(Const.TMB_MAP_PATH).melt(id_vars=['Num Shelves'], var_name=['Shelf']) \
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

        bay_shelf = self.filter_df(self.full_mpis, general_filters).set_index(['scene_fk', 'bay_number']) \
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
                        num_df, {'bay_number': bays[regions[i]:regions[i + 1]]}).shape[0]
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
                seg_list.append(Segment(seg=seg, position=(df[vector] * df['facings']).sum() / facings, facings=facings,
                                        orth_min=mpis_dict[scene]['rect_{}'.format(orth)].min(),
                                        orth_max=mpis_dict[scene]['rect_{}'.format(orth)].max(),
                                        matches=df['matches'].sum()))

            order = [x.seg for x in sorted(seg_list, key=lambda x: x.position)]
            if '_'.join(order) == '_'.join(segments) or \
                    (kpi_line['Reversible'] in ['Y', 'y'] and '_'.join(order) == '_'.join(segments[::-1])):
                flow_count = 1  # 1 is intentional, since loop is smaller than list by 1
                for i in range(1, len(order)):
                    if self.safe_divide(self.seq_axis_engulfs_df(i, seg_list, orth), seg_list[i].facings) >= .1 and \
                            self.safe_divide(self.seq_axis_engulfs_df(i, seg_list, orth, r=1),
                                             seg_list[i - 1].facings) >= .1:
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

    def calculate_max_block_adj_base(self, kpi_name, kpi_line, relevant_scif, general_filters, comp_filter={}):
        allowed_edges = [x.upper() for x in self.read_cell_from_line(kpi_line, Const.EDGES)]
        d = {'A': {}, 'B': {}}
        for k, v in d.items():
            if comp_filter:
                filters = comp_filter[k]
            else:
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
                    result, _ = self.calculate_max_block_adj_base(kpi_name, sub_kpi_line, relevant_scif,
                                                                  general_filters)
                    tally += result['result']
                    size_pass += 1
                except TypeError:  # yeah, i really should define a custom error, but, another day
                    size_skip += 1  # we will ignore subsets that are missing either A group or B group
            if size_pass and not num_count_sizes:  # Family large only needs to be next to one size, so we need to be careful how we increment skip
                skip += 0  # family passed, even if one size failed, so we don't increment skip
            if not size_pass and not num_count_sizes:
                skip += 1  # Family size failed so we increment by one
            else:
                skip += size_skip  # this is the mutipk rt.

        target = len(subsets) * len(sizes) - skip if num_count_sizes else len(
            subsets) - skip  # family only needs to pass one size, multipk both
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
            self.blockchain[
                base] = result, orientation, mpis_dict, blocks  # result_fk = self.result_values_dict[orientation]

        if kpi_line['AntiBlock'] in ['Y', 'y']:
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
            ratio = round(num * 100.0 / den, 2)
        score = 1 if ratio >= target and target else 0
        return ratio, score

    def read_cell_from_line(self, line, col):
        try:
            val = line[col] if not pd.isnull(line[col]) else []
        except:
            val = []
        if val:
            if hasattr(val, 'split'):
                val = self.splitter(val)
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
            if '{}param {}'.format(name, c) in attribs and kpi_line['{}param {}'.format(name, c)] \
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
            res = num * 100.0 / den
        return res

    @staticmethod
    def lvl_name(kpi, lvl):
        return '{} - {}'.format(str(kpi), str(lvl))

    @staticmethod
    def get_fk(df, col):
        if col in Const.LABEL_CONVERTERS:
            col = Const.LABEL_CONVERTERS[col]
        return df[col].iloc[0]

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
            ratio = round((num / float(den)) * 100, 2)
        else:
            ratio = 0

        return ratio, num, den

    def parse_comp_brands(self, out_df, path):
        df = pd.read_excel(path, sheetname=None, header=1)
        df['Granular Groups']['Comparable Brand'] = df['Granular Groups']['Comparable Brand'].apply(lambda x:
                                                                                                    self.splitter(x))
        group = df['Granular Groups link to stores']
        states = set(self.states['name'].unique())

        # Convert assortment style template to one parsable by get_kpi_line_filters
        new_cols = []
        for col in group.columns:
            if Const.COMP_COL_BASE in col:
                n = col.split(' ')[0].replace(Const.COMP_COL_BASE, '')
                if 'Name' in col:
                    new_cols.append('Param {}'.format(n))
                else:
                    new_cols.append('Value {}'.format(n))
            else:
                new_cols.append(col)
        group.columns = new_cols

        # Match correct granular group name to store
        ggn_match = None
        for i, row in group.groupby('Granular Group Name'):  # groupby for compatibility reasons with get_kpi_line_filters
            filters = self.get_kpi_line_filters(row.iloc[0, :])
            # Error checking
            bad_fields = [field for field in filters if field not in self.store_data.columns]
            bad_values = [state for field in filters for state in filters[field] if state not in states]
            if bad_fields or bad_values:
                Log.error("Comparable assortment KPIs issue: \nmissing_fields: {} \nmissing_values: {}".format(
                          bad_fields, bad_values))  # logging inside the loop potentially can cause redundant logs, but i kinda want that.
                continue  # we will cycle through rather than breaking, in case the relevant assortment is untainted
            #  Check if this assortment is the correct one
            if not self.filter_df(self.rel_store_data, filters).empty:
                ggn_match = row['Granular Group Name'].iloc[0]
                break
        if not ggn_match:
            Log.warning('No Comparable Assortment found, Comparable KPIs skipped.')
            return
        return self.filter_df(df['Granular Groups'], {'Granular Group Name': ggn_match})

    def dictify_competitive_brands(self, comp_type, assort):
        opposition = assort[(assort['Start Date'] <= datetime.now()) &
                            (assort['End Date'] >= datetime.now()) &
                            (assort['Comparable Type'] == comp_type)]
        return opposition.set_index('Target Brand')['Comparable Brand'].to_dict()




        # kwargs = {
        #  "_id": "5b0e8150089cba0006ea7ab5d",
        #  "project_name": self.project_name,
        #  "type": self.project_name,
        #  "file_url": path,
        #  "file_name": "lalala OLE!"
        # }
        # ass = assortment_template_loader(kwargs)
        # ass.load_file_to_dataframe()
        # ass.log_unmatched_policies()
        # # ass.is_template_valid()
        #
        # ass.validate_policy_fields()
        #
        # self.errorHandler.are_we_ok()

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

    def add_image_data_to_mpis(self):
        query = '''
                  Select mpip.*, pi.image_direction, pi.face_count, t.name as 'template_name'
                  from probedata.match_product_in_probe mpip
                  left join static_new.product_image pi on mpip.product_image_fk = pi.pk
                  left join probedata.probe pr on mpip.probe_fk = pr.pk
                  left join probedata.scene sc on pr.scene_fk = sc.pk
                  left join static.template t on sc.template_fk = t.pk
                  where sc.session_uid = '{}'
                  '''.format(self.session_uid)
        mpip = pd.read_sql_query(query, self.ps_data_provider.rds_conn.db).merge(self.products, how='left',
                                                                                 on='product_fk', suffixes=['', '_p'])
        self.full_mpis = self.full_mpis.merge(mpip[['pk', Const.FACE_COUNT]], left_on='probe_match_fk', right_on='pk', how='left')
        self.full_mpis[Const.FACE_COUNT].fillna(1, inplace=True)
        self.full_mpis[Const.COUNT] = 1

        return mpip

    def load_state_data(self):
        query = ''' select * from static.state'''
        return pd.read_sql_query(query, self.ps_data_provider.rds_conn.db)

    def load_mpis(self):
        try:
            self.full_mpis = self.match_product_in_scene.merge(self.products, on='product_fk', suffixes=['', '_p']) \
                .merge(self.scene_info, on='scene_fk', suffixes=['', '_s']) \
                .merge(self.templates, on='template_fk', suffixes=['', '_t'])
            self.full_mpis['store_fk'] = self.store_id
            self.full_mpis['face_count'].fillna(1, inplace=True)
            # self.add_image_data_to_mpis()
            self.mpis = self.full_mpis[self.full_mpis['product_type'] != 'Irrelevant']
            self.mpis = self.filter_df(self.mpis, Const.SOS_EXCLUDE_FILTERS, exclude=1)
        except:
            Log.warning('Data does not exist for this session')
            self.global_fail = 1

    def load_prev_prods(self, store, visit_date):
        query = '''
        select distinct
        -- sc.session_uid,
        p.pk as 'original_product_fk',
        p.substitution_product_fk,
        case
            when p.substitution_product_fk is not null
                then p.substitution_product_fk

            else p.pk
        end as 'product_fk'
        from (
                select *
                from probedata.session ses
                where store_fk = {}
                and visit_date < '{}'
                and delete_time is null
                and exclude_status_fk in (1, 4)
                and status = 'Completed'
                and number_of_scenes - number_of_ignored_scenes > 0
                order by visit_date desc
                limit 1
              ) ses
        inner join probedata.scene sc on ses.session_uid = sc.session_uid
        inner join probedata.match_product_in_scene mpis on sc.pk = mpis.scene_fk
        inner join static_new.product p on mpis.product_fk = p.pk
        '''.format(store, datetime.strftime(visit_date, '%Y-%m-%d'), self.manufacturer_fk)
        return pd.read_sql_query(query, self.ps_data_provider.rds_conn.db)

    def get_kpi_function(self, kpi_type):
        """
        transfers every kpi to its own function
        :param kpi_type: value from "sheet" column in the main sheet
        :return: function
        """

        if kpi_type == Const.SHARE_OF_FACINGS:
            return self.calculate_sos_facings
        elif kpi_type == Const.SHARE_OF_SHELF:
            return self.calculate_sos_shelf
        elif kpi_type == Const.ADJACENCY:
            return self.calculate_adjacency_init
        elif kpi_type == Const.SHELF_PLACEMENT:
            return self.calculate_shelf_placement
        elif kpi_type == Const.BAY_COUNT:
            return self.calculate_bay_count
        elif kpi_type == Const.DISTRIBUTION:
            return self.calculate_distribution
        elif kpi_type == Const.ANCHOR:
            return self.calculate_anchor
        elif kpi_type == Const.BASE_MEASUREMENT:
            return self.calculate_base_measure
        elif kpi_type == Const.OUT_OF_STOCK:
            return self.calculate_dynamic_oos
        elif kpi_type == Const.PACK_DISTRIBUTION:
            return self.calculate_pack_distribution
        elif kpi_type == Const.PURITY:
            return self.calculate_purity
        elif kpi_type == Const.NEGATIVE_DISTRIBUTION:
            return self.calculate_negative_distribution()




        # elif kpi_type == Const.SHELF_REGION:
        #     return self.calulate_shelf_region


        # elif kpi_type == Const.BLOCKING:
        #     return self.calculate_block
        # elif kpi_type == Const.BLOCKING_PERCENT:
        #     return self.calculate_block_percent
        # elif kpi_type == Const.BLOCK_ORIENTATION:
        #     return self.calculate_block_orientation
        # elif kpi_type == Const.MULTI_BLOCK:
        #     return self.calculate_multi_block
        # elif kpi_type == Const.MAX_BLOCK_ADJ:
        #     return self.calculate_max_block_adj
        # elif kpi_type == Const.INTEGRATED:
        #     return self.calculate_integrated_core
        # elif kpi_type == Const.BLOCKED_TOGETHER:
        #     return self.calculate_block_together
        # elif kpi_type == Const.SERIAL:
        #     return self.calculate_serial_adj
        # elif kpi_type == Const.SEQUENCE:
        #     return self.calculate_sequence
        # elif kpi_type == Const.RELATIVE_POSTION:
        #     return self.calculate_sequence
        # elif kpi_type == Const.SOS:
        #     return self.calculate_sos
        # elif kpi_type == Const.SAME_AISLE:
        #     return self.calculate_same_aisle
        else:
            Log.warning(
                "The value '{}' in column sheet in the template is not recognized".format(kpi_type))
            return None

    def make_result_values_dict(self):
        query = "SELECT * FROM static.kpi_result_value;"
        return pd.read_sql_query(query, self.ps_data_provider.rds_conn.db).set_index('value')['pk'].to_dict()

    def write_to_db(self, kpi_name=None, score=0, result=None, target=None, numerator_result=0, scene_result_fk=None,
                    denominator_result=None, numerator_id=999, denominator_id=999, ident_result=None, ident_parent=None,
                    kpi_fk = None, hierarchy_only=0, failed=0):
        """
        writes result in the DB
        :param kpi_name: str
        :param score: float
        :param display_text: str
        :param result: str
        :param threshold: int
        """
        # print(kpi_name)
        if numerator_result is False:
            numerator_result = 0
        if not kpi_fk and kpi_name:
            kpi_fk = self.common.get_kpi_fk_by_kpi_type(kpi_name)
        if isinstance(result, str):
            result = self.result_entities[result]
        self.common.write_to_db_result(fk=kpi_fk, score=score, result=result, should_enter=True, target=target,
                                       numerator_result=numerator_result, denominator_result=denominator_result,
                                       numerator_id=numerator_id, denominator_id=denominator_id,
                                       identifier_result=ident_result, identifier_parent=ident_parent,
                                       scene_result_fk=scene_result_fk, hierarchy_only=0)
