import abc

from Trax.Utils.Logging.Logger import Log
import numpy as np
import pandas as pd
from Projects.RINIELSENUS.Utils.Const import TEMPLATE_NAME, DENOMINATOR_FILTER_FIELDS, BLOCK_THRESHOLD, \
    FILTER_NAMING_DICT, MM_TO_FT_RATIO, FACINGS, USE_PROBES, VERTICAL_BLOCK_THRESHOLD
from Projects.RINIELSENUS.Utils.GeneralToolBox import MarsUsGENERALToolBox
from Projects.RINIELSENUS.Utils.ParseTemplates import ParseMarsUsTemplates
from Trax.Utils.DesignPatterns.Decorators import classproperty
# from KPIUtils_v2.Calculations.BlockCalculations import Block
from Projects.RINIELSENUS.Utils.PositionGraph import MarsUsPositionGraphs


__author__ = 'nethanel'


class KpiAtomicKpisCalculator(object):
    def __init__(self, tools, data_provider, preferred_range):
        self._tools = tools
        self._data_provider = data_provider
        self._preferred_range = preferred_range
        self._sales_data = None
        self.survey_response = self._data_provider['survey_responses']
        # self.block = Block(data_provider=data_provider)

    @abc.abstractproperty
    def kpi_type(self):
        pass

    @abc.abstractmethod
    def calculate_atomic_kpi(self, atomic_kpi_data):
        pass

    @staticmethod
    def _create_filter_dict(key, value):
        if value != ['']:
            return {key: value}
        else:
            return {}

    @staticmethod
    def get_denominator_filters(filters):
        out = {}
        for filter_ in DENOMINATOR_FILTER_FIELDS:
            if filters.has_key(filter_):
                out[filter_] = filters[filter_]
        return out

    def get_relevant_scenes_matches(self, scene_type_filter):
        matches = self._tools.match_product_in_scene.copy()
        if scene_type_filter:
            templates = self._data_provider.templates
            relevant_template = templates[templates['template_display_name'].isin(
                scene_type_filter['template_name'])]
            relevant_scenes = self._data_provider.scenes_info.merge(relevant_template, on='template_fk')[
                'scene_fk'].tolist()
            matches = matches[matches['scene_fk'].isin(relevant_scenes)]

        return matches

    def get_scif_matches_by_filters(self, **filters):
        if filters:
            scif = self._data_provider.scene_item_facts
            return len(scif[self._tools.get_filter_condition(scif, **filters)])

    def get_products_by_filters(self, return_value='product_name', **filters):
        if filters:
            scif = self._data_provider.scene_item_facts
            return scif[self._tools.get_filter_condition(scif, **filters)][return_value]

    def get_scif_facing_matches_by_filters(self, **filters):
        if filters:
            scif = self._data_provider.scene_item_facts
            return scif[self._tools.get_filter_condition(scif, **filters)].facings.sum()

    def get_scif_facings_for_scene(self, scene, **filters):
        scif = self._data_provider.scene_item_facts
        scif = scif[(scif['scene_fk'] == scene['scene_fk'])]
        if filters:
            scif = scif[self._tools.get_filter_condition(scif, **filters)]
        if not scif.empty:
            ret = sum(scif[FACINGS])
        else:
            ret = 0
        return ret

    def _get_preferred_range_filter(self):
        return {
            'shelf_number':
                (range(self._preferred_range.iloc[0]['ignored from top'] + 1)
                 [1:], MarsUsGENERALToolBox.EXCLUDE_FILTER),
            'shelf_number_from_bottom':
                (range(self._preferred_range.iloc[0]['ignored from bottom'] + 1)[1:],
                 MarsUsGENERALToolBox.EXCLUDE_FILTER)
        }

    def _get_preferred_range_SPT_filter(self, filters):
        if filters.get('shelf from top'):
            return {'shelf_number': (range(int(float(filters.get('shelf from top')[0])) + 1)[1:])}
        elif filters.get('ignored from top') and filters.get('ignored from bottom'):
            return {
                'shelf_number':
                    (range(int(float(filters.get('ignore from top')[
                     0])) + 1)[1:], MarsUsGENERALToolBox.EXCLUDE_FILTER),
                'shelf_number_from_bottom':
                    (range(int(float(filters.get('ignore from bottom')[
                     0])) + 1)[1:], MarsUsGENERALToolBox.EXCLUDE_FILTER)
            }

    def _get_filtered_products(self, filters):
        products = self._data_provider.products.copy()
        filtered_products_fk = set(
            products[self._tools.get_filter_condition(products, **filters)]['product_fk'].tolist())
        return {'product_fk': list(filtered_products_fk)}

    def _get_allowed_products(self, allowed, filters):
        allowed_products = set()
        allowed.setdefault('product_type', []).extend(self._allowed_products['product_type'])
        for key, value in allowed.items():
            products = self._data_provider.products.copy()
            allowed_bulk = set(
                products[self._tools.get_filter_condition(products, **{key: value})]['product_fk'].tolist())
            if key == 'Private Label':
                section_sub_section_products = self._get_allowed_filters(filters)
                allowed_bulk.intersection_update(section_sub_section_products)
            if key == 'Segment_SPT' and value == ['None']:
                allowed_bulk = set(
                    products[products['Segment_SPT'].isin([None])]['product_fk'].tolist())

            allowed_products.update(allowed_bulk)

        return {'product_fk': list(allowed_products)}

    def _get_allowed_products_without_other(self, allowed, filters):
        allowed_products = set()
        allowed['product_type'] = ['Empty']
        for key, value in allowed.items():
            products = self._data_provider.products.copy()
            allowed_bulk = set(
                products[self._tools.get_filter_condition(products, **{key: value})]['product_fk'].tolist())
            if key == 'Private Label':
                section_sub_section_products = self._get_allowed_filters(filters)
                allowed_bulk.intersection_update(section_sub_section_products)
            allowed_products.update(allowed_bulk)

        return {'product_fk': list(allowed_products)}

    def _get_allowed_filters(self, filters):
        filter_ = {}
        if 'Sub-section' in filters:
            filter_['Sub-section'] = filters['Sub-section']
        else:
            filter_['Section'] = filters['Section']

        products = self._data_provider.products.copy()
        return set(products[self._tools.get_filter_condition(products, **filter_)]['product_fk'])

    @classproperty
    def _allowed_products(self):
        return {'product_type': ['Other', 'Empty']}

    @classproperty
    def _allowed_products_without_other(self):
        return {'product_type': ['Empty']}

    @property
    def sales_data(self):
        if self._sales_data is None:
            self._sales_data = ParseMarsUsTemplates().get_mars_sales_data()
        return self._sales_data

    @property
    def spt_sales_data(self):
        if self._sales_data is None:
            self._sales_data = ParseMarsUsTemplates().get_mars_spt_sales_data()
        return self._sales_data

    def get_sales_ratio(self, set_name, **kargs):
        sales = self.sales_data
        sales = sales.fillna('')
        if 'Customer Brand' in (kargs.keys()):
            sales = sales[(sales['retailer'] == self._data_provider.retailer) &
                          (sales['channel'] == self._data_provider.channel) &
                          ((sales['store type'].str.contains(self._data_provider.store_type)) |
                           (sales['store type'] == '')) &
                          (sales['set'] == set_name.upper()) &
                          (sales['Customer Brand'].isin(kargs['Customer Brand']))]
        else:
            sales = sales[(sales['retailer'] == self._data_provider.retailer) &
                          (sales['channel'] == self._data_provider.channel) &
                          ((sales['store type'].str.contains(self._data_provider.store_type)) |
                           (sales['store type'] == '')) &
                          (sales['set'] == set_name.upper()) &
                          (sales['Customer Brand'] == '')]
        return sales['soa'].iloc[0] if not sales.empty else None

    def get_spt_sales_ratio(self, set_name, **kargs):
        sales = self.spt_sales_data
        sales = sales.fillna('')
        sales = sales[(sales['retailer'] == self._data_provider.retailer) &
                      (sales['channel'] == self._data_provider.channel) &
                      (sales['set'] == set_name.upper())]
        if 'Segment_SPT' in (kargs.keys()):
            sales = sales[sales['Segment_SPT'].isin(kargs['Segment_SPT'])]
        if 'Sub Brand' in (kargs.keys()):
            sales = sales[sales['Sub Brand'].isin(kargs['Sub Brand'])]
        return sales['soa'].iloc[0] if not sales.empty else None

    def get_biggest_scene(self, filters):
        matches = self._tools.match_product_in_scene.copy()
        biggest_scene = matches[self._tools.get_filter_condition(matches, **filters)]
        biggest_scene = biggest_scene.groupby(['scene_fk']).size().reset_index(
            name='counts').sort_values(['counts'], ascending=False)
        return biggest_scene['scene_fk'].values[0]

    @staticmethod
    def _split_filters(all_filters):
        filters = {'all': {}, 'A': {}, 'B': {}, 'C': {}, 'D': {}}
        for key in all_filters:
            key_list = key.split(';')
            if len(key_list) == 1:
                if isinstance(all_filters[key], tuple):
                    filters['all'].setdefault(key, ([], 0))[0].extend(all_filters[key][0])
                else:
                    filters['all'].setdefault(key, []).extend(all_filters[key])
            else:
                filters[key_list[-1]].update({key_list[-2]: all_filters[key]})

        for filter_ in filters:
            filters[filter_] = MarsUsGENERALToolBox.rename_filters_to_match_scif(
                filters[filter_], FILTER_NAMING_DICT)

        return filters

    def log_missing_sales_message(self, atomic_kpi_data, sales):
        if sales == 0:
            msg_base = 'Sales data is 0 for retailer: {}, channel: {}, set: {}'
        else:
            msg_base = 'There are no Sales data is 0 for retailer: {}, channel: {}, set: {}'
        Log.warning(msg_base.format(self._data_provider.retailer,
                                    self._data_provider.channel,
                                    atomic_kpi_data['set']))

    def get_iter_groups(self, scene_avg_shelf, use_probes, threshold, allowed_filter, **filters):
        iter_groups = []
        skipped_scenes = set()
        for index, scene in scene_avg_shelf.iterrows():
            facings = self.get_scif_facings_for_scene(scene, **filters)
            if facings >= threshold:
                if use_probes:
                    matches = self._tools.match_product_in_scene.copy()
                    for probe in matches[matches['scene_fk'] == scene['scene_fk']]['probe_group_id'].unique().tolist():
                        if not self._tools.get_filter_condition(matches[matches['probe_group_id'] == probe], **filters).empty:
                            iter_groups.append((probe, scene))
                else:
                    iter_groups.append((scene['scene_fk'], scene))
            else:
                skipped_scenes.add(scene['scene_fk'])

        return iter_groups, skipped_scenes


class BlockBaseCalculation(KpiAtomicKpisCalculator):
    def calculate_block(self, atomic_kpi_data):
        threshold = float(atomic_kpi_data['filters'].pop(FACINGS)[
                          0]) if atomic_kpi_data['filters'].get(FACINGS) else 0
        use_probes = atomic_kpi_data['filters'].pop(
            USE_PROBES)[0] if atomic_kpi_data['filters'].get(USE_PROBES) else 0
        filters = atomic_kpi_data['filters']
        allowed = atomic_kpi_data['allowed']
        target = atomic_kpi_data['target']
        if atomic_kpi_data['atomic'] == 'Is Nutro Dry Dog food blocked?' or \
                (atomic_kpi_data['atomic'] == 'Nutro Dry Dog and Wet Dog are BOTH BLOCKED'
                 and filters['Sub-section'] == ['DOG MAIN MEAL DRY']):
            allowed_filter = self._get_allowed_products_without_other(allowed, filters)
        else:
            allowed_filter = self._get_allowed_products(allowed, filters)
        scene_type_filter = self._create_filter_dict(
            key=TEMPLATE_NAME, value=atomic_kpi_data['scene_types'])
        filters.update(scene_type_filter)

        scif_matches = self.get_scif_matches_by_filters(**filters)
        if scif_matches == 0:
            return np.nan

        scene_avg_shelf = self._get_relevant_scenes_avg_shelf(filters)

        num_of_scenes = scene_avg_shelf['scene_fk'].nunique()

        iter_groups, skipped_scenes = self.get_iter_groups(
            scene_avg_shelf, use_probes, threshold, allowed_filter, **filters)

        blocked_scenes = 0
        visited = set()

        for item, scene in iter_groups:
            if scene['scene_fk'] in visited:
                continue

            scene_filters = filters.copy()
            scene_filters.update({'scene_fk': scene['scene_fk']})
            if use_probes:
                scene_filters.update({'probe_group_id': item})
            block = self._tools.calculate_block_together(allowed_products_filters=allowed_filter, threshold=threshold,
                                                         minimum_block_ratio=target, vertical=True, **scene_filters)
            if not isinstance(block, dict):
                continue
            # trace_row = {'kpi_display_text': atomic_kpi_data['atomic'], 'scene_id': str(scene['scene_fk']),
            #              'products&brands': self.get_products_by_filters(return_balue='product_name', **scene_filters),
            #              'allowed_products': self.get_products_by_filters(return_balue='product_name', **allowed_filter),
            #              'kpi_pass': block['block']}
            # self._data_provider.trace_container = self._data_provider.trace_container.append(pd.Series(trace_row), ignore_index=True)

            if self.check_block(block, scene['scene_avg_num_of_shelves']):
                blocked_scenes += 1
                visited.add(scene['scene_fk'])

        if float(blocked_scenes) == float(num_of_scenes - len(skipped_scenes))\
                and blocked_scenes > 0:
            return 100
        else:
            return 0

    @abc.abstractproperty
    def kpi_type(self):
        pass

    @abc.abstractmethod
    def check_block(self, block, bay):
        pass

    def _get_relevant_scenes_avg_shelf(self, filters):
        matches = self._tools.match_product_in_scene.copy()
        relevant_matches = matches[self._tools.get_filter_condition(matches, **filters)]
        relevant_scene_bays = relevant_matches[['scene_fk', 'bay_number']].drop_duplicates([
                                                                                           'scene_fk', 'bay_number'])
        scene_bay_shelves = matches.groupby(['scene_fk', 'bay_number'], as_index=False)[
            'shelf_number_from_bottom'].max().rename(columns={'shelf_number_from_bottom': 'num_of_shelves'})
        scene_bay_shelves = scene_bay_shelves.merge(
            relevant_scene_bays, on=['scene_fk', 'bay_number'])
        scene_avg_shelves = scene_bay_shelves.groupby(['scene_fk'], as_index=False)['num_of_shelves'].mean(). \
            rename(columns={'num_of_shelves': 'scene_avg_num_of_shelves'})
        return scene_avg_shelves


class TwoBlocksAtomicKpiCalculation(BlockBaseCalculation):
    def calculate_atomic_kpi(self, atomic_kpi_data):
        col = atomic_kpi_data['filters'].pop(
            'Two Blocks')[0] if 'Two Blocks' in atomic_kpi_data['filters'] else ''
        if not col:
            col = 'Customer Brand'
        if col in FILTER_NAMING_DICT:
            col = FILTER_NAMING_DICT[col]
        result = 100
        for filter in atomic_kpi_data['filters'][col]:
            atomic_kpi_data['filters'][col] = [filter]
            score = self.calculate_block(atomic_kpi_data)
            if not score:
                result = 0
                break
            if np.isnan(score):
                result = np.nan
                break
        return result

    def check_block(self, block, bay):
        return block['block']

    @classproperty
    def kpi_type(self):
        return 'Two Blocks'


class BiggestSceneBlockAtomicKpiCalculation(BlockBaseCalculation):
    def calculate_atomic_kpi(self, atomic_kpi_data):
        threshold = float(atomic_kpi_data['filters'].pop(FACINGS)[
                          0]) if atomic_kpi_data['filters'].get(FACINGS) else 0
        use_probes = atomic_kpi_data['filters'].pop(
            USE_PROBES)[0] if atomic_kpi_data['filters'].get(USE_PROBES) else 0
        filters = atomic_kpi_data['filters']
        allowed = atomic_kpi_data['allowed']
        target = atomic_kpi_data['target']
        allowed_filter = self._get_allowed_products(allowed, filters)

        if atomic_kpi_data['atomic'] == 'Are Meaty Cat Treats blocked?':
            allowed_filter = None

        scene_type_filter = self._create_filter_dict(
            key=TEMPLATE_NAME, value=atomic_kpi_data['scene_types'])
        filters.update(scene_type_filter)

        scif_matches = self.get_scif_matches_by_filters(**filters)
        if scif_matches == 0:
            return np.nan

        biggest_scene = self.get_biggest_scene(filters)
        biggest_scene = self._create_filter_dict(key='scene_fk', value=biggest_scene)
        filters.update(biggest_scene)

        scene_avg_shelf = self._get_relevant_scenes_avg_shelf(filters)

        num_of_scenes = scene_avg_shelf['scene_fk'].nunique()

        iter_groups, skipped_scenes = self.get_iter_groups(
            scene_avg_shelf, use_probes, threshold, allowed_filter, **filters)

        blocked_scenes = 0
        visited = set()

        for item, scene in iter_groups:
            if scene['scene_fk'] in visited:
                continue

            scene_filters = filters.copy()
            scene_filters.update({'scene_fk': scene['scene_fk']})
            if use_probes:
                scene_filters.update({'probe_group_id': item})
            block = self._tools.calculate_block_together(allowed_products_filters=allowed_filter, threshold=threshold,
                                                         minimum_block_ratio=target, vertical=True, **scene_filters)
            if not isinstance(block, dict):
                continue

            if self.check_block(block, scene['scene_avg_num_of_shelves']):
                blocked_scenes += 1
                visited.add(scene['scene_fk'])

        if float(blocked_scenes) == float(num_of_scenes - len(skipped_scenes))\
                and blocked_scenes > 0:
            return 100, block
        else:
            return 0

    def check_block(self, block, bay):
        return block['block']

    @classproperty
    def kpi_type(self):
        return 'Biggest Scene Block'


class BlockAtomicKpiCalculation(BlockBaseCalculation):
    def calculate_atomic_kpi(self, atomic_kpi_data):
        return self.calculate_block(atomic_kpi_data)

    def check_block(self, block, bay):
        return block['block']

    @classproperty
    def kpi_type(self):
        return 'Block per Bay'


class BlockTargetBaseCalculation(KpiAtomicKpisCalculator):
    def calculate_block_target(self, atomic_kpi_data):
        threshold = float(atomic_kpi_data['filters'].pop(FACINGS)[
                          0]) if atomic_kpi_data['filters'].get(FACINGS) else 0
        use_probes = atomic_kpi_data['filters'].pop(
            USE_PROBES)[0] if atomic_kpi_data['filters'].get(USE_PROBES) else 0
        filters = atomic_kpi_data['filters']
        allowed = atomic_kpi_data['allowed']
        target = atomic_kpi_data['target']
        allowed_filter = self._get_allowed_products(allowed, filters)
        scene_type_filter = self._create_filter_dict(
            key=TEMPLATE_NAME, value=atomic_kpi_data['scene_types'])
        filters.update(scene_type_filter)

        scif_matches = self.get_scif_matches_by_filters(**filters)
        if scif_matches == 0:
            return np.nan

        scif_facing_total = self.get_scif_facing_matches_by_filters(**filters)
        sum_products = 0

        scene_avg_shelf = self._get_relevant_scenes_avg_shelf(filters)

        num_of_scenes = scene_avg_shelf['scene_fk'].nunique()

        iter_groups, skipped_scenes = self.get_iter_groups(
            scene_avg_shelf, use_probes, threshold, allowed_filter, **filters)

        blocked_scenes = 0
        visited = set()

        for item, scene in iter_groups:
            if scene['scene_fk'] in visited:
                continue

            scene_filters = filters.copy()
            scene_filters.update({'scene_fk': scene['scene_fk']})
            if use_probes:
                scene_filters.update({'probe_group_id': item})
            block = self._tools.calculate_block_together(allowed_products_filters=allowed_filter, threshold=threshold,
                                                         minimum_block_ratio=target, vertical=True, n_cluster=2,
                                                         **scene_filters)
            if not isinstance(block, dict):
                continue

            visited.add(scene['scene_fk'])
            blocked_scenes += 1

            sum_products += self.get_scif_facing_matches_by_filters(**scene_filters)
            if float(sum_products) / float(scif_facing_total) >= 0.8:
                return 100

        if float(blocked_scenes) == float(num_of_scenes - len(skipped_scenes)) \
                and blocked_scenes > 0:
            return 100
        else:
            return 0

    @abc.abstractproperty
    def kpi_type(self):
        pass

    @abc.abstractmethod
    def check_block(self, block, bay):
        pass

    @abc.abstractproperty
    def target(self):
        pass

    def _get_relevant_scenes_avg_shelf(self, filters):
        matches = self._tools.match_product_in_scene.copy()
        relevant_matches = matches[self._tools.get_filter_condition(matches, **filters)]
        relevant_scene_bays = relevant_matches[['scene_fk', 'bay_number']].drop_duplicates([
                                                                                           'scene_fk', 'bay_number'])
        scene_bay_shelves = matches.groupby(['scene_fk', 'bay_number'], as_index=False)[
            'shelf_number_from_bottom'].max().rename(columns={'shelf_number_from_bottom': 'num_of_shelves'})
        scene_bay_shelves = scene_bay_shelves.merge(
            relevant_scene_bays, on=['scene_fk', 'bay_number'])
        scene_avg_shelves = scene_bay_shelves.groupby(['scene_fk'], as_index=False)['num_of_shelves'].mean(). \
            rename(columns={'num_of_shelves': 'scene_avg_num_of_shelves'})
        return scene_avg_shelves


class BlockTargetAtomicKpiCalculation(BlockTargetBaseCalculation):
    @classproperty
    def target(self):
        return 1

    def calculate_atomic_kpi(self, atomic_kpi_data):
        return self.calculate_block_target(atomic_kpi_data)

    def check_block(self, block, bay):
        return block['block']

    @classproperty
    def kpi_type(self):
        return 'Block per Bay by target'


class VerticalBlockOneSceneAtomicKpiCalculation(BlockBaseCalculation):
    def calculate_atomic_kpi(self, atomic_kpi_data):
        threshold = float(atomic_kpi_data['filters'].pop(FACINGS)[
                          0]) if atomic_kpi_data['filters'].get(FACINGS) else 0
        use_probes = atomic_kpi_data['filters'].pop(
            USE_PROBES)[0] if atomic_kpi_data['filters'].get(USE_PROBES) else 0
        filters = atomic_kpi_data['filters']
        allowed = atomic_kpi_data['allowed']
        target = atomic_kpi_data['target']
        allowed_filter = self._get_allowed_products(allowed, filters)
        scene_type_filter = self._create_filter_dict(
            key=TEMPLATE_NAME, value=atomic_kpi_data['scene_types'])
        filters.update(scene_type_filter)

        scif_matches = self.get_scif_matches_by_filters(**filters)
        if scif_matches == 0:
            return np.nan

        scene_avg_shelf = self._get_relevant_scenes_avg_shelf(filters)

        num_of_scenes = scene_avg_shelf['scene_fk'].nunique()

        iter_groups, skipped_scenes = self.get_iter_groups(
            scene_avg_shelf, use_probes, threshold, allowed_filter, **filters)

        blocked_scenes = 0
        vertical_blocked_scenes = 0
        visited = set()
        visited_vert = set()

        for item, scene in iter_groups:
            if scene['scene_fk'] in visited and scene['scene_fk'] in visited_vert:
                continue

            scene_filters = filters.copy()
            scene_filters.update({'scene_fk': scene['scene_fk']})
            if use_probes:
                scene_filters.update({'probe_group_id': item})
            block = self._tools.calculate_block_together(allowed_products_filters=allowed_filter, threshold=threshold,
                                                         minimum_block_ratio=target, vertical=True, **scene_filters)
            if not isinstance(block, dict):
                continue
            # trace_row = {'kpi_display_text': atomic_kpi_data['atomic'], 'scene_id': str(scene['scene_fk']),
            #              'products&brands': self.get_products_by_filters(return_balue='product_name', **scene_filters),
            #              'allowed_products': self.get_products_by_filters(return_balue='product_name', **allowed_filter),
            #              'kpi_pass': block['block']}
            # self._data_provider.trace_container = self._data_provider.trace_container.append(pd.Series(trace_row), ignore_index=True)

            if self.check_block(block, scene['scene_avg_num_of_shelves']) and scene['scene_fk'] not in visited:
                blocked_scenes += 1
                visited.add(scene['scene_fk'])

            if self.check_vertical_block(block, scene['scene_avg_num_of_shelves']) and scene['scene_fk'] not in visited_vert:
                vertical_blocked_scenes += 1
                visited_vert.add(scene['scene_fk'])

        if float(blocked_scenes) == float(num_of_scenes - len(skipped_scenes)) and float(vertical_blocked_scenes) >= 1:
            return 100
        else:
            return 0

    def check_block(self, block, scene_avg_shelf):
        return block['block']

    def check_vertical_block(self, block, scene_avg_shelf):
        if float(block['shelves']) / float(scene_avg_shelf) > VERTICAL_BLOCK_THRESHOLD:
            return True
        else:
            return False

    @classproperty
    def kpi_type(self):
        return 'Vertical Block One Scene'


class VerticalPreCalcBlockAtomicKpiCalculation(BlockBaseCalculation):
    def calculate_atomic_kpi(self, atomic_kpi_data):
        filters = atomic_kpi_data['filters']
        biggest_scene = self.get_biggest_scene(filters)
        scene_avg_num_of_shelves = self._get_relevant_scenes_avg_shelf(filters).set_index(
            'scene_fk')['scene_avg_num_of_shelves'].to_dict()[biggest_scene]
        if 'results' not in atomic_kpi_data or atomic_kpi_data['results'].empty:
            Log.error('kpi: "{}" not calculated. PreCalc Vertical Block requires Biggest Scene Block dependency'
                      .format(atomic_kpi_data['atomic']))
            return 0

        results = atomic_kpi_data['results']
        blocks = sum(results['errata'].values, [])

        if len(blocks) > 1:
            Log.error('kpi: "{}" currently only configured for 1 dependency'
                      .format(atomic_kpi_data['atomic']))
            return 0

        score = 0
        final = 0
        for block in blocks:
            if not isinstance(block, dict) or not block['block']:
                continue

            if float(block['shelves']) / scene_avg_num_of_shelves > VERTICAL_BLOCK_THRESHOLD:
                score += 1
        if score == len(blocks):
            final = 100
        return final

    @classproperty
    def kpi_type(self):
        return 'PreCalc Vertical Block'


class VerticalBlockAtomicKpiCalculation(BlockBaseCalculation):
    def calculate_atomic_kpi(self, atomic_kpi_data):
        return self.calculate_block(atomic_kpi_data)

    def check_block(self, block, scene_avg_shelf):
        if float(block['shelves']) / float(scene_avg_shelf) > 0.5:
            return True
        else:
            return False

    @classproperty
    def kpi_type(self):
        return 'Vertical Block'


class ShelvedTogetherAtomicKpiCalculation(KpiAtomicKpisCalculator):
    def calculate_atomic_kpi(self, atomic_kpi_data):
        threshold = float(atomic_kpi_data['filters'].pop('Minimum Products')[
                          0]) if atomic_kpi_data['filters'].get('Minimum Products') else 3
        filters = self._split_filters(atomic_kpi_data['filters'])
        scene_type_filter = self._create_filter_dict(key=TEMPLATE_NAME,
                                                     value=atomic_kpi_data['scene_types'])

        scif_filter = scene_type_filter.copy()
        scif_filter.update(filters['A'])
        scif_filter.update(filters['B'])
        scif_matches = self.get_scif_matches_by_filters(**scif_filter)
        if scif_matches == 0:
            return np.nan

        filtered_products_sub_group = filters['A']
        filtered_products_sub_group.update(filters['B'])
        filtered_products_sub_group.update(scene_type_filter)

        filters_separate, relevant_scenes = self._tools.separate_location_filters_from_product_filters(
            **filtered_products_sub_group)
        if len(relevant_scenes) == 0:
            return np.nan
        for scene in relevant_scenes:
            matches = self._tools.match_product_in_scene.copy()
            relevant_probes_group = matches[matches['scene_fk'] == scene]
            for probe_group in relevant_probes_group['probe_group_id'].unique().tolist():
                relevant_probe_group = relevant_probes_group[relevant_probes_group['probe_group_id'] == probe_group]
                matches_a = relevant_probe_group[self._tools.get_filter_condition(
                    relevant_probe_group, **dict(filters['A'], **filters['all']))]
                matches_b = relevant_probe_group[self._tools.get_filter_condition(
                    relevant_probe_group, **dict(filters['B'], **filters['all']))]
                if len(matches_a) >= threshold and len(matches_b) >= threshold:
                    return 100
        return 0

    @classproperty
    def kpi_type(self):
        return 'Shelved Together'


class AnchorAtomicKpiCalculation(KpiAtomicKpisCalculator):
    def calculate_atomic_kpi(self, atomic_kpi_data):
        threshold = float(atomic_kpi_data['filters'].pop('Minimum Products')[
                          0]) if atomic_kpi_data['filters'].get('Minimum Products') else 4
        filters = self._split_filters(atomic_kpi_data['filters'])
        scene_type_filter = self._create_filter_dict(key=TEMPLATE_NAME,
                                                     value=atomic_kpi_data['scene_types'])

        scif_filter = scene_type_filter.copy()
        scif_filter.update(filters['A'])
        scif_filter.update(filters['B'])
        scif_matches = self.get_scif_matches_by_filters(**scif_filter)
        if scif_matches == 0:
            return np.nan

        allowed = atomic_kpi_data['allowed']
        allowed_products = self._get_allowed_products(allowed, filters['A'])
        filtered_products_all = self._get_filtered_products(filters['A'])
        filter_products_after_exclude = {
            'product_fk': list(set(filtered_products_all['product_fk']) - set(allowed_products['product_fk']))}

        filtered_products_sub_group = filters['B']
        filtered_products_sub_group.update(scene_type_filter)

        min_shelves = atomic_kpi_data['target']
        filters_separate, relevant_scenes = self._tools.separate_location_filters_from_product_filters(
            **filtered_products_sub_group)
        if len(relevant_scenes) == 0:
            return np.nan
        for scene in relevant_scenes:
            filters_separate.update({'scene_fk': scene})
            matches = self._tools.match_product_in_scene
            relevant_probe_group = matches[matches['scene_fk'] == scene]
            for probe_group in relevant_probe_group['probe_group_id'].unique().tolist():
                relevant_bay = self.check_bay(
                    relevant_probe_group, probe_group, threshold, **filter_products_after_exclude)
                if not relevant_bay:
                    continue
                for direction in ['left', 'right']:
                    filters_separate.update({'bay_number': relevant_bay[direction]})
                    edge = self._tools.calculate_products_on_edge(position=direction,
                                                                  edge_population=filter_products_after_exclude,
                                                                  min_number_of_shelves=min_shelves,
                                                                  **filters_separate)
                    if edge[0] > 0:
                        return 100
        return 0

    def check_bay(self, matches, probe_group, threshold, **filters):
        relevant_bays = matches[(matches['product_fk'].isin(filters['product_fk'])) & (
            matches['probe_group_id'] == probe_group)]
        relevant_bays['freq'] = relevant_bays.groupby('bay_number')['bay_number'].transform('count')
        relevant_bays = relevant_bays[relevant_bays['freq']
                                      >= threshold]['bay_number'].unique().tolist()
        if relevant_bays:
            relevant_bays.sort()
            return {'left': relevant_bays[0], 'right': relevant_bays[-1]}
        return {}

    @classproperty
    def kpi_type(self):
        return 'Anchor'


class DoubleAnchorAtomicKpiCalculation(KpiAtomicKpisCalculator):
    def calculate_atomic_kpi(self, atomic_kpi_data):
        threshold = float(atomic_kpi_data['filters'].pop('Minimum Products')[
                          0]) if atomic_kpi_data['filters'].get('Minimum Products') else 4
        filters = self._split_filters(atomic_kpi_data['filters'])
        scene_type_filter = self._create_filter_dict(key=TEMPLATE_NAME,
                                                     value=atomic_kpi_data['scene_types'])

        scif_filter = scene_type_filter.copy()
        scif_filter.update(filters['A'])
        scif_filter.update(filters['B'])
        scif_matches = self.get_scif_matches_by_filters(**scif_filter)
        if scif_matches == 0:
            return np.nan

        allowed = atomic_kpi_data['allowed']
        allowed_products = self._get_allowed_products(allowed, filters['A'])
        filtered_products_all = self._get_filtered_products(filters['A'])
        filter_products_after_exclude = {
            'product_fk': list(set(filtered_products_all['product_fk']) - set(allowed_products['product_fk']))}

        filtered_products_sub_group = filters['B']
        filtered_products_sub_group.update(scene_type_filter)

        min_shelves = atomic_kpi_data['target']
        filters_separate, relevant_scenes = self._tools.separate_location_filters_from_product_filters(
            **filtered_products_sub_group)
        if len(relevant_scenes) == 0:
            return np.nan
        for scene in relevant_scenes:
            filters_separate.update({'scene_fk': scene})
            matches = self._tools.match_product_in_scene
            relevant_probe_group = matches[matches['scene_fk'] == scene]
            for probe_group in relevant_probe_group['probe_group_id'].unique().tolist():
                relevant_bay = self.check_bay(relevant_probe_group, probe_group, threshold,
                                              **filter_products_after_exclude)
                # relevant_bay = self.check_bay(scene, threshold, **filter_products_after_exclude)
                if not relevant_bay:
                    continue
                # this specifically checks to make sure each group is on opposite sides of the desired category
                # for instance, this KPI will not pass if the same filter group is on each side
                for situation in [['B', 'C'], ['C', 'B']]:
                    pass_side = 0

                    left_filter = filters[situation[0]]
                    left_filter.update({'bay_number': relevant_bay['left']})
                    left_filter.update({'scene_fk': scene})

                    right_filter = filters[situation[1]]
                    right_filter.update({'bay_number': relevant_bay['right']})
                    right_filter.update({'scene_fk': scene})

                    for direction, direction_filter in {'left': left_filter, 'right': right_filter}.items():
                        edge = self._tools.calculate_products_on_edge(position=direction,
                                                                      edge_population=filter_products_after_exclude,
                                                                      min_number_of_shelves=min_shelves,
                                                                      **direction_filter)
                        if edge[0] > 0:
                            pass_side += 1
                    if pass_side == 2:
                        return 100
        return 0

    def check_bay_no_probe(self, scene, threshold, **filters):
        matches = self._tools.match_product_in_scene
        relevant_bays = matches[
            (matches['product_fk'].isin(filters['product_fk'])) & (matches['scene_fk'] == scene)]
        relevant_bays['freq'] = relevant_bays.groupby('bay_number')['bay_number'].transform('count')
        relevant_bays = relevant_bays[relevant_bays['freq']
                                      >= threshold]['bay_number'].unique().tolist()
        if relevant_bays:
            relevant_bays.sort()
            return {'left': relevant_bays[0], 'right': relevant_bays[-1]}
        return {}

    def check_bay(self, matches, probe_group, threshold, **filters):
        relevant_bays = matches[(matches['product_fk'].isin(filters['product_fk'])) & (
            matches['probe_group_id'] == probe_group)]
        relevant_bays['freq'] = relevant_bays.groupby('bay_number')['bay_number'].transform('count')
        relevant_bays = relevant_bays[relevant_bays['freq']
                                      >= threshold]['bay_number'].unique().tolist()
        if relevant_bays:
            relevant_bays.sort()
            return {'left': relevant_bays[0], 'right': relevant_bays[-1]}
        return {}

    @classproperty
    def kpi_type(self):
        return 'Double Anchor'


class SurveyAtomicKpiCalculation(KpiAtomicKpisCalculator):
    def calculate_atomic_kpi(self, atomic_kpi_data):
        filters = self._split_filters(atomic_kpi_data['filters'])
        survey_question = filters['A']['Question Text']
        survey_excepted_answers = filters['B']['Excepted Answers']

        survey_answer = self.survey_response[self.survey_response['question_text'].isin(
            [survey_question])]['selected_option_text']
        if not survey_answer.empty and (survey_answer.iloc[0] in survey_excepted_answers):
            return 100
        elif survey_answer.empty:
            return np.nan
        return 0

    @classproperty
    def kpi_type(self):
        return 'Survey'


class ShelfLevelAtomicKpiCalculation(KpiAtomicKpisCalculator):
    def calculate_atomic_kpi(self, atomic_kpi_data):
        filters = atomic_kpi_data['filters']
        filters.update(self._get_preferred_range_filter())
        scene_type_filter = self._create_filter_dict(
            key=TEMPLATE_NAME, value=atomic_kpi_data['scene_types'])
        filters.update(scene_type_filter)
        assortment = self._tools.calculate_assortment(**filters)
        return 100 if assortment > 0 else 0

    @classproperty
    def kpi_type(self):
        return 'Shelf Level Availability'


class ShelfLevelSPTAtomicKpiCalculation(KpiAtomicKpisCalculator):
    def calculate_atomic_kpi(self, atomic_kpi_data):
        filters = atomic_kpi_data['filters']
        # filters.update(self._get_preferred_range_SPT_filter(filters))
        filters.update(self._get_preferred_range_filter())

        scene_type_filter = self._create_filter_dict(
            key=TEMPLATE_NAME, value=atomic_kpi_data['scene_types'])
        filters.update(scene_type_filter)
        assortment = self._tools.calculate_assortment(**filters)
        return 100 if assortment > 0 else 0

    @classproperty
    def kpi_type(self):
        return 'Shelf Level Availability SPT'


class ShelfLevelPercentAtomicKpiCalculation(KpiAtomicKpisCalculator):
    def calculate_atomic_kpi(self, atomic_kpi_data):
        filters = atomic_kpi_data['filters']
        scene_type_filter = self._create_filter_dict(
            key=TEMPLATE_NAME, value=atomic_kpi_data['scene_types'])
        filters.update(scene_type_filter)

        scif_filter = scene_type_filter.copy()
        scif_filter.update(filters)
        scif_matches = self.get_scif_matches_by_filters(**scif_filter)
        if scif_matches == 0:
            return np.nan

        filters_pr = filters.copy()
        filters_pr.update(self._get_preferred_range_filter())

        sos = self._tools.calculate_share_of_shelf(sos_filters=filters_pr, **filters)
        return sos * 100

    def get_relevant_scenes_and_shelves(self, filters_for_relevant_shelves, scene_type_filter):
        filters_for_relevant_shelves.update(scene_type_filter)
        filters_for_relevant_shelves.update(self._get_preferred_range_filter())
        matches = self._tools.match_product_in_scene.copy()
        relevant_matches = matches[self._tools.get_filter_condition(
            matches, **filters_for_relevant_shelves)]
        relevant_scene_shelves = relevant_matches[['scene_fk', 'shelf_number']].drop_duplicates(
            ['scene_fk', 'shelf_number'])
        return relevant_scene_shelves

    @classproperty
    def kpi_type(self):
        return 'Shelf Level Percent'


class AdjacencyAtomicKpiCalculation(KpiAtomicKpisCalculator):
    @classproperty
    def kpi_type(self):
        return 'Adjacency'

    def calculate_atomic_kpi(self, atomic_kpi_data):
        target = atomic_kpi_data['target']
        a_target = atomic_kpi_data['filters'].get('Target A')
        if a_target:
            atomic_kpi_data['filters'].pop('Target A')
            a_target = float(a_target[0])
        b_target = atomic_kpi_data['filters'].get('Target B')
        if b_target:
            atomic_kpi_data['filters'].pop('Target B')
            b_target = float(b_target[0])

        all_filters = atomic_kpi_data['filters'].copy()
        filters = self._split_filters(all_filters)
        allowed_filter = self._get_allowed_products(atomic_kpi_data['allowed'], filters['all'])
        allowed_filter_without_other = self._get_allowed_products_without_other(atomic_kpi_data['allowed'],
                                                                                filters['all'])
        scene_type_filter = self._create_filter_dict(
            key=TEMPLATE_NAME, value=atomic_kpi_data['scene_types'])

        scif_filter = scene_type_filter.copy()
        scif_filter.update(filters['all'])
        scif_filter.update(filters['A'])
        scif_matches = self.get_scif_matches_by_filters(**scif_filter)
        if scif_matches == 0:
            return np.nan

        for group in ['B', 'C', 'D']:
            if filters[group]:
                adjacency = self._check_adjacency(filters, 'A', group, scene_type_filter, allowed_filter,
                                                  allowed_filter_without_other, a_target, b_target, target)
                if adjacency:
                    return 100

        return 0

    def _get_group_product_list(self, filters, group):
        products = self._data_provider.products.copy()
        filter_ = filters[group]
        filter_.update(filters['all'])
        # filter_.update({'Sub-section': filters['all']['Sub-section']})
        product_list = products[self._tools.get_filter_condition(
            products, **filter_)]['product_fk'].tolist()
        return product_list

    def _check_groups_adjacency(self, a_product_list, b_product_list, scene_type_filter, allowed_filter,
                                allowed_filter_without_other, check_a_group_blocked, a_target, b_target, target):
        a_b_union = list(set(a_product_list) | set(b_product_list))

        a_filter = {'product_fk': a_product_list}
        b_filter = {'product_fk': b_product_list}
        a_b_filter = {'product_fk': a_b_union}
        a_b_filter.update(scene_type_filter)

        matches = self._tools.match_product_in_scene.copy()
        relevant_scenes = matches[self._tools.get_filter_condition(
            matches, **a_b_filter)]['scene_fk'].unique().tolist()

        result = False
        for scene in relevant_scenes:
            a_filter_for_block = a_filter.copy()
            a_filter_for_block.update({'scene_fk': scene})
            b_filter_for_block = b_filter.copy()
            b_filter_for_block.update({'scene_fk': scene})
            try:
                a_products = self.get_products_by_filters('product_fk', **a_filter_for_block)
                b_products = self.get_products_by_filters('product_fk', **b_filter_for_block)
                if sorted(a_products.tolist()) == sorted(b_products.tolist()):
                    continue
            except:
                pass
            if a_target:
                brand_a_blocked = self._tools.calculate_block_together(allowed_products_filters=allowed_filter,
                                                                       minimum_block_ratio=a_target,
                                                                       vertical=True, **a_filter_for_block)
                if not isinstance(brand_a_blocked, dict):
                    continue

            if b_target:
                brand_b_blocked = self._tools.calculate_block_together(allowed_products_filters=allowed_filter,
                                                                       minimum_block_ratio=b_target,
                                                                       vertical=True, **b_filter_for_block)
                if not isinstance(brand_b_blocked, dict):
                    continue

            a_b_filter_for_block = a_b_filter.copy()
            a_b_filter_for_block.update({'scene_fk': scene})

            block = self._tools.calculate_block_together(allowed_products_filters=allowed_filter_without_other,
                                                         minimum_block_ratio=target, block_of_blocks=True,
                                                         block_products1=a_filter, block_products2=b_filter,
                                                         **a_b_filter_for_block)
            if block:
                return True
        return result

    def _check_adjacency(self, filters, group_a, group_b, scene_type_filter, allowed_filter,
                         allowed_filter_without_other, a_target, b_target, target):
        is_brand_in_a = 'brand_name' in filters[group_a]
        is_brand_in_b = 'brand_name' in filters[group_b]

        check_a_group_blocked = is_brand_in_a and (not is_brand_in_b)

        a_product_list = self._get_group_product_list(filters, group_a)
        b_product_list = self._get_group_product_list(filters, group_b)

        adjacency = self._check_groups_adjacency(a_product_list, b_product_list, scene_type_filter, allowed_filter,
                                                 allowed_filter_without_other, check_a_group_blocked,
                                                 a_target, b_target, target)

        return adjacency


class LinearFairShareAtomicKpiCalculation(KpiAtomicKpisCalculator):
    def calculate_atomic_kpi(self, atomic_kpi_data):
        filters = atomic_kpi_data['filters']
        sales = self.get_sales_ratio(atomic_kpi_data['set'], **filters)
        if not sales:
            self.log_missing_sales_message(atomic_kpi_data, sales)
            return np.nan

        if atomic_kpi_data['atomic'] == 'MARS Space goal':
            sales = 20
        scene_type_filter = self._create_filter_dict(
            key=TEMPLATE_NAME, value=atomic_kpi_data['scene_types'])
        filters.update(scene_type_filter)
        denominator_filters = self.get_denominator_filters(filters)
        sos = self._tools.calculate_linear_share_of_shelf(
            sos_filters=filters, **denominator_filters)
        if sos == None:
            return np.nan
        elif sos == 0:
            return 0
        result = round(((sos * 100) / sales) * 100, 2)
        return result

    @classproperty
    def kpi_type(self):
        return 'Share of Shelf Linear / Share of Sales'


class LinearFairShareNumeratorAtomicKpiCalculation(KpiAtomicKpisCalculator):
    def calculate_atomic_kpi(self, atomic_kpi_data):
        filters = atomic_kpi_data['filters']
        sales = self.get_sales_ratio(atomic_kpi_data['set'], **filters)
        if not sales:
            self.log_missing_sales_message(atomic_kpi_data, sales)
            return np.nan

        if atomic_kpi_data['atomic'] == 'MARS Space goal':
            sales = 20
        scene_type_filter = self._create_filter_dict(
            key=TEMPLATE_NAME, value=atomic_kpi_data['scene_types'])
        filters.update(scene_type_filter)
        denominator_filters = self.get_denominator_filters(filters)
        return self._tools.calculate_linear_share_of_shelf_numerator(sos_filters=filters, **denominator_filters)

    @classproperty
    def kpi_type(self):
        return 'Share of Shelf Linear / Share of Sales - numerator'


class LinearFairShareDenominatorAtomicKpiCalculation(KpiAtomicKpisCalculator):
    def calculate_atomic_kpi(self, atomic_kpi_data):
        filters = atomic_kpi_data['filters']
        sales = self.get_sales_ratio(atomic_kpi_data['set'], **filters)
        if not sales:
            self.log_missing_sales_message(atomic_kpi_data, sales)
            return np.nan

        if atomic_kpi_data['atomic'] == 'MARS Space goal':
            sales = 20
        scene_type_filter = self._create_filter_dict(
            key=TEMPLATE_NAME, value=atomic_kpi_data['scene_types'])
        filters.update(scene_type_filter)
        denominator_filters = self.get_denominator_filters(filters)
        return self._tools.calculate_linear_share_of_shelf_denominator(sos_filters=filters, **denominator_filters)

    @classproperty
    def kpi_type(self):
        return 'Share of Shelf Linear / Share of Sales - denominator'


class LinearFairShareSPTAtomicKpiCalculation(KpiAtomicKpisCalculator):
    def calculate_atomic_kpi(self, atomic_kpi_data):
        filters = atomic_kpi_data['filters']
        sales = self.get_spt_sales_ratio(atomic_kpi_data['set'], **filters)
        if not sales:
            self.log_missing_sales_message(atomic_kpi_data, sales)
            return np.nan

        scene_type_filter = self._create_filter_dict(
            key=TEMPLATE_NAME, value=atomic_kpi_data['scene_types'])
        filters.update(scene_type_filter)
        denominator_filters = self.get_denominator_filters(filters)
        sos = self._tools.calculate_linear_share_of_shelf(
            sos_filters=filters, **denominator_filters)
        if sos == None:
            return np.nan
        elif sos == 0:
            return 0
        result = round(((sos * 100) / sales) * 100, 2)
        return result

    @classproperty
    def kpi_type(self):
        return 'Share of Shelf Linear / Share of Sales SPT'


class LinearFairShareNumeratorSPTAtomicKpiCalculation(KpiAtomicKpisCalculator):
    def calculate_atomic_kpi(self, atomic_kpi_data):
        filters = atomic_kpi_data['filters']
        sales = self.get_spt_sales_ratio(atomic_kpi_data['set'], **filters)
        if not sales:
            self.log_missing_sales_message(atomic_kpi_data, sales)
            return np.nan

        if atomic_kpi_data['atomic'] == 'MARS Space goal':
            sales = 20
        scene_type_filter = self._create_filter_dict(
            key=TEMPLATE_NAME, value=atomic_kpi_data['scene_types'])
        filters.update(scene_type_filter)
        denominator_filters = self.get_denominator_filters(filters)
        return self._tools.calculate_linear_share_of_shelf_numerator(sos_filters=filters, **denominator_filters)

    @classproperty
    def kpi_type(self):
        return 'Share of Shelf Linear / Share of Sales - numerator SPT'


class LinearFairShareDenominatorSPTAtomicKpiCalculation(KpiAtomicKpisCalculator):
    def calculate_atomic_kpi(self, atomic_kpi_data):
        filters = atomic_kpi_data['filters']
        sales = self.get_spt_sales_ratio(atomic_kpi_data['set'], **filters)
        if not sales:
            self.log_missing_sales_message(atomic_kpi_data, sales)
            return np.nan

        if atomic_kpi_data['atomic'] == 'MARS Space goal':
            sales = 20
        scene_type_filter = self._create_filter_dict(
            key=TEMPLATE_NAME, value=atomic_kpi_data['scene_types'])
        filters.update(scene_type_filter)
        denominator_filters = self.get_denominator_filters(filters)
        return self._tools.calculate_linear_share_of_shelf_denominator(sos_filters=filters, **denominator_filters)

    @classproperty
    def kpi_type(self):
        return 'Share of Shelf Linear / Share of Sales - denominator SPT'


class LinearPreferredRangeShareAtomicKpiCalculation(KpiAtomicKpisCalculator):
    def calculate_atomic_kpi(self, atomic_kpi_data):
        sales = self.get_sales_ratio(atomic_kpi_data['set'])
        if not sales:
            self.log_missing_sales_message(atomic_kpi_data, sales)
            return np.nan

        numerator_filters = atomic_kpi_data['filters']
        scene_type_filter = self._create_filter_dict(
            key=TEMPLATE_NAME, value=atomic_kpi_data['scene_types'])
        numerator_filters.update(scene_type_filter)
        numerator_filters.update(self._get_preferred_range_filter())
        denominator_filters = self.get_denominator_filters(numerator_filters)
        denominator_filters.update(self._get_preferred_range_filter())
        sos = self._tools.calculate_linear_share_of_shelf(
            sos_filters=numerator_filters, **denominator_filters)
        if sos == None:
            return np.nan
        elif sos == 0:
            return 0
        result = round(((sos * 100) / sales) * 100, 2)
        return result

    @classproperty
    def kpi_type(self):
        return 'Linear Share of shelf in Preferred Range / Share of Sales'


class LinearPreferredRangeShareNumeratorAtomicKpiCalculation(KpiAtomicKpisCalculator):
    def calculate_atomic_kpi(self, atomic_kpi_data):
        sales = self.get_sales_ratio(atomic_kpi_data['set'])
        if not sales:
            self.log_missing_sales_message(atomic_kpi_data, sales)
            return np.nan

        numerator_filters = atomic_kpi_data['filters']
        scene_type_filter = self._create_filter_dict(
            key=TEMPLATE_NAME, value=atomic_kpi_data['scene_types'])
        numerator_filters.update(scene_type_filter)
        numerator_filters.update(self._get_preferred_range_filter())
        denominator_filters = self.get_denominator_filters(numerator_filters)
        denominator_filters.update(self._get_preferred_range_filter())
        return self._tools.calculate_linear_share_of_shelf_numerator(sos_filters=numerator_filters, **denominator_filters)

    @classproperty
    def kpi_type(self):
        return 'Linear Share of shelf in Preferred Range / Share of Sales - numerator'


class LinearPreferredRangeShareDenominatorAtomicKpiCalculation(KpiAtomicKpisCalculator):
    def calculate_atomic_kpi(self, atomic_kpi_data):
        sales = self.get_sales_ratio(atomic_kpi_data['set'])
        if not sales:
            self.log_missing_sales_message(atomic_kpi_data, sales)
            return np.nan

        numerator_filters = atomic_kpi_data['filters']
        scene_type_filter = self._create_filter_dict(
            key=TEMPLATE_NAME, value=atomic_kpi_data['scene_types'])
        numerator_filters.update(scene_type_filter)
        numerator_filters.update(self._get_preferred_range_filter())
        denominator_filters = self.get_denominator_filters(numerator_filters)
        denominator_filters.update(self._get_preferred_range_filter())
        return self._tools.calculate_linear_share_of_shelf_denominator(sos_filters=numerator_filters, **denominator_filters)

    @classproperty
    def kpi_type(self):
        return 'Linear Share of shelf in Preferred Range / Share of Sales - denominator'


class LinearPreferredRangeShareSPTAtomicKpiCalculation(KpiAtomicKpisCalculator):
    def calculate_atomic_kpi(self, atomic_kpi_data):
        filters = atomic_kpi_data['filters']
        sales = self.get_spt_sales_ratio(atomic_kpi_data['set'])
        if not sales:
            self.log_missing_sales_message(atomic_kpi_data, sales)
            return np.nan

        numerator_filters = atomic_kpi_data['filters']
        scene_type_filter = self._create_filter_dict(
            key=TEMPLATE_NAME, value=atomic_kpi_data['scene_types'])
        numerator_filters.update(scene_type_filter)
        # numerator_filters.update(self._get_preferred_range_SPT_filter(filters))
        numerator_filters.update(self._get_preferred_range_filter())
        denominator_filters = self.get_denominator_filters(numerator_filters)
        # denominator_filters.update(self._get_preferred_range_SPT_filter(filters))
        denominator_filters.update(self._get_preferred_range_filter())
        sos = self._tools.calculate_linear_share_of_shelf(
            sos_filters=numerator_filters, **denominator_filters)
        if sos == None:
            return np.nan
        elif sos == 0:
            return 0
        result = round(((sos * 100) / sales) * 100, 2)
        return result

    @classproperty
    def kpi_type(self):
        return 'Linear Share of shelf in Preferred Range / Share of Sales SPT'


class LinearPreferredRangeShareNumeratorSPTAtomicKpiCalculation(KpiAtomicKpisCalculator):
    def calculate_atomic_kpi(self, atomic_kpi_data):
        filters = atomic_kpi_data['filters']
        sales = self.get_spt_sales_ratio(atomic_kpi_data['set'])
        if not sales:
            self.log_missing_sales_message(atomic_kpi_data, sales)
            return np.nan

        numerator_filters = atomic_kpi_data['filters']
        scene_type_filter = self._create_filter_dict(
            key=TEMPLATE_NAME, value=atomic_kpi_data['scene_types'])
        numerator_filters.update(scene_type_filter)
        # numerator_filters.update(self._get_preferred_range_SPT_filter(filters))
        numerator_filters.update(self._get_preferred_range_filter())
        denominator_filters = self.get_denominator_filters(numerator_filters)
        # denominator_filters.update(self._get_preferred_range_SPT_filter(filters))
        denominator_filters.update(self._get_preferred_range_filter())
        return self._tools.calculate_linear_share_of_shelf_numerator(sos_filters=numerator_filters, **denominator_filters)

    @classproperty
    def kpi_type(self):
        return 'Linear Share of shelf in Preferred Range / Share of Sales - numerator SPT'


class LinearPreferredRangeShareDenominatorSPTAtomicKpiCalculation(KpiAtomicKpisCalculator):
    def calculate_atomic_kpi(self, atomic_kpi_data):
        filters = atomic_kpi_data['filters']
        sales = self.get_spt_sales_ratio(atomic_kpi_data['set'])
        if not sales:
            self.log_missing_sales_message(atomic_kpi_data, sales)
            return np.nan

        numerator_filters = atomic_kpi_data['filters']
        scene_type_filter = self._create_filter_dict(
            key=TEMPLATE_NAME, value=atomic_kpi_data['scene_types'])
        numerator_filters.update(scene_type_filter)
        # numerator_filters.update(self._get_preferred_range_SPT_filter(filters))
        numerator_filters.update(self._get_preferred_range_filter())
        denominator_filters = self.get_denominator_filters(numerator_filters)
        # denominator_filters.update(self._get_preferred_range_SPT_filter(filters))
        denominator_filters.update(self._get_preferred_range_filter())
        return self._tools.calculate_linear_share_of_shelf_denominator(sos_filters=numerator_filters, **denominator_filters)

    @classproperty
    def kpi_type(self):
        return 'Linear Share of shelf in Preferred Range / Share of Sales - denominator SPT'


class ShareOfAssortmentPrAtomicKpiCalculation(KpiAtomicKpisCalculator):
    def calculate_atomic_kpi(self, atomic_kpi_data):
        result = np.nan
        nbil_products = atomic_kpi_data['nbil_products']
        if not nbil_products.empty:
            products = self._data_provider.all_products[['product_ean_code']].copy()
            products.loc[:, 'upc'] = products['product_ean_code'].str.zfill(12)
            relevant_ean_codes = products.merge(nbil_products, on='upc')[
                ['either', 'product_ean_code']]
            if not relevant_ean_codes.empty:
                relevant_ean_codes_either = relevant_ean_codes[relevant_ean_codes['either'] != '']
                relevant_ean_codes = relevant_ean_codes[relevant_ean_codes['either'] == '']

                scene_type_filter = self._create_filter_dict(key=TEMPLATE_NAME,
                                                             value=atomic_kpi_data['scene_types'])
                ean_code_filter = {
                    'product_ean_code': relevant_ean_codes['product_ean_code'].tolist()}
                shelves_filter = self._get_preferred_range_filter()
                filters = {}
                filters.update(scene_type_filter)
                filters.update(ean_code_filter)
                filters.update(shelves_filter)
                num_of_assorted_products = self._tools.calculate_assortment(**filters)
                nbil_product_numbers = len(nbil_products[nbil_products['either'] == ''])
                nbil_either_product_numbers = len(
                    nbil_products[nbil_products['either'] != '']['either'].unique())

                if not relevant_ean_codes_either.empty:
                    for either_number in relevant_ean_codes_either['either'].unique().tolist():
                        relevant_eans = relevant_ean_codes_either[relevant_ean_codes_either['either'] == either_number]
                        ean_code_filter = {
                            'product_ean_code': relevant_eans['product_ean_code'].tolist()}
                        filters.update(ean_code_filter)
                        result = self._tools.calculate_assortment(**filters)
                        if result:
                            num_of_assorted_products += 1

                # scif = self._data_provider.scene_item_facts.copy()
                num_of_expected_product = nbil_product_numbers + nbil_either_product_numbers
                # num_of_expected_product = len(scif[scif['product_ean_code'].isin(relevant_ean_codes)])
                if num_of_expected_product == 0:
                    return np.nan
                result = round((float(num_of_assorted_products) /
                                float(num_of_expected_product)) * 100, 2)

        return result

    @classproperty
    def kpi_type(self):
        return 'NBIL SOA'


class ShareOfAssortmentPrNumeratorAtomicKpiCalculation(KpiAtomicKpisCalculator):
    def calculate_atomic_kpi(self, atomic_kpi_data):
        result = np.nan
        nbil_products = atomic_kpi_data['nbil_products']
        if not nbil_products.empty:
            products = self._data_provider.all_products[['product_ean_code']].copy()
            products.loc[:, 'upc'] = products['product_ean_code'].str.zfill(12)
            relevant_ean_codes = products.merge(nbil_products, on='upc')[
                ['either', 'product_ean_code']]
            if not relevant_ean_codes.empty:
                relevant_ean_codes_either = relevant_ean_codes[relevant_ean_codes['either'] != '']
                relevant_ean_codes = relevant_ean_codes[relevant_ean_codes['either'] == '']

                scene_type_filter = self._create_filter_dict(key=TEMPLATE_NAME,
                                                             value=atomic_kpi_data['scene_types'])
                ean_code_filter = {
                    'product_ean_code': relevant_ean_codes['product_ean_code'].tolist()}
                shelves_filter = self._get_preferred_range_filter()
                filters = {}
                filters.update(scene_type_filter)
                filters.update(ean_code_filter)
                filters.update(shelves_filter)
                num_of_assorted_products = self._tools.calculate_assortment(**filters)

                if not relevant_ean_codes_either.empty:
                    for either_number in relevant_ean_codes_either['either'].unique().tolist():
                        relevant_eans = relevant_ean_codes_either[relevant_ean_codes_either['either'] == either_number]
                        ean_code_filter = {
                            'product_ean_code': relevant_eans['product_ean_code'].tolist()}
                        filters.update(ean_code_filter)
                        result = self._tools.calculate_assortment(**filters)
                        if result:
                            num_of_assorted_products += 1

                result = num_of_assorted_products
        return result

    @classproperty
    def kpi_type(self):
        return 'NBIL SOA - numerator'


class ShareOfAssortmentAtomicKpiCalculationNotPR(KpiAtomicKpisCalculator):
    def calculate_atomic_kpi(self, atomic_kpi_data):
        result = np.nan
        nbil_products = atomic_kpi_data['nbil_products']
        if not nbil_products.empty:
            products = self._data_provider.all_products[['product_ean_code']].copy()
            products.loc[:, 'upc'] = products['product_ean_code'].str.zfill(12)
            relevant_ean_codes = products.merge(nbil_products, on='upc')[
                'product_ean_code'].tolist()
            if relevant_ean_codes:
                scene_type_filter = self._create_filter_dict(key=TEMPLATE_NAME,
                                                             value=atomic_kpi_data['scene_types'])
                ean_code_filter = {'product_ean_code': relevant_ean_codes}
                # shelves_filter = self._get_preferred_range_filter()
                filters = {}
                filters.update(scene_type_filter)
                filters.update(ean_code_filter)
                # filters.update(shelves_filter)
                num_of_assorted_products = self._tools.calculate_assortment(**filters)

                scif = self._data_provider.scene_item_facts.copy()
                num_of_expected_product = len(nbil_products)
                # num_of_expected_product = len(scif[scif['product_ean_code'].isin(relevant_ean_codes)])
                if num_of_expected_product == 0:
                    return np.nan
                result = round((float(num_of_assorted_products) /
                                float(num_of_expected_product)) * 100, 2)

        return result

    @classproperty
    def kpi_type(self):
        return 'NBIL SOA NOT PR'


class ShareOfAssortmentPrSPTAtomicKpiCalculation(KpiAtomicKpisCalculator):
    def calculate_atomic_kpi(self, atomic_kpi_data):
        result = np.nan
        nbil_products = atomic_kpi_data['nbil_products']
        if not nbil_products.empty:
            products = self._data_provider.all_products[['product_ean_code']].copy()
            products.loc[:, 'upc'] = products['product_ean_code'].str.zfill(12)
            relevant_ean_codes = products.merge(nbil_products, on='upc')[
                ['either', 'product_ean_code']]
            if not relevant_ean_codes.empty:
                relevant_ean_codes_either = relevant_ean_codes[relevant_ean_codes['either'] != '']
                relevant_ean_codes = relevant_ean_codes[relevant_ean_codes['either'] == '']

                scene_type_filter = self._create_filter_dict(key=TEMPLATE_NAME,
                                                             value=atomic_kpi_data['scene_types'])
                ean_code_filter = {
                    'product_ean_code': relevant_ean_codes['product_ean_code'].tolist()}
                shelves_filter = self._get_preferred_range_SPT_filter(atomic_kpi_data['filters'])
                filters = {}
                filters.update(scene_type_filter)
                filters.update(ean_code_filter)
                filters.update(shelves_filter)
                num_of_assorted_products = self._tools.calculate_assortment(**filters)
                nbil_product_numbers = len(nbil_products[nbil_products['either'] == ''])
                nbil_either_product_numbers = len(
                    nbil_products[nbil_products['either'] != '']['either'].unique())

                if not relevant_ean_codes_either.empty:
                    for either_number in relevant_ean_codes_either['either'].unique().tolist():
                        relevant_eans = relevant_ean_codes_either[
                            relevant_ean_codes_either['either'] == either_number]
                        ean_code_filter = {
                            'product_ean_code': relevant_eans['product_ean_code'].tolist()}
                        filters.update(ean_code_filter)
                        result = self._tools.calculate_assortment(**filters)
                        if result:
                            num_of_assorted_products += 1

                # scif = self._data_provider.scene_item_facts.copy()
                num_of_expected_product = nbil_product_numbers + nbil_either_product_numbers
                # num_of_expected_product = len(scif[scif['product_ean_code'].isin(relevant_ean_codes)])
                if num_of_expected_product == 0:
                    return np.nan
                result = round((float(num_of_assorted_products) /
                                float(num_of_expected_product)) * 100, 2)

        return result

    @classproperty
    def kpi_type(self):
        return 'NBIL SOA SPT'


class ShareOfAssortmentPrSPTNumeratorAtomicKpiCalculation(KpiAtomicKpisCalculator):
    def calculate_atomic_kpi(self, atomic_kpi_data):
        result = np.nan
        nbil_products = atomic_kpi_data['nbil_products']
        if not nbil_products.empty:
            products = self._data_provider.all_products[['product_ean_code']].copy()
            products.loc[:, 'upc'] = products['product_ean_code'].str.zfill(12)
            relevant_ean_codes = products.merge(nbil_products, on='upc')[
                ['either', 'product_ean_code']]
            if not relevant_ean_codes.empty:
                relevant_ean_codes_either = relevant_ean_codes[relevant_ean_codes['either'] != '']
                relevant_ean_codes = relevant_ean_codes[relevant_ean_codes['either'] == '']

                scene_type_filter = self._create_filter_dict(key=TEMPLATE_NAME,
                                                             value=atomic_kpi_data['scene_types'])
                ean_code_filter = {
                    'product_ean_code': relevant_ean_codes['product_ean_code'].tolist()}
                shelves_filter = self._get_preferred_range_SPT_filter(atomic_kpi_data['filters'])
                filters = {}
                filters.update(scene_type_filter)
                filters.update(ean_code_filter)
                filters.update(shelves_filter)
                num_of_assorted_products = self._tools.calculate_assortment(**filters)

                if not relevant_ean_codes_either.empty:
                    for either_number in relevant_ean_codes_either['either'].unique().tolist():
                        relevant_eans = relevant_ean_codes_either[relevant_ean_codes_either['either'] == either_number]
                        ean_code_filter = {
                            'product_ean_code': relevant_eans['product_ean_code'].tolist()}
                        filters.update(ean_code_filter)
                        result = self._tools.calculate_assortment(**filters)
                        if result:
                            num_of_assorted_products += 1

                result = num_of_assorted_products
        return result

    @classproperty
    def kpi_type(self):
        return 'NBIL SOA - numerator SPT'


class DistributionCalculation(KpiAtomicKpisCalculator):
    def calculate_atomic_kpi(self, atomic_kpi_data):
        scene_type_filter = self._create_filter_dict(
            key=TEMPLATE_NAME, value=atomic_kpi_data['scene_types'])
        filters = atomic_kpi_data['filters']
        filters.update(scene_type_filter)
        result = 100 if self._tools.calculate_availability(**filters) >= 1 else 0
        return result

    @classproperty
    def kpi_type(self):
        return 'NBIL AVAILABILITY'


class BaseSSequenceCalculation(KpiAtomicKpisCalculator):
    @abc.abstractmethod
    def calculate_atomic_kpi(self, atomic_kpi_data):
        pass

    @abc.abstractproperty
    def kpi_type(self):
        pass

    def _get_order_sequence(self, atomic_kpi_data):
        order_by = atomic_kpi_data['filters'].pop('order by')[0]
        if 'order sequence' in atomic_kpi_data['filters']:
            order_sequence = atomic_kpi_data['filters'].pop('order sequence')
        else:
            order_sequence = self._get_attr_order(atomic_kpi_data['filters'], order_by)
        return order_by, order_sequence

    def _get_scene_list(self, filters):
        matches = self._tools.match_product_in_scene.copy()
        scenes_list = matches[self._tools.get_filter_condition(
            matches, **filters)]['scene_fk'].unique()
        return scenes_list

    def _get_scene_sequence_values(self, filters, order_by):
        scif = self._data_provider.scene_item_facts.copy()
        scif = scif[scif[order_by].notnull()]
        scif = scif[self._tools.get_filter_condition(scif, **filters)]
        scene_sequence_values = scif[order_by].unique().tolist()
        return scene_sequence_values

    def _get_attr_order(self, filters, attr):
        products = self._data_provider.products.copy()
        products = products[self._tools.get_filter_condition(products, **filters)]
        unique_attr = products[attr].unique()
        return sorted(unique_attr)


class NegativeSequenceCalculation(BaseSSequenceCalculation):
    def calculate_atomic_kpi(self, atomic_kpi_data):
        result = 0
        order_by, order_sequence = self._get_order_sequence(atomic_kpi_data)
        filters = atomic_kpi_data['filters']
        allowed = atomic_kpi_data['allowed']
        allowed = {'Private Label': (allowed['Private Label'], MarsUsGENERALToolBox.EXCLUDE_FILTER)}
        scene_type_filter = self._create_filter_dict(
            key=TEMPLATE_NAME, value=atomic_kpi_data['scene_types'])
        filters.update(scene_type_filter)
        filters.update(allowed)

        scenes_list = self._get_scene_list(filters)

        for scene in scenes_list:
            for i in xrange(len(order_sequence)-2):
                for j in xrange(len(order_sequence)-2):
                    index = j+i+2
                    if index >= len(order_sequence):
                        continue
                    filters.update({'scene_id': scene})
                    # atomic_kpi_data['filters']['Customer Brand'] = [filter]
                    tested_filters = {order_by: order_sequence[i]}
                    tested_filters.update(allowed)
                    tested_filters = {'product_fk': self.get_products_by_filters(
                        return_value='product_fk', **tested_filters).unique().tolist()}
                    anchor_filters = {order_by: order_sequence[index]}
                    anchor_filters.update(allowed)
                    anchor_filters = {'product_fk': self.get_products_by_filters(
                        return_value='product_fk', **anchor_filters).unique().tolist()}
                    total_score = self._tools.calculate_non_proximity(tested_filters=tested_filters,
                                                                      anchor_filters=anchor_filters,
                                                                      allowed_diagonal=True,
                                                                      **filters)
                    if total_score == False:
                        return 0
                    else:
                        result = 100
        return result

    @classproperty
    def kpi_type(self):
        return 'negative sequence'


class NegativeAdjacencyCalculation(BaseSSequenceCalculation):
    def calculate_atomic_kpi(self, atomic_kpi_data):
        result = 0
        filters = self._split_filters(atomic_kpi_data['filters'])
        scene_type_filter = self._create_filter_dict(
            key=TEMPLATE_NAME, value=atomic_kpi_data['scene_types'])
        scenes_list = self._get_scene_list(scene_type_filter)
        general_filters = filters['all']

        scif_matches = self.get_scif_matches_by_filters(**filters['A'])
        if scif_matches == 0:
            return 0
        scif_matches = self.get_scif_matches_by_filters(**filters['B'])
        if scif_matches == 0:
            return 0

        # a = self._data_provider.scene_item_facts
        # a = a[a['scene_id'].isin(scenes_list)]
        # b = a[a['Customer_Brand'].isin(['MILK-BONE', 'BLUE'])]
        # b['scene_id'].unique().tolist()

        for scene in scenes_list:
            general_filters.update({'scene_id': scene})
            score = self._tools.calculate_non_proximity(tested_filters=filters['A'],
                                                        anchor_filters=filters['B'],
                                                        allowed_diagonal=False,
                                                        **general_filters)
            if score == False:
                return 0
            else:
                result = 100
        return result

    @classproperty
    def kpi_type(self):
        return 'Negative Adjacency'


class SequenceCalculation(BaseSSequenceCalculation):
    def calculate_atomic_kpi(self, atomic_kpi_data):
        result = 0
        directions = ['left', 'right']
        order_by, order_sequence = self._get_order_sequence(atomic_kpi_data)
        filters = atomic_kpi_data['filters'].copy()
        target = atomic_kpi_data['target']
        scene_type_filter = self._create_filter_dict(
            key=TEMPLATE_NAME, value=atomic_kpi_data['scene_types'])
        filters.update(scene_type_filter)

        scenes_list = self._get_scene_list(filters)

        for scene in scenes_list:
            scene_filter = filters.copy()
            scene_filter.update({'scene_fk': scene})
            scene_filter.pop('template_name')

            scene_sequence_values = self._get_scene_sequence_values(scene_filter, order_by)

            sequence_filters = []
            for value in order_sequence:
                if value in scene_sequence_values:
                    organ_filter = filters.copy()
                    organ_filter.pop('template_name')
                    organ_filter.update({order_by: value})
                    sequence_filters.append(organ_filter)

            filters_length = len(sequence_filters)

            if filters_length == 0:
                continue
            if filters_length == 1 and not target:
                result = 100
                continue
            elif filters_length == 1 and target:
                continue
            general_direction_data = {'top': 1000, 'bottom': 1000, 'left': 0, 'right': 0}
            scene_sequence = dict((direction, True) for direction in directions)
            at_least_one = dict((direction, False) for direction in directions)

            for i in range(0, filters_length - 1):
                anchor = sequence_filters[i]
                tested = sequence_filters[i + 1]
                for direction in directions:
                    direction_data = general_direction_data.copy()
                    direction_data[direction] = (1, 1000)
                    seq = self._tools.calculate_relative_position(
                        tested, anchor, direction_data, **scene_filter)
                    at_least_one[direction] = seq or at_least_one[direction]
                    scene_sequence[direction] = seq and at_least_one[direction]
                    if self._both_sides_true(at_least_one) or self._both_sides_false(scene_sequence):
                        return 0
            result = 100
        return result

    @classproperty
    def kpi_type(self):
        return 'sequence'

    @staticmethod
    def _both_sides_true(seq_dict):
        return all(seq for seq in seq_dict.values())

    @staticmethod
    def _both_sides_false(seq_dict):
        return all(not seq for seq in seq_dict.values())


class SequenceSptCalculation(BaseSSequenceCalculation):
    def calculate_atomic_kpi(self, atomic_kpi_data):
        result = 0
        directions = ['left_to_right', 'right_to_left']
        order_by, order_sequence = self._get_order_sequence(atomic_kpi_data)
        filters = atomic_kpi_data['filters'].copy()
        target = atomic_kpi_data['target']
        scene_type_filter = self._create_filter_dict(
            key=TEMPLATE_NAME, value=atomic_kpi_data['scene_types'])
        filters.update(scene_type_filter)

        scenes_list = self._get_scene_list(filters)

        for scene in scenes_list:
            scene_filter = filters.copy()
            scene_filter.update({'scene_fk': scene})
            scene_filter.pop('template_name')
            edges_blocks = {}

            scene_sequence_values = self._get_scene_sequence_values(scene_filter, order_by)

            sequence_filters = []
            for value in order_sequence:
                if value in scene_sequence_values:
                    organ_filter = filters.copy()
                    organ_filter.pop('template_name')
                    organ_filter.update({order_by: value})
                    organ_filter.update({'scene_fk': scene})
                    sequence_filters.append(organ_filter)

            filters_length = len(sequence_filters)

            if filters_length == 0:
                continue
            if filters_length == 1 and not target:
                result = 100
                continue
            elif filters_length == 1 and target:
                continue

            scene_sequence = dict((direction, True) for direction in directions)
            at_least_one = dict((direction, False) for direction in directions)

            edges_blocks = self._edges_blocks(sequence_filters)

            for i in range(0, filters_length - 1):
                anchor = sequence_filters[i]
                tested = sequence_filters[i + 1]
                anchor_edge_block = edges_blocks[i]
                tested_edge_block = edges_blocks[i + 1]
                for direction in directions:
                    seq = self.calculate_relative_block_position(
                        anchor_edge_block, tested_edge_block, direction)
                    at_least_one[direction] = seq or at_least_one[direction]
                    scene_sequence[direction] = seq and at_least_one[direction]
                    if self._both_sides_true(at_least_one) or self._both_sides_false(scene_sequence):
                        return 0
            result = 100
        return result

    @staticmethod
    def _both_sides_true(seq_dict):
        return all(seq for seq in seq_dict.values())

    @staticmethod
    def _both_sides_false(seq_dict):
        return all(not seq for seq in seq_dict.values())

    def _edges_blocks(self, sequence_filters):
        result_block_edges = {}
        for i in xrange(len(sequence_filters)):
            filters = sequence_filters[i]
            result = self._tools.block.calculate_block_edges(
                biggest_block=True, include_empty=True, **filters)
            result_block_edges[i] = result['visual']
        return result_block_edges

    @staticmethod
    def calculate_relative_block_position(edges_a, edges_b, direction):
        if edges_a and edges_b:
            if direction == 'left_to_right':
                if edges_a['right'] <= edges_b['left']:
                    return True
            elif direction == 'right_to_left':
                if edges_b['right'] <= edges_a['left']:
                    return True
        return False

    @classproperty
    def kpi_type(self):
        return 'sequence spt'


class VerticalSequenceCalculation(BaseSSequenceCalculation):
    def calculate_atomic_kpi(self, atomic_kpi_data):
        result = 0
        order_by, order_sequence = self._get_order_sequence(atomic_kpi_data)
        filters = atomic_kpi_data['filters'].copy()
        target = atomic_kpi_data['target']
        scene_type_filter = self._create_filter_dict(
            key=TEMPLATE_NAME, value=atomic_kpi_data['scene_types'])
        filters.update(scene_type_filter)

        scenes_list = self._get_scene_list(filters)

        for scene in scenes_list:
            scene_filter = filters.copy()
            scene_filter.update({'scene_fk': scene})
            scene_sequence_filter = scene_filter.copy()
            scene_sequence_filter.update({order_by: order_sequence})
            scene_sequence_values = self._get_scene_sequence_values(scene_sequence_filter, order_by)

            sequence_value_count = len(scene_sequence_values)
            if sequence_value_count == 0:
                continue
            if sequence_value_count == 1 and not target:
                result = 100
                continue
            elif sequence_value_count == 1 and target:
                continue

            for i in range(0, sequence_value_count - 1):
                scene_value_filter = scene_filter.copy()
                scene_value_filter.update({order_by: [order_sequence[i]]})
                group_a_block = self._tools.calculate_block_together(minimum_block_ratio=0.01, biggest_block=True,
                                                                     **scene_value_filter)
                scene_value_filter.update({order_by: [order_sequence[i + 1]]})
                group_b_block = self._tools.calculate_block_together(minimum_block_ratio=0.01, biggest_block=True,
                                                                     **scene_value_filter)

                if (not group_a_block) or (not group_b_block):
                    Log.warning('vertical sequence - products are in scene but not in graph')
                    return np.nan

                if (not group_a_block['shelf_numbers']) or (not group_b_block['shelf_numbers']):
                    Log.warning('vertical sequence - products are in scene but not in graph')
                    return np.nan

                group_a_bottom_shelf = max(group_a_block['shelf_numbers'])
                group_b_top_shelf = min(group_b_block['shelf_numbers'])
                if group_b_top_shelf <= group_a_bottom_shelf:
                    return 0

            result = 100

        return result

    @classproperty
    def kpi_type(self):
        return 'Vertical Sequence'


class VerticalSequenceAvgShelfCalculation(BaseSSequenceCalculation):
    def calculate_atomic_kpi(self, atomic_kpi_data):
        result = 0
        order_by, order_sequence = self._get_order_sequence(atomic_kpi_data)
        filters = atomic_kpi_data['filters'].copy()
        scene_type_filter = self._create_filter_dict(
            key=TEMPLATE_NAME, value=atomic_kpi_data['scene_types'])
        filters.update(scene_type_filter)
        filters.update({order_by: order_sequence})

        matches = self._tools.match_product_in_scene.copy()
        relevant_product_list = matches[self._tools.get_filter_condition(matches, **filters)]
        if not relevant_product_list.empty:
            biggest_scene = relevant_product_list.groupby(['scene_fk']).size().reset_index(
                name='counts').sort_values(['counts'], ascending=False)
            biggest_scene = biggest_scene['scene_fk'].values[0]

            scene_filter = filters.copy()
            scene_filter.update({'scene_fk': biggest_scene})
            scene_sequence_values = self._get_scene_sequence_values(scene_filter, order_by)

            sequence_value_count = len(scene_sequence_values)
            if sequence_value_count == 0:
                return np.nan
            if sequence_value_count == 1:
                return 100

            for i in range(0, sequence_value_count - 1):
                scene_value_filter = scene_filter.copy()
                scene_value_filter.update({order_by: order_sequence[i]})
                group_a_avg_shelf = self._tools.calculate_avg_shelf(**scene_value_filter)
                scene_value_filter.update({order_by: order_sequence[i + 1]})
                group_b_avg_shelf = self._tools.calculate_avg_shelf(**scene_value_filter)

                if (not group_a_avg_shelf) or (not group_b_avg_shelf):
                    Log.warning('vertical sequence - products are in scene but have shelf number')
                    return np.nan

                if group_b_avg_shelf >= group_a_avg_shelf:
                    return 0

                result = 100

        return result

    @classproperty
    def kpi_type(self):
        return 'Vertical Sequence Avg Shelf'


class ShelfLengthCalculationBase(KpiAtomicKpisCalculator):
    def calculate_atomic_kpi(self, atomic_kpi_data):
        filters = atomic_kpi_data['filters']
        scene_type_filter = self._create_filter_dict(
            key=TEMPLATE_NAME, value=atomic_kpi_data['scene_types'])
        filters.update(scene_type_filter)

        scif_matches = self.get_scif_matches_by_filters(**filters)
        if scif_matches == 0:
            return np.nan

        target = atomic_kpi_data['target']
        matches = self._tools.match_product_in_scene
        max_shelf_per_bay = matches.groupby(['bay_number', 'scene_id'], as_index=False).agg(
            {'shelf_number_from_bottom': 'max'})
        matches = matches[self._tools.get_filter_condition(matches, **filters)]
        matches.loc[:, 'width'] = matches['width_mm_advance'].fillna(matches['width_mm'])
        bay_width = matches.groupby(['bay_number', 'scene_id'], as_index=False).agg(
            {'shelf_number_from_bottom': 'max', 'width': 'sum'})
        bay_width = bay_width.rename(
            index=str, columns={'shelf_number_from_bottom': 'shelf_number_from_bottom_filtered'})
        bay_width_merged = pd.merge(max_shelf_per_bay, bay_width, on=['bay_number', 'scene_id'])
        bay_width_merged['avg_length'] = bay_width_merged['width'] / \
            bay_width_merged['shelf_number_from_bottom'] / MM_TO_FT_RATIO
        length = bay_width_merged['avg_length'].sum()
        result = self.get_result(length, target)
        return result

    @abc.abstractmethod
    def get_result(self, length, target):
        return 100 if length > target else 0

    @abc.abstractproperty
    def kpi_type(self):
        return 'Shelf length'


class ShelfLengthNumeratorCalculationBase(KpiAtomicKpisCalculator):
    def calculate_atomic_kpi(self, atomic_kpi_data):
        filters = atomic_kpi_data['filters']
        scene_type_filter = self._create_filter_dict(
            key=TEMPLATE_NAME, value=atomic_kpi_data['scene_types'])
        filters.update(scene_type_filter)

        scif_matches = self.get_scif_matches_by_filters(**filters)
        if scif_matches == 0:
            return np.nan

        target = atomic_kpi_data['target']
        matches = self._tools.match_product_in_scene
        max_shelf_per_bay = matches.groupby(['bay_number', 'scene_id'], as_index=False).agg(
            {'shelf_number_from_bottom': 'max'})
        matches = matches[self._tools.get_filter_condition(matches, **filters)]
        matches.loc[:, 'width'] = matches['width_mm_advance'].fillna(matches['width_mm'])
        bay_width = matches.groupby(['bay_number', 'scene_id'], as_index=False).agg(
            {'shelf_number_from_bottom': 'max', 'width': 'sum'})
        bay_width = bay_width.rename(
            index=str, columns={'shelf_number_from_bottom': 'shelf_number_from_bottom_filtered'})
        bay_width_merged = pd.merge(max_shelf_per_bay, bay_width, on=['bay_number', 'scene_id'])
        bay_width_merged['avg_length'] = bay_width_merged['width'] / \
            bay_width_merged['shelf_number_from_bottom'] / MM_TO_FT_RATIO
        length = bay_width_merged['avg_length'].sum()
        # result = self.get_result(length, target)
        return length

    @abc.abstractmethod
    def get_result(self, length, target):
        return 100 if length > target else 0

    @classproperty
    def kpi_type(self):
        return 'Shelf length numerator'


class ShelfLengthGreaterThenCalculation(ShelfLengthCalculationBase):
    def get_result(self, length, target):
        return 100 if length > target else 0

    @classproperty
    def kpi_type(self):
        return 'Shelf length'


class ShelfLengthSmallerThenCalculation(ShelfLengthCalculationBase):
    def get_result(self, length, target):
        return 100 if length <= target else 0

    @classproperty
    def kpi_type(self):
        return 'Shelf length less than'


class NumOfShelvesCalculation(KpiAtomicKpisCalculator):
    def calculate_atomic_kpi(self, atomic_kpi_data):
        filters = atomic_kpi_data['filters']
        scene_type_filter = self._create_filter_dict(
            key=TEMPLATE_NAME, value=atomic_kpi_data['scene_types'])
        filters.update(scene_type_filter)

        scif_matches = self.get_scif_matches_by_filters(**filters)
        if scif_matches == 0:
            return np.nan

        target = atomic_kpi_data['target']
        matches = self._tools.match_product_in_scene
        matches = matches[self._tools.get_filter_condition(matches, **filters)]
        if matches.empty:
            return 0
        shelves = matches['shelf_number'].nunique()
        result = 100 if shelves >= target else 0
        return result

    @classproperty
    def kpi_type(self):
        return 'Num of shelves'


class MiddleShelfCalculation(KpiAtomicKpisCalculator):
    def calculate_atomic_kpi(self, atomic_kpi_data):
        filters = self._split_filters(atomic_kpi_data['filters'])

        scene_type_filter = self._create_filter_dict(
            key=TEMPLATE_NAME, value=atomic_kpi_data['scene_types'])
        filters_for_relevant_shelves = filters['A'].copy()
        relevant_scene_shelves = self.get_relevant_scenes_and_shelves(
            filters_for_relevant_shelves, scene_type_filter)

        for idx, row in relevant_scene_shelves.iterrows():
            scene = row['scene_fk']
            shelf = row['shelf_number']
            scene_shelf_filter = {'scene_fk': scene, 'shelf_number': shelf}
            filters['A'].update(scene_shelf_filter)
            filters['B'].update(scene_shelf_filter)
            sos = self._tools.calculate_linear_share_of_shelf(
                sos_filters=filters['B'], **filters['A'])
            if sos and sos >= atomic_kpi_data['target']:
                return 100
        return 0

    def get_relevant_scenes_and_shelves(self, filters_for_relevant_shelves, scene_type_filter):
        filters_for_relevant_shelves.update(scene_type_filter)
        filters_for_relevant_shelves.update(self._get_preferred_range_filter())
        matches = self._tools.match_product_in_scene.copy()
        relevant_matches = matches[self._tools.get_filter_condition(
            matches, **filters_for_relevant_shelves)]
        relevant_scene_shelves = relevant_matches[['scene_fk', 'shelf_number']].drop_duplicates(
            ['scene_fk', 'shelf_number'])
        return relevant_scene_shelves

    @classproperty
    def kpi_type(self):
        return 'Middle shelf'
