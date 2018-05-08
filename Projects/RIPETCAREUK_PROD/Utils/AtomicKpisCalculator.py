import abc

import pandas as pd
from Trax.Utils.DesignPatterns.Decorators import classproperty

from Projects.RIPETCAREUK_PROD.Utils.Const import PRODUCT_EAN_CODES, SEPARATOR, PRODUCT_TYPE, BRAND, TEMPLATE_DISPLAY_NAME, ATT1
from Projects.RIPETCAREUK_PROD.Utils.GeneralToolBox import MarsUkGENERALToolBox
from Projects.RIPETCAREUK_PROD.Utils.ParseTemplates import SosConsts


class KpiAtomicKpisCalculator(object):

    def __init__(self, store_type, tools, data_provider, writer):
        self._store_type = store_type
        self._tools = tools
        self._data_provider = data_provider
        self._writer = writer

    def calculate(self, kpi_name, kpi_set_name, target_template_data, relevant_scene_types, kpi_weight):
        """
        This function calculates BCCP Atomics (including both surveys and availability), and saves the result to the DB.
        """
        atomic_kpi_results = pd.DataFrame(columns=['include_in_scoring', 'score_group',
                                                   'group_weight', 'score', 'result'])
        kpi_calculated = False
        weight_delta = 0
        kpi_target_template_data = self._filter_template_data_by_kpi_name_(target_template_data, kpi_name)
        for atomic_kpi_ind, atomic_kpi_data in kpi_target_template_data.iterrows():
            target = self._get_target_by_store_type(atomic_kpi_data, self._store_type)
            atomic_kpi_name = atomic_kpi_data['Atomic Kpi NAME']
            kpi_name = atomic_kpi_data['KPI Name']
            if self._is_target_valid(target):
                if 'Position on Shelf - Position' in kpi_set_name:
                    result, score, target, dist_score = self.calculate_atomic_kpi(atomic_kpi_data, relevant_scene_types, target)
                    atomic_kpi_result = self._create_atomic_kpi_result(atomic_kpi_data, result, score, dist_score)
                else:
                    result, score, target = self.calculate_atomic_kpi(atomic_kpi_data, relevant_scene_types, target)
                    atomic_kpi_result = self._create_atomic_kpi_result(atomic_kpi_data, result, score)
                self._writer.write_to_db_level_3_result(atomic_kpi_name=atomic_kpi_name, kpi_name=kpi_name,
                                                        kpi_set_name=kpi_set_name, score=score,
                                                        result=result, threshold=target)

                atomic_kpi_results = atomic_kpi_results.append(atomic_kpi_result)
                kpi_calculated = True
        if not kpi_calculated:
            weight_delta = kpi_weight
        return pd.DataFrame.from_records(atomic_kpi_results), weight_delta

    @abc.abstractproperty
    def kpi_type(self):
        pass

    @property
    def _entities_sheet_name_sif_name_mapping(self):
        return {
            'Brand': 'brand_name',
            'Sub-Category': 'sub_category'
        }

    @abc.abstractmethod
    def calculate_atomic_kpi(self, atomic_kpi_data, relevant_scene_types, target):
        pass

    @staticmethod
    def _filter_template_data_by_kpi_name_(target_template_data, kpi_name):
        cond = (
            (target_template_data['KPI Name'] == kpi_name)
        )
        return target_template_data.loc[cond, :]

    @staticmethod
    def _is_target_valid(target):
        return isinstance(target, (int, float)) or target.isdigit()

    @staticmethod
    def _get_target_by_store_type(atomic_kpi_data, store_type):
        if store_type in atomic_kpi_data.index:
            target = atomic_kpi_data[store_type]
        else:
            target = ''
        return target

    @staticmethod
    def _create_atomic_kpi_result(atomic_kpi_data, result, score, dist_score=None):
        score_group = atomic_kpi_data['score group'] if 'score group' in atomic_kpi_data.index else None
        group_value = atomic_kpi_data['group weight'] if 'group weight' in atomic_kpi_data.index else None
        include_in_scoring = atomic_kpi_data['Include in Scoring'] \
            if 'Include in Scoring' in atomic_kpi_data.index else 'Yes'
        if dist_score is None:
            atomic_kpi_result = {
                'include_in_scoring': include_in_scoring,
                'score_group': score_group,
                'group_weight': group_value,
                'score': score,
                'result': result
            }
        else:
            if dist_score == 0:
                atomic_kpi_result = {
                    'include_in_scoring': include_in_scoring,
                    'score_group': score_group,
                    'group_weight': group_value,
                    'score': score,
                    'result': result,
                    'failed': 0,
                    'extra_scores_for_missing_prods': 100
                }
            else:
                if result == 0:
                    atomic_kpi_result = {
                        'include_in_scoring': include_in_scoring,
                        'score_group': score_group,
                        'group_weight': group_value,
                        'score': score,
                        'result': result,
                        'failed': 100,
                        'extra_scores_for_missing_prods': 0
                    }
                else:
                    atomic_kpi_result = {
                        'include_in_scoring': include_in_scoring,
                        'score_group': score_group,
                        'group_weight': group_value,
                        'score': score,
                        'result': result,
                        'failed': 0,
                        'extra_scores_for_missing_prods': 0
                    }

        return pd.DataFrame.from_records([atomic_kpi_result])

    def _get_scene_item_fact_key_for_entity(self, entity_type_sheet):
        if entity_type_sheet != '':
            return self._entities_sheet_name_sif_name_mapping[entity_type_sheet]
        else:
            return entity_type_sheet

    @staticmethod
    def _create_filter_dict(key, value):
        if value != ['']:
            return {key: value}
        else:
            return {}

    @staticmethod
    def _get_products_list(atomic_kpi_definition):
        products = str(atomic_kpi_definition[PRODUCT_EAN_CODES]).replace('\n', '').split(
            SEPARATOR)
        return products

    @staticmethod
    def _get_product_types_list(atomic_kpi_definition):
        products = str(atomic_kpi_definition[PRODUCT_TYPE]).replace('\n', '').split(
            SEPARATOR)
        return products

    @staticmethod
    def _get_att1_list(atomic_kpi_definition):
        products = str(atomic_kpi_definition[ATT1]).replace('\n', '').split(
            SEPARATOR)
        return products

    @staticmethod
    def _get_brands_list(atomic_kpi_definition):
        products = str(atomic_kpi_definition[BRAND]).replace('\n', '').split(
            SEPARATOR)
        return products


class SosCalculator(KpiAtomicKpisCalculator):

    @classproperty
    def kpi_type(cls):
        return 'SOS Linear'

    def calculate_atomic_kpi(self, atomic_kpi_data, relevant_scene_types, target):
        sos_target = float(target)/100
        denominator, denominator_type, numerator, numerator_type = self._get_numerator_denominator(atomic_kpi_data)
        denominator_filter, numerator_filter = self._get_numerator_denominator_filters(denominator, denominator_type,
                                                                                       numerator, numerator_type,
                                                                                       relevant_scene_types)
        result = self._tools.calculate_linear_share_of_shelf(numerator_filter,
                                                             include_empty=MarsUkGENERALToolBox.INCLUDE_EMPTY,
                                                             **denominator_filter)
        score = 100 if result >= sos_target else 0
        return result, score, sos_target

    def _get_numerator_denominator_filters(self, denominator, denominator_type, numerator, numerator_type,
                                           relevant_scene_types):
        numerator_filter = {numerator_type: numerator}
        numerator_filter.update({'rlv_sos_sc': 1})
        relevant_scene_types_filter = self._create_filter_dict(key=TEMPLATE_DISPLAY_NAME, value=relevant_scene_types)
        denominator_filter = self._create_filter_dict(key=denominator_type, value=denominator)
        denominator_filter.update(relevant_scene_types_filter)
        denominator_filter.update({'rlv_sos_sc': 1})
        numerator_filter.update(denominator_filter)
        return denominator_filter, numerator_filter

    def _get_numerator_denominator(self, atomic_kpi_data):
        numerator_type_sheet = atomic_kpi_data[SosConsts.SOS_NUM_ENTITY]
        numerator_type_scif = self._get_scene_item_fact_key_for_entity(numerator_type_sheet)
        numerator = self._get_numerator_list(atomic_kpi_data)
        denominator_type_sheet = atomic_kpi_data[SosConsts.SOS_DENOM_ENTITY]
        denominator_type_scif = self._get_scene_item_fact_key_for_entity(denominator_type_sheet)
        denominator = self._get_denominator_list(atomic_kpi_data)
        return denominator, denominator_type_scif, numerator, numerator_type_scif

    @staticmethod
    def _get_numerator_list(atomic_kpi_definition):
        numerator = str(atomic_kpi_definition[SosConsts.SOS_NUMERATOR]).replace('\n', '').split(
            SEPARATOR)
        return numerator

    @staticmethod
    def _get_denominator_list(atomic_kpi_definition):
        numerator = str(atomic_kpi_definition[SosConsts.SOS_DENOMINATOR]).replace('\n', '').split(
            SEPARATOR)
        return numerator


class ShelfLevelCalculator(KpiAtomicKpisCalculator):

    @classproperty
    def kpi_type(cls):
        return 'Strict Shelf Level Position'

    def calculate_atomic_kpi(self, atomic_kpi_data, relevant_scene_types, target):
        shelves_list = [int(i) for i in str(target).split(SEPARATOR)]
        max_shelf_in_session = self._data_provider.matches.shelf_number_from_bottom.max()
        shelves_not_allowed = [i for i in xrange(1, max_shelf_in_session + 1) if i not in shelves_list]
        filters = {}
        scene_types_filter = self._create_filter_dict(key=TEMPLATE_DISPLAY_NAME, value=relevant_scene_types)
        products = self._get_products_list(atomic_kpi_data)
        products_filter = {'product_ean_code': products}
        filters.update(scene_types_filter)
        filters.update(products_filter)
        dist_result = self._tools.calculate_assortment(**filters)
        result = self._tools.calculate_shelf_level_assortment(shelves=shelves_not_allowed, **filters)
        if result > 0 or dist_result == 0:
            result = 0
            score = 0
        else:
            result = 1
            score = 100

        return result, score, None, dist_result

    @staticmethod
    def _is_target_valid(target):
        shelves_target = [i for i in str(target).split(SEPARATOR)]
        return all(isinstance(item, (int, float)) or item.isdigit() for item in shelves_target)


class ClipStripCalculator(KpiAtomicKpisCalculator):

    @classproperty
    def kpi_type(cls):
        return 'Clip strips facings'

    def calculate_atomic_kpi(self, atomic_kpi_data, relevant_scene_types, target):
        target = int(target)
        filters = {}
        scene_types_filter = self._create_filter_dict(key=TEMPLATE_DISPLAY_NAME, value=relevant_scene_types)
        # product_types = self._get_product_types_list(atomic_kpi_data)
        att1 = self._get_att1_list(atomic_kpi_data)
        products_filter = {'att1': att1}
        brands = self._get_brands_list(atomic_kpi_data)
        brands_filter = {'brand_name': brands}
        filters.update(scene_types_filter)
        filters.update(products_filter)
        filters.update(brands_filter)
        result = self._tools.calculate_availability(**filters)
        score = 100 if result >= target else 0
        return result, score, target


class NumOfFacingsCalculator(KpiAtomicKpisCalculator):

    @classproperty
    def kpi_type(cls):
        return 'Num Of Facings'

    def calculate_atomic_kpi(self, atomic_kpi_data, relevant_scene_types, target):
        target = int(target)
        filters = {}
        scene_types_filter = self._create_filter_dict(key=TEMPLATE_DISPLAY_NAME, value=relevant_scene_types)
        products = self._get_products_list(atomic_kpi_data)
        products_filter = {'product_ean_code': products}
        filters.update(scene_types_filter)
        filters.update(products_filter)
        result = self._tools.calculate_availability(**filters)
        score = 100 if result >= target else 0
        return result, score, target
