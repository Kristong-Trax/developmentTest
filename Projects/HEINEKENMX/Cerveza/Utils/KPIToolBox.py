
from Trax.Algo.Calculations.Core.DataProvider import Data
from Trax.Utils.Logging.Logger import Log
from KPIUtils_v2.Utils.GlobalScripts.Scripts import GlobalSessionToolBox
import pandas as pd

from Projects.HEINEKENMX.Cerveza.Data.LocalConsts import Consts

# from Trax.Data.ProfessionalServices.PsConsts.DataProvider import
# from Trax.Data.ProfessionalServices.PsConsts.DB import
# from Trax.Data.ProfessionalServices.PsConsts.PS import
# from Trax.Data.ProfessionalServices.PsConsts.GlobalConsts import
# from Trax.Data.ProfessionalServices.PsConsts.Messages import
# from Trax.Data.ProfessionalServices.PsConsts.Custom import
# from Trax.Data.ProfessionalServices.PsConsts.OldDB import

# from KPIUtils_v2.Calculations.AssortmentCalculations import Assortment
# from KPIUtils_v2.Calculations.AvailabilityCalculations import Availability
# from KPIUtils_v2.Calculations.NumberOfScenesCalculations import NumberOfScenes
# from KPIUtils_v2.Calculations.PositionGraphsCalculations import PositionGraphs
# from KPIUtils_v2.Calculations.SOSCalculations import SOS
# from KPIUtils_v2.Calculations.SequenceCalculations import Sequence
# from KPIUtils_v2.Calculations.SurveyCalculations import Survey

# from KPIUtils_v2.Calculations.CalculationsUtils import GENERALToolBoxCalculations
from KPIUtils_v2.GlobalDataProvider.PsDataProvider import PsDataProvider
from Projects.HEINEKENMX.Cerveza.Data.LocalConsts import Consts
from Projects.HEINEKENMX.Utils.PlanogramUtil import HeinekenRealogram

__author__ = 'huntery'

MATCH_PRODUCT_IN_PROBE_FK = 'match_product_in_probe_fk'
MATCH_PRODUCT_IN_PROBE_STATE_REPORTING_FK = 'match_product_in_probe_state_reporting_fk'


class CervezaToolBox(GlobalSessionToolBox):

    def __init__(self, data_provider, output, common):
        GlobalSessionToolBox.__init__(self, data_provider, output, common)
        self.scene_types = self.scif['template_name'].unique().tolist()
        self.gz = self.store_info['additional_attribute_4'].iloc[0]
        self.city = self.store_info['address_city'].iloc[0]
        self.relevant_targets = self._get_relevant_external_targets(kpi_operation_type='planograma_cerveza')
        self.invasion_targets = self._get_relevant_external_targets(kpi_operation_type='invasion')
        self._determine_target_product_fks()
        self.leading_products = self._get_leading_products_from_scif()
        self.scene_realograms = self._calculate_scene_realograms()
        self.ps_data_provider = PsDataProvider(self.data_provider)
        self.match_product_in_probe_state_reporting = self.ps_data_provider.get_match_product_in_probe_state_reporting()

    def main_calculation(self):
        kpi_fk = self.get_kpi_fk_by_kpi_type(Consts.CERVEZA)
        parent_fk = self.get_parent_fk(Consts.CERVEZA)
        kpi_max_points = self.get_kpi_points(Consts.CERVEZA)

        score = 0
        score += self.calculate_mercadeo()
        score += self.calculate_surtido()

        result = score / kpi_max_points

        self.write_to_db(fk=kpi_fk, numerator_id=self.manufacturer_fk, denominator_id=self.store_id,
                         result=result * 100, score=score, weight=kpi_max_points, target=kpi_max_points,
                         identifier_result=kpi_fk, identifier_parent=parent_fk, should_enter=True)
        return score

    def calculate_surtido(self):
        kpi_fk = self.get_kpi_fk_by_kpi_type(Consts.SURTIDO)
        parent_fk = self.get_parent_fk(Consts.SURTIDO)
        max_kpi_points = self.get_kpi_points(Consts.SURTIDO)
        weight = self.get_kpi_weight(Consts.SURTIDO)

        score = 0
        score += self.calculate_calificador()
        score += self.calculate_prioritario()
        score += self.calculate_opcional()

        result = score / max_kpi_points

        self.write_to_db(fk=kpi_fk, numerator_id=self.manufacturer_fk, denominator_id=self.store_id,
                         result=result * 100, score=score, weight=weight, target=max_kpi_points,
                         identifier_result=kpi_fk, identifier_parent=parent_fk, should_enter=True)
        return score

    def calculate_calificador(self):
        kpi_fk = self.get_kpi_fk_by_kpi_type(Consts.CALIFICADOR)
        parent_fk = self.get_parent_fk(Consts.CALIFICADOR)
        max_kpi_points = self.get_kpi_points(Consts.CALIFICADOR)
        weight = self.get_kpi_weight(Consts.CALIFICADOR)

        relevant_template = self.relevant_targets[(self.relevant_targets['Nombre de Tarea'].isin(self.scene_types)) &
                                                  (self.relevant_targets['TIPO DE SKU'] == 'C')]

        relevant_template = pd.merge(relevant_template, self.scif[['product_fk', 'facings']],
                                     how='left', left_on='target_product_fk', right_on='product_fk')
        relevant_template['facings'].fillna(0, inplace=True)
        relevant_template['in_session'] = relevant_template['facings'] > 0

        self._calculate_calificador_sku(relevant_template)

        if relevant_template.empty:
            result = 0
        else:
            result = relevant_template['in_session'].sum() / float(len(relevant_template))

        score = result * max_kpi_points

        self.write_to_db(fk=kpi_fk, numerator_id=self.manufacturer_fk, denominator_id=self.store_id,
                         numerator_result=relevant_template['in_session'].sum(),
                         denominator_result=len(relevant_template),
                         result=result * 100, score=score, weight=weight, target=max_kpi_points,
                         identifier_result=kpi_fk, identifier_parent=parent_fk, should_enter=True)

        return score

    def _calculate_calificador_sku(self, relevant_template):
        kpi_fk = self.get_kpi_fk_by_kpi_type(Consts.CALIFICADOR_SKU)
        parent_fk = self.get_parent_fk(Consts.CALIFICADOR_SKU)

        for sku in relevant_template.itertuples():
            result = 1 if sku.in_session else 0
            self.write_to_db(fk=kpi_fk, numerator_id=sku.target_product_fk, denominator_id=self.store_id,
                             result=result, identifier_parent=parent_fk, identifier_result=kpi_fk,
                             should_enter=True)

    def calculate_prioritario(self):
        kpi_fk = self.get_kpi_fk_by_kpi_type(Consts.PRIORITARIO)
        parent_fk = self.get_parent_fk(Consts.PRIORITARIO)
        max_kpi_points = self.get_kpi_points(Consts.PRIORITARIO)
        weight = self.get_kpi_weight(Consts.PRIORITARIO)

        relevant_template = self.relevant_targets[(self.relevant_targets['Nombre de Tarea'].isin(self.scene_types)) &
                                                  (self.relevant_targets['TIPO DE SKU'] == 'P')]

        relevant_template = pd.merge(relevant_template, self.scif[['product_fk', 'facings']],
                                     how='left', left_on='target_product_fk', right_on='product_fk')
        relevant_template['facings'].fillna(0, inplace=True)
        relevant_template['in_session'] = relevant_template['facings'] > 0

        self._calculate_prioritario_sku(relevant_template)

        if relevant_template.empty:
            result = 0
        else:
            result = relevant_template['in_session'].sum() / float(len(relevant_template))

        score = result * max_kpi_points

        self.write_to_db(fk=kpi_fk, numerator_id=self.manufacturer_fk, denominator_id=self.store_id,
                         numerator_result=relevant_template['in_session'].sum(),
                         denominator_result=len(relevant_template),
                         result=result * 100, score=score, weight=weight, target=max_kpi_points,
                         identifier_result=kpi_fk, identifier_parent=parent_fk, should_enter=True)

        return score

    def _calculate_prioritario_sku(self, relevant_template):
        kpi_fk = self.get_kpi_fk_by_kpi_type(Consts.PRIORITARIO_SKU)
        parent_fk = self.get_parent_fk(Consts.PRIORITARIO_SKU)

        for sku in relevant_template.itertuples():
            result = 1 if sku.in_session else 0
            self.write_to_db(fk=kpi_fk, numerator_id=sku.target_product_fk, denominator_id=self.store_id,
                             result=result, identifier_parent=parent_fk, identifier_result=kpi_fk,
                             should_enter=True)

    def calculate_opcional(self):
        kpi_fk = self.get_kpi_fk_by_kpi_type(Consts.OPCIONAL)
        parent_fk = self.get_parent_fk(Consts.OPCIONAL)
        max_kpi_points = self.get_kpi_points(Consts.OPCIONAL)
        weight = self.get_kpi_weight(Consts.OPCIONAL)

        relevant_template = self.relevant_targets[(self.relevant_targets['Nombre de Tarea'].isin(self.scene_types)) &
                                                  (self.relevant_targets['TIPO DE SKU'] == 'O')]

        relevant_template = pd.merge(relevant_template, self.scif[['product_fk', 'facings']],
                                     how='left', left_on='target_product_fk', right_on='product_fk')
        relevant_template['facings'].fillna(0, inplace=True)
        relevant_template['in_session'] = relevant_template['facings'] > 0

        self._calculate_opcional_sku(relevant_template)

        if relevant_template.empty:
            result = 0
        else:
            result = relevant_template['in_session'].sum() / float(len(relevant_template))

        score = result * max_kpi_points

        self.write_to_db(fk=kpi_fk, numerator_id=self.manufacturer_fk, denominator_id=self.store_id,
                         numerator_result=relevant_template['in_session'].sum(),
                         denominator_result=len(relevant_template),
                         result=result * 100, score=score, weight=weight, target=max_kpi_points,
                         identifier_result=kpi_fk, identifier_parent=parent_fk, should_enter=True)

        return score

    def _calculate_opcional_sku(self, relevant_template):
        kpi_fk = self.get_kpi_fk_by_kpi_type(Consts.OPCIONAL_SKU)
        parent_fk = self.get_parent_fk(Consts.OPCIONAL_SKU)

        for sku in relevant_template.itertuples():
            result = 1 if sku.in_session else 0
            self.write_to_db(fk=kpi_fk, numerator_id=sku.target_product_fk, denominator_id=self.store_id,
                             result=result, identifier_parent=parent_fk, identifier_result=kpi_fk,
                             should_enter=True)

    def calculate_mercadeo(self):
        kpi_fk = self.get_kpi_fk_by_kpi_type(Consts.MERCADEO)
        parent_fk = self.get_parent_fk(Consts.MERCADEO)
        max_kpi_points = self.get_kpi_points(Consts.MERCADEO)
        weight = self.get_kpi_weight(Consts.MERCADEO)

        score = 0
        score += self.calculate_acomodo()
        score += self.calculate_frentes()
        score += self.calculate_huecos()
        score += self.calculate_invasion()

        result = score / max_kpi_points

        self.write_to_db(fk=kpi_fk, numerator_id=self.manufacturer_fk, denominator_id=self.store_id,
                         result=result * 100, score=score, weight=weight, target=max_kpi_points,
                         identifier_result=kpi_fk, identifier_parent=parent_fk, should_enter=True)
        return score

    def calculate_invasion(self):
        kpi_fk = self.get_kpi_fk_by_kpi_type(Consts.INVASION)
        parent_fk = self.get_parent_fk(Consts.INVASION)
        max_kpi_points = self.get_kpi_points(Consts.INVASION)
        weight = self.get_kpi_weight(Consts.INVASION)

        result = 0
        for invasion_row in self.invasion_targets.itertuples():
            if 'Cerveza' not in getattr(invasion_row, "_1"):
                continue
            relevant_scif = self.scif[self.scif['template_name'] == getattr(invasion_row, "_1")]
            if relevant_scif.empty:
                continue
            manufacturers = [x.strip() for x in getattr(invasion_row, 'Manufacturer').split(',')]
            categories = [x.strip() for x in getattr(invasion_row, 'Category').split(',')]
            relevant_scif = \
                relevant_scif[(relevant_scif['manufacturer_name'].isin(manufacturers)) &
                              (relevant_scif['category'].isin(categories))]
            if relevant_scif.empty:
                result = 1
                break

        score = result * max_kpi_points

        self.write_to_db(fk=kpi_fk, numerator_id=self.manufacturer_fk, denominator_id=self.store_id,
                         result=result * 100, score=score, weight=weight, target=max_kpi_points,
                         identifier_result=kpi_fk, identifier_parent=parent_fk, should_enter=True)
        return score

    def calculate_huecos(self):
        kpi_fk = self.get_kpi_fk_by_kpi_type(Consts.HUECOS)
        parent_fk = self.get_parent_fk(Consts.HUECOS)
        max_kpi_points = self.get_kpi_points(Consts.HUECOS)
        weight = self.get_kpi_weight(Consts.HUECOS)

        relevant_scene_types = self.relevant_targets[Consts.TEMPLATE_SCENE_TYPE].unique().tolist()

        relevant_scif = self.scif[self.scif['template_name'].isin(relevant_scene_types)]
        if relevant_scif.empty:
            result = 0
        else:
            empty_scif = relevant_scif[relevant_scif['product_type'] == 'Empty']
            result = 1 if empty_scif.empty else 0

        score = result * max_kpi_points

        self.write_to_db(fk=kpi_fk, numerator_id=self.manufacturer_fk, denominator_id=self.store_id,
                         result=result * 100, score=score, weight=weight, target=max_kpi_points,
                         identifier_result=kpi_fk, identifier_parent=parent_fk, should_enter=True)
        return score

    def calculate_frentes(self):
        kpi_fk = self.get_kpi_fk_by_kpi_type(Consts.FRENTES)
        parent_fk = self.get_parent_fk(Consts.FRENTES)
        max_kpi_points = self.get_kpi_points(Consts.FRENTES)
        weight = self.get_kpi_weight(Consts.FRENTES)

        valid_scene_types = self.relevant_targets[Consts.TEMPLATE_SCENE_TYPE].unique().tolist()
        relevant_scif = self.scif[self.scif['template_name'].isin(valid_scene_types)]
        relevant_scif.groupby('product_fk', as_index=False)['facings'].sum()
        relevant_target_skus = \
            self.relevant_targets[self.relevant_targets['Nombre de Tarea'].isin(
                relevant_scif['template_name'].unique().tolist())]
        relevant_target_skus = relevant_target_skus[relevant_target_skus['target_product_fk'].isin(
            self._get_products_relevant_to_category())]
        relevant_target_skus = relevant_target_skus.groupby('target_product_fk', as_index=False)['Frentes'].sum()
        relevant_target_skus.rename(columns={'Frentes': 'target'}, inplace=True)
        relevant_target_skus = pd.merge(relevant_target_skus, relevant_scif, how='left',
                                        left_on='target_product_fk',
                                        right_on='product_fk').fillna(0)

        relevant_target_skus['meets_target'] = relevant_target_skus['facings'] >= relevant_target_skus['target']
        relevant_target_skus['meets_target'] = relevant_target_skus['meets_target'].apply(lambda x: 1 if x else 0)
        count_of_passing_skus = relevant_target_skus['meets_target'].sum()

        self._calculate_frentes_sku(relevant_target_skus)

        if len(relevant_target_skus) == 0:
            result = 0
        else:
            result = count_of_passing_skus / float(len(relevant_target_skus))
        score = result * max_kpi_points
        self.write_to_db(fk=kpi_fk, numerator_id=self.manufacturer_fk, denominator_id=self.store_id,
                         numerator_result=count_of_passing_skus, denominator_result=len(relevant_target_skus),
                         result=result * 100, score=score, weight=weight, target=max_kpi_points,
                         identifier_parent=parent_fk, identifier_result=kpi_fk, should_enter=True)
        return score

    def _calculate_frentes_sku(self, relevant_target_skus):
        kpi_fk = self.get_kpi_fk_by_kpi_type(Consts.FRENTES_SKU)
        parent_fk = self.get_parent_fk(Consts.FRENTES_SKU)

        for sku_row in relevant_target_skus.itertuples():
            self.write_to_db(fk=kpi_fk, numerator_id=sku_row.target_product_fk, denominator_id=self.store_id,
                             numerator_result=sku_row.facings, result=sku_row.meets_target, target=sku_row.target,
                             identifier_parent=parent_fk, should_enter=True)

    def calculate_acomodo(self):
        kpi_fk = self.get_kpi_fk_by_kpi_type(Consts.ACOMODO)
        parent_fk = self.get_parent_fk(Consts.ACOMODO)
        max_kpi_points = self.get_kpi_points(Consts.ACOMODO)
        weight = self.get_kpi_weight(Consts.ACOMODO)

        scene_result = 0
        count = 0
        for scene_id, scene_realogram in self.scene_realograms.items():
            count += 1
            scene_result += self.calculate_acomodo_scene(scene_realogram)

        if count == 0:
            result = 0
        else:
            result = scene_result / float(count)

        score = result * max_kpi_points

        self.write_to_db(fk=kpi_fk, numerator_id=self.manufacturer_fk, denominator_id=self.store_id,
                         numerator_result=result, denominator_result=count,
                         result=result * 100, score=score, weight=weight, target=max_kpi_points,
                         identifier_parent=parent_fk, identifier_result=kpi_fk, should_enter=True)
        return score

    def calculate_acomodo_scene(self, scene_realogram):
        kpi_fk = self.get_kpi_fk_by_kpi_type(Consts.ACOMODO_SCENE)
        parent_fk = self.get_parent_fk(Consts.ACOMODO_SCENE)
        identifier_result = self.get_dictionary(kpi_fk=kpi_fk, scene_fk=scene_realogram.scene_fk)

        result = self.calculate_colcado_correct(scene_realogram)
        self.calculate_colcado_incorrect(scene_realogram)
        self.calculate_extra(scene_realogram)

        self.write_to_db(fk=kpi_fk, numerator_id=scene_realogram.template_fk,
                         denominator_id=self.store_id, context_id=scene_realogram.scene_fk, result=result,
                         score=result * 100, identifier_parent=parent_fk,
                         identifier_result=identifier_result, should_enter=True)
        return result

    def calculate_colcado_correct(self, scene_realogram):
        kpi_fk = self.get_kpi_fk_by_kpi_type(Consts.COLCADO_CORRECT)
        parent_fk = self.get_parent_fk(Consts.COLCADO_CORRECT)
        identifier_result = self.get_dictionary(kpi_fk=kpi_fk, scene_fk=scene_realogram.scene_fk)
        identifier_parent = self.get_dictionary(kpi_fk=parent_fk, scene_fk=scene_realogram.scene_fk)

        self._calculate_colcado_correct_sku(scene_realogram)

        number_of_positions_in_planogram = float(scene_realogram.number_of_positions_in_planogram)

        self.mark_tags_in_explorer(scene_realogram.correctly_placed_tags['probe_match_fk'].dropna().unique().tolist(),
                                   Consts.COLCADO_CORRECT)

        self.write_to_db(fk=kpi_fk, numerator_id=self.manufacturer_fk,
                         denominator_id=scene_realogram.template_fk,
                         numerator_result=len(scene_realogram.correctly_placed_tags),
                         denominator_result=scene_realogram.number_of_skus_in_planogram,
                         result=len(scene_realogram.correctly_placed_tags) / number_of_positions_in_planogram * 100,
                         score=len(scene_realogram.correctly_placed_tags) / number_of_positions_in_planogram,
                         context_id=scene_realogram.scene_fk,
                         identifier_result=identifier_result, identifier_parent=identifier_parent, should_enter=True)

        return len(scene_realogram.correctly_placed_tags) / number_of_positions_in_planogram

    def _calculate_colcado_correct_sku(self, scene_realogram):
        kpi_fk = self.get_kpi_fk_by_kpi_type(Consts.COLCADO_CORRECT_SKU)
        parent_fk = self.get_parent_fk(Consts.COLCADO_CORRECT_SKU)
        identifier_parent = self.get_dictionary(kpi_fk=parent_fk, scene_fk=scene_realogram.scene_fk)

        correctly_placed_skus = scene_realogram.calculate_correctly_placed_skus()
        for sku_row in correctly_placed_skus.itertuples():
            self.write_to_db(fk=kpi_fk, numerator_id=sku_row.target_product_fk,
                             denominator_id=scene_realogram.template_fk, numerator_result=sku_row.facings,
                             denominator_result=scene_realogram.number_of_skus_in_planogram, result=sku_row.facings,
                             context_id=scene_realogram.scene_fk,
                             identifier_parent=identifier_parent, should_enter=True)

    def calculate_colcado_incorrect(self, scene_realogram):
        kpi_fk = self.get_kpi_fk_by_kpi_type(Consts.COLCADO_INCORRECT)
        parent_fk = self.get_parent_fk(Consts.COLCADO_INCORRECT)
        identifier_result = self.get_dictionary(kpi_fk=kpi_fk, scene_fk=scene_realogram.scene_fk)
        identifier_parent = self.get_dictionary(kpi_fk=parent_fk, scene_fk=scene_realogram.scene_fk)

        self._calculate_colcado_incorrect_sku(scene_realogram)

        number_of_positions_in_planogram = float(scene_realogram.number_of_positions_in_planogram)

        self.mark_tags_in_explorer(scene_realogram.incorrectly_placed_tags['probe_match_fk'].dropna().unique().tolist(),
                                   Consts.COLCADO_INCORRECT)

        self.write_to_db(fk=kpi_fk, numerator_id=self.manufacturer_fk,
                         denominator_id=scene_realogram.template_fk,
                         numerator_result=len(scene_realogram.incorrectly_placed_tags),
                         denominator_result=scene_realogram.number_of_skus_in_planogram,
                         result=len(scene_realogram.incorrectly_placed_tags) / number_of_positions_in_planogram * 100,
                         score=len(scene_realogram.incorrectly_placed_tags) / number_of_positions_in_planogram,
                         context_id=scene_realogram.scene_fk,
                         identifier_result=identifier_result, identifier_parent=identifier_parent, should_enter=True)
        return

    def _calculate_colcado_incorrect_sku(self, scene_realogram):
        kpi_fk = self.get_kpi_fk_by_kpi_type(Consts.COLCADO_INCORRECT_SKU)
        parent_fk = self.get_parent_fk(Consts.COLCADO_INCORRECT_SKU)
        identifier_parent = self.get_dictionary(kpi_fk=parent_fk, scene_fk=scene_realogram.scene_fk)

        incorrectly_placed_skus = scene_realogram.calculate_incorrectly_placed_skus()
        for sku_row in incorrectly_placed_skus.itertuples():
            self.write_to_db(fk=kpi_fk, numerator_id=sku_row.target_product_fk,
                             denominator_id=scene_realogram.template_fk, numerator_result=sku_row.facings,
                             denominator_result=scene_realogram.number_of_skus_in_planogram, result=sku_row.facings,
                             context_id=scene_realogram.scene_fk,
                             identifier_parent=identifier_parent, should_enter=True)

    def calculate_extra(self, scene_realogram):
        kpi_fk = self.get_kpi_fk_by_kpi_type(Consts.EXTRA)
        parent_fk = self.get_kpi_fk_by_kpi_type(Consts.EXTRA)
        identifier_result = self.get_dictionary(kpi_fk=kpi_fk, scene_fk=scene_realogram.scene_fk)
        identifier_parent = self.get_dictionary(kpi_fk=parent_fk, scene_fk=scene_realogram.scene_fk)

        self._calculate_extra_sku(scene_realogram)

        number_of_positions_in_planogram = float(scene_realogram.number_of_positions_in_planogram)

        self.mark_tags_in_explorer(scene_realogram.extra_tags['probe_match_fk'].dropna().unique().tolist(),
                                   Consts.EXTRA)

        self.write_to_db(fk=kpi_fk, numerator_id=self.manufacturer_fk,
                         denominator_id=scene_realogram.template_fk,
                         numerator_result=len(scene_realogram.extra_tags),
                         denominator_result=scene_realogram.number_of_skus_in_planogram,
                         result=len(scene_realogram.extra_tags) / number_of_positions_in_planogram * 100,
                         score=len(scene_realogram.extra_tags) / number_of_positions_in_planogram,
                         context_id=scene_realogram.scene_fk,
                         identifier_result=identifier_result, identifier_parent=identifier_parent, should_enter=True)
        return

    def _calculate_extra_sku(self, scene_realogram):
        kpi_fk = self.get_kpi_fk_by_kpi_type(Consts.EXTRA_SKU)
        parent_fk = self.get_parent_fk(Consts.EXTRA_SKU)
        identifier_parent = self.get_dictionary(kpi_fk=parent_fk, scene_fk=scene_realogram.scene_fk)

        extra_skus = scene_realogram.calculate_extra_skus()
        for sku_row in extra_skus.itertuples():
            self.write_to_db(fk=kpi_fk, numerator_id=sku_row.target_product_fk,
                             denominator_id=scene_realogram.template_fk, numerator_result=sku_row.facings,
                             denominator_result=scene_realogram.number_of_skus_in_planogram, result=sku_row.facings,
                             context_id=scene_realogram.scene_fk,
                             identifier_parent=identifier_parent, should_enter=True)

    def _calculate_scene_realograms(self):
        scene_realograms = {}
        for scene_type in self.relevant_targets[Consts.TEMPLATE_SCENE_TYPE].unique().tolist():
            if scene_type in self.scif['template_name'].unique().tolist():
                template_fk = self._get_template_fk(scene_type)
                for scene_fk in self.scif[self.scif['template_name'] == scene_type]['scene_id'].unique().tolist():
                    scene_mpis = self.matches[self.matches['scene_fk'] == scene_fk]
                    scene_mpis = self._convert_mpis_product_fks_to_leads(scene_mpis)
                    scene_realogram = HeinekenRealogram(scene_mpis, scene_type, template_fk,
                                                        self.relevant_targets,
                                                        products_to_filter_by=self._get_products_relevant_to_category())
                    scene_realograms[scene_fk] = scene_realogram
        return scene_realograms

    def _get_products_relevant_to_category(self):
        products_relevant_to_category = \
            self.all_products[self.all_products['category_fk'] == 44]['product_fk'].unique().tolist()
        return products_relevant_to_category

    def _get_relevant_external_targets(self, kpi_operation_type=None):
        if kpi_operation_type == 'planograma_cerveza':
            template_df = pd.read_excel(Consts.TEMPLATE_PATH, sheetname='Planograma_cerveza', header=1)
            template_df = template_df[(template_df['GZ'].str.encode('utf-8') == self.gz.encode('utf-8')) &
                                      (template_df['Ciudad'].str.encode('utf-8') == self.city.encode('utf-8'))]

            template_df['Puertas'] = template_df['Puertas'].fillna(1)
            template_df = template_df[['GZ', 'Ciudad', 'Nombre de Tarea', 'Puertas', 'EAN Code', 'Product Name',
                                      'TIPO DE SKU', 'x', 'y', 'Frentes']]
            return template_df
        elif kpi_operation_type == 'invasion':
            template_df = pd.read_excel(Consts.TEMPLATE_PATH, sheetname='Invasion', header=1)
            template_df = template_df[(template_df['NOMBRE DE TAREA'].str.contains('Cerveza'))]
            return template_df.dropna(subset=['Manufacturer', 'Category'])
        else:
            return pd.DataFrame()

    def _determine_target_product_fks(self):
        target_products = self.all_products[['product_fk', 'product_ean_code']]
        target_products.rename(columns={'product_fk': 'target_product_fk'}, inplace=True)
        target_products.dropna(inplace=True)
        target_products['product_ean_code'] = target_products['product_ean_code'].astype('int')
        self.relevant_targets['EAN Code'] = self.relevant_targets['EAN Code'].astype('int')
        self.relevant_targets = pd.merge(self.relevant_targets, target_products, how='left', left_on='EAN Code',
                                         right_on='product_ean_code')
        self.relevant_targets.dropna(subset=['target_product_fk'], inplace=True)

    def _get_leading_products_from_scif(self):
        leading_mappings = self.scif[['product_fk', 'substitution_product_fk']].drop_duplicates()
        leading_mappings.loc[leading_mappings['substitution_product_fk'].isna(), 'substitution_product_fk'] = \
            leading_mappings.loc[leading_mappings['substitution_product_fk'].isna(), 'product_fk']
        return leading_mappings

    @staticmethod
    def _convert_mpis_product_fks_to_leads(mpis):
        mpis['leading_product_fk'] = mpis['substitution_product_fk']
        return mpis

    def get_parent_fk(self, kpi_name):
        parent_kpi_name = Consts.KPIS_HIERARCHY[kpi_name]
        parent_fk = self.get_kpi_fk_by_kpi_type(parent_kpi_name)
        return parent_fk

    @staticmethod
    def get_kpi_weight(kpi_name):
        weight = Consts.KPI_WEIGHTS[kpi_name]
        return weight

    @staticmethod
    def get_kpi_points(kpi_name):
        weight = Consts.KPI_POINTS[kpi_name]
        return weight

    def _get_template_fk(self, template_name):
        return self.templates[self.templates['template_name'] == template_name]['template_fk'].iloc[0]

    def mark_tags_in_explorer(self, probe_match_fk_list, mpipsr_name):
        if not probe_match_fk_list:
            return
        try:
            match_type_fk = \
                self.match_product_in_probe_state_reporting[
                    self.match_product_in_probe_state_reporting['name'] == mpipsr_name][
                    'match_product_in_probe_state_reporting_fk'].values[0]
        except IndexError:
            Log.warning('Name not found in match_product_in_probe_state_reporting table: {}'.format(mpipsr_name))
            return

        match_product_in_probe_state_values_old = self.common.match_product_in_probe_state_values
        match_product_in_probe_state_values_new = pd.DataFrame(columns=[MATCH_PRODUCT_IN_PROBE_FK,
                                                                        MATCH_PRODUCT_IN_PROBE_STATE_REPORTING_FK])
        match_product_in_probe_state_values_new[MATCH_PRODUCT_IN_PROBE_FK] = probe_match_fk_list
        match_product_in_probe_state_values_new[MATCH_PRODUCT_IN_PROBE_STATE_REPORTING_FK] = match_type_fk

        self.common.match_product_in_probe_state_values = pd.concat([match_product_in_probe_state_values_old,
                                                                     match_product_in_probe_state_values_new])

        return

