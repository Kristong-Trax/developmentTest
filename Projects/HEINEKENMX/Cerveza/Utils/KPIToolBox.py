
from Trax.Algo.Calculations.Core.DataProvider import Data
from Trax.Utils.Logging.Logger import Log
from KPIUtils_v2.Utils.GlobalScripts.Scripts import GlobalSessionToolBox
import pandas as pd

from Projects.HEINEKENMX.Cerveza.Data.LocalConsts import Consts

# from KPIUtils_v2.Utils.Consts.DataProvider import
# from KPIUtils_v2.Utils.Consts.DB import
# from KPIUtils_v2.Utils.Consts.PS import
# from KPIUtils_v2.Utils.Consts.GlobalConsts import
# from KPIUtils_v2.Utils.Consts.Messages import
# from KPIUtils_v2.Utils.Consts.Custom import
# from KPIUtils_v2.Utils.Consts.OldDB import

# from KPIUtils_v2.Calculations.AssortmentCalculations import Assortment
# from KPIUtils_v2.Calculations.AvailabilityCalculations import Availability
# from KPIUtils_v2.Calculations.NumberOfScenesCalculations import NumberOfScenes
# from KPIUtils_v2.Calculations.PositionGraphsCalculations import PositionGraphs
# from KPIUtils_v2.Calculations.SOSCalculations import SOS
# from KPIUtils_v2.Calculations.SequenceCalculations import Sequence
# from KPIUtils_v2.Calculations.SurveyCalculations import Survey

# from KPIUtils_v2.Calculations.CalculationsUtils import GENERALToolBoxCalculations
from Projects.HEINEKENMX.Cerveza.Data.LocalConsts import Consts

__author__ = 'huntery'


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

    def main_calculation(self):
        kpi_fk = self.get_kpi_fk_by_kpi_type(Consts.CERVEZA)
        parent_fk = self.get_parent_fk(Consts.CERVEZA)
        weight = self.get_kpi_weight(Consts.CERVEZA)

        score = 0
        score += self.calculate_mercadeo()
        score += self.calculate_surtido()

        self.write_to_db(fk=kpi_fk, numerator_id=self.manufacturer_fk, denominator_id=self.store_id,
                         result=score, score=score, weight=weight,
                         identifier_result=kpi_fk, identifier_parent=parent_fk, should_enter=True)
        return score

    def calculate_surtido(self):
        kpi_fk = self.get_kpi_fk_by_kpi_type(Consts.SURTIDO)
        parent_fk = self.get_parent_fk(Consts.SURTIDO)
        weight = self.get_kpi_weight(Consts.SURTIDO)

        score = 0
        score += self.calculate_calificador()
        score += self.calculate_prioritario()
        score += self.calculate_opcional()

        self.write_to_db(fk=kpi_fk, numerator_id=self.manufacturer_fk, denominator_id=self.store_id,
                         result=score, score=score, weight=weight,
                         identifier_result=kpi_fk, identifier_parent=parent_fk, should_enter=True)
        return score

    def calculate_calificador(self):
        kpi_fk = self.get_kpi_fk_by_kpi_type(Consts.CALIFICADOR)
        parent_fk = self.get_parent_fk(Consts.CALIFICADOR)
        weight = self.get_kpi_weight(Consts.CALIFICADOR)

        relevant_template = self.relevant_targets[(self.relevant_targets['Nombre de Tarea'].isin(self.scene_types)) &
                                                  (self.relevant_targets['TIPO DE SKU'] == 'C')]

        relevant_template = pd.merge(relevant_template, self.scif[['product_fk', 'facings']],
                                     how='left', left_on='target_product_fk', right_on='product_fk')
        relevant_template['facings'].fillna(0, inplace=True)
        relevant_template['in_session'] = relevant_template['facings'] > 0

        self._calculate_calificador_sku(relevant_template)

        result = relevant_template['in_session'].sum() / len(relevant_template)

        score = result * weight

        self.write_to_db(fk=kpi_fk, numerator_id=self.manufacturer_fk, denominator_id=self.store_id,
                         numerator_result=relevant_template['in_session'].sum(),
                         denominator_result=len(relevant_template),
                         result=result, score=score, weight=weight,
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
        weight = self.get_kpi_weight(Consts.PRIORITARIO)

        relevant_template = self.relevant_targets[(self.relevant_targets['Nombre de Tarea'].isin(self.scene_types)) &
                                                  (self.relevant_targets['TIPO DE SKU'] == 'P')]

        relevant_template = pd.merge(relevant_template, self.scif[['product_fk', 'facings']],
                                     how='left', left_on='target_product_fk', right_on='product_fk')
        relevant_template['facings'].fillna(0, inplace=True)
        relevant_template['in_session'] = relevant_template['facings'] > 0

        self._calculate_calificador_sku(relevant_template)

        result = relevant_template['in_session'].sum() / len(relevant_template)

        score = result * weight

        self.write_to_db(fk=kpi_fk, numerator_id=self.manufacturer_fk, denominator_id=self.store_id,
                         numerator_result=relevant_template['in_session'].sum(),
                         denominator_result=len(relevant_template),
                         result=result, score=score, weight=weight,
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
        weight = self.get_kpi_weight(Consts.OPCIONAL)

        relevant_template = self.relevant_targets[(self.relevant_targets['Nombre de Tarea'].isin(self.scene_types)) &
                                                  (self.relevant_targets['TIPO DE SKU'] == 'O')]

        relevant_template = pd.merge(relevant_template, self.scif[['product_fk', 'facings']],
                                     how='left', left_on='target_product_fk', right_on='product_fk')
        relevant_template['facings'].fillna(0, inplace=True)
        relevant_template['in_session'] = relevant_template['facings'] > 0

        self._calculate_calificador_sku(relevant_template)

        result = relevant_template['in_session'].sum() / len(relevant_template)

        score = result * weight

        self.write_to_db(fk=kpi_fk, numerator_id=self.manufacturer_fk, denominator_id=self.store_id,
                         numerator_result=relevant_template['in_session'].sum(),
                         denominator_result=len(relevant_template),
                         result=result, score=score, weight=weight,
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
        weight = self.get_kpi_weight(Consts.MERCADEO)

        score = 0
        score += self.calculate_acomodo()
        score += self.calculate_frentes()
        score += self.calculate_huecos()
        score += self.calculate_invasion()

        self.write_to_db(fk=kpi_fk, numerator_id=self.manufacturer_fk, denominator_id=self.store_id,
                         result=score, score=score, weight=weight,
                         identifier_result=kpi_fk, identifier_parent=parent_fk, should_enter=True)
        return score

    def calculate_invasion(self):
        kpi_fk = self.get_kpi_fk_by_kpi_type(Consts.INVASION)
        parent_fk = self.get_parent_fk(Consts.INVASION)
        weight = self.get_kpi_weight(Consts.INVASION)
        result = 1
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
            if not relevant_scif.empty:
                result = 0
                break

        score = result * weight

        self.write_to_db(fk=kpi_fk, numerator_id=self.manufacturer_fk, denominator_id=self.store_id,
                         result=result, score=score, weight=weight,
                         identifier_result=kpi_fk, identifier_parent=parent_fk, should_enter=True)
        return score

    def calculate_huecos(self):
        kpi_fk = self.get_kpi_fk_by_kpi_type(Consts.HUECOS)
        parent_fk = self.get_parent_fk(Consts.HUECOS)
        weight = self.get_kpi_weight(Consts.HUECOS)

        empty_scif = self.scif[self.scif['product_type'] == 'Empty']
        result = 1 if empty_scif.empty else 0

        score = result * weight

        self.write_to_db(fk=kpi_fk, numerator_id=self.manufacturer_fk, denominator_id=self.store_id,
                         result=result, score=score, weight=weight,
                         identifier_result=kpi_fk, identifier_parent=parent_fk, should_enter=True)
        return score

    def calculate_frentes(self):
        kpi_fk = self.get_kpi_fk_by_kpi_type(Consts.FRENTES)
        parent_fk = self.get_parent_fk(Consts.FRENTES)
        weight = self.get_kpi_weight(Consts.FRENTES)

        valid_scene_types = self.relevant_targets[Consts.TEMPLATE_SCENE_TYPE].unique().tolist()
        relevant_scif = self.scif[self.scif['template_name'].isin(valid_scene_types)]
        relevant_scif.groupby('product_fk', as_index=False)['facings'].sum()
        relevant_target_skus = self.relevant_targets.groupby('target_product_fk', as_index=False)['Frentes'].sum()
        relevant_target_skus.rename(columns={'Frentes': 'target'}, inplace=True)
        relevant_target_skus = pd.merge(relevant_target_skus, relevant_scif, how='left',
                                        left_on='target_product_fk',
                                        right_on='product_fk').fillna(0)

        relevant_target_skus['meets_target'] = relevant_target_skus['facings'] > relevant_target_skus['target']
        count_of_passing_skus = relevant_target_skus['meets_target'].sum()

        self._calculate_frentes_sku(relevant_target_skus)

        result = count_of_passing_skus / len(relevant_target_skus)
        score = result * weight
        self.write_to_db(fk=kpi_fk, numerator_id=self.manufacturer_fk, denominator_id=self.store_id,
                         numerator_result=count_of_passing_skus, denominator_result=len(relevant_target_skus),
                         result=result, score=score, weight=weight,
                         identifier_parent=parent_fk, identifier_result=kpi_fk, should_enter=True)
        return score

    def _calculate_frentes_sku(self, relevant_target_skus):
        kpi_fk = self.get_kpi_fk_by_kpi_type(Consts.FRENTES_SKU)
        parent_fk = self.get_parent_fk(Consts.FRENTES_SKU)

        for sku_row in relevant_target_skus.itertuples():
            self.write_to_db(fk=kpi_fk, numerator_id=sku_row.product_fk, denominator_id=self.store_id,
                             numerator_result=sku_row.facings, result=sku_row.meets_target, target=sku_row.target,
                             identifier_parent=parent_fk, should_enter=True)

    def calculate_acomodo(self):
        kpi_fk = self.get_kpi_fk_by_kpi_type(Consts.ACOMODO)
        parent_fk = self.get_parent_fk(Consts.ACOMODO)
        weight = self.get_kpi_weight(Consts.ACOMODO)

        scene_result = 0
        count = 0
        for scene_id, scene_realogram in self.scene_realograms.items():
            count += 1
            scene_result += self.calculate_acomodo_scene(scene_realogram)

        if count == 0:
            result = 0
        else:
            result = scene_result / count

        score = result * weight

        self.write_to_db(fk=kpi_fk, numerator_id=self.manufacturer_fk, denominator_id=self.store_id,
                         numerator_result=result, denominator_result=count,
                         result=result, score=score, weight=weight,
                         identifier_parent=parent_fk, identifier_result=kpi_fk, should_enter=True)
        return score

    def calculate_acomodo_scene(self, scene_realogram):
        kpi_fk = self.get_kpi_fk_by_kpi_type(Consts.ACOMODO_SCENE)
        parent_fk = self.get_parent_fk(Consts.ACOMODO_SCENE)

        result = self.calculate_colcado_correct(scene_realogram)
        self.calculate_colcado_incorrect(scene_realogram)
        self.calculate_extra(scene_realogram)

        self.write_to_db(fk=kpi_fk, numerator_id=scene_realogram.template_fk,
                         denominator_id=self.store_id, context_id=scene_realogram.scene_fk, result=result,
                         score=result, identifier_parent=parent_fk,
                         identifier_result=kpi_fk, should_enter=True)
        return result

    def calculate_colcado_correct(self, scene_realogram):
        kpi_fk = self.get_kpi_fk_by_kpi_type(Consts.COLCADO_CORRECT)
        parent_fk = self.get_parent_fk(Consts.COLCADO_CORRECT)

        self._calculate_colcado_correct_sku(scene_realogram)

        number_of_positions_in_planogram = scene_realogram.number_of_positions_in_planogram

        self.write_to_db(fk=kpi_fk, numerator_id=self.manufacturer_fk,
                         denominator_id=scene_realogram.template_fk,
                         numerator_result=len(scene_realogram.correctly_placed_tags),
                         denominator_result=scene_realogram.number_of_skus_in_planogram,
                         result=len(scene_realogram.correctly_placed_tags) / number_of_positions_in_planogram,
                         context_id=scene_realogram.scene_fk,
                         identifier_result=kpi_fk, identifier_parent=parent_fk, should_enter=True)

        return len(scene_realogram.correctly_placed_tags) / number_of_positions_in_planogram

    def _calculate_colcado_correct_sku(self, scene_realogram):
        kpi_fk = self.get_kpi_fk_by_kpi_type(Consts.COLCADO_CORRECT_SKU)
        parent_fk = self.get_parent_fk(Consts.COLCADO_CORRECT_SKU)

        correctly_placed_skus = scene_realogram.calculate_correctly_placed_skus()
        for sku_row in correctly_placed_skus.itertuples():
            self.write_to_db(fk=kpi_fk, numerator_id=sku_row.target_product_fk,
                             denominator_id=scene_realogram.template_fk, numerator_result=sku_row.facings,
                             denominator_result=scene_realogram.number_of_skus_in_planogram, result=sku_row.facings,
                             context_id=scene_realogram.scene_fk, identifier_parent=parent_fk, should_enter=True)

    def calculate_colcado_incorrect(self, scene_realogram):
        kpi_fk = self.get_kpi_fk_by_kpi_type(Consts.COLCADO_INCORRECT)
        parent_fk = self.get_parent_fk(Consts.COLCADO_INCORRECT)

        self._calculate_colcado_incorrect_sku(scene_realogram)

        number_of_positions_in_planogram = scene_realogram.number_of_positions_in_planogram

        self.write_to_db(fk=kpi_fk, numerator_id=self.manufacturer_fk,
                         denominator_id=scene_realogram.template_fk,
                         numerator_result=len(scene_realogram.incorrectly_placed_tags),
                         denominator_result=scene_realogram.number_of_skus_in_planogram,
                         result=len(scene_realogram.incorrectly_placed_tags) / number_of_positions_in_planogram,
                         context_id=scene_realogram.scene_fk,
                         identifier_result=kpi_fk, identifier_parent=parent_fk, should_enter=True)
        return

    def _calculate_colcado_incorrect_sku(self, scene_realogram):
        kpi_fk = self.get_kpi_fk_by_kpi_type(Consts.COLCADO_INCORRECT_SKU)
        parent_fk = self.get_parent_fk(Consts.COLCADO_INCORRECT_SKU)

        incorrectly_placed_skus = scene_realogram.calculate_incorrectly_placed_skus()
        for sku_row in incorrectly_placed_skus.itertuples():
            self.write_to_db(fk=kpi_fk, numerator_id=sku_row.target_product_fk,
                             denominator_id=scene_realogram.template_fk, numerator_result=sku_row.facings,
                             denominator_result=scene_realogram.number_of_skus_in_planogram, result=sku_row.facings,
                             context_id=scene_realogram.scene_fk, identifier_parent=parent_fk, should_enter=True)

    def calculate_extra(self, scene_realogram):
        kpi_fk = self.get_kpi_fk_by_kpi_type(Consts.EXTRA)
        parent_fk = self.get_kpi_fk_by_kpi_type(Consts.EXTRA)

        self._calculate_extra_sku(scene_realogram)

        number_of_positions_in_planogram = scene_realogram.number_of_positions_in_planogram

        self.write_to_db(fk=kpi_fk, numerator_id=self.manufacturer_fk,
                         denominator_id=scene_realogram.template_fk,
                         numerator_result=len(scene_realogram.extra_tags),
                         denominator_result=scene_realogram.number_of_skus_in_planogram,
                         result=len(scene_realogram.extra_tags) / number_of_positions_in_planogram,
                         context_id=scene_realogram.scene_fk,
                         identifier_result=kpi_fk, identifier_parent=parent_fk, should_enter=True)
        return

    def _calculate_extra_sku(self, scene_realogram):
        kpi_fk = self.get_kpi_fk_by_kpi_type(Consts.EXTRA_SKU)
        parent_fk = self.get_parent_fk(Consts.EXTRA_SKU)

        extra_skus = scene_realogram.calculate_extra_skus()
        for sku_row in extra_skus.itertuples():
            self.write_to_db(fk=kpi_fk, numerator_id=sku_row.target_product_fk,
                             denominator_id=scene_realogram.template_fk, numerator_result=sku_row.facings,
                             denominator_result=scene_realogram.number_of_skus_in_planogram, result=sku_row.facings,
                             context_id=scene_realogram.scene_fk, identifier_parent=parent_fk, should_enter=True)

    def _calculate_scene_realograms(self):
        scene_realograms = {}
        for scene_type in self.relevant_targets[Consts.TEMPLATE_SCENE_TYPE].unique().tolist():
            if scene_type in self.scif['template_name'].unique().tolist():
                template_fk = self._get_template_fk(scene_type)
                for scene_fk in self.scif[self.scif['template_name'] == scene_type]['scene_id'].unique().tolist():
                    scene_mpis = self.matches[self.matches['scene_fk'] == scene_fk]
                    scene_mpis = self._convert_mpis_product_fks_to_leads(scene_mpis)
                    scene_realogram = CervezaRealogram(scene_mpis, scene_type, template_fk, self.relevant_targets)
                    scene_realograms[scene_fk] = scene_realogram
        return scene_realograms

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

    def get_kpi_weight(self, kpi_name):
        weight = Consts.KPI_POINTS[kpi_name]
        return weight

    def _get_template_fk(self, template_name):
        return self.templates[self.templates['template_name'] == template_name]['template_fk'].iloc[0]


class CervezaRealogram(object):
    def __init__(self, mpis, scene_type, template_fk, planogram_template_data):
        self.scene_fk = self._get_scene_fk(mpis)
        self.mpis = mpis[mpis['scene_fk'] == self.scene_fk]
        self.template_fk = template_fk
        self.scene_type = scene_type
        self.planograms = self._generate_planograms_by_door(planogram_template_data)
        self.realogram = self._generate_realogram()
        self.correctly_placed_tags = self._calculate_correctly_placed_tags()
        self.incorrectly_placed_tags = self._calculate_incorrectly_placed_tags()
        self.extra_tags = self._calculate_extra_tags()
        self.extra_facings = self._calculate_extra_facings()
        self.number_of_skus_in_planogram = self._get_number_of_skus_in_planogram()
        self.number_of_positions_in_planogram = self._get_number_of_positions_in_planogram()

    def _get_scene_fk(self, mpis):
        return mpis['scene_fk'].iloc[0]

    def _generate_planograms_by_door(self, planogram_template_data):
        planograms = {}
        relevant_template = \
            planogram_template_data[planogram_template_data[Consts.TEMPLATE_SCENE_TYPE] == self.scene_type]

        for door in relevant_template[Consts.TEMPLATE_DOOR_ID].unique().tolist():
            door_template = relevant_template[relevant_template[Consts.TEMPLATE_DOOR_ID] == door]
            planograms[door] = self._expand_template_data_into_planogram(door_template)

        return planograms

    @staticmethod
    def _expand_template_data_into_planogram(template_data):
        planogram = pd.DataFrame(columns=['target_product_fk', 'target_shelf', 'target_sequence_number'])
        for product_line in template_data.sort_values(
                by=[Consts.TEMPLATE_SHELF_NUMBER, Consts.TEMPLATE_SEQUENCE_NUMBER]).itertuples():
            for i in range(int(getattr(product_line, Consts.TEMPLATE_FACINGS_COUNT))):
                planogram.loc[len(planogram), planogram.columns.tolist()] = \
                    [product_line.target_product_fk,
                     getattr(product_line, Consts.TEMPLATE_SHELF_NUMBER),
                     getattr(product_line, Consts.TEMPLATE_SEQUENCE_NUMBER) + i]
        return planogram

    def _generate_realogram(self):
        realograms = []
        for door_id, door_planogram in self.planograms.items():
            door_realogram = self.mpis[self.mpis['bay_number'] == door_id]
            door_realogram = pd.merge(door_realogram, door_planogram, how='outer',
                                      left_on=['shelf_number', 'facing_sequence_number'],
                                      right_on=['target_shelf', 'target_sequence_number'])
            products_in_planogram = door_planogram['target_product_fk'].unique().tolist()
            door_realogram['sku_in_planogram'] = pd.np.nan  # add column to DF
            # flag tags for whether or not they exist in the planogram
            door_realogram.loc[door_realogram['target_shelf'].isna(), 'sku_in_planogram'] = \
                door_realogram.loc[door_realogram['target_shelf'].isna(),
                                   'leading_product_fk'].isin(products_in_planogram)
            realograms.append(door_realogram)
        return pd.concat(realograms)

    def _calculate_correctly_placed_tags(self):
        return self.realogram[(self.realogram['leading_product_fk'] == self.realogram['target_product_fk'])]

    def _calculate_incorrectly_placed_tags(self):
        return self.realogram[(self.realogram['leading_product_fk'] != self.realogram['target_product_fk']) |
                              (self.realogram['leading_product_fk']).isna()]

    def _calculate_extra_tags(self):
        return self.realogram[(self.realogram['sku_in_planogram'].notna()) &
                              (self.realogram['sku_in_planogram'] == False) &
                              (self.realogram['target_shelf'].isna())]

    def _calculate_extra_facings(self):
        return self.realogram[(self.realogram['sku_in_planogram'].notna()) &
                              (self.realogram['sku_in_planogram']) &
                              (self.realogram['target_shelf'].isna())]

    def calculate_correctly_placed_skus(self):
        incorrectly_placed_skus = self.incorrectly_placed_tags['target_product_fk'].unique().tolist()
        correctly_placed_skus = \
            self.correctly_placed_tags[~self.correctly_placed_tags['target_product_fk'].isin(incorrectly_placed_skus)]
        correctly_placed_skus = \
            correctly_placed_skus.groupby('target_product_fk', as_index=False)['scene_match_fk'].count()
        correctly_placed_skus.rename(columns={'scene_match_fk': 'facings'}, inplace=True)
        return correctly_placed_skus

    def calculate_incorrectly_placed_skus(self):
        incorrectly_placed_skus = \
            self.incorrectly_placed_tags.groupby('target_product_fk', as_index=False)['scene_match_fk'].count()
        incorrectly_placed_skus.rename(columns={'scene_match_fk': 'facings'}, inplace=True)
        return incorrectly_placed_skus

    def calculate_extra_skus(self):
        extra_skus = \
            self.extra_tags.groupby('target_product_fk', as_index=False)['scene_match_fk'].count()
        extra_skus.rename(columns={'scene_match_fk': 'facings'}, inplace=True)
        return extra_skus

    def _get_number_of_skus_in_planogram(self):
        planogram_skus = []
        for door_planogram in self.planograms.values():
            door_skus = door_planogram['target_product_fk'].unique().tolist()
            for sku in door_skus:
                if sku not in planogram_skus:
                    planogram_skus.append(sku)
        return len(planogram_skus)

    def _get_number_of_positions_in_planogram(self):
        number_of_positions = 0
        for door_planogram in self.planograms.values():
            number_of_positions += len(door_planogram)
        return number_of_positions



