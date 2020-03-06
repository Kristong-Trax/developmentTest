
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
        self.gz = self.store_info['additional_attribute_4'].iloc[0]
        self.city = self.store_info['address_city'].iloc[0]
        self.relevant_targets = self._get_relevant_external_targets()
        self._determine_target_product_fks()
        self.leading_products = self._get_leading_products_from_scif()
        self.scene_realograms = self._calculate_scene_realograms()

    def main_calculation(self):
        kpi_fk = self.get_kpi_fk_by_kpi_type(Consts.CERVEZA)
        parent_fk = self.get_parent_fk(Consts.CERVEZA)

        score = 0
        score += self.calculate_mercadeo()
        score += self.calculate_sutrido()

        self.write_to_db(fk=kpi_fk, numerator_id=self.manufacturer_fk, denominator_id=self.store_id,
                         result=score,
                         identifier_result=kpi_fk, identifier_parent=parent_fk, should_enter=True)
        return score

    def calculate_mercadeo(self):
        kpi_fk = self.get_kpi_fk_by_kpi_type(Consts.MERCADEO)
        parent_fk = self.get_parent_fk(Consts.MERCADEO)

        score = 0
        score += self.calculate_acomodo()
        score += self.calculate_frentes()
        score += self.calculate_huecos()
        score += self.calculate_invasion()

        self.write_to_db(fk=kpi_fk, numerator_id=self.manufacturer_fk, denominator_id=self.store_id,
                         result=score,
                         identifier_result=kpi_fk, identifier_parent=parent_fk, should_enter=True)
        return score

    def calculate_sutrido(self):
        return 0

    def calculate_invasion(self):
        return 0

    def calculate_huecos(self):
        kpi_fk = self.get_kpi_fk_by_kpi_type(Consts.HUECOS)
        parent_fk = self.get_parent_fk(Consts.HUECOS)

        empty_scif = self.scif[self.scif['product_type'] == 'Empty']
        result = 0 if empty_scif.empty else 0

        self.write_to_db(fk=kpi_fk, numerator_id=self.manufacturer_fk, denominator_id=self.store_id,
                         result=result,
                         identifier_result=kpi_fk, identifier_parent=parent_fk, should_enter=True)
        return result

    def calculate_frentes(self):
        sku_kpi_fk = self.get_kpi_fk_by_kpi_type(Consts.FRENTES_SKU)
        kpi_fk = self.get_kpi_fk_by_kpi_type(Consts.FRENTES)
        parent_fk = self.get_kpi_fk_by_kpi_type(Consts.FRENTES)

        valid_scene_types = self.relevant_targets[Consts.TEMPLATE_SCENE_TYPE].unique().tolist()
        relevant_scif = self.scif[self.scif['template_name'].isin(valid_scene_types)]
        relevant_scif.groupby('product_fk', as_index=False)['facings'].sum()
        relevant_target_skus = self.relevant_targets.groupby('product_fk', as_index=False)['Frentes'].sum()
        relevant_target_skus.rename(columns={'Frentes': 'target'}, inplace=True)
        relevant_target_skus = pd.merge(relevant_target_skus, relevant_scif, how='left',
                                        on='product_fk').fillna(0)

        count_of_passing_skus = 0

        for sku_row in relevant_target_skus.itertuples():
            sku_result = 1 if sku_row.facings >= sku_row.target else 0
            count_of_passing_skus += sku_result
            self.write_to_db(fk=sku_kpi_fk, numerator_id=sku_row.product_fk, denominator_id=self.store_id,
                             numerator_result=sku_row.facings, result=sku_result, target=sku_row.target,
                             identifier_parent=kpi_fk, should_enter=True)

        result = count_of_passing_skus / len(relevant_target_skus)
        self.write_to_db(fk=kpi_fk, numerator_id=self.manufacturer_fk, denominator_id=self.store_id,
                         numerator_result=count_of_passing_skus, denominator_result=len(relevant_target_skus),
                         result=result,
                         identifier_parent=parent_fk, identifier_result=kpi_fk, should_enter=True)
        return result

    def calculate_acomodo(self):
        result = 0
        for scene_id, scene_realogram in self.scene_realograms.items():
            result += self.calculate_acomodo_scene(scene_realogram)
        return result

    def calculate_acomodo_scene(self, scene_realogram):
        kpi_fk = self.get_kpi_fk_by_kpi_type(Consts.ACOMODO_SCENE)
        parent_fk = self.get_parent_fk(Consts.ACOMODO_SCENE)

        result = self.calculate_colcado_correct(scene_realogram)
        self.calculate_colcado_incorrect()
        self.calculate_extra()

        self.write_to_db(fk=kpi_fk, numerator_id=scene_realogram.template_fk,
                         denominator_id=self.store_id, result=result, identifier_parent=parent_fk,
                         identifier_result=kpi_fk, should_enter=True)
        return result

    def calculate_colcado_correct(self, scene_realogram):
        sku_kpi_fk = self.get_kpi_fk_by_kpi_type(Consts.COLCADO_CORRECT_SKU)
        kpi_fk = self.get_kpi_fk_by_kpi_type(Consts.COLCADO_CORRECT)
        parent_fk = self.get_parent_fk(Consts.COLCADO_CORRECT)

        correctly_placed_skus = scene_realogram.calculate_correctly_placed_skus()
        for sku_row in correctly_placed_skus.itertuples():
            self.write_to_db(fk=sku_kpi_fk, numerator_id=sku_row.target_product_fk,
                             denominator_id=scene_realogram.template_fk, numerator_result=sku_row.facings,
                             denominator_result=scene_realogram.number_of_skus_in_planogram, result=sku_row.facings,
                             context_id=scene_realogram.scene_fk, identifier_parent=kpi_fk, should_enter=True)

        number_of_positions_in_planogram = scene_realogram.number_of_positions_in_planogram

        self.write_to_db(fk=kpi_fk, numerator_id=self.manufacturer_fk,
                         denominator_id=scene_realogram.template_fk,
                         numerator_result=len(scene_realogram.correctly_placed_tags),
                         denominator_result=len(scene_realogram.number_of_skus_in_planogram),
                         result=len(scene_realogram.correctly_placed_tags) / number_of_positions_in_planogram,
                         context_id=scene_realogram.scene_fk,
                         identifier_result=kpi_fk, identifier_parent=parent_fk, should_enter=True)

        return len(scene_realogram.correctly_placed_tags) / number_of_positions_in_planogram

    def calculate_colcado_incorrect(self, scene_realogram):
        sku_kpi_fk = self.get_kpi_fk_by_kpi_type(Consts.COLCADO_INCORRECT_SKU)
        kpi_fk = self.get_kpi_fk_by_kpi_type(Consts.COLCADO_INCORRECT)
        parent_fk = self.get_parent_fk(Consts.COLCADO_INCORRECT)

        incorrectly_placed_skus = scene_realogram.calculate_incorrectly_placed_skus()
        for sku_row in incorrectly_placed_skus.itertuples():
            self.write_to_db(fk=sku_kpi_fk, numerator_id=sku_row.target_product_fk,
                             denominator_id=scene_realogram.template_fk, numerator_result=sku_row.facings,
                             denominator_result=scene_realogram.number_of_skus_in_planogram, result=sku_row.facings,
                             context_id=scene_realogram.scene_fk, identifier_parent=kpi_fk, should_enter=True)

        number_of_positions_in_planogram = scene_realogram.number_of_positions_in_planogram

        self.write_to_db(fk=kpi_fk, numerator_id=self.manufacturer_fk,
                         denominator_id=scene_realogram.template_fk,
                         numerator_result=len(scene_realogram.incorrectly_placed_tags),
                         denominator_result=len(scene_realogram.number_of_skus_in_planogram),
                         result=len(scene_realogram.incorrectly_placed_tags) / number_of_positions_in_planogram,
                         context_id=scene_realogram.scene_fk,
                         identifier_result=kpi_fk, identifier_parent=parent_fk, should_enter=True)
        return

    def calculate_extra(self, scene_realogram):
        sku_kpi_fk = self.get_kpi_fk_by_kpi_type(Consts.EXTRA_SKU)
        kpi_fk = self.get_kpi_fk_by_kpi_type(Consts.EXTRA)
        parent_fk = self.get_kpi_fk_by_kpi_type(Consts.EXTRA)

        extra_skus = scene_realogram.calculate_extra_skus()
        for sku_row in extra_skus.itertuples():
            self.write_to_db(fk=sku_kpi_fk, numerator_id=sku_row.target_product_fk,
                             denominator_id=scene_realogram.template_fk, numerator_result=sku_row.facings,
                             denominator_result=scene_realogram.number_of_skus_in_planogram, result=sku_row.facings,
                             context_id=scene_realogram.scene_fk, identifier_parent=kpi_fk, should_enter=True)

        number_of_positions_in_planogram = scene_realogram.number_of_positions_in_planogram

        self.write_to_db(fk=kpi_fk, numerator_id=self.manufacturer_fk,
                         denominator_id=scene_realogram.template_fk,
                         numerator_result=len(scene_realogram.extra_tags),
                         denominator_result=len(scene_realogram.number_of_skus_in_planogram),
                         result=len(scene_realogram.extra_tags) / number_of_positions_in_planogram,
                         context_id=scene_realogram.scene_fk,
                         identifier_result=kpi_fk, identifier_parent=parent_fk, should_enter=True)
        return

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

    def _get_relevant_external_targets(self):
        template_df = pd.read_excel(Consts.TEMPLATE_PATH, sheetname='Planograma_cerveza', header=1)
        template_df = template_df[(template_df['GZ'].str.encode('utf-8') == self.gz.encode('utf-8')) &
                                  (template_df['Ciudad'].str.encode('utf-8') == self.city.encode('utf-8'))]

        template_df['Puertas'] = template_df['Puertas'].fillna(1)
        template_df = template_df[['GZ', 'Ciudad', 'Nombre de Tarea', 'Puertas', 'EAN Code', 'Product Name',
                                  'TIPO DE SKU', 'x', 'y', 'Frentes']]
        return template_df

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

    def _convert_mpis_product_fks_to_leads(self, mpis):
        mpis_with_leading_products = pd.merge(mpis, self.leading_products,
                                              how='left', left_on='product_fk', right_on='substitution_product_fk')
        mpis_with_leading_products.rename(columns={'substitution_product_fk': 'leading_product_fk'}, inplace=True)
        return mpis_with_leading_products

    def get_parent_fk(self, kpi_name):
        parent_kpi_name = Consts.KPIS_HIERARCHY[kpi_name]
        parent_fk = self.get_kpi_fk_by_kpi_type(parent_kpi_name)
        return parent_fk

    def _get_template_fk(self, template_name):
        return self.templates[self.templates['template_name'] == template_name]['pk'].iloc[0]


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



