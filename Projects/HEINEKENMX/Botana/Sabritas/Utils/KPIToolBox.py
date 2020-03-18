from Trax.Utils.Logging.Logger import Log
from KPIUtils_v2.Utils.GlobalScripts.Scripts import GlobalSessionToolBox
import pandas as pd
from Projects.HEINEKENMX.Refresco.Pepsi.Utils.Const import Const

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

__author__ = 'nicolaske'


class PepsiToolBox(GlobalSessionToolBox):

    def __init__(self, data_provider, output, common):
        GlobalSessionToolBox.__init__(self, data_provider, output, common)
        self.main_template, self.invasion_template = self.get_template()
        
        self.relevant_scenes_exist_value = self.do_relevant_scenes_exist()
        self.relevant_scif = self.scif[self.scif['template_name'].isin(self.relevant_scenes_exist_value)]

    def main_calculation(self):
        score = self.calculate_refrescos_pepsi()
        return score

    def calculate_refrescos_pepsi(self):
        kpi_name = Const.KPI_REFRESCO
        kpi_fk = self.get_kpi_fk_by_kpi_type(kpi_name)
        parent_fk = self.get_parent_fk(kpi_name)
        # kpi_weight = Const.KPI_WEIGHTS[kpi_name]
        max_possible_point = Const.KPI_POINTS[kpi_name]

        mercadeo = self.calculate_mercadeo()
        surtido = self.calculate_surtido()

        scores = [surtido, mercadeo]
        score = self.calculate_sum_scores(scores)
        # score = round(((ratio * .01) * kpi_weight), 2)

        ratio = (score / max_possible_point) * 100
        self.write_to_db(fk=kpi_fk, numerator_id=self.manufacturer_fk, denominator_id=self.store_id,
                         result=ratio,
                         score=score, weight=max_possible_point, target=max_possible_point,
                         identifier_parent=parent_fk, identifier_result=kpi_fk, should_enter=True)
        return score

    def calculate_mercadeo(self):
        kpi_name = Const.KPI_MERCADEO
        kpi_fk = self.get_kpi_fk_by_kpi_type(kpi_name)
        parent_fk = self.get_parent_fk(kpi_name)
        kpi_weight = Const.KPI_WEIGHTS[kpi_name]
        max_possible_point = Const.KPI_POINTS[kpi_name]
        score = 0

        score += self.calculate_empty_exist()
        score +=self.calculate_invasion()
        score += self.calculate_acamodo()  #acomodo
        score += self.calculate_facing_count() #frentes

        ratio = (score / max_possible_point) * 100
        # score = round(((ratio * .01) * kpi_weight), 2)

        self.write_to_db(fk=kpi_fk, numerator_id=self.manufacturer_fk, denominator_id=self.store_id,
                         result=ratio,
                         score=score, weight=kpi_weight, target=max_possible_point,
                         identifier_parent=parent_fk, identifier_result=kpi_fk, should_enter=True)
        return score

    def calculate_surtido(self):
        score = self.calculate_distribution()
        return score

    def calculate_average_ratio(self, ratio_list):
        ratio_sum = 0
        list_count = len(ratio_list)
        for ratio in ratio_list:
            ratio_sum += ratio

        if list_count != 0:
            final_ratio = round(ratio_sum / float(list_count), 2)
        else:
            final_ratio = 0
        return final_ratio

    def calculate_sum_scores(self, score_list):
        score_sum = 0

        for score in score_list:
            score_sum += score

        return score_sum



    def calculate_empty_exist(self):
        kpi_name = Const.KPI_EMPTY
        kpi_fk = self.get_kpi_fk_by_kpi_type(kpi_name)
        parent_fk = self.get_parent_fk(kpi_name)
        kpi_weight = Const.KPI_WEIGHTS[kpi_name]
        kpi_point = Const.KPI_POINTS[kpi_name]

        ratio = 0

        if self.relevant_scif.empty:
            score = 0
        else:
            empty_df = self.relevant_scif[self.relevant_scif['product_type'] == Const.EMPTY]

            if empty_df.empty:
                ratio = 100
            else:
                ratio = 0

            score = round(((ratio * .01) * kpi_point), 2)
        self.write_to_db(fk=kpi_fk, numerator_id=self.manufacturer_fk, denominator_id=self.store_id,
                         result=ratio,
                         score=score, weight=kpi_weight, target=kpi_point,
                         identifier_parent=parent_fk, identifier_result=kpi_fk, should_enter=True)
        return score

    def calculate_facing_count(self):
        # place holding these for now, will fix tomorrow feb 19
        kpi_fk = self.get_kpi_fk_by_kpi_type(Const.KPI_FRENTES)
        parent_fk = self.get_parent_fk(Const.KPI_FRENTES)
        max_kpi_points = Const.KPI_POINTS[Const.KPI_FRENTES]
        weight = Const.KPI_WEIGHTS[Const.KPI_FRENTES]

        valid_scene_types = self.main_template['NOMBRE DE TAREA'].unique().tolist()
        valid_scene_types = [x for x in valid_scene_types if 'Pepsi' in x]
        relevant_scif = self.scif[self.scif['template_name'].isin(valid_scene_types)]
        relevant_scif.groupby('product_fk', as_index=False)['facings'].sum()
        relevant_target_skus = \
            self.main_template[self.main_template['NOMBRE DE TAREA'].isin(
                relevant_scif['template_name'].unique().tolist())]
        all_products = self.all_products
        all_products.product_ean_code.fillna(value=-1, inplace=True)
        all_products.product_ean_code = all_products.product_ean_code.astype(int)

        relevant_target_skus = pd.merge(relevant_target_skus, all_products, how='left',
                 left_on='PRODUCT EAN',
                 right_on='product_ean_code')

        relevant_target_skus = relevant_target_skus.groupby('product_fk', as_index=False)['FRENTES'].sum()
        relevant_target_skus.rename(columns={'FRENTES': 'target'}, inplace=True)
        relevant_target_skus = pd.merge(relevant_target_skus, relevant_scif, how='left',
                                        left_on='product_fk',
                                        right_on='product_fk').fillna(0)

        relevant_target_skus['meets_target'] = relevant_target_skus['facings'] >= relevant_target_skus['target']
        relevant_target_skus['meets_target'] = relevant_target_skus['meets_target'].apply(lambda x: 1 if x else 0)
        count_of_passing_skus = relevant_target_skus['meets_target'].sum()

        self._calculate_frentes_sku(relevant_target_skus)

        result = count_of_passing_skus / float(len(relevant_target_skus))
        score = result * max_kpi_points
        self.write_to_db(fk=kpi_fk, numerator_id=self.manufacturer_fk, denominator_id=self.store_id,
                         numerator_result=count_of_passing_skus, denominator_result=len(relevant_target_skus),
                         result=result * 100, score=score, weight=weight, target=max_kpi_points,
                         identifier_parent=parent_fk, identifier_result=kpi_fk, should_enter=True)
        return score


    def _calculate_frentes_sku(self, relevant_target_skus):
        kpi_fk = self.get_kpi_fk_by_kpi_type(Const.KPI_FRENTES_SKU)
        parent_fk = self.get_parent_fk(Const.KPI_FRENTES_SKU)

        for sku_row in relevant_target_skus.itertuples():
            self.write_to_db(fk=kpi_fk, numerator_id=sku_row.product_fk, denominator_id=self.store_id,
                             numerator_result=sku_row.facings, result=sku_row.meets_target, target=sku_row.target,
                             identifier_parent=parent_fk, should_enter=True)

    def calculate_distribution(self):
        kpi_name = Const.KPI_DISTRIBUTION
        kpi_fk = self.get_kpi_fk_by_kpi_type(kpi_name)
        parent_fk = self.get_parent_fk(kpi_name)

        parent_kpi_name = Const.KPIS_HIERACHY[kpi_name]
        kpi_weight = Const.KPI_WEIGHTS[parent_kpi_name]
        kpi_point = Const.KPI_POINTS[parent_kpi_name]
        grand_parent_fk = self.get_parent_fk(parent_kpi_name)
        max_possible_point = Const.KPI_POINTS[parent_kpi_name]


        # place holding these for now, will fix tomorrow feb 19
        score = 0
        numerator_facings = 0
        denominator_facings = 0
        total_ean_codes = 0

        if self.relevant_scif.empty:
            score = 0
            self.write_to_db(fk=kpi_fk, numerator_id=self.manufacturer_fk, numerator_result=numerator_facings,
                             denominator_id=self.store_id, denominator_result=denominator_facings, score=score,
                             identifier_parent=parent_fk, identifier_result=kpi_fk, should_enter=True)
        else:
            scene_ids = list(self.relevant_scif.scene_id.unique())

            for scene_id in scene_ids:
                scene_name = self.relevant_scif['template_name'][self.relevant_scif['scene_id'] == scene_id].iloc[0]
                template_name_fk = self.relevant_scif['template_fk'][self.relevant_scif['scene_id'] == scene_id].iloc[0]

                frentes_target_df = self.main_template[self.main_template['NOMBRE DE TAREA'] == scene_name]
                ean_codes = frentes_target_df['PRODUCT EAN'].unique().tolist()

                found_ean = 0
                for ean_code in ean_codes:

                    product_fks = self.all_products['product_fk'][
                        self.all_products['product_ean_code'] == ean_code]

                    if not product_fks.empty:
                        total_ean_codes += 1
                        product_fk = self.all_products['product_fk'][
                            self.all_products['product_ean_code'] == ean_code].iloc[0]

                        try:

                            found_sku_df = self.relevant_scif[
                                self.relevant_scif['product_ean_code'] == str(ean_code)]

                            if found_sku_df.empty:
                                score = 0
                                found_ean += 1

                            else:
                                score = 100

                        except:
                            Log.warning("Distribution KPI Failed.")

                        if product_fk != -1:
                            self.write_to_db(fk=kpi_fk, numerator_id=product_fk, numerator_result=ean_code,
                                             denominator_id=template_name_fk,
                                             result=score, score=score,
                                             context_id=scene_id,
                                             identifier_parent=parent_fk, identifier_result=kpi_fk, should_enter=True)

        if total_ean_codes != 0:
            numerator_facings = found_ean
            denominator_facings = total_ean_codes
            ratio = round(numerator_facings / float(denominator_facings), 2) * 100
        else:
            ratio = 0

        score = round(((ratio * .01) * kpi_point), 2)
        self.write_to_db(fk=parent_fk, numerator_id=self.manufacturer_fk,
                         numerator_result=numerator_facings,
                         denominator_id=self.store_id,
                         denominator_result=denominator_facings,
                         result=ratio, score=score, weight=kpi_weight, target=max_possible_point,
                         identifier_parent=grand_parent_fk, identifier_result=parent_fk, should_enter=True)
        return score



    def calculate_acamodo(self):
        kpi_name = Const.KPI_ACAMODO
        kpi_fk = self.get_kpi_fk_by_kpi_type(kpi_name)
        parent_fk = self.get_parent_fk(kpi_name)


        kpi_weight = Const.KPI_WEIGHTS[kpi_name]
        kpi_point = Const.KPI_POINTS[kpi_name]

        final_ratio = self.calculate_acamodo_scene()

        score = round(((final_ratio * .01) * kpi_point), 2)
        self.write_to_db(fk=kpi_fk, numerator_id=self.manufacturer_fk,
                             denominator_id=self.store_id,
                             result=final_ratio,
                             score=score,
                             weight=kpi_weight, target=kpi_point,
                             identifier_parent=parent_fk,
                             identifier_result=kpi_fk,
                             should_enter=True)
        return score


    def calculate_acamodo_scene(self):
        kpi_name = Const.KPI_ACAMODO_SCENE
        kpi_fk = self.get_kpi_fk_by_kpi_type(kpi_name)
        parent_fk = self.get_parent_fk(kpi_name)


        numerator_facings = 0
        denominator_facings = 0
        scene_ratios = []

        if self.relevant_scif.empty:
            score = 0
            self.write_to_db(fk=kpi_fk, numerator_id=self.manufacturer_fk, numerator_result=numerator_facings,
                             denominator_id=self.store_id, denominator_result=denominator_facings, score=score,
                             )
        else:
            scene_ids = list(self.relevant_scif.scene_id.unique())
            for scene_id in scene_ids:
                scene_name = self.relevant_scif['template_name'][self.relevant_scif['scene_id'] == scene_id].iloc[0]
                template_name_fk = self.relevant_scif['template_fk'][self.relevant_scif['scene_id'] == scene_id].iloc[0]
                frentes_target_df = self.main_template[self.main_template['NOMBRE DE TAREA'] == scene_name]
                ean_codes = frentes_target_df['PRODUCT EAN'].unique().tolist()
                # ean_codes_count = len(ean_codes)
                # passing_ean = 0
                all_products = self.all_products
                all_products.product_ean_code.fillna(value=-1, inplace=True)
                all_products.product_ean_code = all_products.product_ean_code.astype(int)
                target_df_merged = pd.merge(all_products, frentes_target_df, left_on='product_ean_code', right_on='PRODUCT EAN',
                                how='right')

                relevant_matches = self.matches[self.matches['scene_fk'] == scene_id]

                acomodo_merged_df = pd.merge(target_df_merged, relevant_matches, on='product_fk',
                                how='left')
                acomodo_merged_df['passed'] = 0
                acomodo_merged_df['passed'].loc[(acomodo_merged_df['PUERTA'] == acomodo_merged_df['bay_number']) & (
                        acomodo_merged_df['PARRILLA'] == acomodo_merged_df['shelf_number'])] = 100

                ratio = self.calculate_acomodo_sku(acomodo_merged_df, template_name_fk)
                scene_ratios.append(ratio)


                self.write_to_db(fk=kpi_fk, numerator_id=template_name_fk,
                                 denominator_id=self.store_id,
                                 context_id=scene_id,
                                 result=ratio,
                                 score=ratio,
                                 identifier_parent=parent_fk, identifier_result=kpi_fk,
                                 should_enter=True)


        final_ratio = self.calculate_average_ratio(scene_ratios)
        return final_ratio

    def calculate_acomodo_sku(self, df, template_name_fk):
        kpi_name = Const.KPI_ACAMODO_SKU
        kpi_fk = self.get_kpi_fk_by_kpi_type(kpi_name)
        parent_fk = self.get_parent_fk(kpi_name)
        numerator = 0
        denominator = 0

        relevant_columns = ['scene_fk', 'product_fk', 'PUERTA', 'PARRILLA', 'shelf_number', 'bay_number', 'passed']
        df_fixed = df[relevant_columns].drop_duplicates()

        product_fks = df_fixed.product_fk.unique().tolist()
        df_grouped = df_fixed.groupby(['product_fk', 'PARRILLA', 'PUERTA'])[
            'product_fk', 'PARRILLA', 'PUERTA', 'passed'].sum()

        for product_fk in product_fks:
            did_pass = (df_grouped['passed'][df_grouped['product_fk'] == product_fk] == 100).all()
            if did_pass == True:
                pass_value = 1
            else:
                pass_value = 0

            if not pd.isna(product_fk):
                denominator += 1
                if pass_value == 1:
                    numerator += 1

                self.write_to_db(fk=kpi_fk, numerator_id=product_fk,
                                 # numerator_result=sku_row.PUERTA,
                                 denominator_id=template_name_fk,
                                 # denominator_result=sku_row.bay_number,
                                 # result=sku_row.shelf_number,
                                 # target=sku_row.PARRILLA,
                                 score=pass_value, context_id=scene_fk,
                                 identifier_parent=parent_fk,
                                 identifier_result=kpi_fk,
                                 should_enter=True)
        if denominator != 0:
            ratio = (numerator / float(denominator)) * 100
        else:
            ratio = 0

        return ratio


    def calculate_invasion(self):
        kpi_name = Const.KPI_INVASION
        kpi_fk = self.get_kpi_fk_by_kpi_type(kpi_name)
        parent_fk = self.get_parent_fk(kpi_name)
        kpi_weight = Const.KPI_WEIGHTS[kpi_name]
        kpi_point =  Const.KPI_POINTS[kpi_name]
        ratio = 0

        if self.relevant_scif.empty:
            score = 0
        else:
            relevant_invasion_df = self.invasion_template[
                self.invasion_template['NOMBRE DE TAREA'].isin(self.relevant_scenes_exist_value)]
            inv_manufacturer = self.sanitize_values(relevant_invasion_df['Manufacturer '].iloc[0])
            inv_category = self.sanitize_values(relevant_invasion_df['Category'].iloc[0])
            inv_product = self.sanitize_values(relevant_invasion_df['product_type'].iloc[0])

            invasion_df = self.relevant_scif[(self.relevant_scif['manufacturer_name'].isin(inv_manufacturer)) | (
                self.relevant_scif['category'].isin(inv_category)) | (
                                                 self.relevant_scif['product_type'].isin(inv_product))]
            if invasion_df.empty:
                ratio = 100
            else:
                ratio = 0

            score = round(((ratio * .01) * kpi_point), 2)

        self.write_to_db(fk=kpi_fk, numerator_id=self.manufacturer_fk, denominator_id=self.store_id,
                         result=ratio,
                         score=score, weight=kpi_weight, target= kpi_point,
                         identifier_parent=parent_fk, identifier_result=kpi_fk, should_enter=True)
        return score

    def get_template(self):
        template_beidas_df = pd.read_excel(Const.KPI_TEMPLATE, sheetname=Const.sheetname_Bebidas, header=1)
        template_invasion_df = pd.read_excel(Const.KPI_TEMPLATE, sheetname=Const.sheetname_Invasion, header=1)
        return template_beidas_df, template_invasion_df

    def get_parent_fk(self, kpi_name):
        parent_kpi_name = Const.KPIS_HIERACHY[kpi_name]
        parent_fk = self.get_kpi_fk_by_kpi_type(parent_kpi_name)
        return parent_fk

    def get_grand_parent_fk(self, kpi_name):
        parent_kpi_name = Const.KPIS_HIERACHY[kpi_name]
        parent_fk = self.get_kpi_fk_by_kpi_type(parent_kpi_name)
        return parent_fk

    def do_relevant_scenes_exist(self):
        scene_types = '|'.join(Const.RELEVANT_SCENES_TYPES)
        relevant_scenes = self.scif['template_name'][
            self.scif["template_name"].str.contains(scene_types)].unique()
        return relevant_scenes

    def sanitize_values(self, values):
        list_values = values.split(",")
        return list_values
