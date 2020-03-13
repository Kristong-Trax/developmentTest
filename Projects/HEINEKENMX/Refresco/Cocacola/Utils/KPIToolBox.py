from Trax.Utils.Logging.Logger import Log
from KPIUtils_v2.Utils.GlobalScripts.Scripts import GlobalSessionToolBox
import pandas as pd
from Projects.HEINEKENMX.Refresco.Cocacola.Utils.Const import Const

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


class CocacolaToolBox(GlobalSessionToolBox):

    def __init__(self, data_provider, output, common):
        GlobalSessionToolBox.__init__(self, data_provider, output, common)
        self.main_template, self.invasion_template = self.get_template()
        self.manufacturer_pk = \
            self.all_products['manufacturer_name'][self.all_products['manufacturer_name'] == Const.COCACOLA].iloc[0]
        self.relevant_scenes_exist_value = self.do_relevant_scenes_exist()
        self.relevant_scif = self.scif[self.scif['template_name'].isin(self.relevant_scenes_exist_value)]

    def main_calculation(self):
        score = self.calculate_refrescos_coca()
        return score

    def calculate_refrescos_coca(self):
        kpi_name = Const.KPI_REFRESCO
        kpi_fk = self.get_kpi_fk_by_kpi_type(kpi_name)
        parent_fk = self.get_parent_fk(kpi_name)
        kpi_weight = Const.KPI_WEIGHTS[kpi_name]

        mercadeo = self.calculate_mercadeo()
        surtido = self.calculate_surtido()

        scores = [surtido, mercadeo]
        score = self.calculate_sum_scores(scores)
        # score = round(((ratio * .01) * kpi_weight), 2)
        self.write_to_db(fk=kpi_fk, numerator_id=self.manufacturer_pk, denominator_id=self.store_id,
                         result=score,
                         score=score,
                         identifier_parent=parent_fk, identifier_result=kpi_fk, should_enter=True)
        return score

    def calculate_mercadeo(self):
        kpi_name = Const.KPI_MERCADEO
        kpi_fk = self.get_kpi_fk_by_kpi_type(kpi_name)
        parent_fk = self.get_parent_fk(kpi_name)
        kpi_weight = Const.KPI_WEIGHTS[kpi_name]

        empty_ratio = self.calculate_empty_exist()
        invasion_ratio = self.calculate_invasion()
        shelf_position_ratio = self.calculate_shelf_position()
        facing_ratio = self.calculate_facing_count()

        scores = [empty_ratio, invasion_ratio, shelf_position_ratio, facing_ratio]
        score = self.calculate_sum_scores(scores)
        # score = round(((ratio * .01) * kpi_weight), 2)

        self.write_to_db(fk=kpi_fk, numerator_id=self.manufacturer_pk, denominator_id=self.store_id,
                         result=score,
                         score=score,
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

        if self.relevant_scif.empty:
            score = 0
        else:
            empty_df = self.relevant_scif[self.relevant_scif['product_type'] == Const.EMPTY]

            if empty_df.empty:
                ratio = 100
            else:
                ratio = 0

            score = round(((ratio * .01) * kpi_weight), 2)
        self.write_to_db(fk=kpi_fk, numerator_id=self.manufacturer_pk, denominator_id=self.store_id,
                         ratio=ratio,
                         score=score,
                         identifier_parent=parent_fk, identifier_result=kpi_fk, should_enter=True)
        return score

    def calculate_facing_count(self):
        kpi_name = Const.KPI_FACINGS
        kpi_fk = self.get_kpi_fk_by_kpi_type(kpi_name)
        parent_fk = self.get_parent_fk(kpi_name)
        parent_kpi_name = Const.KPIS_HIERACHY[kpi_name]
        grand_parent_fk = self.get_parent_fk(parent_kpi_name)
        kpi_weight = Const.KPI_WEIGHTS[parent_kpi_name]
        # place holding these for now, will fix tomorrow feb 19
        score = 0
        numerator_facings = 0
        denominator_facings = 0
        total_ean_codes = 0

        if self.relevant_scif.empty:
            score = 0
            self.write_to_db(fk=kpi_fk, numerator_id=self.manufacturer_pk, numerator_result=numerator_facings,
                             denominator_id=self.store_id, denominator_result=denominator_facings, score=score,
                             )

        else:

            scene_ids = list(self.relevant_scif.scene_id.unique())
            for scene_id in scene_ids:
                scene_name = self.relevant_scif['template_name'][self.relevant_scif['scene_id'] == scene_id].iloc[0]
                frentes_target_df = self.main_template[self.main_template['NOMBRE DE TAREA'] == scene_name]
                ean_codes = frentes_target_df['PRODUCT EAN'].unique().tolist()
                ean_code_count = len(ean_codes)
                total_ean_codes += ean_code_count
                passing_ean = 0

                for ean_code in ean_codes:
                    try:
                        ean_product_target = \
                            frentes_target_df['FRENTES'][frentes_target_df['PRODUCT EAN'] == ean_code].iloc[0]

                        found_sku_count = self.relevant_scif['facings'][
                            self.relevant_scif['product_ean_code'] == str(ean_code)].iloc[0]
                        product_fk = self.all_products['product_fk'][
                            self.all_products['product_ean_code'] == str(ean_code)].iloc[0]
                        if found_sku_count >= ean_product_target:
                            score = 100
                            passing_ean += 1
                        else:
                            score = 0

                    except:
                        product_fk = -1
                        found_sku_count = 0
                        ean_product_target = 0

                    self.write_to_db(fk=kpi_fk, numerator_id=product_fk, numerator_result=ean_code,
                                     denominator_id=self.store_id,
                                     result=found_sku_count, score=score, target=ean_product_target,
                                     identifier_parent=parent_fk, identifier_result=kpi_fk, should_enter=True)

        if total_ean_codes != 0:
            numerator_facings = passing_ean
            denominator_facings = total_ean_codes

            ratio = round(passing_ean / float(total_ean_codes), 2) * 100
        else:
            ratio = 0

        score = round(((ratio * .01) * kpi_weight), 2)
        self.write_to_db(fk=parent_fk,
                         numerator_id=self.manufacturer_fk,
                         numerator_result=numerator_facings,
                         denominator_id=self.store_id,
                         denominator_result=denominator_facings,
                         result=ratio, score=score,
                         identifier_parent=grand_parent_fk, identifier_result=parent_fk, should_enter=True)

        return ratio

    def calculate_distribution(self):
        kpi_name = Const.KPI_DISTRIBUTION
        kpi_fk = self.get_kpi_fk_by_kpi_type(kpi_name)
        parent_fk = self.get_parent_fk(kpi_name)

        parent_kpi_name = Const.KPIS_HIERACHY[kpi_name]
        kpi_weight = Const.KPI_WEIGHTS[parent_kpi_name]
        grand_parent_fk = self.get_parent_fk(parent_kpi_name)


        # place holding these for now, will fix tomorrow feb 19
        score = 0
        numerator_facings = 0
        denominator_facings = 0
        total_ean_codes = 0

        if self.relevant_scif.empty:
            score = 0
            self.write_to_db(fk=kpi_fk, numerator_id=self.manufacturer_pk, numerator_result=numerator_facings,
                             denominator_id=self.store_id, denominator_result=denominator_facings, score=score,
                             identifier_parent=parent_fk, identifier_result=kpi_fk, should_enter=True)
        else:
            scene_ids = list(self.relevant_scif.scene_id.unique())

            for scene_id in scene_ids:
                scene_name = self.relevant_scif['template_name'][self.relevant_scif['scene_id'] == scene_id].iloc[0]
                template_name_fk = self.relevant_scif['template_fk'][self.relevant_scif['scene_id'] == scene_id].iloc[0]

                frentes_target_df = self.main_template[self.main_template['NOMBRE DE TAREA'] == scene_name]
                ean_codes = frentes_target_df['PRODUCT EAN'].unique().tolist()
                ean_code_count = len(ean_codes)
                total_ean_codes += ean_code_count
                found_ean = 0
                for ean_code in ean_codes:

                    product_fks = self.all_products['product_fk'][
                        self.all_products['product_ean_code'] == str(ean_code)]

                    if product_fks.empty:
                        product_fk = -1
                    else:
                        product_fk = self.all_products['product_fk'][
                            self.all_products['product_ean_code'] == str(ean_code)].iloc[0]


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

        score = round(((ratio * .01) * kpi_weight), 2)
        self.write_to_db(fk=parent_fk, numerator_id=self.manufacturer_fk,
                         numerator_result=numerator_facings,
                         denominator_id=self.store_id,
                         denominator_result=denominator_facings,
                         result=ratio, score=score,
                         identifier_parent=grand_parent_fk, identifier_result=parent_fk, should_enter=True)
        return score


    def calculate_shelf_position(self):
        kpi_name = Const.KPI_POSITION
        kpi_fk = self.get_kpi_fk_by_kpi_type(kpi_name)
        parent_fk = self.get_parent_fk(kpi_name)

        parent_kpi_name = Const.KPIS_HIERACHY[kpi_name]
        grand_parent_fk = self.get_parent_fk(parent_kpi_name)

        grand_parent_kpi_name = Const.KPIS_HIERACHY[parent_kpi_name]
        great_grand_parent_fk = self.get_parent_fk(grand_parent_kpi_name)

        numerator_facings = 0
        denominator_facings = 0
        scene_ratios = []

        if self.relevant_scif.empty:
            score = 0
            self.write_to_db(fk=kpi_fk, numerator_id=self.manufacturer_pk, numerator_result=numerator_facings,
                             denominator_id=self.store_id, denominator_result=denominator_facings, score=score,
                             )
        else:
            scene_ids = list(self.relevant_scif.scene_id.unique())
            for scene_id in scene_ids:
                scene_name = self.relevant_scif['template_name'][self.relevant_scif['scene_id'] == scene_id].iloc[0]
                scene_name_fk = self.relevant_scif['template_fk'][self.relevant_scif['scene_id'] == scene_id].iloc[0]
                frentes_target_df = self.main_template[self.main_template['NOMBRE DE TAREA'] == scene_name]
                ean_codes = frentes_target_df['PRODUCT EAN'].unique().tolist()
                ean_codes_count = len(ean_codes)
                passing_ean = 0
                for ean_code in ean_codes:
                    filtered_frentes_df = frentes_target_df[frentes_target_df['PRODUCT EAN'] == ean_code]

                    score = 100

                    if score != 0:
                        try:
                            bay = 0
                            shelf = 0
                            product_fk = self.all_products['product_fk'][
                                self.all_products['product_ean_code'] == str(ean_code)]

                            if product_fk.empty:
                                product_fk = -1
                            else:
                                product_fk = self.all_products['product_fk'][
                                    self.all_products['product_ean_code'] == str(ean_code)].iloc[0]

                            for index, row in filtered_frentes_df.iterrows():
                                target_bay = row['PUERTA']
                                target_shelf = row['PARRILLA']
                                if str(ean_code) in self.relevant_scif['product_ean_code'].tolist():
                                    relevant_matches = self.matches[self.matches['scene_fk'] == scene_id]

                                    filtered_matches_df = relevant_matches[
                                        (relevant_matches['bay_number'] == target_bay) & (
                                                relevant_matches['shelf_number'] == target_shelf) & (
                                                relevant_matches['product_fk'] == product_fk)]

                                    if filtered_matches_df.empty:
                                        filtered_product_only_df = relevant_matches[['product_fk', 'bay_number', 'shelf_number']][relevant_matches['product_fk'] == product_fk].drop_duplicates()

                                        if filtered_product_only_df.empty:
                                            score = 0
                                            self.write_to_db(fk=kpi_fk, numerator_id=bay,
                                                             numerator_result=target_bay,
                                                             denominator_id=shelf,
                                                             denominator_result=target_shelf,
                                                             score=score, context_id=product_fk,
                                                             result= ean_code,
                                                             identifier_parent=parent_fk, identifier_result=kpi_fk,
                                                             should_enter=True)
                                        else:
                                            for p_index, p_row in filtered_product_only_df.iterrows():
                                                score = 0
                                                bay = p_row['bay_number']
                                                shelf = p_row['shelf_number']

                                                self.write_to_db(fk=kpi_fk, numerator_id=bay,
                                                                 numerator_result=target_bay,
                                                                 denominator_id=shelf,
                                                                 denominator_result=target_shelf,
                                                                 score=score, context_id=product_fk,
                                                                 result=ean_code,
                                                                 identifier_parent=parent_fk,
                                                                 identifier_result=kpi_fk,
                                                                 should_enter=True)
                                    else:
                                        score = 100
                                        bay = target_bay
                                        shelf = target_shelf

                                        self.write_to_db(fk=kpi_fk, numerator_id=bay,
                                                         numerator_result= target_bay,
                                                         denominator_id= shelf,
                                                         denominator_result= target_shelf,
                                                         score=score, context_id=product_fk,
                                                         result=ean_code,
                                                         identifier_parent=parent_fk, identifier_result=kpi_fk,
                                                         should_enter=True)


                                else:
                                    score = 0

                                    self.write_to_db(fk=kpi_fk,
                                                     numerator_id=product_fk,
                                                     numerator_result=target_bay,
                                                     denominator_id=scene_name_fk,
                                                     denominator_result=bay,
                                                     score=score, context_id=product_fk,
                                                     result=shelf,
                                                     target=target_shelf,
                                                     identifier_parent=parent_fk, identifier_result=kpi_fk,
                                                     should_enter=True)


                        except:
                            Log.warning("Failed in acomodo")

                if ean_codes_count != 0:
                    ratio = round(passing_ean / float(ean_codes_count), 2) * 100
                else:
                    ratio = 0
                scene_ratios.append(ratio)
                self.write_to_db(fk=parent_fk, numerator_id=scene_name_fk,
                                 numerator_result= passing_ean,
                                 denominator_id=self.store_id,
                                 denominator_result=ean_codes_count,
                                 context_id=scene_id,
                                 result=ratio,
                                 score=ratio,
                                 identifier_parent=grand_parent_fk, identifier_result=parent_fk,
                                 should_enter=True)

            kpi_weight = Const.KPI_WEIGHTS[grand_parent_kpi_name]
            final_ratio = self.calculate_average_ratio(scene_ratios)
            score = round(((final_ratio * .01) * kpi_weight), 2)
            self.write_to_db(fk=grand_parent_fk, numerator_id=self.manufacturer_fk,
                                 denominator_id=self.store_id,
                                 result=final_ratio,
                                 score=score,
                                 identifier_parent=great_grand_parent_fk,
                                 identifier_result=grand_parent_fk,
                                 should_enter=True)
        return final_ratio

    def calculate_invasion(self):
        kpi_name = Const.KPI_INVASION
        kpi_fk = self.get_kpi_fk_by_kpi_type(kpi_name)
        parent_fk = self.get_parent_fk(kpi_name)
        kpi_weight = Const.KPI_WEIGHTS[kpi_name]

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

            score = round(((ratio * .01) * kpi_weight), 2)

        self.write_to_db(fk=kpi_fk, numerator_id=self.manufacturer_pk, denominator_id=self.store_id,
                         ratio=ratio,
                         score=score,
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
        relevant_scenes = self.scif['template_name'][
            self.scif["template_name"].str.contains(Const.RELEVANT_SCENES_TYPES)].unique()
        return relevant_scenes

    def sanitize_values(self, values):
        list_values = values.split(",")
        return list_values
