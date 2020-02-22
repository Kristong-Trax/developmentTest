from Trax.Algo.Calculations.Core.DataProvider import Data
from Trax.Utils.Logging.Logger import Log
from KPIUtils_v2.Utils.GlobalScripts.Scripts import GlobalSessionToolBox
import pandas as pd
from Projects.HEINEKENMX.Cocacola.Utils.Const import Const

from Projects.HEINEKENMX.Data.LocalConsts import Consts

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
        # self.calculate_empty_exist()
        # self.calculate_invasion()
        # self.calculate_shelf_position()
        # self.calculate_distribution()
        self.calculate_facing_count()

    def calculate_empty_exist(self):
        for kpi_name in Const.KPI_EMPTY:
            kpi_fk = self.get_kpi_fk_by_kpi_type(kpi_name)
            parent_fk = self.get_parent_fk(kpi_name)

            if self.relevant_scif.empty:
                score = 0
            else:
                empty_df = self.relevant_scif[self.relevant_scif['product_type'] == Const.EMPTY]

                if empty_df.empty:
                    score = 1
                else:
                    score = 0

            self.write_to_db(fk=kpi_fk, numerator_id=self.manufacturer_pk, denominator_id=self.store_id,
                             score=score,
                             identifier_parent=parent_fk, identifier_result=kpi_fk, should_enter=True)

    def calculate_facing_count(self):
        for kpi_name in Const.KPI_FACINGS:
            kpi_fk = self.get_kpi_fk_by_kpi_type(kpi_name)
            parent_fk = self.get_parent_fk(kpi_name)

            # place holding these for now, will fix tomorrow feb 19
            score = 0
            numerator_facings = 0
            denominator_facings = 0

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
                            else:
                                score = 0

                        except:
                            product_fk = -1

                        self.write_to_db(fk=kpi_fk, numerator_id=product_fk, numerator_result=ean_code,
                                         denominator_id=scene_id,
                                         result=found_sku_count, score=score, target=ean_product_target,
                                         identifier_parent=parent_fk, identifier_result=kpi_fk, should_enter=True)

    def calculate_distribution(self):
        # blah blah
        pass

    def calculate_shelf_position(self):
        for kpi_name in Const.KPI_POSITION:
            kpi_fk = self.get_kpi_fk_by_kpi_type(kpi_name)
            parent_fk = self.get_parent_fk(kpi_name)

            numerator_facings = 0
            denominator_facings = 0

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

                    for ean_code in ean_codes:
                        filtered_frentes_df = frentes_target_df[frentes_target_df['PRODUCT EAN'] == ean_code]

                        score = 100

                        if score != 0:
                            try:
                                target_shelves = frentes_target_df['PARRILLA'][
                                    frentes_target_df['PRODUCT EAN'] == ean_code].unique().tolist()
                                target_bays = frentes_target_df['PUERTA'][
                                    frentes_target_df['PRODUCT EAN'] == ean_code].unique().tolist()
                                product_fk = self.all_products['product_fk'][
                                    self.all_products['product_ean_code'] == str(ean_code)].iloc[0]
                                if str(ean_code) in self.relevant_scif['product_ean_code'].tolist():
                                    for index, row in filtered_frentes_df.iterrows():
                                        bay = row['PUERTA']
                                        shelf = row['PARRILLA']
                                        relevant_matches = self.matches[self.matches['scene_fk'] == scene_id]

                                        filtered_matches_df = relevant_matches[
                                            (relevant_matches['bay_number'] == bay) & (
                                                    relevant_matches['shelf_number'] == shelf) & (
                                                    relevant_matches['product_fk'] == product_fk)]

                                        if filtered_matches_df.empty:
                                            score = 0
                                            break
                                        else:
                                            pass
                            except:
                                Log.warning("Failed to find product fk based on EAN CODE")
                        else:
                            score = 0
                            self.write_to_db(fk=kpi_fk, numerator_id=self.manufacturer_pk, denominator_id=self.store_id,
                                             score=score, context_id=product_fk,
                                             identifier_parent=parent_fk, identifier_result=kpi_fk, should_enter=True)

    def calculate_invasion(self):
        for kpi_name in Const.KPI_INVASION:
            kpi_fk = self.get_kpi_fk_by_kpi_type(kpi_name)
            parent_fk = self.get_parent_fk(kpi_name)

            if self.relevant_scif.empty:
                score = 0
            else:
                possible_points = 0
                relevant_invasion_df = self.invasion_template[
                    self.invasion_template['NOMBRE DE TAREA'].isin(self.relevant_scenes_exist_value)]
                inv_manufacturer = self.sanitize_values(relevant_invasion_df['Manufacturer '].iloc[0])
                inv_category = self.sanitize_values(relevant_invasion_df['Category'].iloc[0])
                inv_product = self.sanitize_values(relevant_invasion_df['product_type'].iloc[0])

                invasion_df = self.relevant_scif[(self.relevant_scif['manufacturer_name'].isin(inv_manufacturer)) | (
                    self.relevant_scif['category'].isin(inv_category)) | (
                                                     self.relevant_scif['product_type'].isin(inv_product))]
                if invasion_df.empty:
                    score = 1
                else:
                    score = 0

            self.write_to_db(fk=kpi_fk, numerator_id=self.manufacturer_pk, denominator_id=self.store_id,
                             score=score,
                             identifier_parent=parent_fk, identifier_result=kpi_fk, should_enter=True)

    def get_template(self):
        template_beidas_df = pd.read_excel(Const.KPI_TEMPLATE, sheetname=Const.sheetname_Bebidas, header=1)
        template_invasion_df = pd.read_excel(Const.KPI_TEMPLATE, sheetname=Const.sheetname_Invasion, header=1)
        return template_beidas_df, template_invasion_df

    def get_parent_fk(self, kpi_name):
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
