from Trax.Algo.Calculations.Core.DataProvider import Data
from Trax.Utils.Logging.Logger import Log
from KPIUtils_v2.Utils.GlobalScripts.Scripts import GlobalSceneToolBox
import pandas as pd
import os

# from Projects.CCANDINAAR.Data.LocalConsts import Consts

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
TEMPLATE_PATH = os.path.join(os.path.dirname(os.path.realpath(__file__)), '..', 'Data', 'CCAndinaAR_template_v2.xlsx')

__author__ = 'nicolaske'


class ToolBox(GlobalSceneToolBox):

    def __init__(self, data_provider, output):
        GlobalSceneToolBox.__init__(self, data_provider, output)
        self.templates = {}
        self.match_product_in_scene = self.data_provider[Data.MATCHES]

    def main_calculation(self):
        sheet_list = pd.read_excel(TEMPLATE_PATH, None).keys()

        for sheet in sheet_list:
            self.templates[sheet] = pd.read_excel(TEMPLATE_PATH, sheet)
            self.templates[sheet] = self.templates[sheet][self.templates[sheet]['Relevance Type'] == 'Scene']
        self.calculate_aggregation()
        # self.calculate_availability()

    def calculate_aggregation(self):
        # The KPI will show an aggregated count of SKUs by scene which also includes an aggregation of empties as well
        for row in self.templates['Aggregation'].itertuples():
            kpi_name = row.KPI_Name
            kpi_fk = self.get_kpi_fk_by_kpi_name(kpi_name)
            empty_product_type_scif = self.scif[self.scif['product_type'].isin(['Empty'])]  # product_fk
            if not empty_product_type_scif.empty:
                empty_product_id = 0 #product_fk always to represent General Empty
                empty_template_id = empty_product_type_scif.template_fk.iat[0]
                empty_product_numerator_result = sum(empty_product_type_scif['tagged'])
                empty_product_denominator_result = sum(empty_product_type_scif['facings'])

                self.common.write_to_db_result(kpi_fk, numerator_id=empty_product_id,
                                               denominator_id=empty_template_id, context_id=empty_product_id,
                                               numerator_result=empty_product_numerator_result,
                                               denominator_result=empty_product_denominator_result, by_scene=True)


            sku_product_type_scif = self.scif[self.scif['product_type'].isin(['SKU'])]
            if not sku_product_type_scif.empty:
                for row in sku_product_type_scif.itertuples():
                    sku_product_numerator_result = row.tagged
                    sku_product_denominator_result = row.facings
                    sku_product_id = row.product_fk
                    sku_template_id = row.template_fk

                    self.common.write_to_db_result(kpi_fk, numerator_id=sku_product_id,
                                                   denominator_id=sku_template_id, context_id=sku_product_id,
                                                   numerator_result=sku_product_numerator_result,
                                                   denominator_result=sku_product_denominator_result,by_scene=True)

            other_product_type_scif = self.scif[self.scif.product_type.isin(['Other'])]
            if not other_product_type_scif.empty:
                for row in other_product_type_scif.itertuples():
                    other_product_numerator_result = row.tagged
                    other_product_denominator_result = row.facings
                    other_product_id = row.product_fk
                    other_template_id = row.template_fk

                    self.common.write_to_db_result(kpi_fk, numerator_id=other_product_id,
                                                   denominator_id=other_template_id, context_id=other_product_id,
                                                   numerator_result=other_product_numerator_result,
                                                   denominator_result=other_product_denominator_result, by_scene=True)

            irrelevant_product_type_scif = self.scif[self.scif.product_type.isin(['Irrelevant'])]
            if not irrelevant_product_type_scif.empty:
                for row in irrelevant_product_type_scif.itertuples():
                    irrelevant_product_numerator_result = row.tagged
                    irrelevant_product_denominator_result = row.facings
                    irrelevant_product_id = row.product_fk
                    irrelevant_template_id = row.template_fk

                    self.common.write_to_db_result(kpi_fk, numerator_id=irrelevant_product_id,
                                                   denominator_id=irrelevant_template_id, context_id=irrelevant_product_id,
                                                   numerator_result=irrelevant_product_numerator_result,
                                                   denominator_result=irrelevant_product_denominator_result, by_scene=True)


    def calculate_availability(self):
        scenes_templates_df = self.scif[['scene_fk', 'template_name', ]]
        scenes_templates_df.drop_duplicates(inplace=True)

        self.matches = pd.merge(self.scif, self.matches, how='right', left_on='item_id', right_on='product_fk')
        self.matches['template_name'] = self.matches['template_name'].str.encode('utf-8')

        for i, row in self.templates['Availability'].iterrows():
            kpi_name = row['KPI Name']
            kpi_fk = self.get_kpi_fk_by_kpi_name(kpi_name)
            parent_kpi_name = row['Parent KPI']
            # scene_list = self.matches['scene_fk'].unique().tolist()
            size_filter = row['size']
            size_unit_filter = row['size_unit']
            operation_type = row['operator']

            scene_types = self.split_values(row['template_name'], encode=True)

            filtered_matches = self.matches[(self.matches['template_name'].isin(scene_types))]
            if filtered_matches.empty:
                pass
            else:

                if operation_type == '<':
                    size_matches = filtered_matches[['size']][
                        (filtered_matches['shelf_number'] == 1) &
                        (filtered_matches['product_type'] == 'SKU') &
                        (filtered_matches['size_unit'] == size_unit_filter) &
                        (filtered_matches['size'] < size_filter)]

                if operation_type == '>':
                    size_matches = filtered_matches[['size']][
                        (filtered_matches['shelf_number'] == 1) &
                        (filtered_matches['product_type'] == 'SKU') &
                        (filtered_matches['size_unit'] == size_unit_filter) &
                        (filtered_matches['size'] > size_filter)]

                if size_matches.empty:
                    score = 0
                else:
                    score = 1

                scene = self.scene_info['scene_fk'].iloc[0]
                if not pd.isnull(parent_kpi_name):
                    self.common.write_to_db_result(kpi_fk, numerator_id=self.manufacturer_fk,
                                                   denominator_id=scene, identifier_parent=parent_kpi_name,
                                                   result=score, score=score,
                                                   should_enter=True, by_scene=True)

                else:
                    self.common.write_to_db_result(kpi_fk, numerator_id=self.manufacturer_fk,
                                                   denominator_id=scene,
                                                   result=score, score=score,
                                                   should_enter=True, identifier_result=kpi_name)

    def split_values(self, row, encode=False):
        try:
            split_values = row.split(',')
            if encode == True:
                encoded_list = []
                for item in split_values:
                    encoded_list.append(item.encode('utf-8'))
                    return encoded_list
            else:

                return split_values

        except:
            return []
