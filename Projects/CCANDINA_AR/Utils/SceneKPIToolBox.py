from Trax.Algo.Calculations.Core.DataProvider import Data
from Trax.Utils.Logging.Logger import Log
from KPIUtils_v2.Utils.GlobalScripts.Scripts import GlobalSceneToolBox
import pandas as pd
import os
from Projects.CCANDINA_AR.Data.LocalConsts import Consts

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
TEMPLATE_PATH = os.path.join(os.path.dirname(os.path.realpath(__file__)), '..', 'Data', 'CCAndinaAR_template_v1.xlsx')

__author__ = 'nicolaske'


class ToolBox(GlobalSceneToolBox):

    def __init__(self, data_provider, output):
        GlobalSceneToolBox.__init__(self, data_provider, output)
        self.templates = {}

    def main_calculation(self):
        sheet_list = pd.read_excel(TEMPLATE_PATH, None).keys()

        for sheet in sheet_list:
            self.templates[sheet] = pd.read_excel(TEMPLATE_PATH, sheet)
            self.templates[sheet] = self.templates[sheet][self.templates[sheet]['Relevance Type'] == 'Scene']
        self.calculate_availability()

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
