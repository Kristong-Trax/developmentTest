from Trax.Algo.Calculations.Core.DataProvider import Data
from Trax.Utils.Logging.Logger import Log
from KPIUtils_v2.Utils.GlobalScripts.Scripts import GlobalSessionToolBox
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
TEMPLATE_PATH = os.path.join(os.path.dirname(os.path.realpath(__file__)), '..', 'Data', 'CCAndinaAR_template_v3.xlsx')

__author__ = 'nicolaske'


class ToolBox(GlobalSessionToolBox):

    def __init__(self, data_provider, output):
        GlobalSessionToolBox.__init__(self, data_provider, output)
        self.templates = {}
        self.scene_kpi_results_fix = {}
        self.scene_kpi_ratio_pass = {}
        self.scenes_result = self.data_provider.scene_kpi_results

    def main_calculation(self):
        sheet_list = pd.read_excel(TEMPLATE_PATH, None).keys()

        for sheet in sheet_list:
            self.templates[sheet] = pd.read_excel(TEMPLATE_PATH, sheet)
            self.templates[sheet] = self.templates[sheet][self.templates[sheet]['Relevance Type'] == 'Session']

        self.calculate_availability()
        self.calculate_bay_count()

    def calculate_availability(self):
        matches_df = pd.merge(self.scif, self.matches, how='right', left_on=['item_id','scene_id'], right_on=['product_fk','scene_fk'])
        matches_df['template_name'] = matches_df['template_name'].str.encode('utf-8')

        for i, row in self.templates['Availability'].iterrows():

            kpi_name = row['KPI Name']
            kpi_fk = self.get_kpi_fk_by_kpi_name(kpi_name)
            child_kpi_name = row['Child KPI']
            parent_kpi_name = row['Parent KPI']
            scene_list = matches_df['scene_id'].unique().tolist()
            size_filter = row['size']
            size_unit_filter = row['size_unit']
            operation_type = row['operator']
            scene_types = self.split_values(row['template_name'], encode=True)

            if not pd.isnull(parent_kpi_name):
                self.scene_kpi_results_fix[kpi_name] = 0
                total_relevant_scenes = 0
                passing_scenes = 0

                for scene in scene_list:

                    filtered_matches = matches_df[(matches_df['template_name'].isin(scene_types)) &
                                                  (matches_df['scene_id'] == scene)]

                    if filtered_matches.empty:
                        pass
                    else:
                        self.scene_kpi_results_fix[kpi_name] = 1
                        filtered_matches.loc[filtered_matches.size_unit == 'l', 'size'] *= 1000
                        filtered_matches.loc[filtered_matches.size_unit == 'l', 'size_unit'] = 'ml'
                        template_fk = filtered_matches['template_fk'].iloc[0]
                        if operation_type == '<':
                            size_matches = filtered_matches[['size']][
                                (filtered_matches['shelf_number'] == 1) &
                                (filtered_matches['product_type'] == 'SKU') &
                                (filtered_matches['size_unit'] == size_unit_filter) &
                                (filtered_matches['size'] >= size_filter)]

                        if operation_type == '>':
                            size_matches = filtered_matches[['size']][
                                (filtered_matches['shelf_number'] == 1) &
                                (filtered_matches['product_type'] == 'SKU') &
                                (filtered_matches['size_unit'] == size_unit_filter) &
                                (filtered_matches['size'] <= size_filter)]

                        if size_matches.empty:
                            score = 1
                            passing_scenes += 1


                        else:
                            score = 0
                            self.scene_kpi_results_fix[kpi_name] = 0

                        if not pd.isnull(parent_kpi_name):
                            self.common.write_to_db_result(kpi_fk, numerator_id=template_fk,
                                                           denominator_id=scene, denominator_result=scene,
                                                           identifier_parent=parent_kpi_name,
                                                           result=score, score=score,
                                                           context_id=template_fk,
                                                           should_enter=True)
                        total_relevant_scenes += 1

                passing_percentage = (round((passing_scenes / float(total_relevant_scenes)), 2) * 100)
                self.scene_kpi_results_fix[kpi_name] = passing_percentage

            if not pd.isnull(child_kpi_name):
                kpi_name = row['KPI Name']
                kpi_fk = self.get_kpi_fk_by_kpi_name(kpi_name)

                if self.scene_kpi_results_fix[child_kpi_name] == 1:
                    score = 100
                else:
                    score = 0

                self.common.write_to_db_result(kpi_fk, numerator_id=self.manufacturer_fk,
                                               denominator_id=self.store_id,
                                               result=passing_percentage, score=score,
                                               should_enter=True, identifier_result=kpi_name)

    def calculate_bay_count(self):
        scenes_templates_df = self.scif[['scene_fk', 'template_name']]
        scenes_templates_df.drop_duplicates(inplace=True)

        matches_df = pd.merge(scenes_templates_df, self.matches, how='right', on='scene_fk')

        for i, row in self.templates['Bay Count'].iterrows():
            kpi_name = row['KPI Name']
            kpi_fk = self.get_kpi_fk_by_kpi_name(kpi_name)
            parent_kpi_name = row['Parent KPI']
            scene_list = matches_df['scene_fk'].unique().tolist()

            scene_types = self.split_values(row['template_name'])
            overall_bay_count = 0

            if not pd.isnull(parent_kpi_name):
                parent_fk = self.get_kpi_fk_by_kpi_name(parent_kpi_name)

            for scene in scene_list:
                filtered_matches = matches_df[
                    (matches_df['scene_fk'] == scene) & (matches_df['template_name'].isin(scene_types))]
                if filtered_matches.empty:
                    pass
                else:
                    template_fk = self.scif['template_fk'][self.scif['scene_id'] == scene ].iloc[0]
                    bays_found = max(filtered_matches['bay_number'].unique().tolist())

                    overall_bay_count += bays_found

                    if not pd.isnull(parent_kpi_name):
                        self.common.write_to_db_result(kpi_fk, numerator_id=self.manufacturer_fk,
                                                       denominator_id=scene, denominator_result=scene,
                                                       identifier_parent=parent_kpi_name,
                                                       result=bays_found, score=bays_found,
                                                       context_id=template_fk,
                                                       should_enter=True)

            if pd.isnull(parent_kpi_name):
                self.common.write_to_db_result(kpi_fk, numerator_id=self.manufacturer_fk,
                                               denominator_id=self.store_id,
                                               result=overall_bay_count, score=overall_bay_count,
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
