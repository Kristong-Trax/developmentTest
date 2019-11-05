from Trax.Algo.Calculations.Core.DataProvider import Data
from Trax.Cloud.Services.Connector.Keys import DbUsers
from KPIUtils_v2.DB.PsProjectConnector import PSProjectConnector
from KPIUtils_v2.Calculations.AssortmentCalculations import Assortment
from Trax.Algo.Calculations.Core.GraphicalModel.AdjacencyGraphs import AdjacencyGraph
import ast
import math
# from Trax.Utils.Logging.Logger import Log
import pandas as pd
from collections import defaultdict
import os

from KPIUtils_v2.DB.CommonV2 import Common
# from KPIUtils_v2.Calculations.AssortmentCalculations import Assortment
# from KPIUtils_v2.Calculations.AvailabilityCalculations import Availability
# from KPIUtils_v2.Calculations.NumberOfScenesCalculations import NumberOfScenes
# from KPIUtils_v2.Calculations.PositionGraphsCalculations import PositionGraphs
# from KPIUtils_v2.Calculations.SOSCalculations import SOS
# from KPIUtils_v2.Calculations.SequenceCalculations import Sequence
# from KPIUtils_v2.Calculations.SurveyCalculations import Survey
from KPIUtils_v2.GlobalDataProvider.PsDataProvider import PsDataProvider
# from KPIUtils_v2.Calculations.CalculationsUtils import GENERALToolBoxCalculations

from Projects.MONDELEZDMIUS.Utils.Const import Const

__author__ = 'nicolaske'


class SceneMONDELEZDMIUSToolBox:

    def __init__(self, data_provider, common, output):
        self.output = output
        self.data_provider = data_provider
        self.common = common
        self.project_name = self.data_provider.project_name
        self.session_uid = self.data_provider.session_uid
        self.session_info = self.data_provider[Data.SESSION_INFO]
        self.scene_info = self.data_provider[Data.SCENES_INFO]
        self.scene = self.scene_info.loc[0, 'scene_fk']
        self.templates = self.data_provider[Data.TEMPLATES]
        self.products = self.data_provider[Data.PRODUCTS]
        self.all_products = self.data_provider[Data.ALL_PRODUCTS]
        self.match_product_in_scene = self.data_provider[Data.MATCHES]

        self.visit_date = self.data_provider[Data.VISIT_DATE]
        self.store_id = self.data_provider[Data.STORE_FK]
        self.store_info = self.data_provider[Data.STORE_INFO]
        self.scif = self.data_provider[Data.SCENE_ITEM_FACTS]
        # self.scif = self.scif[~(self.scif['product_type'] == 'Irrelevant')]
        self.rds_conn = PSProjectConnector(self.project_name, DbUsers.CalculationEng)
        self.kpi_static_data = self.common.get_kpi_static_data()
        self.mdis = self.get_match_display_in_scene()
        self.mpis = self.get_mpis()
        self.manufacturer_fk = self.products['manufacturer_fk'][self.products['manufacturer_name'] ==
                                                                'MONDELEZ INTERNATIONAL, INC.'].iloc[0]
        self.static_task_area_location = self.get_store_task_area()
        # self.vtw_points_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), '..', 'Data',
        #                                 "VTW_POINTS_SCORE.xlsx")

        self.dmi_template = os.path.join(os.path.dirname(os.path.realpath(__file__)), '..', 'Data',
                                         "MondelezDMI_KPITemplatev4.xlsx")
        self.points_template = pd.read_excel(self.dmi_template, sheetname='VTW_POINTS')
        self.goldzone_template = pd.read_excel(self.dmi_template, sheetname='GOLD_ZONE')

        self.assortment = Assortment(self.data_provider, common=self.common)
        # self.ps_data_provider = PsDataProvider(self.data_provider, self.output)
        self.store_areas = self.get_store_area_df()

    def main_calculation(self, *args, **kwargs):
        """
        This function calculates the KPI results.
        """
        self.check_store_id()
        self.calculate_assortment()
        self.calculate_VTW_and_vehicle()
        self.calculate_display_non_scripted()
        self.calculate_location()
        self.calculate_gold_zone()

    def check_store_id(self):
        if type(self.store_id) == type(None):
            try:
                self.store_id = self.store_info['store_fk'].iloc[0]
            except:
                pass

    def calculate_VTW_and_vehicle(self):
        vtw_kpi_fk = self.common.get_kpi_fk_by_kpi_name(Const.VTW_KPI)
        vehicle_kpi_fk = self.common.get_kpi_fk_by_kpi_name(Const.DISPLAY_VEHICLE_KPI)
        score = 0

        if not self.mdis.empty:
            merged_mdis = self.mdis.merge(self.points_template, how='left', left_on='display_name', right_on='display')
            not_allowed_duplicate_df = merged_mdis[merged_mdis['multiple'] == 'n']
            not_allowed_duplicate_df.drop_duplicates(subset='display_name', keep='first', inplace=True)
            allowed_duplicate_df = merged_mdis[merged_mdis['multiple'] == 'y']
            fixed_df = pd.concat([not_allowed_duplicate_df, allowed_duplicate_df])
            score = int(fixed_df['score'].sum())

            pallets_tagged_df = fixed_df[fixed_df['display'] == 'Pallet - Full - PRD']

            if not pallets_tagged_df.empty:
                pallets_tagged = len(pallets_tagged_df)
                pallet_current_score = pallets_tagged * 16
                pallet_actual_score = math.ceil(pallets_tagged / 4.0) * 16
                score = score - ( pallet_current_score - pallet_actual_score)



            self.common.write_to_db_result(fk=vtw_kpi_fk, numerator_id=self.manufacturer_fk, numerator_result=score,
                                           denominator_id=self.store_id, denominator_result=1,
                                           result=score, score=score, scene_result_fk=self.scene, should_enter=True,
                                           by_scene=True)


            # Using unique display df to iterate, but using duplicates for pallet logic
            unique_display_df = fixed_df[['display_name', 'display_fk']].drop_duplicates(subset='display_name', keep='first')
            try:
                for row in unique_display_df.itertuples():
                    vehicle_display_fk = row.display_fk

                    multiple = \
                        self.points_template['multiple'][self.points_template['display'] == row.display_name].iloc[0]

                    display_point = int(
                        self.points_template['score'][self.points_template['display'] == row.display_name].iloc[0])

                    if row.display_name.__contains__('Pallet') and multiple == 'y':
                        pallet_tagged = len(fixed_df[fixed_df['display_name'] == row.display_name])
                        display_count = math.ceil((pallet_tagged / 4.0))
                    elif multiple == 'y':
                        display_count = len(fixed_df[fixed_df['display_name'] == row.display_name])
                    else:
                        display_count = 1
                    vehicle_score = display_count * display_point
                    self.common.write_to_db_result(fk=vehicle_kpi_fk, numerator_id=vehicle_display_fk,
                                                   numerator_result=display_count,
                                                   denominator_id=self.store_id, denominator_result=1,
                                                   result=Const.RESULT_YES, score=vehicle_score,
                                                   scene_result_fk=self.scene,
                                                   should_enter=True,
                                                   by_scene=True)
            except:
                pass

    def calculate_gold_zone(self):
        result = Const.RESULT_YES

        kpi_fk = self.common.get_kpi_fk_by_kpi_name(Const.GOLDZONE_KPI)

        store_area_df_filtered = self.store_areas[self.store_areas['scene_fk'] == self.scene]
        if not store_area_df_filtered.empty:
            store_area_name = store_area_df_filtered['store_area_name'].iloc[0]
            if store_area_name in self.goldzone_template.location.unique().tolist():
                # store_area_pk = store_area_df_filtered['pk'].iloc[0]
                score = 1
                result = Const.RESULT_YES

            else:
                # store_area_pk = 0
                score = 0
                result = Const.RESULT_NO

            self.common.write_to_db_result(fk=kpi_fk, numerator_id=self.manufacturer_fk, numerator_result=score,
                                           denominator_id=self.store_id, denominator_result=1,
                                           result=result, score=score, scene_result_fk=self.scene, should_enter=True,
                                           by_scene=True)

    def calculate_display_non_scripted(self):
        score = 1
        result = Const.RESULT_YES
        kpi_fk = self.common.get_kpi_fk_by_kpi_name(Const.NON_SCRIPTED_KPI)

        FD_kpi_kpi = self.common.get_kpi_fk_by_kpi_name(Const.FD_COMPLIANCE_KPI)
        scripted_kpi = self.common.get_kpi_fk_by_kpi_name(Const.SCRIPTED_COMPLIANCE_KPI)
        if self.store_assortment.empty:
            self.common.write_to_db_result(fk=kpi_fk, numerator_id=self.manufacturer_fk, numerator_result=score,
                                           denominator_id=self.store_id, denominator_result=1,
                                           result=result, score=score, scene_result_fk=self.scene,
                                           should_enter=True,
                                           by_scene=True)

        else:
            filtered_assortment = self.store_assortment[(self.store_assortment['kpi_fk_lvl3'] == FD_kpi_kpi) |
                                                        (self.store_assortment['kpi_fk_lvl3'] == scripted_kpi)]
            if not filtered_assortment.empty:
                for i, row in filtered_assortment.iterrows():

                    if Const.PPG_COLUMN_NAME in filtered_assortment.columns.to_list():
                        ppg = row[Const.PPG_COLUMN_NAME]

                        if not pd.isnull(ppg):
                            filtered_scif_count = len(self.scif[self.scif['PPG'] == ppg])
                            if filtered_scif_count > 0:
                                score = 0
                                result = Const.RESULT_NO
                                break

                    if Const.SUB_PPG_COLUMN_NAME in filtered_assortment.columns.to_list():
                        sub_ppg = row[Const.SUB_PPG_COLUMN_NAME]

                        if not pd.isnull(sub_ppg):
                            filtered_scif_count = len(self.scif[self.scif['Sub PPG'] == sub_ppg])
                            if filtered_scif_count > 0:
                                score = 0
                                result = Const.RESULT_NO
                                break

                self.common.write_to_db_result(fk=kpi_fk, numerator_id=self.manufacturer_fk, numerator_result=score,
                                               denominator_id=self.store_id, denominator_result=1,
                                               result=result, score=score, scene_result_fk=self.scene,
                                               should_enter=True,
                                               by_scene=True)

    def calculate_location(self):
        result = Const.RESULT_YES

        kpi_fk = self.common.get_kpi_fk_by_kpi_name(Const.DISPLAY_LOCATION_KPI)

        store_area_df_filtered = self.store_areas[self.store_areas['scene_fk'] == self.scene]
        if not store_area_df_filtered.empty:
            storea_area_pk = store_area_df_filtered['pk'].iloc[0]

            self.common.write_to_db_result(fk=kpi_fk, numerator_id=storea_area_pk, numerator_result=1,
                                           denominator_id=self.store_id, denominator_result=1,
                                           result=result, score=1, scene_result_fk=self.scene, should_enter=True,
                                           by_scene=True)

    def get_match_display_in_scene(self):
        query = """select mdis.scene_fk, mdis.display_fk, d.display_name, mdis.rect_x, mdis.rect_y, 
                d.display_brand_fk from probedata.match_display_in_scene mdis
                left join static.display d on mdis.display_fk = d.pk
                 where mdis.scene_fk in ({});""" \
            .format(self.scene)

        # .format(','.join([str(x) for x in self.scif['scene_fk'].unique().tolist()]))

        cur = self.rds_conn.db.cursor()
        cur.execute(query)
        res = cur.fetchall()
        df = pd.DataFrame(list(res), columns=['scene_fk', 'display_fk', 'display_name', 'x', 'y', 'display_brand_fk'])
        df.drop_duplicates(subset=['display_fk', 'x', 'y'], keep='first', inplace=True)
        return df

    def get_store_task_area(self):
        query = """SELECT * FROM static.store_task_area_group_items
                    where is_used = 1;"""

        cur = self.rds_conn.db.cursor()
        cur.execute(query)
        res = cur.fetchall()
        df = pd.DataFrame(list(res))
        return df

    def get_mpis(self):
        if self.scif.empty:
            return None
        else:
            mpis = self.match_product_in_scene.merge(self.products, on='product_fk', suffixes=['', '_p']) \
                .merge(self.scene_info, on='scene_fk', suffixes=['', '_s']) \
                .merge(self.templates, on=Const.template_fk, suffixes=['', '_t'])
            return mpis

    def calculate_assortment(self):
        self.store_assortment = self.assortment.store_assortment
        self.store_assortment.reset_index(inplace=True)
        if self.store_assortment.empty:
            return
        else:
            ppg_list = [ast.literal_eval(x) for x in self.store_assortment['additional_attributes'].to_list() if x]
            # ppg_list['MONDELEZ BRAND NAME = PPG'] = ppg_list['MONDELEZ BRAND NAME = PPG'].str[1:]
            # ppg_list['MONDELEZ Sub Brand = Sub PPG'] = ppg_list['MONDELEZ Sub Brand = Sub PPG'].str[1:]
            df_assortment = pd.DataFrame(ppg_list)
            merged_assortment = self.store_assortment.join(df_assortment)
            self.store_assortment = merged_assortment

    def get_store_area_df(self):
        query = """
                select st.pk, sst.scene_fk, st.name, sc.session_uid from probedata.scene_store_task_area_group_items sst
                join static.store_task_area_group_items st on st.pk=sst.store_task_area_group_item_fk
                join probedata.scene sc on sc.pk=sst.scene_fk
                where sc.delete_time is null and sc.session_uid = '{}';
                """.format(self.session_uid)

        cur = self.rds_conn.db.cursor()
        cur.execute(query)
        res = cur.fetchall()

        df = pd.DataFrame(list(res), columns=['pk', 'scene_fk', 'store_area_name', 'session_uid'])

        return df
