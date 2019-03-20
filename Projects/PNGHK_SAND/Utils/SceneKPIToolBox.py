import pandas as pd
import os

from Trax.Algo.Calculations.Core.DataProvider import Data
from KPIUtils_v2.Calculations.CalculationsUtils.GENERALToolBoxCalculations import GENERALToolBox
from Projects.PNGHK_SAND.Data.Const import Const
from Trax.Utils.Logging.Logger import Log


__author__ = 'ilays'

PATH = os.path.join(os.path.dirname(os.path.realpath(__file__)), '..', 'Data', '06_PNGHK_template_2019_08_03.xlsx')

class SceneToolBox:

    def __init__(self, data_provider, common):
        self.data_provider = data_provider
        self.common = common
        self.project_name = self.data_provider.project_name
        self.products = self.data_provider[Data.PRODUCTS]
        self.all_products = self.data_provider[Data.ALL_PRODUCTS]
        self.match_product_in_scene = self.data_provider[Data.MATCHES]
        self.scene_info = self.data_provider[Data.SCENES_INFO]
        self.store_id = self.data_provider[Data.STORE_INFO]['store_fk'].values[0]
        self.scif = self.data_provider[Data.SCENE_ITEM_FACTS]
        self.kpi_results_queries = []
        self.df = pd.DataFrame()
        self.tools = GENERALToolBox(data_provider)
        self.osd_rules_sheet = pd.read_excel(PATH, Const.OSD_RULES).fillna("")
        self.store_info = self.data_provider[Data.STORE_INFO]

    def main_calculation(self):
        """
        This function calculates the KPI results.
        """
        df = pd.merge(self.match_product_in_scene, self.products, on="product_fk", how="left")
        distinct_session_fk = self.scif[['scene_fk', 'template_name', 'template_fk']].drop_duplicates()
        df = pd.merge(df, distinct_session_fk, on="scene_fk", how="left")
        self.calculate_osd(df)

    def calculate_osd(self, df):
        if df.empty:
            return
        scene_type = df['template_name'].values[0]
        row = self.find_row_osd(scene_type)
        if row.empty:
            return
        results_list = []
        
        # filter df include OSD when needed
        shelfs_to_include = row[Const.OSD_NUMBER_OF_SHELVES].values[0]
        if row[Const.STORAGE_EXCLUSION_PRICE_TAG].values[0] == Const.NO:
            if shelfs_to_include != "":
                shelfs_to_include = int(shelfs_to_include)
                results_list.append(df[df['shelf_number_from_bottom'] > shelfs_to_include])

        # if no osd rule is applied
        if row[Const.HAS_OSD].values[0] == Const.NO:
            return

        # filter df to have only shelves with given ean code
        if row[Const.HAS_OSD].values[0] == Const.YES:
            products_to_filter = row[Const.POSM_EAN_CODE].values[0].split(",")
            products_df = df[df['product_ean_code'].isin(products_to_filter)][['scene_fk',
                                                                                           'bay_number',
                                                                                           'shelf_number']]
            products_df = products_df.drop_duplicates()
            const_scene_df = df.copy()
            if not products_df.empty:
                for index, p in products_df.iterrows():
                    scene_df = const_scene_df[((const_scene_df['scene_fk'] == p['scene_fk']) &
                                               (const_scene_df['shelf_number'] == p['shelf_number']))]
                    results_list.append(scene_df)
        if len(results_list) == 0:
            return
        kpi_fk = self.common.get_kpi_fk_by_kpi_name(Const.OSD_KPI)
        if kpi_fk is None:
            Log.warning("There is no matching Kpi fk for kpi name: " + Const.OSD_KPI)
            return
        try:
            template_fk = self.scene_info['template_fk'].values[0]
        except:
            template_fk = -1
        self.common.write_to_db_result(fk=kpi_fk, numerator_id=self.store_id, result=1, by_scene=True,
                                       denominator_id=template_fk)
        
    def find_row_osd(self, s):
        rows = self.osd_rules_sheet[self.osd_rules_sheet[Const.SCENE_TYPE] == s]
        row = rows[rows[Const.RETAILER] == self.store_info['retailer_name'].values[0]]
        return row