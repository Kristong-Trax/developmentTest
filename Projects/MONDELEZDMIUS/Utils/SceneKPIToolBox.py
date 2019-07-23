
from Trax.Algo.Calculations.Core.DataProvider import Data
from Trax.Cloud.Services.Connector.Keys import DbUsers
from KPIUtils_v2.DB.PsProjectConnector import PSProjectConnector
from Trax.Algo.Calculations.Core.GraphicalModel.AdjacencyGraphs import AdjacencyGraph

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
        self.store_id = self.data_provider.store_fk
        self.scif = self.data_provider.scene_item_facts
        # self.scif = self.scif[~(self.scif['product_type'] == 'Irrelevant')]
        self.rds_conn = PSProjectConnector(self.project_name, DbUsers.CalculationEng)
        self.kpi_static_data = self.common.get_kpi_static_data()
        self.mdis = self.get_match_display_in_scene()
        self.manufacturer_fk = self.products['manufacturer_fk'][self.products['manufacturer_name'] ==
                                                                'MONDELEZ INTERNATIONAL, INC.'].iloc[0]
        self.vtw_points_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), '..', 'Data',
                                        "VTW_POINTS_SCORE.xlsx")


    def main_calculation(self, *args, **kwargs):
        """
        This function calculates the KPI results.
        """
        # self.calculate_VTW()


    def calculate_VTW(self):
        kpi_fk = self.common.get_kpi_fk_by_kpi_name(Const.VTW_KPI)
        self.points_template = pd.read_excel(self.vtw_points_path)

        for i, row in self.mdis.iterrows():

            try:
                score = self.points_template['score'][self.points_template['display'] == row['display_name']].iloc[0]
            except:
                score = 0
            self.common.write_to_db_result(fk=kpi_fk, numerator_id=self.manufacturer_fk, denominator_id=self.store_id,
                                       result = score, score=score, scene_result_fk = self.scene ,should_enter= True, by_scene = True)





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
        # we need to remove duplicate results
        # this should never happen, but it did...
        df.drop_duplicates(subset=['display_fk', 'x', 'y'], keep='first', inplace=True)
        return df

