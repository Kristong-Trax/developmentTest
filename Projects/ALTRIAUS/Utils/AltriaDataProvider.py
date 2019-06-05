import pandas as pd
from KPIUtils_v2.DB.PsProjectConnector import PSProjectConnector
from Trax.Algo.Calculations.Core.DataProvider import Data
from Trax.Cloud.Services.Connector.Keys import DbUsers

__author__ = 'Hunter'


TOP_LEFT_CORNER = 'Top Left Corner'
BOTTOM_RIGHT_CORNER = 'Bottom Right Corner'


class AltriaDataProvider:

    def __init__(self, data_provider):
        self.data_provider = data_provider
        self.project_name = data_provider.project_name
        self.session_uid = self.data_provider.session_uid
        self.session_fk = self.data_provider.session_id
        self.rds_conn = PSProjectConnector(self.project_name, DbUsers.CalculationEng)
        self.scif = self.data_provider[Data.SCENE_ITEM_FACTS]
        self.scene_fks = self.scif['scene_fk'].unique().tolist()
        self._mdis = pd.DataFrame()

    @property
    def mdis(self):
        if self._mdis.empty:
            self._mdis = self.get_match_display_in_scene()
        return self._mdis

    def get_match_display_in_scene(self):
        query = """select mdis.scene_fk, mdis.display_fk, d.display_name, mdis.rect_x, mdis.rect_y, 
                d.display_brand_fk from probedata.match_display_in_scene mdis
                left join static.display d on mdis.display_fk = d.pk
                where mdis.scene_fk in ({});""".format(','.join([str(x) for x in self.scene_fks]))

        cur = self.rds_conn.db.cursor()
        cur.execute(query)
        res = cur.fetchall()
        df = pd.DataFrame(list(res), columns=['scene_fk', 'display_fk', 'display_name', 'x', 'y', 'display_brand_fk'])
        return df

    def generate_polygon_masks(self):
        top_left_points = self.mdis[self.mdis['display_name'] == TOP_LEFT_CORNER]
        bottom_right = self.mdis[self.mdis['display_name'] == BOTTOM_RIGHT_CORNER]

    def plot_mdis_points(self):
        import matplotlib.pyplot as plt
        top_left_points = self.mdis[self.mdis['display_name'] == TOP_LEFT_CORNER]
        bottom_right = self.mdis[self.mdis['display_name'] == BOTTOM_RIGHT_CORNER]
        plt.plot(top_left_points['x'].tolist(), top_left_points['y'].tolist(), 'bo')
        plt.plot(bottom_right['x'].tolist(), bottom_right['y'].tolist(), 'ro')
        # plt.axis([self.mdis['x'].min(), self.mdis['x'].max(), self.mdis['y'].max(), self.mdis['y'].min()])
        plt.gca().invert_yaxis()
        plt.show()
        return plt
