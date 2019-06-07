import pandas as pd
import numpy as np
from KPIUtils_v2.DB.PsProjectConnector import PSProjectConnector
from Trax.Algo.Calculations.Core.DataProvider import Data
from Trax.Cloud.Services.Connector.Keys import DbUsers
from Trax.Utils.Logging.Logger import Log

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
        a = self.generate_polygon_masks()
        self.match_product_in_scene = self.data_provider[Data.MATCHES]
        a = self.get_products_contained_in_displays(self.match_product_in_scene)

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

    def get_products_contained_in_displays(self, match_product_in_scene_df, top_left_display_name=None,
                                           bottom_right_display_name=None, y_axis_threshold=50, dropna=True):
        if not top_left_display_name:
            top_left_display_name = TOP_LEFT_CORNER
        if not bottom_right_display_name:
            bottom_right_display_name = BOTTOM_RIGHT_CORNER

        polygon_mask_df = self.generate_polygon_masks(top_left_display_name, bottom_right_display_name,
                                                      y_axis_threshold)

        match_product_in_scene_df = \
            match_product_in_scene_df.reindex(columns=match_product_in_scene_df.columns.tolist() + polygon_mask_df.columns.tolist())

        for item in match_product_in_scene_df.itertuples():
            relevant_polygon_mask_df = polygon_mask_df[(polygon_mask_df['left_bound'] < item.rect_x) &
                                                       (polygon_mask_df['right_bound'] > item.rect_x) &
                                                       (polygon_mask_df['top_bound'] < item.rect_y) &
                                                       (polygon_mask_df['bottom_bound'] > item.rect_y) &
                                                       (polygon_mask_df['scene_fk'] == item.scene_fk)]
            if not relevant_polygon_mask_df.empty:
                match_product_in_scene_df.loc[item.Index, polygon_mask_df.columns.tolist()] = \
                    relevant_polygon_mask_df.iloc[0]

        if dropna:
            match_product_in_scene_df.dropna(subset=polygon_mask_df.columns.tolist(), inplace=True)

        return match_product_in_scene_df

    def generate_polygon_masks(self, top_left_display_name=TOP_LEFT_CORNER,
                               bottom_right_display_name=BOTTOM_RIGHT_CORNER, y_axis_threshold=50):
        polygon_mask_df = pd.DataFrame(columns=['scene_fk', 'display_name', 'left_bound', 'right_bound',
                                                'top_bound', 'bottom_bound', 'center_x', 'center_y'])
        top_left_points = self.mdis[self.mdis['display_name'] == top_left_display_name].reset_index(drop=True)
        bottom_right_points = self.mdis[self.mdis['display_name'] == bottom_right_display_name].reset_index(drop=True)

        # we need to make sure there aren't any orphaned tags
        if len(top_left_points) != len(bottom_right_points):
            Log.warning('The number of top left and bottom right corners are not equal!')
            # if there are orphaned tags, we should use the set with the fewest points as the anchor points set
            if len(top_left_points) < len(bottom_right_points):
                anchor_points = top_left_points
                opposite_points = bottom_right_points
            elif len(top_left_points) > len(bottom_right_points):
                anchor_points = bottom_right_points
                opposite_points = top_left_points
        # if there aren't any orphaned tags, we will use top left points and work down and to the right
        else:
            anchor_points = top_left_points
            opposite_points = bottom_right_points

        # since the x and y axes aren't normalized to scale, we need a threshold to prevent incorrect pairs
        y_min = top_left_points['y'].min()
        y_max = bottom_right_points['y'].max()
        y_range = (y_max - y_min) * y_axis_threshold / 100

        # pair every anchor point with the closest opposite point
        for anchor_point in anchor_points.itertuples():
            if anchor_points.equals(top_left_points):
                other_point_domain = opposite_points[(opposite_points['x'] > anchor_point.x) &
                                                     (opposite_points['y'] > anchor_point.y) &
                                                     (opposite_points['y'] < anchor_point.y + y_range)]
            elif anchor_points.equals(bottom_right_points):
                other_point_domain = opposite_points[(opposite_points['x'] < anchor_point.x) &
                                                     (opposite_points['y'] < anchor_point.y) &
                                                     (opposite_points['y'] > anchor_point.y - y_range)]
            point = (anchor_point.x, anchor_point.y)
            opposite_point = self.get_closest_point(point, other_point_domain)
            opposite_point_x = opposite_point['x'].iloc[0]
            opposite_point_y = opposite_point['y'].iloc[0]

            polygon_mask_df.loc[len(polygon_mask_df)] = [anchor_point.scene_fk, anchor_point.display_name,
                                                         anchor_point.x, opposite_point_x,
                                                         anchor_point.y, opposite_point_y,
                                                         np.nan, np.nan]
            opposite_points.drop(opposite_point.index, inplace=True)

        if len(opposite_points) > 0 and len(opposite_points) % 2 == 0:
            Log.info('Attempting to pair incorrectly-tagged displays')
            opposite_points = opposite_points.sort_values(['y', 'x'], ascending=True).reset_index(drop=True)
            while not opposite_points.empty:
                anchor_point = opposite_points.iloc[0]
                opposite_points.drop(anchor_point.name, inplace=True)

                point = (anchor_point['x'], anchor_point['y'])
                opposite_point = self.get_closest_point(point, opposite_points)
                opposite_point_x = opposite_point['x'].iloc[0]
                opposite_point_y = opposite_point['y'].iloc[0]

                polygon_mask_df.loc[len(polygon_mask_df)] = [anchor_point.scene_fk, anchor_point.display_name,
                                                             anchor_point['x'], opposite_point_x,
                                                             anchor_point['y'], opposite_point_y,
                                                             np.nan, np.nan]
                opposite_points.drop(opposite_point.index, inplace=True)

        elif len(opposite_points) > 0 and len(opposite_points) % 2 != 0:
            Log.warning('There are an uneven number of remaining displays, so repair cannot be completed')

        polygon_mask_df['center_x'] = \
            polygon_mask_df['left_bound'] + (polygon_mask_df['right_bound'] - polygon_mask_df['left_bound']) / 2
        polygon_mask_df['center_y'] = \
            polygon_mask_df['top_bound'] + (polygon_mask_df['bottom_bound'] - polygon_mask_df['top_bound']) / 2

        return polygon_mask_df.reset_index(drop=True)

    @staticmethod
    def get_closest_point(origin_point, other_points_df):
        other_points = other_points_df[['x', 'y']].values
        distances = np.sum((other_points - origin_point)**2, axis=1)
        closest_point = other_points[np.argmin(distances)]
        return other_points_df[(other_points_df['x'] == closest_point[0]) & (other_points_df['y'] == closest_point[1])]

    # functions used for debugging
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
