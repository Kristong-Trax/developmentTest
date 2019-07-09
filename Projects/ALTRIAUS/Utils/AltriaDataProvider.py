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
        self.match_product_in_scene = self.data_provider[Data.MATCHES]

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
                                           bottom_right_display_name=None, y_axis_threshold=40, dropna=True):
        """
        This function takes in an match_product_in_scene (MPIS) dataframe, display name top left/bottom right names,
        and returns a filtered version of MPIS with the dimensions of the polygons each item was found in.
        :param pd.DataFrame match_product_in_scene_df: match_product_in_scene dataframe (can be pre-filtered)
        :param str top_left_display_name: name of the 'Top Left Corner' of the display
        :param str bottom_right_display_name: name of the 'Bottom Right Corner' of the display
        :param int y_axis_threshold: The percentage of the max y-range that the opposite point (e.g. Bottom Right)
        is allowed to be from the anchor point (e.g. Top Let).
        :param bool dropna: Drop MPIS records from the returned dataframe that are not found in any display polygon
        :return: match_product_in_scene (MPIS) dataframe merged with polygon data
        """

        if not top_left_display_name:
            top_left_display_name = TOP_LEFT_CORNER
        if not bottom_right_display_name:
            bottom_right_display_name = BOTTOM_RIGHT_CORNER

        # build a dataframe with all polygon masks that match the display names provided
        polygon_mask_df = self.generate_polygon_masks(top_left_display_name, bottom_right_display_name,
                                                      y_axis_threshold)

        # add the columns from the polygon dataframe to MPIS
        match_product_in_scene_df = \
            match_product_in_scene_df.reindex(columns=match_product_in_scene_df.columns.tolist() + polygon_mask_df.columns.tolist())

        # this isn't very efficient, but it conditionally merges rows from the polygon dataframe
        # to any applicable products/POS items that are inside the polygon
        for item in match_product_in_scene_df.itertuples():
            relevant_polygon_mask_df = polygon_mask_df[(polygon_mask_df['left_bound'] < item.rect_x) &
                                                       (polygon_mask_df['right_bound'] > item.rect_x) &
                                                       (polygon_mask_df['top_bound'] < item.rect_y) &
                                                       (polygon_mask_df['bottom_bound'] > item.rect_y) &
                                                       (polygon_mask_df['scene_fk'] == item.scene_fk)]
            # if the dataframe is empty, there wasn't a match
            if not relevant_polygon_mask_df.empty:
                match_product_in_scene_df.loc[item.Index, polygon_mask_df.columns.tolist()] = \
                    relevant_polygon_mask_df.iloc[0]

        if dropna:
            # get rid of any results that didn't fall into a polygon
            match_product_in_scene_df.dropna(subset=polygon_mask_df.columns.tolist(), inplace=True)

        return match_product_in_scene_df

    def generate_polygon_masks(self, top_left_display_name=TOP_LEFT_CORNER,
                               bottom_right_display_name=BOTTOM_RIGHT_CORNER, y_axis_threshold=40):
        """
        Internal function for generating the polygon masks. Probably shouldn't be called directly unless you have
        a special use case.
        :param top_left_display_name:
        :param bottom_right_display_name:
        :param y_axis_threshold:
        :return:
        """
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
            # we only care about points that are lower and further right than the anchor point
            if anchor_points.equals(top_left_points):
                other_point_domain = opposite_points[(opposite_points['x'] > anchor_point.x) &
                                                     (opposite_points['y'] > anchor_point.y) &
                                                     (opposite_points['y'] < anchor_point.y + y_range)]
            # ... or if the anchor points are the bottom right, we only care about points that are higher
            # and further to the left than the anchor point
            elif anchor_points.equals(bottom_right_points):
                other_point_domain = opposite_points[(opposite_points['x'] < anchor_point.x) &
                                                     (opposite_points['y'] < anchor_point.y) &
                                                     (opposite_points['y'] > anchor_point.y - y_range)]

            if other_point_domain.empty:
                # this shouldn't happen in a perfect world
                continue

            # make a tuple for the static method that gets the closest point
            point = (anchor_point.x, anchor_point.y)
            # get the closet point
            opposite_point = self.get_closest_point(point, other_point_domain)
            opposite_point_x = opposite_point['x'].iloc[0]
            opposite_point_y = opposite_point['y'].iloc[0]

            # append the newly derived polygon to the dataframe to be returned
            polygon_mask_df.loc[len(polygon_mask_df)] = [anchor_point.scene_fk, anchor_point.display_name,
                                                         anchor_point.x, opposite_point_x,
                                                         anchor_point.y, opposite_point_y,
                                                         np.nan, np.nan]
            opposite_points.drop(opposite_point.index, inplace=True)

        # if we used the bottom right points as the anchors, we need to invert the column names
        if anchor_points.equals(bottom_right_points):
            polygon_mask_df.rename(columns={'left_bound': 'right_bound', 'right_bound': 'left_bound',
                                            'top_bound': 'bottom_bound', 'bottom_bound': 'top_bound'}, inplace=True)

        # we need to handle orphaned records now
        if len(opposite_points) > 0 and len(opposite_points) % 2 == 0:
            Log.info('Attempting to pair incorrectly-tagged displays')
            # order points from top -> bottom and left -> right
            opposite_points = opposite_points.sort_values(['y', 'x'], ascending=True).reset_index(drop=True)
            while not opposite_points.empty:
                # get first anchor point (most top, most left of remaining dataframe)
                anchor_point = opposite_points.iloc[0]
                # pop that anchor record so we don't match it to anything else later
                opposite_points.drop(anchor_point.name, inplace=True)

                point = (anchor_point['x'], anchor_point['y'])
                opposite_point = self.get_closest_point(point, opposite_points)
                opposite_point_x = opposite_point['x'].iloc[0]
                opposite_point_y = opposite_point['y'].iloc[0]

                polygon_mask_df.loc[len(polygon_mask_df)] = [anchor_point.scene_fk, anchor_point.display_name,
                                                             anchor_point['x'], opposite_point_x,
                                                             anchor_point['y'], opposite_point_y,
                                                             np.nan, np.nan]
                # pop the opposite point we selected so we don't pair it to anything else
                opposite_points.drop(opposite_point.index, inplace=True)

        # yeah... there's nothing we can do about this
        elif len(opposite_points) > 0 and len(opposite_points) % 2 != 0:
            Log.warning('There are an uneven number of remaining displays, so repair cannot be completed')

        # compute polygon centers
        polygon_mask_df['center_x'] = \
            polygon_mask_df['left_bound'] + (polygon_mask_df['right_bound'] - polygon_mask_df['left_bound']) / 2
        polygon_mask_df['center_y'] = \
            polygon_mask_df['top_bound'] + (polygon_mask_df['bottom_bound'] - polygon_mask_df['top_bound']) / 2

        # make the index pretty and then return
        return polygon_mask_df.reset_index(drop=True)

    @staticmethod
    def get_closest_point(origin_point, other_points_df):
        other_points = other_points_df[['x', 'y']].values
        # Euclidean geometry magic
        distances = np.sum((other_points - origin_point)**2, axis=1)
        # get the shortest hypotenuse
        try:
            closest_point = other_points[np.argmin(distances)]
        except ValueError:
            Log.error('Unable to find a matching opposite point for supplied anchor!')
            return other_points_df
        return other_points_df[(other_points_df['x'] == closest_point[0]) & (other_points_df['y'] == closest_point[1])]

    # functions used for debugging
    def plot_mdis_points(self):
        """
        This function should only be called when debugging locally - it generates a pretty graph that shows all of the
        tagged display corners
        :return:
        """
        import matplotlib.pyplot as plt
        top_left_points = self.mdis[self.mdis['display_name'] == TOP_LEFT_CORNER]
        bottom_right = self.mdis[self.mdis['display_name'] == BOTTOM_RIGHT_CORNER]
        plt.plot(top_left_points['x'].tolist(), top_left_points['y'].tolist(), 'bo')
        plt.plot(bottom_right['x'].tolist(), bottom_right['y'].tolist(), 'ro')
        # plt.axis([self.mdis['x'].min(), self.mdis['x'].max(), self.mdis['y'].max(), self.mdis['y'].min()])
        plt.gca().invert_yaxis()
        plt.show()
        return plt
