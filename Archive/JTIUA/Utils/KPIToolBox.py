
import os
import numpy as np
import pandas as pd
from datetime import datetime

from Trax.Algo.Calculations.Core.DataProvider import Data
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
from Trax.Utils.Conf.Keys import DbUsers
from KPIUtils_v2.DB.PsProjectConnector import PSProjectConnector
from Trax.Utils.Logging.Logger import Log
from Trax.Data.Utils.MySQLservices import get_table_insertion_query as insert

from Projects.JTIUA.Utils.Fetcher import JTIUAQueries
from Projects.JTIUA.Utils.GeneralToolBox import JTIUAGENERALToolBox
from Projects.JTIUA.Utils.ParseTemplates import parse_template

TEMPLATE_PATH = os.path.join(os.path.dirname(os.path.realpath(__file__)), '..', 'Data', 'Template.xlsx')

__author__ = 'Nimrod'

KPI_RESULT = 'report.kpi_results'
KPK_RESULT = 'report.kpk_results'
KPS_RESULT = 'report.kps_results'


def log_runtime(description, log_start=False):
    def decorator(func):
        def wrapper(*args, **kwargs):
            calc_start_time = datetime.utcnow()
            if log_start:
                Log.info('{} started at {}'.format(description, calc_start_time))
            result = func(*args, **kwargs)
            calc_end_time = datetime.utcnow()
            Log.info('{} took {}'.format(description, calc_end_time - calc_start_time))
            return result
        return wrapper
    return decorator


class JTIUAConsts(object):

    KIOSK = 'Kiosk'
    STORE_FRONT = 'Hot Spot of Store (PANORAMA)'
    STORE_BACK = 'Store (BACKWALL)'
    HOTSPOT = 'Hot Spot'
    HOTSPOT_RANGE = (500, 1000, 100, 1000)    # Top, Right, Bottom, Left in MM

    KPI_NAME = 'KPI Name'
    KPI_GROUP = 'KPI Group'
    INCLUDE_SCENE_TYPES = 'Scene Types to Include'
    EXCLUDE_SCENE_TYPES = 'Scene Types to Exclude'
    KPI_TYPE = 'KPI Type'
    MANUFACTURER = 'Manufacturer'
    WEIGHT = 'Weight'

    SCENE_TYPE = 'Scene Type'
    DISPLAY_TYPE = 'Display Type'
    DISPLAY_BRAND = 'Display Brand'
    DISPLAY_NAME = 'Display Name'
    NEAR_WINDOW = 'In a HS near window'
    IN_HS = 'In a HS'
    NOT_IN_HS = 'Not in a HS'

    SHARE_OF_HOTSPOT = 'Share of Hotspot'
    SHARE_OF_POSM = 'Share of POSM'
    LINEAR_SOS = 'Share of Shelf Linear'

    SEPARATOR = ','


class JTIUAToolBox:

    LEVEL1 = 1
    LEVEL2 = 2
    LEVEL3 = 3

    def __init__(self, data_provider, output):
        self.k_engine = BaseCalculationsScript(data_provider, output)
        self.output = output
        self.data_provider = data_provider
        self.project_name = self.data_provider.project_name
        self.session_uid = self.data_provider.session_uid
        self.products = self.data_provider[Data.PRODUCTS]
        self.all_products = self.data_provider[Data.ALL_PRODUCTS]
        self.visit_date = self.data_provider[Data.VISIT_DATE]
        self.session_info = self.data_provider[Data.SESSION_INFO]
        self.store_id = self.data_provider[Data.STORE_FK]
        store_type = self.data_provider[Data.STORE_INFO]['store_type'].values[0]
        self.store_type = '' if not store_type else store_type
        self.scif = self.data_provider[Data.SCENE_ITEM_FACTS]
        self.rds_conn = PSProjectConnector(self.project_name, DbUsers.CalculationEng)
        self.tools = JTIUAGENERALToolBox(self.data_provider, self.output, rds_conn=self.rds_conn)
        self.matches = self.tools.match_product_in_scene
        self.scene_info = self.tools.scenes_info
        self.match_display = self.get_match_display()
        self.template_data = parse_template(TEMPLATE_PATH, 'KPIs')
        self.posm_template = parse_template(TEMPLATE_PATH, 'POSM',
                                            columns_for_vertical_padding=[JTIUAConsts.SCENE_TYPE,
                                                                          JTIUAConsts.DISPLAY_TYPE,
                                                                          JTIUAConsts.DISPLAY_BRAND])
        self.kpi_static_data = self.get_kpi_static_data()
        self.hotspots = self.calculate_hotspot_range()
        self.posm_scoring = self.calculate_posm_scoring()
        self.kpi_results_queries = []

    def get_match_display(self):
        """
        This function extracts the display matches data and saves it into one global data frame.
        The data is taken from probedata.match_display_in_scene.
        """
        query = JTIUAQueries.get_match_display(self.session_uid)
        match_display = pd.read_sql_query(query, self.rds_conn.db)
        match_display = match_display.merge(self.scene_info, on='scene_fk', how='left')
        return match_display

    def get_kpi_static_data(self):
        """
        This function extracts the static KPI data and saves it into one global data frame.
        The data is taken from static.kpi / static.atomic_kpi / static.kpi_set.
        """
        query = JTIUAQueries.get_all_kpi_data()
        kpi_static_data = pd.read_sql_query(query, self.rds_conn.db)
        return kpi_static_data

    def main_calculation(self):
        """
        This function calculates the KPI results.
        """
        main_score = 0
        main_set = self.template_data[~self.template_data[JTIUAConsts.KPI_GROUP].apply(bool)].iloc[0]
        pillar_data = self.template_data[self.template_data[JTIUAConsts.KPI_GROUP] == main_set[JTIUAConsts.KPI_NAME]]
        for x, pillar in pillar_data.iterrows():
            kpi_data = self.template_data[self.template_data[JTIUAConsts.KPI_GROUP] == pillar[JTIUAConsts.KPI_NAME]]
            pillar_score = 0
            for y, params in kpi_data.iterrows():
                kpi_type = params[JTIUAConsts.KPI_TYPE]
                filters = self.get_filters(params)
                if kpi_type == JTIUAConsts.LINEAR_SOS:
                    sos_filters = {'manufacturer_name': filters.pop('manufacturer_name')}
                    kpi_score = self.tools.calculate_linear_share_of_shelf(sos_filters, self.tools.INCLUDE_EMPTY,
                                                                           **filters)
                elif kpi_type == JTIUAConsts.SHARE_OF_HOTSPOT:
                    kpi_score = self.calculate_linear_sos_within_hotspot(filters)
                elif kpi_type == JTIUAConsts.SHARE_OF_POSM:
                    numerator = self.posm_scoring.get(filters['manufacturer_name'][0], 0)
                    denominator = sum(self.posm_scoring.values())
                    kpi_score = 0 if not denominator else numerator / float(denominator)
                else:
                    Log.warning("KPI type '{}' is not supported".format(kpi_type))
                    continue
                kpi_score *= 100
                kpi_weight = float(params[JTIUAConsts.WEIGHT])
                pillar_score += kpi_score * kpi_weight
                kpi_fk = self.kpi_static_data[self.kpi_static_data['atomic_kpi_name'] ==
                                              params[JTIUAConsts.KPI_NAME]]['atomic_kpi_fk'].values[0]
                self.write_to_db_result(kpi_fk, kpi_score, level=self.LEVEL3)
            pillar_weight = float(pillar[JTIUAConsts.WEIGHT])
            main_score += pillar_score * pillar_weight
            pillar_fk = self.kpi_static_data[self.kpi_static_data['kpi_name'] ==
                                             pillar[JTIUAConsts.KPI_NAME]]['kpi_fk'].values[0]
            self.write_to_db_result(pillar_fk, pillar_score, level=self.LEVEL2)
        main_fk = self.kpi_static_data[self.kpi_static_data['kpi_set_name'] ==
                                       main_set[JTIUAConsts.KPI_NAME]]['kpi_set_fk'].values[0]
        self.write_to_db_result(main_fk, main_score, level=self.LEVEL1)

    def get_filters(self, params):
        """
        This function extracts the relevant manufacturers and scene types for a KPI.
        """
        filters = {}
        if params[JTIUAConsts.INCLUDE_SCENE_TYPES]:
            filters['template_name'] = params[JTIUAConsts.INCLUDE_SCENE_TYPES].split(JTIUAConsts.SEPARATOR)
        elif params[JTIUAConsts.EXCLUDE_SCENE_TYPES]:
            filters['template_name'] = (params[JTIUAConsts.EXCLUDE_SCENE_TYPES].split(JTIUAConsts.SEPARATOR),
                                        self.tools.EXCLUDE_FILTER)
        if params[JTIUAConsts.MANUFACTURER]:
            filters['manufacturer_name'] = params[JTIUAConsts.MANUFACTURER].split(JTIUAConsts.SEPARATOR)
        filters['product_type'] = ('Irrelevant', self.tools.EXCLUDE_FILTER)
        return filters

    def calculate_posm_scoring(self):
        """
        This runs once per session - it checks every relevant POSM and calculates its score based on its manufacturer
        and position in store. Later it returns to total POSM score per manufacturer.
        """
        posm_scoring = {}
        match_display = self.match_display[self.match_display['display_type'].isin(
            self.posm_template[JTIUAConsts.DISPLAY_TYPE].unique().tolist())]
        for x, posm in match_display.iterrows():
            posm_data = self.posm_template[(self.posm_template[JTIUAConsts.SCENE_TYPE] == posm['template_name']) &
                                           (self.posm_template[JTIUAConsts.DISPLAY_BRAND] == posm['manufacturer']) &
                                           (self.posm_template[JTIUAConsts.DISPLAY_NAME].str.contains(posm['display_name'], regex=False))]
            if not posm_data.empty:
                posm_data = posm_data.iloc[0]
                posm_manufacturer = posm['manufacturer']
                if posm_manufacturer not in posm_scoring.keys():
                    posm_scoring[posm_manufacturer] = 0
                hotspot_data = self.hotspots.get(posm['scene_id'])
                if hotspot_data and \
                    hotspot_data[3] <= posm['rect_x'] <= hotspot_data[1] and \
                                        hotspot_data[0] <= posm['rect_y'] <= hotspot_data[2]:
                    posm_scoring[posm_manufacturer] += float(posm_data[JTIUAConsts.IN_HS])
                else:
                    posm_scoring[posm_manufacturer] += float(posm_data[JTIUAConsts.NOT_IN_HS])
        return posm_scoring

    def calculate_linear_sos_within_hotspot(self, filters):
        """
        This function calculates a manufacturer/s' linear share of shelf, only for SKUs located in a hot-spot.
        """
        relevant_scenes = self.scene_info[self.scene_info['template_name'].isin(filters['template_name'])]['scene_fk']
        total_shelf_width = 0
        manufacturer_shelf_width = 0
        for scene in set(relevant_scenes):
            hotspot_data = self.hotspots.get(scene)
            if hotspot_data:
                matches_for_scene = self.matches[(self.matches['scene_fk'] == scene) &
                                                 (self.matches['rect_x'].between(hotspot_data[3], hotspot_data[1])) &
                                                 (self.matches['rect_y'].between(hotspot_data[0], hotspot_data[2])) &
                                                 (~self.matches['product_type'].isin(['Irrelevant']))]
                total_shelf_width += matches_for_scene['width_mm_advance'].sum()
                manufacturer_shelf_width += matches_for_scene[matches_for_scene['manufacturer_name'].isin(
                    filters['manufacturer_name'])]['width_mm_advance'].sum()
        return 0 if not total_shelf_width else manufacturer_shelf_width / float(total_shelf_width)

    def calculate_hotspot_range(self):
        """
        This function calculates the the hot-spot area in every scene (if a hot-spot is indeed tagged),
        and returns a dictionary per scene ID.
        """
        hotspots = {}
        top, right, bottom, left = JTIUAConsts.HOTSPOT_RANGE
        if self.store_type.endswith(JTIUAConsts.KIOSK):
            relevant_scenes = self.scene_info[self.scene_info['template_name'] == JTIUAConsts.KIOSK]
            if not relevant_scenes.empty:
                for scene in relevant_scenes['scene_fk'].unique():
                    pixel_per_mm = self.get_pixel_per_mm(scene)
                    hotspot_data = self.match_display[(self.match_display['scene_id'] == scene) &
                                                      (self.match_display['display_type'] == JTIUAConsts.HOTSPOT)]
                    if not hotspot_data.empty:
                        hotspot = hotspot_data.sort_values(by=['rect_x'], ascending=True).iloc[0]
                    else:
                        rect_x, rect_y = self.get_center_of_scene(scene)
                        hotspot = dict(rect_x=rect_x, rect_y=rect_y)
                    hotspots[scene] = (hotspot['rect_y'] - top * pixel_per_mm,
                                       hotspot['rect_x'] + right * pixel_per_mm,
                                       hotspot['rect_y'] + bottom * pixel_per_mm,
                                       hotspot['rect_x'] - left * pixel_per_mm)
        else:
            front_scenes = self.scene_info[self.scene_info['template_name'] == JTIUAConsts.STORE_FRONT]
            back_scenes = self.scene_info[self.scene_info['template_name'] == JTIUAConsts.STORE_BACK]
            if len(back_scenes) == 1:
                back_scene = back_scenes.iloc[0]['scene_fk']
                pixel_per_mm = self.get_pixel_per_mm(back_scene)
                if len(front_scenes) == 1:
                    front_scene = front_scenes.iloc[0]['scene_fk']
                    hotspot_data = self.match_display[(self.match_display['scene_id'] == front_scene) &
                                                      (self.match_display['display_type'] == JTIUAConsts.HOTSPOT)]
                    if not hotspot_data.empty:
                        hotspot = hotspot_data.sort_values(by=['rect_x'], ascending=True).iloc[0]
                        bay_number = hotspot['bay_number']
                        shelf_number = self.matches[self.matches['scene_fk'] == front_scene]['shelf_number'].max()
                        x_position, y_position = self.get_center_of_shelf_and_bay(back_scene, bay_number, shelf_number)
                    else:
                        x_position, y_position = self.get_center_of_scene(back_scene)
                else:
                    x_position, y_position = self.get_center_of_scene(back_scene)
                hotspots[back_scene] = (y_position - top * pixel_per_mm,
                                        x_position + right * pixel_per_mm,
                                        y_position + bottom * pixel_per_mm,
                                        x_position - left * pixel_per_mm)
        return hotspots

    def get_center_of_scene(self, scene):
        """
        This function returns the center point of a scene,
        based on its highest rect_x and rect_y values of the tagged SKUs.
        """
        Log.info("Setting the center of scene '{}' as a HS".format(scene))
        matches = self.matches[self.matches['scene_fk'] == scene]
        center_x = np.mean(matches['rect_x'].min(), matches['rect_x'].max())
        center_y = np.mean(matches['rect_y'].min(), matches['rect_y'].max())
        return center_x, center_y

    def get_center_of_shelf_and_bay(self, scene, bay, shelf):
        """
        This function returns the center point of a shelf and a bay in a given scene. This is calculated by the average
         between the far-left edge of the first SKU and the far-right edge of the last SKU.
        """
        matches = self.matches[(self.matches['scene_fk'] == scene) & (self.matches['bay_number'] == bay) &
                               (self.matches['shelf_number'] == shelf)]
        center_x = matches['rect_x'].mean()
        center_y = matches['rect_y'].mean()
        return center_x, center_y

    def get_pixel_per_mm(self, scene_id):
        """
        This function calculates the ration between a pixel and a millimeter in a scene.
        """
        facings = self.matches[(self.matches['scene_fk'] == scene_id) &
                               (~self.matches['product_type'].isin(['Other', 'Empty', 'Irrelevant']))]
        facings = facings.sort_values(by=['rect_x'], ascending=True)
        conversion = (facings.iloc[-1]['rect_x'] - facings.iloc[0]['rect_x']) / float(facings.iloc[-1]['x_mm'] - facings.iloc[0]['x_mm'])
        conversion *= facings.iloc[-1]['width_mm'] / float(facings.iloc[-1]['width_mm_advance'])
        return conversion

    def write_to_db_result(self, fk, score, level):
        """
        This function creates the result data frame of every KPI (atomic KPI/KPI/KPI set),
        and appends the insert SQL query into the queries' list, later to be written to the DB.
        """
        attributes = self.create_attributes_dict(fk, score, level)
        if level == self.LEVEL1:
            table = KPS_RESULT
        elif level == self.LEVEL2:
            table = KPK_RESULT
        elif level == self.LEVEL3:
            table = KPI_RESULT
        else:
            return
        query = insert(attributes, table)
        self.kpi_results_queries.append(query)

    def create_attributes_dict(self, fk, score, level):
        """
        This function creates a data frame with all attributes needed for saving in KPI results tables.

        """
        if level == self.LEVEL1:
            kpi_set_name = self.kpi_static_data[self.kpi_static_data['kpi_set_fk'] == fk]['kpi_set_name'].values[0]
            attributes = pd.DataFrame([(kpi_set_name, self.session_uid, self.store_id, self.visit_date.isoformat(),
                                        format(score, '.2f'), fk)],
                                      columns=['kps_name', 'session_uid', 'store_fk', 'visit_date', 'score_1',
                                               'kpi_set_fk'])
        elif level == self.LEVEL2:
            kpi_name = self.kpi_static_data[self.kpi_static_data['kpi_fk'] == fk]['kpi_name'].values[0]
            attributes = pd.DataFrame([(self.session_uid, self.store_id, self.visit_date.isoformat(),
                                        fk, kpi_name, score)],
                                      columns=['session_uid', 'store_fk', 'visit_date', 'kpi_fk', 'kpk_name', 'score'])
        elif level == self.LEVEL3:
            score = round(score, 2)
            data = self.kpi_static_data[self.kpi_static_data['atomic_kpi_fk'] == fk]
            atomic_kpi_name = data['atomic_kpi_name'].values[0]
            kpi_fk = data['kpi_fk'].values[0]
            kpi_set_name = self.kpi_static_data[self.kpi_static_data['atomic_kpi_fk'] == fk]['kpi_set_name'].values[0]
            attributes = pd.DataFrame([(atomic_kpi_name, self.session_uid, kpi_set_name, self.store_id,
                                        self.visit_date.isoformat(), datetime.utcnow().isoformat(),
                                        score, kpi_fk, fk)],
                                      columns=['display_text', 'session_uid', 'kps_name', 'store_fk', 'visit_date',
                                               'calculation_time', 'result', 'kpi_fk', 'atomic_kpi_fk'])
        else:
            attributes = pd.DataFrame()
        return attributes.to_dict()

    @log_runtime('Saving to DB')
    def commit_results_data(self):
        """
        This function writes all KPI results to the DB, and commits the changes.
        """
        cur = self.rds_conn.db.cursor()
        delete_queries = JTIUAQueries.get_delete_session_results_query(self.session_uid)
        for query in delete_queries:
            cur.execute(query)
        for query in self.kpi_results_queries:
            cur.execute(query)
        self.rds_conn.db.commit()
