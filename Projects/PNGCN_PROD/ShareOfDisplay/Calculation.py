import pandas as pd

from Trax.Analytics.Calculation.PNGCN_PROD.EmptySpacesKpi import EmptySpaceKpiGenerator
from Projects.PNGCN_PROD.ShareOfDisplay.ExcludeDataProvider import ShareOfDisplayDataProvider, Fields
from Trax.Utils.Logging.Logger import Log
#from Trax.Cloud.Services.Connector.Logger import LoggerInitializer


__Author__ = 'Dudi_S'

CUBE = 1
NON_BRANDED_CUBE = 5
CUBE_DISPLAYS = [CUBE, NON_BRANDED_CUBE]
CUBE_TOTAL_DISPLAYS = [10, 11, 12, 13, 14, 15, 16,
                       17, 18, 19, 20, 21, 22, 23, 24]
PROMOTION_WALL_DISPLAYS = [25]


class PNGShareOfDisplay(object):
    def __init__(self, project_connector, session_uid, data_provider=None):
        self.session_uid = session_uid
        self.project_connector = project_connector
        if data_provider is not None:
            self.data_provider = data_provider
            self.project_connector = data_provider.project_connector
            self.on_ace = True
        else:
            self.on_ace = False
            self.data_provider = ShareOfDisplayDataProvider(project_connector, self.session_uid)
        self.cur = self.project_connector.db.cursor()
        self.log_prefix = 'Share_of_display for session: {}, project {}'.format(self.session_uid,
                                                                                self.project_connector.project_name)
        Log.info(self.log_prefix + ' Starting calculation')
        self.match_display_in_scene = pd.DataFrame({})
        self.match_product_in_scene = pd.DataFrame({})
        self.displays = pd.DataFrame({})

    def process_session(self):
        try:
            Log.debug(self.log_prefix + ' Retrieving data')
            self.match_display_in_scene = self._get_match_display_in_scene_data()
            # if there are no display tags there's no need to retrieve the rest of the data.
            if self.match_display_in_scene.empty:
                Log.debug(self.log_prefix + ' No display tags')
                self._delete_previous_data()

            else:
                self.displays = self._get_displays_data()
                self.match_product_in_scene = self._get_match_product_in_scene_data()
                self._delete_previous_data()
                self._handle_non_cube_non_promotion_display()
                self._handle_promotion_wall_display()
                self._handle_cube_display()
                if self.on_ace:
                    Log.debug(self.log_prefix + ' Committing share of display calculations')
                    self.project_connector.db.commit()
                Log.info(self.log_prefix + ' Finished calculation')
        except Exception as e:
            Log.error('Share of display calculation for session: \'{0}\' error: {1}'.format(self.session_uid, str(e)))
            raise e

    def _handle_non_cube_non_promotion_display(self):
        """
        Handling all display tags which are not cubes and not promotion wall.
        Each tag in a scene is a display with a single bay.
        :return:
        """
        Log.debug(self.log_prefix + ' Starting non cube non promotion display')
        # filtering all promotion wall and cube tags
        display_non_cube_non_promotion_wall_with_bays = \
            self.match_display_in_scene[~self.match_display_in_scene['display_fk'].isin(CUBE_DISPLAYS +
                                                                                        CUBE_TOTAL_DISPLAYS +
                                                                                        PROMOTION_WALL_DISPLAYS)]
        if not display_non_cube_non_promotion_wall_with_bays.empty:
            display_non_cube_non_promotion_wall_with_id_and_bays = \
                self._insert_into_display_surface(display_non_cube_non_promotion_wall_with_bays)
            # only valid tags are relevant
            display_non_cube_non_promotion_wall_with_id_and_valid_bays = \
                self._filter_valid_bays(display_non_cube_non_promotion_wall_with_id_and_bays)
            self._calculate_share_of_display(display_non_cube_non_promotion_wall_with_id_and_valid_bays)

    def _handle_promotion_wall_display(self):
        """
        Handles promotion wall display. All display tags in a scene are aggregated to one display with multiple bays
        :return:
        """
        Log.debug(self.log_prefix + ' Starting promotion display')
        promotion_tags = \
            self.match_display_in_scene[self.match_display_in_scene['display_fk'].isin(PROMOTION_WALL_DISPLAYS)]
        if not promotion_tags.empty:
            display_promotion_wall = promotion_tags.groupby(['display_fk', 'scene_fk'],
                                                            as_index=False).display_size.sum()
            display_promotion_wall_with_id = self._insert_into_display_surface(display_promotion_wall)
            promotion_wall_bays = promotion_tags[['scene_fk', 'bay_number']].copy()
            promotion_wall_bays.drop_duplicates(['scene_fk', 'bay_number'], inplace=True)
            # only valid tags are relevant
            promotion_wall_valid_bays = self._filter_valid_bays(promotion_wall_bays)
            display_promotion_wall_with_id_and_bays = \
                display_promotion_wall_with_id.merge(promotion_wall_valid_bays, on='scene_fk')
            self._calculate_share_of_display(display_promotion_wall_with_id_and_bays)

    def _handle_cube_display(self):
        """
        Handles cube displays. All tags are aggregated to one display per scene with multiple tags.
        If there are cube/non branded cube tags the bays will be taken from them, otherwise from the 'total' tags.
        If there are 'total' tags the size will be calculated from them, otherwise from the cube/non branded cube tags
        :return:
        """
        Log.debug(self.log_prefix + ' Starting cube display')
        cube_tags = self.match_display_in_scene[self.match_display_in_scene['display_fk'].isin(CUBE_DISPLAYS)]
        total_cube_tags = \
            self.match_display_in_scene[self.match_display_in_scene['display_fk'].isin(CUBE_TOTAL_DISPLAYS)]
        scenes = cube_tags.scene_fk.tolist() + total_cube_tags.scene_fk.tolist()
        scenes = list(set(scenes))
        cube_bays = pd.DataFrame({})
        cube_display = pd.DataFrame({})
        for scene in scenes:
            cube_tags_scene = cube_tags[cube_tags['scene_fk'] == scene]
            total_cube_tags_scene = total_cube_tags[total_cube_tags['scene_fk'] == scene]
            if not (total_cube_tags_scene.empty and cube_tags_scene.empty):
                if total_cube_tags_scene.empty:
                    cube_bays_scene = cube_tags_scene[['scene_fk', 'bay_number']].copy()
                    cube_display_scene = cube_tags_scene.groupby('scene_fk', as_index=False).display_size.sum()
                elif cube_tags_scene.empty:
                    cube_bays_scene = total_cube_tags_scene[['scene_fk', 'bay_number']].copy()
                    cube_display_scene = total_cube_tags_scene.groupby('scene_fk', as_index=False).display_size.sum()
                else:
                    cube_bays_scene = cube_tags_scene[['scene_fk', 'bay_number']].copy()
                    cube_display_scene = total_cube_tags_scene.groupby('scene_fk', as_index=False).display_size.sum()
                cube_display_scene['display_fk'] = \
                    NON_BRANDED_CUBE if (NON_BRANDED_CUBE in cube_tags_scene['display_fk'].values and
                                         CUBE not in cube_tags_scene['display_fk'].values) else CUBE
                cube_display = cube_display.append(cube_display_scene, ignore_index=True)
                cube_bays = cube_bays.append(cube_bays_scene, ignore_index=True)
        if not cube_bays.empty:
            cube_bays.drop_duplicates(['scene_fk', 'bay_number'], inplace=True)
        if not cube_display.empty:
            # only valid tags are relevant
            cube_valid_bays = self._filter_valid_bays(cube_bays)
            cube_display_with_id = self._insert_into_display_surface(cube_display)
            cube_display_with_id_and_bays = cube_display_with_id.merge(cube_valid_bays, on=['scene_fk'])
            self._calculate_share_of_display(cube_display_with_id_and_bays, all_skus=0)

    def _calculate_share_of_display(self, display_with_id_and_bays, all_skus=1):
        """
        Cross information between display and bays to match_product_in_scene.
        Calculating sos of the product on the display in order to calculate its area in the store.
        For sos calculation - stacked tags are not taken into account
        :param display_with_id_and_bays:
        :param all_skus:
        :return:
        """
        if not display_with_id_and_bays.empty:
            # merging between display data and product tags, by scene and bay number
            display_visit = display_with_id_and_bays.merge(self.match_product_in_scene, on=['scene_fk', 'bay_number'])
            if display_visit.empty:
                Log.debug(self.log_prefix + ' no product found on the displays')
                return
            display_facings_for_product = display_visit.groupby(['display_surface_fk', 'scene_fk', 'product_fk', 'own',
                                                                 'template_fk', 'display_fk', 'display_size'],
                                                                as_index=False).facings.sum()
            # for sos purposes filtering out stacking tags
            display_visit_stacking = display_visit[display_visit['display_fk'] == 27]
            display_visit_stacking.drop(['status', 'stacking_layer'], axis=1)
            display_visit = display_visit[display_visit['display_fk'] != 27]
            display_visit = display_visit[(display_visit['stacking_layer'] == 1)]\
                .drop(['status', 'stacking_layer'], axis=1)
            display_visit = display_visit.append(display_visit_stacking)
            display_facings_for_product = self._exclude_sos(display_facings_for_product)

            # checking if required to calculate based on all sku's in the display or bottom shelf only (max shelf)
            if not all_skus:
                display_visit = display_visit[display_visit['shelf_number_from_bottom'] == 1]
            if not display_visit.empty:
                # summing facings and length for sos calculation
                display_visit_by_display_product = \
                    display_visit.groupby(['display_surface_fk', 'product_fk', 'template_fk',
                                           'display_size', 'display_fk'], as_index=False)['linear', 'facings'].sum()
                display_visit_by_display_product = self._exclude_sos(display_visit_by_display_product)

                display_visit_by_display = \
                    display_visit_by_display_product[display_visit_by_display_product['in_sos'] == 1] \
                        .groupby(['display_surface_fk'], as_index=False)
                display_tot_linear = display_visit_by_display.linear.sum().rename(columns={'linear': 'tot_linear'})
                display_tot_facings = display_visit_by_display.facings.sum().rename(columns={'facings': 'tot_facings'})
                display_visit_by_display_product_enrich_totals = \
                    display_visit_by_display_product \
                        .merge(display_tot_linear, on='display_surface_fk') \
                        .merge(display_tot_facings, on='display_surface_fk')

                display_visit_by_display_product_enrich_sos_type = display_visit_by_display_product_enrich_totals.merge(
                    self.displays, on='display_fk')
                facings_sos_condition = ((display_visit_by_display_product_enrich_sos_type['sos_type_fk'] == 2) &
                                         (display_visit_by_display_product_enrich_sos_type['in_sos'] == 1))
                display_visit_by_display_product_enrich_sos_type.loc[facings_sos_condition, 'product_size'] = \
                    display_visit_by_display_product_enrich_sos_type['facings'] / \
                    display_visit_by_display_product_enrich_sos_type['tot_facings'] * \
                    display_visit_by_display_product_enrich_sos_type['display_size']

                linear_sos_condition = ((display_visit_by_display_product_enrich_sos_type['sos_type_fk'] == 1) &
                                        (display_visit_by_display_product_enrich_sos_type['in_sos'] == 1))
                display_visit_by_display_product_enrich_sos_type.loc[linear_sos_condition, 'product_size'] = \
                    display_visit_by_display_product_enrich_sos_type['linear'] / \
                    display_visit_by_display_product_enrich_sos_type['tot_linear'] * \
                    display_visit_by_display_product_enrich_sos_type['display_size']

                not_in_sos_condition = display_visit_by_display_product_enrich_sos_type['in_sos'] == 0
                display_visit_by_display_product_enrich_sos_type.loc[not_in_sos_condition, 'product_size'] = 0

                display_visit_summary = \
                    display_visit_by_display_product_enrich_sos_type.drop(['linear', 'tot_linear', 'tot_facings',
                                                                           'display_size', 'sos_type_fk',
                                                                           'facings', 'template_fk',
                                                                           'in_sos', 'display_fk', ], axis=1)
                display_visit_summary = display_facings_for_product.merge(display_visit_summary, how='left',
                                                                          on=['display_surface_fk', 'product_fk'])
                display_visit_summary['product_size'].fillna(0, inplace=True)
            else:
                display_visit_summary = display_facings_for_product
                display_visit_summary['product_size'] = 0

            display_visit_summary_list_of_dict = display_visit_summary.to_dict('records')
            self._insert_into_display_visit_summary(display_visit_summary_list_of_dict)

    def _exclude_sos(self, df):
        """
        Calculate 'in_sos' field according to exclude rules.
        :param df:
        :return:
        """
        Log.debug(self.log_prefix + ' calculating in_sos')
        excluded_templates = self.data_provider._data[Fields.SOS_EXCLUDED_TEMPLATES]
        excluded_templates['excluded_templates'] = 1

        excluded_template_products = self.data_provider._data[Fields.SOS_EXCLUDED_TEMPLATE_PRODUCTS]
        excluded_template_products['excluded_template_products'] = 1

        excluded_products = self.data_provider._data[Fields.SOS_EXCLUDED_PRODUCTS]
        excluded_products['excluded_products'] = 1

        df = df.merge(excluded_templates, how='left', on='template_fk') \
               .merge(excluded_products, how='left', on='product_fk') \
               .merge(excluded_template_products, how='left', on=['product_fk', 'template_fk'])

        condition = (df['excluded_templates'] == 1) | \
                    (df['excluded_template_products'] == 1) | (df['excluded_products'] == 1)

        df = df.drop(['excluded_templates', 'excluded_template_products', 'excluded_products'], axis=1)

        df.loc[condition, 'in_sos'] = 0
        df.loc[~condition, 'in_sos'] = 1
        return df

    def _insert_into_display_visit_summary(self, display_visit_summary_list_of_dict):
        Log.debug(self.log_prefix + ' Inserting to display_item_facts')
        query = ''' insert into report.display_item_facts
                        (
                            display_surface_fk
                            , item_id
                            , in_sos
                            , own
                            , product_size
                            ,facings
                        )
                    values
                        {};'''
        for display in display_visit_summary_list_of_dict[:-1]:
            query_line = self._get_query_line(display) + ',' + '{}'
            query = query.format(query_line)
        query = query.format(self._get_query_line(display_visit_summary_list_of_dict[-1]))
        self.cur.execute(query)

    @staticmethod
    def _get_query_line(display):
        # consider to use existing code which transforms df into query
        return '(' + \
               str(int(display['display_surface_fk'])) + ',' + \
               str(int(display['product_fk'])) + ',' + \
               str(int(display['in_sos'])) + ',' + \
               str(int(display['own'])) + ',' + \
               str(round(display['product_size'], 2)) + ',' + \
               str(display['facings']) + \
               ')'

    def _get_session_info(self):
        query = ''' select pk as session_fk, store_fk, visit_date, session_uid
                    from probedata.session where session_uid = \'{}\''''.format(self.session_uid)
        session_info = pd.read_sql_query(query, self.project_connector.db)
        return session_info

    def _get_displays_data(self):
        query = ''' select
                        pk as display_fk
                        ,sos_type_fk
                    from
                        static.display;'''
        displays = pd.read_sql_query(query, self.project_connector.db)
        return displays

    @staticmethod
    def _filter_valid_bays(df_with_bays):
        return df_with_bays[(df_with_bays['bay_number'] != -1) & (df_with_bays['bay_number'].notnull())]

    def _delete_previous_data(self):
        """
        Deletes previous data from table: probedata.display_surface (by updating delete_time)
        and report.display_visit_summary. Using temp table for scenes list to prevent a lock
        :return:
        """
        Log.debug(self.log_prefix + ' Deleting existing data in display_surface and display_visit_summary')
        drop_temp_table_query = "drop temporary table if exists probedata.t_scenes_to_delete_displays;"
        queries = [
            drop_temp_table_query,
            """ create temporary table probedata.t_scenes_to_delete_displays as
                select pk as scene_fk from probedata.scene where session_uid = '{}';""".format(self.session_uid),
            """ delete report.display_item_facts, probedata.display_surface
                from probedata.t_scenes_to_delete_displays
                 join probedata.display_surface
                    on probedata.display_surface.scene_fk = probedata.t_scenes_to_delete_displays.scene_fk
                 left join report.display_item_facts
                    on report.display_item_facts.display_surface_fk = probedata.display_surface.pk;""",
            drop_temp_table_query
            ]
        for query in queries:
            self.cur.execute(query)

    def _insert_into_display_surface(self, display_surface):
        """
        Inserts into probedata.display_surface the displays identified in each scene and its size.
        For each display it updates the new record pk in order to use as a foreign key when inserting into
        report.display_visit_summary.
        :param display_surface:
        :return:
        """
        # Optional performance improvement
        # 1.Use df instead of to_dict
        Log.debug(self.log_prefix + ' Inserting to probedata.display_surface')
        display_surface_dict = display_surface.to_dict('records')
        query = '''insert into probedata.display_surface
                        (
                            scene_fk
                            , display_fk
                            , surface
                        )
                        values
                         {};'''
        for display in display_surface_dict[:-1]:
            query_line = self._get_display_surface_query_line(display) + ',' + '{}'
            query = query.format(query_line)
        query = query.format(self._get_display_surface_query_line(display_surface_dict[-1]))
        self.cur.execute(query)
        last_insert_id = self.cur.lastrowid
        row_count = self.cur.rowcount
        if row_count == len(display_surface_dict):
            for j in range(0, len(display_surface_dict)):
                display_surface_dict[j]['display_surface_fk'] = last_insert_id
                last_insert_id += 1
        else:
            msg = self.log_prefix + ' error: not all display were inserted.'
            Log.error(msg)
            raise Exception(msg)

        return pd.DataFrame(display_surface_dict)

    @staticmethod
    def _get_display_surface_query_line(display):
        return '({0}, {1}, {2})'.format(display['scene_fk'], display['display_fk'], display['display_size'])

    def _get_match_display_in_scene_data(self):
        query = ''' select
                        mds.display_fk
                        ,mds.scene_fk
                        ,ds.size as display_size
                        ,mds.bay_number
                    from
                            probedata.match_display_in_scene mds
                        join
                            probedata.match_display_in_scene_info mdici
                             on mdici.pk = mds.match_display_in_scene_info_fk AND mdici.delete_time IS NULL
                        join
                            probedata.stitching_scene_info ssi
                             ON ssi.pk = mdici.stitching_scene_info_fk
                             AND ssi.delete_time IS NULL
                        join
                            static.display ds on ds.pk = mds.display_fk
                        join
                            probedata.scene sc on sc.pk=mds.scene_fk
                             and sc.session_uid = \'{}\''''.format(self.session_uid)
        match_display_in_scene = pd.read_sql_query(query, self.project_connector.db)
        return match_display_in_scene

    def _get_match_product_in_scene_data(self):
        query = ''' select
                        mps.scene_fk
                        ,mps.product_fk
                        ,mps.bay_number
                        ,mps.shelf_number
                        ,mps.status
                        ,mps.stacking_layer
                        ,mps.shelf_number_from_bottom
                        ,1 as facings
                        ,(shelf_px_right-shelf_px_left) as 'linear'
                        ,b.manufacturer_fk = 4 as own
                        ,sc.template_fk
                    from
                            probedata.match_product_in_scene mps
                        join
                            static.product p on p.pk = mps.product_fk and mps.status <> 2
                        join
                            static.brand b on b.pk = p.brand_fk
                        join
                            probedata.scene sc on sc.pk = mps.scene_fk
                             and sc.session_uid = '{0}'
                        join
                            static.template t on t.pk = sc.template_fk
                             and t.is_recognition = 1
                    '''.format(self.session_uid)
        match_product_in_scene = pd.read_sql_query(query, self.project_connector.db)
        return match_product_in_scene


def calculate_share_of_display(project_conn, session, data_provider=None):
    PNGShareOfDisplay(project_conn, session, data_provider).process_session()

# if __name__ == '__main__':
#     # Config.init()
#     LoggerInitializer.init('TREX')
#     conn = ProjectConnector('integ3', DbUsers.CalculationEng)
#     # session = 'd4b46e1d-b4ed-406d-a20b-8ec9b64f5c5e'
#     # session = '14f71194-e843-4a3c-94f0-f712775e8ea2'
#     # session = '9f0244de-66c3-4378-8b46-16c9f79dc978'
#     # # session = 'a6785107-4a59-4aec-943e-8f710c2aa46d'
#     # session = 'ff0a6857-52c8-4bbf-8f4b-697b141fcb9a'
#     session = '26a1aea8-39a0-4d6c-af49-10a20ba4a885'
#     data_provider = ACEDataProvider('integ3')
#     data_provider.load_session_data(session)
#     calculate(conn, session, data_provider)
