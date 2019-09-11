# from Trax.Analytics.Calculation.PNGCN_PROD.EmptySpacesKpi import EmptySpaceKpiGenerator
#from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Projects.PNGCNTEST.ShareOfDisplay.ExcludeDataProvider import ShareOfDisplayDataProvider, Fields
from Trax.Utils.Logging.Logger import Log
import pandas as pd
import numpy as np


__Author__ = 'Dudi_S'

CUBE = 'Cube'
NON_BRANDED_CUBE = 'Non branded cube'
CUBE_DISPLAYS = [CUBE, NON_BRANDED_CUBE]
CUBE_FK = 1
NON_BRANDED_CUBE_FK = 5
CUBE_TOTAL_DISPLAYS = ['Total 1 cube', 'Total 2 cubes', 'Total 3 cubes', 'Total 4 cubes', 'Total 5 cubes',
                       'Total 6 cubes', 'Total 7 cubes', 'Total 8 cubes', 'Total 9 cubes', 'Total 10 cubes',
                       'Total 11 cubes', 'Total 12 cubes', 'Total 13 cubes', 'Total 14 cubes', 'Total 15 cubes']
FOUR_SIDED = ['4 Sided Display']
FOUR_SIDED_FK = 31
FOUR_SIDED_TOTAL_DISPLAYS = ['Total 1 4-sided display', 'Total 2 4-sided display', 'Total 3 4-sided display',
                             'Total 4 4-sided display', 'Total 5 4-sided display']
PROMOTION_WALL_DISPLAYS = ['Product Strip']
TABLE_DISPLAYS = ['Table']
TABLE_TOTAL_DISPLAYS = ['Table Display']

class PNGShareOfDisplay(object):
    def __init__(self, project_connector, session_uid, data_provider=None):
        self.session_uid = session_uid
        self.project_connector = project_connector
        if data_provider is not None:
            self.data_provider = data_provider
            # self.project_connector = data_provider.project_connector
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
        self.valid_facing_product = {}

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
                self._handle_promotion_wall_display()
                self._handle_cube_or_4_sided_display()
                self._handle_table_display()
                self._handle_rest_display()
                if self.on_ace:
                    Log.debug(self.log_prefix + ' Committing share of display calculations')
                    self.project_connector.db.commit()
                Log.info(self.log_prefix + ' Finished calculation')
        except Exception as e:
            Log.error('Share of display calculation for session: \'{0}\' error: {1}'.format(self.session_uid, str(e)))
            raise e

    def _handle_rest_display(self):
        """
        Handling all display tags which are not cubes and not promotion wall.
        Each tag in a scene is a display with a single bay.
        :return:
        """
        Log.debug(self.log_prefix + ' Starting non cube non promotion display')
        # filtering all rest displays tags
        display_non_cube_non_promotion_wall_with_bays = \
            self.match_display_in_scene[~self.match_display_in_scene['display_name'].isin(CUBE_DISPLAYS +
                                                                                          CUBE_TOTAL_DISPLAYS +
                                                                                          PROMOTION_WALL_DISPLAYS +
                                                                                          TABLE_DISPLAYS +
                                                                                          TABLE_TOTAL_DISPLAYS +
                                                                                          FOUR_SIDED +
                                                                                          FOUR_SIDED_TOTAL_DISPLAYS)]
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
            self.match_display_in_scene[self.match_display_in_scene['display_name'].isin(PROMOTION_WALL_DISPLAYS)]
        if not promotion_tags.empty:
            promotion_display_name = promotion_tags['display_name'].values[0]
            display_promotion_wall = promotion_tags.groupby(['display_fk', 'scene_fk'],
                                                            as_index=False).display_size.sum()
            display_promotion_wall['display_name'] = promotion_display_name
            display_promotion_wall_with_id = self._insert_into_display_surface(display_promotion_wall)
            promotion_wall_bays = promotion_tags[['scene_fk', 'bay_number']].copy()
            promotion_wall_bays.drop_duplicates(['scene_fk', 'bay_number'], inplace=True)
            # only valid tags are relevant
            promotion_wall_valid_bays = self._filter_valid_bays(promotion_wall_bays)
            display_promotion_wall_with_id_and_bays = \
                display_promotion_wall_with_id.merge(promotion_wall_valid_bays, on='scene_fk')
            self._calculate_share_of_display(display_promotion_wall_with_id_and_bays)

    def _handle_cube_or_4_sided_display(self):
        """
        Handles cube displays. All tags are aggregated to one display per scene with multiple tags.
        If there are cube/non branded cube tags the bays will be taken from them, otherwise from the 'total' tags.
        If there are 'total' tags the size will be calculated from them, otherwise from the cube/non branded cube tags
        :return:
        """
        Log.debug(self.log_prefix + ' Starting cube display')
        tags = self.match_display_in_scene[self.match_display_in_scene['display_name'].isin(CUBE_DISPLAYS + FOUR_SIDED)]
        total_tags = \
            self.match_display_in_scene[self.match_display_in_scene['display_name'].isin(CUBE_TOTAL_DISPLAYS + FOUR_SIDED_TOTAL_DISPLAYS)]
        # remove mixed scenes with table and cube tags together
        table_tags = self.match_display_in_scene[self.match_display_in_scene['display_name'].isin(TABLE_DISPLAYS)]
        table_scenes = table_tags.scene_fk.tolist()
        scenes = tags.scene_fk.tolist() + total_tags.scene_fk.tolist()
        mixed_with_table_scenes = list(set(scenes)&set(table_scenes))
        scenes = list(set(scenes)-set(mixed_with_table_scenes))
        bays = pd.DataFrame({})
        display = pd.DataFrame({})
        for scene in scenes:
            tags_scene = tags[tags['scene_fk'] == scene]
            total_tags_scene = total_tags[total_tags['scene_fk'] == scene]
            if not (total_tags_scene.empty and tags_scene.empty):
                if total_tags_scene.empty:
                    bays_scene = tags_scene[['scene_fk', 'bay_number']].copy()
                    display_scene = tags_scene.groupby('scene_fk', as_index=False).display_size.sum()
                elif tags_scene.empty:
                    bays_scene = total_tags_scene[['scene_fk', 'bay_number']].copy()
                    display_scene = total_tags_scene.groupby('scene_fk', as_index=False).display_size.sum()
                else:
                    bays_scene = tags_scene[['scene_fk', 'bay_number']].copy()
                    display_scene = total_tags_scene.groupby('scene_fk', as_index=False).display_size.sum()
                # if there is even 1 cube tag in the scene, all display tags will be considered as cube
                if not (tags_scene[tags_scene['display_name'].isin(CUBE_DISPLAYS + CUBE_TOTAL_DISPLAYS)]).empty:
                    display_scene['display_fk'] = \
                        NON_BRANDED_CUBE_FK if (NON_BRANDED_CUBE in tags_scene['display_name'].values and
                                             CUBE not in tags_scene['display_name'].values) else CUBE_FK
                else:
                    display_scene['display_fk'] = FOUR_SIDED_FK

                display = display.append(display_scene, ignore_index=True)
                bays = bays.append(bays_scene, ignore_index=True)
                if display['display_fk'].values[0] == CUBE_FK:
                    display['display_name'] = CUBE
                elif display['display_fk'].values[0] == NON_BRANDED_CUBE_FK:
                    display['display_name'] = NON_BRANDED_CUBE
                else:
                    display['display_name'] = TABLE_DISPLAYS[0]

        if not bays.empty:
            bays.drop_duplicates(['scene_fk', 'bay_number'], inplace=True)
        if not display.empty:
            # only valid tags are relevant
            valid_bays = self._filter_valid_bays(bays)
            display_with_id = self._insert_into_display_surface(display)
            display_with_id_and_bays = display_with_id.merge(valid_bays, on=['scene_fk'])
            self._calculate_share_of_display(display_with_id_and_bays, all_skus=0)

    def _handle_table_display(self):
        """
            Handles table displays. All tags are aggregated to one display per scene with multiple tags.
            If there are cube/non branded cube tags also -> the display will be considered as Table display and cube size
            will be considered as 3 table size (3*table size) -> number of cubes will be determined by Total_CUBE tag,
            if there is no total_cube tag and only cube tags, we will ignore them and only table calculation will be applied
            :return:
            """
        Log.debug(self.log_prefix + ' Starting table display')
        table_tags = self.match_display_in_scene[self.match_display_in_scene['display_name'].isin(TABLE_DISPLAYS)]
        total_cube_tags = self.match_display_in_scene[self.match_display_in_scene['display_name'].isin(CUBE_TOTAL_DISPLAYS)]
        other_tags = \
            self.match_display_in_scene[~self.match_display_in_scene['display_name'].isin(TABLE_DISPLAYS +
                                                                                          TABLE_TOTAL_DISPLAYS + CUBE_TOTAL_DISPLAYS + CUBE_DISPLAYS)]
        table_scenes = table_tags.scene_fk.tolist()
        other_scenes = other_tags.scene_fk.tolist()
        total_cube_scenes = total_cube_tags.scene_fk.tolist()
        mixed_scenes = list(set(table_scenes)&set(total_cube_scenes))  # scenes with total cube tag & table tag
        # scenes = list((set(table_scenes)|set(mixed_scenes))-set(other_scenes))
        scenes = list(set(table_scenes)-set(other_scenes))
        table_bays = pd.DataFrame({})
        table_display = pd.DataFrame({})
        if not table_tags.empty:
            table_display_fk = table_tags['display_fk'].values[0]
            table_display_name = table_tags['display_name'].values[0]
        for scene in scenes:
            cube_size = 0
            table_tags_scene = table_tags[table_tags['scene_fk'] == scene]
            table_bays_scene = table_tags_scene[['scene_fk', 'bay_number']].copy()
            number_of_captured_sides = len(table_tags_scene.groupby(['scene_fk', 'bay_number']).display_size.sum())
            table_size = table_tags_scene['display_size'].values[0]
            if scene in mixed_scenes:
                # add the size of cube (1 cube = 3 tables), all_cube_tags includes cube and total in order to include all products
                all_cube_tags = self.match_display_in_scene[self.match_display_in_scene['display_name'].isin(CUBE_TOTAL_DISPLAYS + CUBE_DISPLAYS)]
                all_cube_tags_scene = all_cube_tags[all_cube_tags['scene_fk'] == scene]
                cube_bays_scene = all_cube_tags_scene[['scene_fk', 'bay_number']].copy()
                cube_size = all_cube_tags_scene.loc[all_cube_tags_scene['display_name'].isin(CUBE_TOTAL_DISPLAYS)].display_size.sum()
            if number_of_captured_sides ==1:
                size = min(table_tags_scene.groupby(['scene_fk', 'bay_number']).display_size.sum())
                display_size = size + (cube_size * 3 * table_size)
            else:
                try:
                    min_side = min(table_tags_scene.groupby(['scene_fk', 'bay_number']).display_fk.count())
                    max_side = max(table_tags_scene.groupby(['scene_fk', 'bay_number']).display_fk.count())
                    display_size = (min_side * max_side * table_size) + (cube_size * 3 * table_size)
                except Exception as e:
                    display_size = (cube_size * 3 * table_size) # table bays are not valid
            table_display = table_display.append({'scene_fk': scene, 'display_fk': table_display_fk,
                                                  'display_size': display_size, 'display_name': table_display_name}, ignore_index=True)
            table_bays = table_bays.append(table_bays_scene, ignore_index=True)
            if scene in mixed_scenes:
                table_bays = table_bays.append(cube_bays_scene, ignore_index=True)
        if not table_bays.empty:
            table_bays.drop_duplicates(['scene_fk', 'bay_number'], inplace=True)
        if not table_display.empty:
            # only valid tags are relevant
            table_valid_bays = self._filter_valid_bays(table_bays)
            table_display_with_id = self._insert_into_display_surface(table_display)
            table_display_with_id_and_bays = table_display_with_id.merge(table_valid_bays, on=['scene_fk'])
            self._calculate_share_of_display(table_display_with_id_and_bays, all_skus=0)

        return

    def get_products_brand(self):
        query = """
                select p.pk as product_fk,b.name as brand_name from static_new.product p
                join static_new.brand b on b.pk = p.brand_fk;
                """
        self.project_connector.disconnect_rds()
        self.project_connector.connect_rds()
        res = pd.read_sql_query(query, self.project_connector.db)
        return res

    def get_display_group(self, display_group):
        query = '''select pk
                    from static.custom_entity
                        where name = '{}';'''.format(display_group)

        display_group_fk = pd.read_sql_query(query, self.project_connector.db)
        return display_group_fk.pk[0]

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
            display_visit_stacking = display_visit[display_visit['display_name'].isin(TABLE_DISPLAYS)]
            display_visit_stacking.drop(['status', 'stacking_layer'], axis=1)
            display_visit = display_visit[~display_visit['display_name'].isin(TABLE_DISPLAYS)]
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
                # irrlevant products, should be counted in total facings/ linear of display for product size value,
                #  but should be removed from display size share %
                irrelvant_products = self.data_provider.all_products.loc[self.data_provider.all_products['product_type'] == 'Irrelevant'][
                    'product_fk'].tolist()
                not_in_sos_condition = ((display_visit_by_display_product_enrich_sos_type['in_sos'] == 0) |
                (display_visit_by_display_product_enrich_sos_type['product_fk'].isin(irrelvant_products)))
                display_visit_by_display_product_enrich_sos_type.loc[not_in_sos_condition, 'product_size'] = 0

                display_visit_summary = \
                    display_visit_by_display_product_enrich_sos_type.drop(['linear', 'tot_linear', 'tot_facings',
                                                                           'display_size', 'sos_type_fk',
                                                                           'facings', 'template_fk',
                                                                           'in_sos', 'display_fk', ], axis=1)

                # This will check which products are a part of brands that have more then 2 facing in the display

                displays = display_visit_summary['display_surface_fk'].unique()
                brands = self.get_products_brand()
                merged_displays = display_facings_for_product.merge(brands, how='left', on='product_fk')
                for current_display in displays:
                    self.valid_facing_product[current_display] = []
                    current_display_products = merged_displays[
                        merged_displays['display_surface_fk'] == current_display]
                    brands_in_display = current_display_products['brand_name'].unique()
                    for brand in brands_in_display:
                        if current_display_products[current_display_products['brand_name'] == brand]['facings'].sum() > 2:
                            self.valid_facing_product[current_display].extend(
                                current_display_products[current_display_products['brand_name'] == brand]['product_fk'])

                display_visit_summary = display_facings_for_product.merge(display_visit_summary, how='left',
                                                                          on=['display_surface_fk', 'product_fk'])
                display_visit_summary['product_size'].fillna(0, inplace=True)
            else:
                display_visit_summary = display_facings_for_product
                display_visit_summary['product_size'] = 0

            display_visit_summary = self.remove_by_facing(display_visit_summary)
            display_visit_summary_list_of_dict = display_visit_summary.to_dict('records')
            self._insert_into_display_visit_summary(display_visit_summary_list_of_dict)

    def remove_by_facing(self, df):
        """
        This will return a dataframe of products from the visit where the brand has 2 or more then 2 facings.
        """
        df['to_remove'] = 1
        for i in xrange(len(df)):
            row = df.iloc[i]
            if row['product_fk'] in self.valid_facing_product.get(row['display_surface_fk'], []):
                df.ix[i, 'to_remove'] = 0
        result_df = df[df['to_remove'] == 0]
        result_df = result_df.drop(['to_remove'], axis=1, )
        return result_df

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
        self.cur = self.project_connector.db.cursor()
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
        self.project_connector.db.commit()

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
        self.project_connector.disconnect_rds()
        self.project_connector.connect_rds()
        self.cur = self.project_connector.db.cursor()
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
        self.project_connector.db.commit()
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
        local_con = self.project_connector.db
        query = ''' select
                        mds.display_fk
                        ,ds.display_name
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
        match_display_in_scene = pd.read_sql_query(query, local_con)
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
#     conn = PSProjectConnector('integ3', DbUsers.CalculationEng)
#     # session = 'd4b46e1d-b4ed-406d-a20b-8ec9b64f5c5e'
#     # session = '14f71194-e843-4a3c-94f0-f712775e8ea2'
#     # session = '9f0244de-66c3-4378-8b46-16c9f79dc978'
#     # # session = 'a6785107-4a59-4aec-943e-8f710c2aa46d'
#     # session = 'ff0a6857-52c8-4bbf-8f4b-697b141fcb9a'
#     session = '26a1aea8-39a0-4d6c-af49-10a20ba4a885'
#     data_provider = ACEDataProvider('integ3')
#     data_provider.load_session_data(session)
#     calculate(conn, session, data_provider)
