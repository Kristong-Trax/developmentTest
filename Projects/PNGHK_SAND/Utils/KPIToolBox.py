import pandas as pd
import os

from Trax.Algo.Calculations.Core.DataProvider import Data
from Trax.Cloud.Services.Connector.Keys import DbUsers
from KPIUtils_v2.DB.PsProjectConnector import PSProjectConnector
from Projects.PNGHK_SAND.Data.Const import Const
from KPIUtils_v2.DB.CommonV2 import Common
from Trax.Utils.Logging.Logger import Log
# from KPIUtils_v2.Calculations.AssortmentCalculations import Assortment
# from KPIUtils_v2.Calculations.AvailabilityCalculations import Availability
# from KPIUtils_v2.Calculations.NumberOfScenesCalculations import NumberOfScenes
# from KPIUtils_v2.Calculations.PositionGraphsCalculations import PositionGraphs
# from KPIUtils_v2.Calculations.SOSCalculations import SOS
# from KPIUtils_v2.Calculations.SequenceCalculations import Sequence
# from KPIUtils_v2.Calculations.SurveyCalculations import Survey

from KPIUtils_v2.Calculations.CalculationsUtils.GENERALToolBoxCalculations import GENERALToolBox

__author__ = 'ilays'

KPI_RESULT = 'report.kpi_results'
KPK_RESULT = 'report.kpk_results'
KPS_RESULT = 'report.kps_results'
PATH = os.path.join(os.path.dirname(os.path.realpath(__file__)), '..', 'Data', 'PNGHK_template_20181226.xlsx')

class PNGHKToolBox:
    LEVEL1 = 1
    LEVEL2 = 2
    LEVEL3 = 3

    def __init__(self, data_provider, output):
        self.output = output
        self.data_provider = data_provider
        self.common = Common(self.data_provider)
        self.project_name = self.data_provider.project_name
        self.session_uid = self.data_provider.session_uid
        self.products = self.data_provider[Data.PRODUCTS]
        self.all_products = self.data_provider[Data.ALL_PRODUCTS]
        self.match_product_in_scene = self.data_provider[Data.MATCHES]
        self.visit_date = self.data_provider[Data.VISIT_DATE]
        self.session_info = self.data_provider[Data.SESSION_INFO]
        self.scene_info = self.data_provider[Data.SCENES_INFO]
        self.store_info = self.data_provider[Data.STORE_INFO]
        self.store_id = self.data_provider[Data.STORE_FK]
        self.scif = self.data_provider[Data.SCENE_ITEM_FACTS]
        self.rds_conn = PSProjectConnector(self.project_name, DbUsers.CalculationEng)
        self.kpi_static_data = self.common.get_kpi_static_data()
        self.kpi_results_queries = []
        self.kpis_sheet = pd.read_excel(PATH, Const.KPIS).fillna("")
        self.osd_rules_sheet = pd.read_excel(PATH, Const.OSD_RULES).fillna("")
        self.kpi_excluding = pd.DataFrame()
        self.df = pd.DataFrame()
        self.tools = GENERALToolBox(self.data_provider)
        # self.merged_additional_data = self.get_additional_product_data()


    # scene_recognition from table select * from probedata.match_display_in_scene


    # def get_additional_product_data(self):
    #     #####
    #     # This queries are temporary until given in data provider.
    #     #####
    #
    #     price_query = \
    #         """
    #         SELECT
    #                 p.scene_fk as scene_fk,
    #                 mpip.product_fk as product_fk,
    #                 COALESCE(pr.substitution_product_fk, mpip.product_fk) as substitution_product_fk,
    #                 mpn.match_product_in_probe_fk as probe_match_fk,
    #                 mpn.value as price_value,
    #                 mpas.state as number_attribute_state
    #         FROM
    #                 probedata.match_product_in_probe_number_attribute_value mpn
    #                 LEFT JOIN static.number_attribute_brands_scene_types mpna on mpn.number_attribute_fk = mpna.pk
    #                 LEFT JOIN static.match_product_in_probe_attributes_state mpas ON mpn.attribute_state_fk = mpas.pk
    #                 JOIN probedata.match_product_in_probe mpip ON mpn.match_product_in_probe_fk = mpip.pk
    #                 JOIN probedata.probe p ON p.pk = mpip.probe_fk
    #                 JOIN static_new.product pr ON pr.pk = mpip.product_fk
    #         WHERE
    #                 p.session_uid = "{0}"
    #         """.format(self.session_uid)
    #
    #     date_query = \
    #         """
    #         SELECT
    #                 p.scene_fk as scene_fk,
    #                 mpip.product_fk as product_fk,
    #                 COALESCE(pr.substitution_product_fk, mpip.product_fk) as substitution_product_fk,
    #                 mpd.match_product_in_probe_fk as probe_match_fk,
    #                 mpd.value as date_value,
    #                 mpd.original_value,
    #                 mpas.state as date_attribute_state
    #         FROM
    #                 probedata.match_product_in_probe_date_attribute_value mpd
    #                 LEFT JOIN static.date_attribute_brands_scene_types mpda ON mpd.date_attribute_fk = mpda.pk
    #                 LEFT JOIN static.match_product_in_probe_attributes_state mpas ON mpd.attribute_state_fk = mpas.pk
    #                 JOIN probedata.match_product_in_probe mpip ON mpd.match_product_in_probe_fk = mpip.pk
    #                 JOIN probedata.probe p ON p.pk = mpip.probe_fk
    #                 JOIN static_new.product pr ON pr.pk = mpip.product_fk
    #         WHERE
    #                 p.session_uid = "{0}"
    #         """.format(self.session_uid)
    #
    #     price_attr = pd.read_sql_query(price_query, self.rds_conn.db)
    #     date_attr = pd.read_sql_query(date_query, self.rds_conn.db)
    #     matches = self.data_provider[Data.MATCHES]
    #
    #     merged_pricing_data = price_attr.merge(matches[['scene_fk', 'product_fk', 'probe_match_fk']],
    #                                            on=['probe_match_fk', 'product_fk', 'scene_fk'])
    #     merged_dates_data = date_attr.merge(matches[['scene_fk', 'product_fk', 'probe_match_fk']],
    #                                         on=['probe_match_fk', 'product_fk', 'scene_fk'])
    #
    #     merged_pricing_data.dropna(subset=['price_value'], inplace=True)
    #     merged_dates_data.dropna(subset=['original_value'], inplace=True)
    #
    #     if not merged_pricing_data.empty:
    #         try:
    #             merged_pricing_data = merged_pricing_data.groupby(['scene_fk', 'substitution_product_fk'],
    #                                                               as_index=False)[['price_value']].first()
    #         except Exception as e:
    #             merged_pricing_data['price_value'] = 0
    #             merged_pricing_data = merged_pricing_data.groupby(['scene_fk', 'substitution_product_fk'],
    #                                                               as_index=False)[['price_value']].first()
    #             Log.info('There are missing numeric values: {}'.format(e))
    #
    #     if not merged_dates_data.empty:
    #         merged_dates_data['fixed_date'] = merged_dates_data.apply(lambda row: self._get_formate_date(row), axis=1)
    #         try:
    #             merged_dates_data = merged_dates_data.groupby(['scene_fk', 'substitution_product_fk'],
    #                                                           as_index=False)[['fixed_date']].first()
    #         except Exception as e:
    #             merged_dates_data = merged_dates_data.groupby(['scene_fk', 'substitution_product_fk'],
    #                                                           as_index=False)[['fixed_date']].first()
    #             Log.info('There is a dates integrity issue: {}'.format(e))
    #     else:
    #         merged_dates_data['fixed_date'] = None
    #
    #     merged_additional_data = self.scif\
    #         .merge(merged_pricing_data, how='left',
    #                left_on=['scene_id', 'item_id'],
    #                right_on=['scene_fk', 'substitution_product_fk'])\
    #         .merge(merged_dates_data, how='left',
    #                left_on=['scene_id', 'item_id'],
    #                right_on=['scene_fk', 'substitution_product_fk'])\
    #         .merge(self.all_products, how='left',
    #                left_on='item_id',
    #                right_on='product_fk',
    #                suffixes=['', '_all_products'])\
    #         .dropna(subset=['fixed_date', 'price_value'],
    #                 how='all')
    #
    #     return merged_additional_data

    def main_calculation(self, *args, **kwargs):
        """
        This function calculates the KPI results.
        """
        df = pd.merge(self.match_product_in_scene, self.products, on="product_fk", how="left")
        distinct_session_fk = self.scif[['scene_fk', 'template_name']].drop_duplicates()
        self.df = pd.merge(df, distinct_session_fk, on="scene_fk", how="left")
        kpi_ids = self.kpis_sheet[Const.KPI_ID].drop_duplicates().tolist()
        for id in kpi_ids:
            kpi_df = self.kpis_sheet[self.kpis_sheet[Const.KPI_ID] == id]
            self.handle_atomic(kpi_df)
        # self.common.commit_results_data()

    def handle_atomic(self, kpi_df):
        kpi_type = kpi_df[Const.KPI_TYPE].values[0].strip()
        self.kpi_excluding = kpi_df[[Const.EXCLUDE_EMPTY, Const.EXCLUDE_HANGER, Const.EXCLUDE_IRRELEVANT,
                            Const.EXCLUDE_POSM, Const.EXCLUDE_OTHER, Const.STACKING, Const.EXCLUDE_SKU,
                                                        Const.EXCLUDE_STOCK, Const.EXCLUDE_OSD]].iloc[0]
        # if kpi_type == Const.FSOS:
        #     self.calculate_facings_sos_kpi(kpi_df)
        if kpi_type == Const.LSOS:
            self.calculate_linear_sos_kpi(kpi_df)
        elif kpi_type == Const.DISPLAY_NUMBER:
            pass

    def calculate_facings_sos_kpi(self, kpi_df):
        kpi_name = kpi_df[Const.KPI_NAME].values[0]
        try:
            kpi_fk = self.common.get_kpi_fk_by_kpi_type(kpi_name)
        except IndexError:
            Log.warning("There is no matching Kpi fk for kpi name: " + kpi_name)
            return
        entity_name = kpi_df[Const.NUMERATOR_ENTITY].values[0]
        entity_name_for_fk = Const.NAME_TO_FK[entity_name]
        for i, row in kpi_df.iterrows():
            filters = {}
            df = self.filter_df(row)
            category = row[Const.CATEGORY]
            if category != "":
                denominator_id = self.all_products[self.all_products['category']==category]['category_fk'].iloc[0]
                filters['category'] = category
            else:
                denominator_id = self.store_id
            all_denominators = df[entity_name].drop_duplicates().tolist()
            if row[Const.NUMERATOR] != "":
                all_denominators = [row[Const.NUMERATOR]]
            for entity in all_denominators:
                filters[entity_name] = entity
                numerator = self.tools.get_filter_condition(df, **filters).sum()
                del filters[entity_name]
                denominator = self.tools.get_filter_condition(df, **filters).sum()
                result = float(numerator) / float(denominator)
                numerator_id = df[df[entity_name] == entity][entity_name_for_fk].values[0]
                self.common.write_to_db_result(fk=kpi_fk, numerator_id=numerator_id, denominator_id=denominator_id,
                                               numerator_result=numerator, denominator_result=denominator,
                                               result=result, score=result)

    def calculate_linear_sos_kpi(self, kpi_df):
        kpi_name = kpi_df[Const.KPI_NAME].values[0]
        try:
            kpi_fk = self.common.get_kpi_fk_by_kpi_type(kpi_name)
        except IndexError:
            Log.warning("There is no matching Kpi fk for kpi name: " + kpi_name)
            return
        entity_name = kpi_df[Const.NUMERATOR_ENTITY].values[0]
        entity_name_for_fk = Const.NAME_TO_FK[entity_name]
        for i, row in kpi_df.iterrows():
            filters = {}
            df = self.filter_df(row)
            category = row[Const.CATEGORY]
            scene_size = row[Const.SCENE_SIZE]
            if category != "":
                if category == Const.EACH:
                    categories = Const.CATEGORIES
                else:
                    categories = [category]
            else:
                categories = [""]
            for category in categories:
                if category != "":
                    denominator_id = self.all_products[self.all_products['category'] == category]['category_fk'].iloc[0]
                    filters['category'] = category
                else:
                    denominator_id = self.store_id
                all_denominators = df[entity_name].drop_duplicates().values.tolist()
                if row[Const.NUMERATOR] != "":
                    all_denominators = [row[Const.NUMERATOR]]
                for entity in all_denominators:
                    filters[entity_name] = entity
                    numerator = df[self.tools.get_filter_condition(df, **filters)]['width_mm_x'].sum()
                    del filters[entity_name]
                    denominator = scene_size if scene_size != "" else \
                                            df[self.tools.get_filter_condition(df, **filters)]['width_mm_x'].sum()
                    result = float(numerator) / float(denominator)
                    try:
                        numerator_id = self.all_products[self.all_products[entity_name] == 'PG'][entity_name_for_fk].values[0]
                    except:
                        Log.warning("No entity in this name " + entity)
                        numerator_id = -1
                    self.common.write_to_db_result(fk=kpi_fk, numerator_id=numerator_id, denominator_id=denominator_id,
                                                   numerator_result=numerator, denominator_result=denominator,
                                                   result=result, score=result)

    def filter_df(self, kpi_df):
        df = self.df.copy()
        # filter scene_types

        scene_types = kpi_df[Const.SCENE_TYPE].split(',')
        if scene_types != "":
            scene_types = [item.strip() for item in scene_types]
            df = df[df['template_name'].isin(scene_types)]

        # filter category
        category = kpi_df[Const.CATEGORY].strip()
        if category != "":
            df = df[df['category'] == category]

        # filter excludings
        return self.filter_excluding(df)

    def filter_osd(self, df):
        df_list = []
        scene_types = set(df['template_name'])
        for s in scene_types:
            scene_df = df[df['template_name'] == s]
            row = self.find_row_osd(s)
            if row.empty:
                df_list.append(scene_df)
                continue

            # if no osd rule is applied
            if (row[Const.HAS_OSD].values[0] == Const.NO) and (row[Const.HAS_HOTSPOT].values[0] == Const.NO):
                df_list.append(scene_df)
                continue

            # filter df to only shelf_to_include or higher shelves
            shelfs_to_include = row[Const.OSD_NUMBER_OF_SHELVES].values[0]
            if shelfs_to_include != "":
                shelfs_to_include = int(shelfs_to_include)
                scene_df = scene_df[scene_df['shelf_number'] <= shelfs_to_include]

            # filter df to remove shelves with given ean code
            if row[Const.HAS_OSD].values[0] == Const.YES:
                products_to_filter = row[Const.POSM_EAN_CODE].values[0].split(",")
                products_df = scene_df[scene_df['product_ean_code'].isin(products_to_filter)][['scene_fk',
                                                                                        'bay_number','shelf_number']]
                for index, p in products_df.iterrows():
                    scene_df = scene_df[~((scene_df['scene_fk'] == p['scene_fk']) &
                                          (scene_df['shelf_number'] == p['shelf_number']))]
                df_list.append(scene_df)

                # filter df to remove shelves with given ean code (only on the same bay)
            elif row[Const.HAS_HOTSPOT].values[0] == Const.YES:
                products_to_filter = row[Const.POSM_EAN_CODE_HOTSPOT].split(",")
                products_df = scene_df[scene_df['product_ean_code'].isin(products_to_filter)][['scene_fk',
                                                                                               'bay_number',
                                                                                               'shelf_number']]
                for index, p in products_df.iterrows():
                    scene_df = scene_df[~((scene_df['scene_fk'] == p['scene_fk']) &
                                          (scene_df['bay_number'] == p['bay_number']) &
                                          (scene_df['shelf_number'] == p['shelf_number']))]
                df_list.append(scene_df)
        final_df = pd.concat(df_list)
        return final_df

    def find_row_osd(self, s):
        rows = self.osd_rules_sheet[self.osd_rules_sheet[Const.SCENE_TYPE] == s]
        row = rows[rows[Const.RETAILER] == self.store_info['retailer_name'].values[0]]
        return row

    def filter_excluding(self, df):
        if self.kpi_excluding[Const.EXCLUDE_IRRELEVANT] == Const.EXCLUDE:
            df = df[df['product_type'] != 'Irrelevant']
        if self.kpi_excluding[Const.EXCLUDE_OTHER] == Const.EXCLUDE:
            df = df[df['product_type'] != 'Other']
        if self.kpi_excluding[Const.EXCLUDE_EMPTY] == Const.EXCLUDE:
            df = df[df['product_type'] != 'Empty']
        if self.kpi_excluding[Const.EXCLUDE_POSM] == Const.EXCLUDE:
            df = df[df['product_type'] != 'POS']
        if self.kpi_excluding[Const.STACKING] == Const.EXCLUDE:
            df = df[df['stacking_layer'] == 1]
        if self.kpi_excluding[Const.EXCLUDE_OSD] == Const.EXCLUDE:
            df = self.filter_osd(df)

        #???
        # if self.kpi_excluding[Const.EXCLUDE_SKU == Const.EXCLUDE]:
        #     df = df[df['product_type'] != 'SKU']

        return df