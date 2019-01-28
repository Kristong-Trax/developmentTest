import pandas as pd
import os

from Trax.Algo.Calculations.Core.DataProvider import Data
from Trax.Cloud.Services.Connector.Keys import DbUsers
from KPIUtils_v2.DB.PsProjectConnector import PSProjectConnector
from Projects.PNGHK_SAND.Data.Const import Const
from KPIUtils_v2.DB.CommonV2 import Common
from Trax.Utils.Logging.Logger import Log
from KPIUtils_v2.Calculations.CalculationsUtils.GENERALToolBoxCalculations import GENERALToolBox

__author__ = 'ilays'

PATH = os.path.join(os.path.dirname(os.path.realpath(__file__)), '..', 'Data', 'PNGHK_template_2019_24_01.xlsx')

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
        self.templates = self.data_provider[Data.ALL_TEMPLATES]
        # self.merged_additional_data = self.get_additional_product_data()

    # scene_recognition from table select * from probedata.match_display_in_scene

    # smart attributes from:
    # select * from probedata.match_product_in_probe;
    # select * from probedata.match_product_in_probe_state_value;

    def main_calculation(self, *args, **kwargs):
        """
        This function calculates the KPI results.
        """
        df = pd.merge(self.match_product_in_scene, self.products, on="product_fk", how="left")
        distinct_session_fk = self.scif[['scene_fk', 'template_name', 'template_fk']].drop_duplicates()
        self.df = pd.merge(df, distinct_session_fk, on="scene_fk", how="left")
        kpi_ids = self.kpis_sheet[Const.KPI_ID].drop_duplicates().tolist()
        for id in kpi_ids:
            kpi_df = self.kpis_sheet[self.kpis_sheet[Const.KPI_ID] == id]
            self.handle_atomic(kpi_df)
        self.common.commit_results_data()

    def handle_atomic(self, kpi_df):
        kpi_type = kpi_df[Const.KPI_TYPE].values[0].strip()
        if kpi_type == Const.FSOS:
            self.calculate_facings_sos_kpi(kpi_df)
        if kpi_type == Const.LSOS:
            self.calculate_linear_sos_kpi(kpi_df)
        if kpi_type == Const.DISPLAY_NUMBER:
            self.calculate_display_kpi(kpi_df)

    def calculate_display_kpi(self, kpi_df):
        total_numerator = 0
        total_denominator = 0
        total_save = True
        kpi_name = kpi_df[Const.KPI_NAME].values[0]
        kpi_fk = self.common.get_kpi_fk_by_kpi_name(kpi_name)
        if kpi_fk is None:
            Log.warning("There is no matching Kpi fk for kpi name: " + kpi_name)
            return
        for i, row in kpi_df.iterrows():
            self.kpi_excluding = row[[Const.EXCLUDE_EMPTY, Const.EXCLUDE_HANGER, Const.EXCLUDE_IRRELEVANT,
                                         Const.EXCLUDE_POSM, Const.EXCLUDE_OTHER, Const.STACKING, Const.EXCLUDE_SKU,
                                         Const.EXCLUDE_STOCK, Const.EXCLUDE_OSD]]
            per_scene_type = row[Const.PER_SCENE_TYPE]
            df = self.filter_df(row)
            if per_scene_type == Const.EACH:
                total_save = False
                scene_types = row[Const.SCENE_TYPE].split(',')
                if scene_types != "":
                    scene_types = [item.strip() for item in scene_types]
                for sc in scene_types:
                    df_scene = df[df['template_name'] == sc]
                    denominator = len(set(df_scene['scene_fk']))
                    if denominator == 0:
                        continue
                    numerator = len(set(df_scene[df_scene['product_type'] == 'SKU']['scene_fk']))
                    result = float(numerator) / float(denominator)
                    context_id = df_scene['template_fk'].values[0]
                    self.common.write_to_db_result(fk=kpi_fk, numerator_id=self.store_id, denominator_id=self.store_id,
                                                   context_id=context_id,
                                                   numerator_result=numerator, denominator_result=denominator,
                                                   result=result, score=result)
            else:
                total_numerator += len(set(df[df['product_type'] == 'SKU']['scene_fk']))
                total_denominator += len(set(df['scene_fk']))
        if total_save:
            result = float(total_numerator) / float(total_denominator) if (total_denominator != 0) else 0
            self.common.write_to_db_result(fk=kpi_fk, numerator_id=self.store_id, denominator_id=self.store_id,
                                           numerator_result=total_numerator, denominator_result=total_denominator,
                                           result=result, score=result)


    def calculate_facings_sos_kpi(self, kpi_df):
        kpi_name = kpi_df[Const.KPI_NAME].values[0]
        kpi_fk = self.common.get_kpi_fk_by_kpi_name(kpi_name)
        if kpi_fk is None:
            Log.warning("There is no matching Kpi fk for kpi name: " + kpi_name)
            return
        entity_name = kpi_df[Const.NUMERATOR_ENTITY].values[0]
        entity_name_for_fk = Const.NAME_TO_FK[entity_name]

        # iterate all categories (if kpi_df length > 1)
        for i, row in kpi_df.iterrows():
            filters = {}
            self.kpi_excluding = row[[Const.EXCLUDE_EMPTY, Const.EXCLUDE_HANGER, Const.EXCLUDE_IRRELEVANT,
                                      Const.EXCLUDE_POSM, Const.EXCLUDE_OTHER, Const.STACKING, Const.EXCLUDE_SKU,
                                      Const.EXCLUDE_STOCK, Const.EXCLUDE_OSD]]
            df = self.filter_df(row)
            if df.empty:
                continue

            category = row[Const.CATEGORY]
            if category != "":
                denominator_id = self.all_products[self.all_products['category']==category]['category_fk'].iloc[0]
                filters['category'] = category
            else:
                denominator_id = self.store_id
            all_denominators = df[entity_name].drop_duplicates().tolist()
            if row[Const.NUMERATOR] != "":
                all_denominators = [row[Const.NUMERATOR]]
            denominator = self.tools.get_filter_condition(df, **filters).sum()

            # iterate all enteties
            for entity in all_denominators:
                filters[entity_name] = entity
                numerator = self.tools.get_filter_condition(df, **filters).sum()
                del filters[entity_name]
                if numerator == 0:
                    continue
                result = float(numerator) / float(denominator)
                numerator_id = df[df[entity_name] == entity][entity_name_for_fk].values[0]
                self.common.write_to_db_result(fk=kpi_fk, numerator_id=numerator_id, denominator_id=denominator_id,
                                               numerator_result=numerator, denominator_result=denominator,
                                               result=result, score=result)

    def calculate_linear_sos_kpi(self, kpi_df):
        kpi_name = kpi_df[Const.KPI_NAME].values[0]
        kpi_fk = self.common.get_kpi_fk_by_kpi_name(kpi_name, get_numerator=False)
        if kpi_fk is None:
            Log.warning("There is no matching Kpi fk for kpi name: " + kpi_name)
            return
        entity_name = kpi_df[Const.NUMERATOR_ENTITY].values[0]
        entity_name_for_fk = Const.NAME_TO_FK[entity_name]
        results_dict = {}
        for i, row in kpi_df.iterrows():
            filters = {}
            scene_size = row[Const.SCENE_SIZE]
            self.kpi_excluding = row[[Const.EXCLUDE_EMPTY, Const.EXCLUDE_HANGER, Const.EXCLUDE_IRRELEVANT,
                                      Const.EXCLUDE_POSM, Const.EXCLUDE_OTHER, Const.STACKING, Const.EXCLUDE_SKU,
                                      Const.EXCLUDE_STOCK, Const.EXCLUDE_OSD]]
            # filter df to the specific template row
            df = self.filter_df(row)
            if df.empty:
                continue

            if row[Const.PER_SCENE_TYPE] == Const.EACH:
                scene_types = row[Const.SCENE_TYPE].split(",")
                scene_types = [item.strip() for item in scene_types]
            else:
                scene_types = [""]

            # Iterate scene types
            for sc in scene_types:
                if sc != "":
                    try:
                        context_id = self.templates[self.templates['template_name'] == sc]['template_fk'].iloc[0]
                    except:
                        Log.warning("No scene type with the following name: " + str(sc))
                        continue
                    filters['template_name'] = sc
                else:
                    context_id = 0

                category = row[Const.CATEGORY]
                if category != "":
                    if category == Const.EACH:
                        categories = set(self.df['category'])
                    else:
                        categories = [category]
                else:
                    categories = [""]

                # Iterate categories
                for category in categories:
                    if category != "":
                        denominator_id = self.all_products[self.all_products['category'] == category]['category_fk'].iloc[0]
                        filters['category'] = category
                    else:
                        denominator_id = self.store_id

                    all_numerators = df[entity_name].drop_duplicates().values.tolist()
                    if row[Const.NUMERATOR] != "":
                        all_numerators = [row[Const.NUMERATOR]]
                    denominator = scene_size if scene_size != "" else \
                        df[self.tools.get_filter_condition(df, **filters)]['width_mm_x'].sum()
                    if denominator == 0:
                        continue
                    for entity in all_numerators:
                        filters[entity_name] = entity
                        numerator = df[self.tools.get_filter_condition(df, **filters)]['width_mm_x'].sum()
                        del filters[entity_name]
                        if numerator == 0:
                            continue
                        result = float(numerator) / float(denominator) if denominator != 0 else 0
                        try:
                            numerator_id = self.all_products[self.all_products[entity_name] ==
                                                             entity][entity_name_for_fk].values[0]
                        except:
                            Log.warning("No entity in this name " + entity)
                            numerator_id = -1
                        if (numerator_id, denominator_id, context_id) not in results_dict.keys():
                            results_dict[numerator_id, denominator_id, context_id] = [result, numerator, denominator]
                        else:
                            results_dict[numerator_id, denominator_id] = map(sum,
                                 zip(results_dict[numerator_id, denominator_id, context_id],
                                                                [result, numerator, denominator]))

        for numerator_id, denominator_id, context_id in results_dict.keys():
            result, numerator, denominator = results_dict[numerator_id, denominator_id, context_id]
            self.common.write_to_db_result(fk=kpi_fk, numerator_id=numerator_id, denominator_id=denominator_id,
                                            context_id=context_id, numerator_result=numerator,
                                           denominator_result=denominator, result=result, score=result)

    def filter_df(self, kpi_df):
        df = self.df.copy()
        # filter scene_types

        scene_types = kpi_df[Const.SCENE_TYPE].split(',')
        if scene_types != "":
            scene_types = [item.strip() for item in scene_types]
            df = df[df['template_name'].isin(scene_types)]

        # filter category
        category = kpi_df[Const.CATEGORY].strip()
        if (category != "" and category != Const.EACH):
            df = df[df['category'] == category]

        # filter excludings
        return self.filter_excluding(df)

    def filter_out_osd(self, df):
        if df.empty:
            return df
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
                scene_df = scene_df[scene_df['shelf_number_from_bottom'] <= shelfs_to_include]

            # filter df to remove shelves with given ean code
            if row[Const.HAS_OSD].values[0] == Const.YES:
                products_to_filter = row[Const.POSM_EAN_CODE].values[0].split(",")
                products_df = scene_df[scene_df['product_ean_code'].isin(products_to_filter)][['scene_fk',
                                                                                        'bay_number','shelf_number']]
                if not products_df.empty:
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
                if not products_df.empty:
                    for index, p in products_df.iterrows():
                        scene_df = scene_df[~((scene_df['scene_fk'] == p['scene_fk']) &
                                             (scene_df['bay_number'] == p['bay_number']) &
                                             (scene_df['shelf_number'] == p['shelf_number']))]
                df_list.append(scene_df)
        final_df = pd.concat(df_list)
        return final_df

    def filter_in_osd(self, df):
        df_list = []
        scene_types = set(df['template_name'])
        for s in scene_types:
            scene_df = df[df['template_name'] == s]
            row = self.find_row_osd(s)
            if row.empty:
                continue

            # filter df to only shelf_to_include or higher shelves
            if row[Const.STORAGE_EXCLUSION_PRICE_TAG == 'N']:
                shelfs_to_include = row[Const.OSD_NUMBER_OF_SHELVES].values[0]
                if shelfs_to_include != "":
                    shelfs_to_include = int(shelfs_to_include)
                    df_list.append(scene_df[scene_df['shelf_number_from_bottom'] > shelfs_to_include])

            # if no osd rule is applied
            if (row[Const.HAS_OSD].values[0] == Const.NO):
                continue

            # filter df to have only shelves with given ean code
            if row[Const.HAS_OSD].values[0] == Const.YES:
                products_to_filter = row[Const.POSM_EAN_CODE].values[0].split(",")
                products_df = scene_df[scene_df['product_ean_code'].isin(products_to_filter)][['scene_fk',
                                                                                        'bay_number','shelf_number']]

                const_scene_df = scene_df.copy()
                if not products_df.empty:
                    for index, p in products_df.iterrows():
                        scene_df = const_scene_df[((const_scene_df['scene_fk'] == p['scene_fk']) &
                                              (const_scene_df['shelf_number'] == p['shelf_number']))]
                        df_list.append(scene_df)
        if len(df_list) != 0:
            final_df = pd.concat(df_list)
        else:
            final_df = pd.DataFrame(columns=df.columns)
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
        # if self.kpi_excluding[Const.EXCLUDE_HANGER] == Const.EXCLUDE:
        #     self.exclude hanger()
        # if self.kpi_excluding[Const.EXCLUDE_STOCK] == Const.EXCLUDE:
        #     self.exclude stock()
        if self.kpi_excluding[Const.EXCLUDE_OSD] == Const.EXCLUDE:
            df = self.filter_out_osd(df)
        elif self.kpi_excluding[Const.EXCLUDE_SKU] == Const.EXCLUDE:
            df = self.filter_in_osd(df)
        return df