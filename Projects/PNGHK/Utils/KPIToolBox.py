import pandas as pd
import os

from Trax.Algo.Calculations.Core.DataProvider import Data
from Trax.Cloud.Services.Connector.Keys import DbUsers
from KPIUtils_v2.DB.PsProjectConnector import PSProjectConnector
from Projects.PNGHK.Data.Const import Const
from KPIUtils_v2.DB.CommonV2 import Common
from Trax.Utils.Logging.Logger import Log
from KPIUtils_v2.Calculations.CalculationsUtils.GENERALToolBoxCalculations import GENERALToolBox

__author__ = 'ilays'

PATH = os.path.join(os.path.dirname(os.path.realpath(__file__)), '..', 'Data', '06_PNGHK_template_2019_15_04.xlsx')


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
        self.match_probe_in_scene = self.get_product_special_attribute_data(self.session_uid)
        self.kpi_static_data = self.common.get_kpi_static_data()
        self.kpi_results_queries = []
        self.kpis_sheet = pd.read_excel(PATH, Const.KPIS).fillna("")
        self.osd_rules_sheet = pd.read_excel(PATH, Const.OSD_RULES).fillna("")
        self.kpi_excluding = pd.DataFrame()
        self.df = pd.DataFrame()
        self.tools = GENERALToolBox(self.data_provider)
        self.templates = self.data_provider[Data.ALL_TEMPLATES]

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
        ratio = 1
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
                        denominator_id = self.all_products[self.all_products['category'] ==
                                                                category]['category_fk'].iloc[0]
                        filters['category'] = category
                        all_numerators = self.df[self.df['category'] ==
                                               category][entity_name].drop_duplicates().values.tolist()
                    else:
                        denominator_id = self.store_id
                        all_numerators = df[entity_name].drop_duplicates().values.tolist()

                    if row[Const.NUMERATOR] != "":
                        all_numerators = [row[Const.NUMERATOR]]
                    denominator = df[self.tools.get_filter_condition(df, **filters)]['width_mm_advance'].sum()
                    if denominator == 0:
                        continue
                    if scene_size != "":
                        ratio = scene_size / denominator
                        denominator = scene_size
                    for entity in all_numerators:
                        filters[entity_name] = entity
                        numerator = df[self.tools.get_filter_condition(df, **filters)]['width_mm_advance'].sum()
                        del filters[entity_name]
                        if scene_size != "":
                            numerator = numerator * ratio
                        try:
                            numerator_id = self.all_products[self.all_products[entity_name] ==
                                                             entity][entity_name_for_fk].values[0]
                        except:
                            Log.warning("No entity in this name " + entity)
                            numerator_id = -1
                        if (numerator_id, denominator_id, context_id) not in results_dict.keys():
                            results_dict[numerator_id, denominator_id, context_id] = [numerator, denominator]
                        else:
                            results_dict[numerator_id, denominator_id, context_id] = map(sum,
                                                         zip(results_dict[numerator_id, denominator_id, context_id],
                                                             [numerator, denominator]))
        if len(results_dict) == 0:
            return

        results_as_df = pd.DataFrame.from_dict(results_dict, orient="index")

        # numerator became column 0, denominator column 1, result will enter column 2
        filtered_results_as_df = results_as_df[results_as_df[0] != 0]
        filtered_df = filtered_results_as_df.copy()
        filtered_df[2] = filtered_results_as_df[0] / filtered_results_as_df[1]
        filtered_results_as_dict = filtered_df.to_dict(orient="index")

        for numerator_id, denominator_id, context_id in filtered_results_as_dict.keys():
            result_row = filtered_results_as_dict[numerator_id, denominator_id, context_id]
            result, numerator, denominator = result_row[2], result_row[0], result_row[1]
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

        # filter excludings
        df = self.filter_excluding(df)

        # filter category
        category = kpi_df[Const.CATEGORY].strip()
        if (category != "" and category != Const.EACH):
            df = df[df['category'] == category]

        return df

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
                scene_df = scene_df[scene_df['shelf_number_from_bottom'] < shelfs_to_include]

            # filter df to remove shelves with given ean code
            if row[Const.HAS_OSD].values[0] == Const.YES:
                products_to_filter = row[Const.POSM_EAN_CODE].values[0].split(",")
                if products_to_filter != "":
                    products_to_filter = [item.strip() for item in products_to_filter]
                products_df = scene_df[scene_df['product_ean_code'].isin(products_to_filter)][['scene_fk',
                                                                                               'shelf_number']]
                products_df = products_df.drop_duplicates()
                if not products_df.empty:
                    for index, p in products_df.iterrows():
                        scene_df = scene_df[~((scene_df['scene_fk'] == p['scene_fk']) &
                                              (scene_df['shelf_number'] == p['shelf_number']))]
                df_list.append(scene_df)

            # filter df to remove shelves with given ean code (only on the same bay)
            if row[Const.HAS_HOTSPOT].values[0] == Const.YES:
                products_to_filter = row[Const.POSM_EAN_CODE_HOTSPOT].values[0].split(",")
                if products_to_filter != "":
                    products_to_filter = [item.strip() for item in products_to_filter]
                products_df = scene_df[scene_df['product_ean_code'].isin(products_to_filter)][['scene_fk',
                                                                                               'bay_number',
                                                                                               'shelf_number']]
                products_df = products_df.drop_duplicates()
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
            const_scene_df = scene_df.copy()
            row = self.find_row_osd(s)
            if row.empty:
                continue

            # filter df include OSD when needed
            shelfs_to_include = row[Const.OSD_NUMBER_OF_SHELVES].values[0]
            if shelfs_to_include != "":
                shelfs_to_include = int(shelfs_to_include)
                df_list.append(scene_df[scene_df['shelf_number_from_bottom'] >= shelfs_to_include])

            # if no osd rule is applied
            if row[Const.HAS_OSD].values[0] == Const.NO:
                continue

            # filter df to have only shelves with given ean code
            if row[Const.HAS_OSD].values[0] == Const.YES:
                products_to_filter = row[Const.POSM_EAN_CODE].values[0].split(",")
                if products_to_filter != "":
                    products_to_filter = [item.strip() for item in products_to_filter]
                products_df = scene_df[scene_df['product_ean_code'].isin(products_to_filter)][['scene_fk',
                                                                                               'shelf_number']]
                products_df = products_df.drop_duplicates()
                if not products_df.empty:
                    for index, p in products_df.iterrows():
                        scene_df = const_scene_df[((const_scene_df['scene_fk'] == p['scene_fk']) &
                                                   (const_scene_df['shelf_number'] == p['shelf_number']))]
                        df_list.append(scene_df)

            if row[Const.HAS_HOTSPOT].values[0] == Const.YES:
                products_to_filter = row[Const.POSM_EAN_CODE_HOTSPOT].values[0].split(",")
                if products_to_filter != "":
                    products_to_filter = [item.strip() for item in products_to_filter]
                products_df = scene_df[scene_df['product_ean_code'].isin(products_to_filter)][['scene_fk',
                                                                                               'bay_number',
                                                                                               'shelf_number']]
                products_df = products_df.drop_duplicates()
                if not products_df.empty:
                    for index, p in products_df.iterrows():
                        scene_df = const_scene_df[~((const_scene_df['scene_fk'] == p['scene_fk']) &
                                              (const_scene_df['bay_number'] == p['bay_number']) &
                                              (const_scene_df['shelf_number'] == p['shelf_number']))]
                        df_list.append(scene_df)
        if len(df_list) != 0:
            final_df = pd.concat(df_list)
        else:
            final_df = pd.DataFrame(columns=df.columns)
        final_df = final_df.drop_duplicates()
        return final_df

    def find_row_osd(self, s):
        rows = self.osd_rules_sheet[self.osd_rules_sheet[Const.SCENE_TYPE].str.encode("utf8") == s.encode("utf8")]
        row = rows[rows[Const.RETAILER] == self.store_info['retailer_name'].values[0]]
        return row

    def filter_excluding(self, df):

        if self.kpi_excluding[Const.EXCLUDE_OSD] == Const.EXCLUDE:
            df = self.filter_out_osd(df)
        elif self.kpi_excluding[Const.EXCLUDE_SKU] == Const.EXCLUDE:
            df = self.filter_in_osd(df)

        if self.kpi_excluding[Const.EXCLUDE_STOCK] == Const.EXCLUDE:
            df = self.exclude_special_attribute_products(df, Const.DB_STOCK_NAME)
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
        return df

    def exclude_special_attribute_products(self, df, smart_attribute):
        """
        Helper to exclude smart_attribute products
        :return: filtered df without smart_attribute products
        """
        if self.match_probe_in_scene.empty:
            return df
        smart_attribute_df = self.match_probe_in_scene[self.match_probe_in_scene['name'] == smart_attribute]
        if smart_attribute_df.empty:
            return df
        match_product_in_probe_fks = smart_attribute_df['match_product_in_probe_fk'].tolist()
        df = df[~df['probe_match_fk'].isin(match_product_in_probe_fks)]
        return df

    def get_product_special_attribute_data(self, session_uid):
        query = """
                SELECT * FROM probedata.match_product_in_probe_state_value A
                left join probedata.match_product_in_probe B on B.pk = A.match_product_in_probe_fk
                left join static.match_product_in_probe_state C on C.pk = A.match_product_in_probe_state_fk
                left join probedata.probe on probe.pk = probe_fk
                where session_uid = '{}';
            """.format(session_uid)

        df = pd.read_sql_query(query, self.rds_conn.db)
        return df
