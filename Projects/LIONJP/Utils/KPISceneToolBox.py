import os
import pandas as pd

from Trax.Algo.Calculations.Core.DataProvider import Data
from Trax.Cloud.Services.Connector.Keys import DbUsers

from Projects.LIONJP.Utils.SequenceCalculationsV2 import Sequence
from KPIUtils_v2.Calculations.CalculationsUtils.Constants import AdditionalAttr
from KPIUtils_v2.DB.PsProjectConnector import PSProjectConnector
from KPIUtils_v2.GlobalDataProvider.PsDataProvider import PsDataProvider

from Trax.Utils.Logging.Logger import Log
from Projects.LIONJP.Data.Consts import Consts

__author__ = 'satya'
is_debug = True


class LionJPSceneToolBox:

    def __init__(self, data_provider, output, common):
        self.output = output
        self.data_provider = data_provider
        self.common = common
        self.project_name = self.data_provider.project_name
        self.session_uid = self.data_provider.session_uid
        self.products = self.data_provider[Data.PRODUCTS]
        self.templates = self.data_provider[Data.TEMPLATES]
        self.all_products = self.data_provider[Data.ALL_PRODUCTS]
        self.scif = self.data_provider.scene_item_facts
        self.match_product_in_scene = self.data_provider[Data.MATCHES]
        self.visit_date = self.data_provider[Data.VISIT_DATE]
        self.session_info = self.data_provider[Data.SESSION_INFO]
        self.scene_info = self.data_provider[Data.SCENES_INFO]
        self.store_info = self.data_provider[Data.STORE_INFO]
        self.store_id = self.store_info.iloc[0].store_fk
        self.store_type = self.data_provider.store_type
        self.rds_conn = PSProjectConnector(self.project_name, DbUsers.CalculationEng)
        self.kpi_static_data = self.common.get_kpi_static_data()
        self.ps_data_provider = PsDataProvider(self.data_provider, self.output)
        self.targets = self.ps_data_provider.get_kpi_external_targets()
        self.match_display_in_scene = self.data_provider.match_display_in_scene
        self.setup_file = "setup.xlsx"
        self.kpi_sheet = self.get_setup(Consts.KPI_SHEET_NAME)
        self.kpi_template_file = "kpi_template.xlsx"
        self.kpi_template = self.get_kpi_template(Consts.KPI_CONFIG_SHEET)

    @staticmethod
    def get_minimum_facings(df, block_percentage):
        facings = 0
        if df.empty:
            return facings

        entities = df.groupby('entity')['product_fk'].count()
        percentage = entities.apply(lambda entity_facings: entity_facings * (block_percentage / 100.00))
        if len(percentage) > 1:
            facings = percentage.min()
        else:
            # when one of the entity is missing in the scene
            facings = 0

        if (facings > 0) and (facings < 1):
            facings = 1
        else:
            facings = int(facings)
        return facings

    def get_custom_entity_fk(self, name):
        query = """
            SELECT pk FROM static.custom_entity
            WHERE name='{}'
            """.format(name)
        data = pd.read_sql_query(query, self.rds_conn.db)
        return None if data.empty else data.values[0][0]

    def get_product_fks(self, ean_codes):
        product_pks = []
        query = """
            SELECT pk FROM static_new.product
            WHERE ean_code in {}
            """.format(ean_codes)
        query = query.replace(",)", ")")
        data = pd.read_sql_query(query, self.rds_conn.db)
        if data.empty:
            return product_pks
        else:
            product_pks = list(data['pk'].unique())
        return product_pks

    def get_kpi_template(self, sheet_name):
        kpi_template_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), '..', 'Data')
        kpi_template_path = os.path.join(kpi_template_path, self.kpi_template_file)
        kpi_template = pd.read_excel(kpi_template_path, sheet_name=sheet_name, skiprows=1)
        return kpi_template

    def get_kpi_level_2_fk(self, kpi_level_2_type):
        query = \
            """
            SELECT pk FROM static.kpi_level_2
            WHERE type = '{}';
            """.format(kpi_level_2_type)
        data = pd.read_sql_query(query, self.rds_conn.db)
        return None if data.empty else data.values[0][0]

    def get_kpi_name(self, kpi_level_2_fk):
        query = \
            """
            SELECT type kpi_name FROM static.kpi_level_2
            WHERE pk = {};
            """.format(kpi_level_2_fk)
        data = pd.read_sql_query(query, self.rds_conn.db)
        return None if data.empty else data.values[0][0]

    def get_setup(self, sheet_name):
        setup_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), '..', 'Data')
        setup_path = os.path.join(setup_path, self.setup_file)
        setup = pd.read_excel(setup_path, sheet_name=sheet_name)
        return setup

    def main_calculation(self):
        """
        This function calculates the KPI results.
        """
        try:
            if self.kpi_sheet.empty:
                Log.error("'kpi_list' sheet in setup file is empty.")
                return
            kpi_types = [x.strip() for x in self.kpi_sheet[Consts.KPI_TYPE].unique()]

            for kpi_type in kpi_types:
                kpis = self.kpi_sheet[self.kpi_sheet[Consts.KPI_TYPE] == kpi_type]
                if kpi_type == Consts.FSOS:
                    self.main_sos_calculations(kpis)
                elif kpi_type == Consts.ADJACENCY:
                    print("scene_fk:{}".format(self.data_provider.scene_id))
                    self.main_adjacency_calculations(kpis)
                else:
                    Log.warning("KPI_TYPE:{kt} not found in setup=>kpi_list sheet.".format(kt=kpi_type))
                    continue
            # self.common.commit_results_data()
            return
        except Exception as err:
            Log.error("LionJP KPI calculation failed due to the following error: {}".format(err))

    def main_sos_calculations(self, kpis):
        for row_number, row_data in kpis.iterrows():
            if row_data[Consts.KPI_NAME] == Consts.FACINGS_IN_CELL_PER_PRODUCT:
                self.calculate_facings_in_cell_per_product()

    def main_adjacency_calculations(self, kpis):
        for row_number, row_data in kpis.iterrows():
            if row_data[Consts.KPI_NAME] == Consts.ADJACENCY_PRODUCT_GROUP_IN_SCENE_TYPE:
                self.calculate_adjacency_per_scene(Consts.ADJACENCY_PRODUCT_GROUP_IN_SCENE_TYPE)
            else:
                Log.warning("KPI_NAME:{kn} not found in setup=>kpi_list sheet.".format(kn=row_data[Consts.KPI_NAME]))

    @staticmethod
    def exclude_and_include_filter(adj_config):
        allowed_filters = []
        exclude_filters = []

        include_empty = adj_config['include_empty']
        include_irrelevant = adj_config['include_irrelevant']

        if include_empty == "include":
            allowed_filters.append("Empty")
        else:
            exclude_filters.append("Empty")

        if include_irrelevant == "include":
            allowed_filters.append("Irrelevant")
        else:
            exclude_filters.append("Irrelevant")

        exclude_filters.append("POS")

        if len(allowed_filters) == 0:
            allowed_products_filters = None
        else:
            allowed_products_filters = {"product_type": allowed_filters}

        if not pd.isnull(adj_config['include_other_ean_codes']):
            include_others_eans = ["{}".format(other.strip()) for other in adj_config['include_other_ean_codes'].split(",")]
            allowed_products_filters["product_ean_code"] = include_others_eans

        if len(exclude_filters) == 0:
            exclude_products_filters = None
        else:
            exclude_products_filters = {"product_type": exclude_filters}

        return exclude_products_filters, allowed_products_filters

    def build_entity_groups(self, adj_config, scene_fk):

        entity_1_type = adj_config['entity_1_type']
        entity_2_type = adj_config['entity_2_type']
        entity_1_values = [item.strip() for item in adj_config['entity_1_values'].strip().split(",")]
        entity_2_values = [item.strip() for item in adj_config['entity_2_values'].strip().split(",")]

        entities = [{"entity_name": "entity_1", "entity_type": entity_1_type, "entity_values": entity_1_values},
                    {"entity_name": "entity_2", "entity_type": entity_2_type, "entity_values": entity_2_values}]

        df_entities = pd.DataFrame()
        for entity in entities:
            entity_type = entity['entity_type']
            entity_values = entity['entity_values']
            if entity_type == "product":
                df_entity = self.data_provider.all_products[['product_fk', 'product_ean_code']].copy()
                df_entity = df_entity[['product_fk']][(df_entity['product_ean_code'].isin(entity_values))]
                df_entity['entity'] = entity['entity_name']
            elif entity_type == "brand":
                df_entity = self.data_provider.all_products[['product_fk', 'brand_name']].copy()
                df_entity = df_entity[['product_fk']][(df_entity['brand_name'].isin(entity_values))]
                df_entity['entity'] = entity['entity_name']
            elif entity_type == "category":
                df_entity = self.data_provider.all_products[['product_fk', 'category_name']].copy()
                df_entity = df_entity[['product_fk']][(df_entity['category_name'].isin(entity_values))]
                df_entity['entity'] = entity['entity_name']
            elif entity_type == "sub_category":
                df_entity = self.data_provider.all_products[['product_fk', 'sub_category_name']].copy()
                df_entity = df_entity[['product_fk']][(df_entity['sub_category_name'].isin(entity_values))]
                df_entity['entity'] = entity['entity_name']
            else:
                Log.error("{} invalid entity_type".format(entity_type))
                return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()
            df_entities = df_entities.append(df_entity)

        ## Adding include_other_ean_codes to entity groups if included
        if not pd.isnull(adj_config['include_other_ean_codes']):
            include_others = tuple(["{}".format(other.strip()) for other in adj_config['include_other_ean_codes'].split(",")])
            include_others_pks = []
            if len(include_others) != 0:
                product_fks = self.get_product_fks(include_others)
                include_others_pks.extend(product_fks)
            if len(include_others_pks) > 0:
                # add th ese product fks to both entity group
                for others_product_fk in include_others_pks:
                    row = {"entity": "other", "product_fk": others_product_fk}
                    df_entities = df_entities.append(row, ignore_index=True)
                    # for entity in entities:
                    #     row = {"product_fk": others_product_fk, "entity": "other"}
                    #     df_entities = df_entities.append(row,  ignore_index=True)
                    # row = {"product_fk": others_product_fk}
                    # df_entities = df_entities.append(row, ignore_index=True)

        df_entities = df_entities.reset_index(drop=True)

        product_pks = list(df_entities['product_fk'].unique())

        df_product_pks_in_scene = self.scif[(self.scif['scene_id'] == scene_fk) &
                                            (self.scif['item_id'].isin(product_pks)) &
                                            (self.scif['facings_ign_stack'] > 0)]

        if df_product_pks_in_scene.empty:
            if is_debug:
                Log.warning("Products:{} not in scene{}:scene_fk".format(product_pks, scene_fk))
            return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()
        else:
            df_custom_matches = self.data_provider.matches.copy()
            df_custom_matches = df_custom_matches.merge(df_entities, on="product_fk")
            df_custom_matches = df_custom_matches[(df_custom_matches['scene_fk'] == scene_fk) &
                                                  # (df_custom_matches['face_count'] > 0) &
                                                  (df_custom_matches['stacking_layer'] == 1)]

            blocking_percentage = adj_config['min_block_percentage']

            if int(adj_config['min_overlap_percentage']) != 0:
                overlap_percentage = int(adj_config['min_overlap_percentage'])/100.00
            else:
                overlap_percentage = 0.1

            df_custom_matches = df_custom_matches.drop_duplicates()

            df = df_custom_matches[(df_custom_matches['scene_fk'] == scene_fk)]
            minimum_tags_per_entity = self.get_minimum_facings(df, blocking_percentage)
            population = {'entity': list(df_entities['entity'].unique())}
            exclude_filter, allowed_products_filter = self.exclude_and_include_filter(adj_config)

        if adj_config['orientation'].lower() == "vertical":
            direction = "DOWN"
        elif adj_config['orientation'].lower() == "horizontal":
            direction = "RIGHT"
        else:
            Log.warning(
                "Invalid direction:{}. Resetting to default orientation RIGHT".format(adj_config['orientation']))
            direction = "RIGHT"

        sequence_params = {AdditionalAttr.DIRECTION: direction,
                           AdditionalAttr.EXCLUDE_FILTER: exclude_filter,
                           AdditionalAttr.CHECK_ALL_SEQUENCES: True,
                           AdditionalAttr.STRICT_MODE: False,
                           AdditionalAttr.REPEATING_OCCURRENCES: True,
                           AdditionalAttr.INCLUDE_STACKING: False,
                           AdditionalAttr.ALLOWED_PRODUCTS_FILTERS: allowed_products_filter,
                           AdditionalAttr.MIN_TAGS_OF_ENTITY: minimum_tags_per_entity,
                           AdditionalAttr.ADJACENCY_OVERLAP_RATIO: overlap_percentage}

        return population, sequence_params, df_custom_matches

    def calculate_adjacency_per_scene(self, kpi_name):
        allowed_entities = ["product", "brand", "category", "sub_category"]
        kpi_config = self.kpi_template[self.kpi_template["kpi_name"] == kpi_name].copy()
        kpi_config['visit_date'] = pd.to_datetime(self.visit_date)
        kpi_config['start_date'] = pd.to_datetime(kpi_config['start_date'])
        kpi_config['end_date'] = pd.to_datetime(kpi_config['end_date'])
        kpi_config = kpi_config[kpi_config['visit_date'].between(kpi_config['start_date'],
                                                                 kpi_config['end_date'],
                                                                 inclusive=True)]

        kpi_level_2_fk = self.get_kpi_level_2_fk(kpi_name)

        if kpi_config.empty:
            message = "kpi_name:{} ".format(kpi_name)
            message += " not found in static.kpi_level_2 table"
            Log.warning(message)
            return

        for idx, adj_config in kpi_config.iterrows():
            custom_entity_pk = self.get_custom_entity_fk(adj_config['report_label'])
            scene_types = [x.strip() for x in adj_config['scene_type'].split(",")]
            df_scene_template = self.scif[['scene_fk', 'template_fk']][self.scif['template_name'].isin(scene_types)]
            df_scene_template = df_scene_template.drop_duplicates()
            for row_num, row_data in df_scene_template.iterrows():
                scene_fk = row_data['scene_fk']
                template_fk = row_data['template_fk']

                if adj_config['entity_1_type'] in allowed_entities and adj_config['entity_2_type'] in allowed_entities:
                    location = {"scene_fk": scene_fk}
                    population, sequence_params, custom_matches = self.build_entity_groups(adj_config, scene_fk)

                    if custom_matches.empty:
                        if is_debug:
                            Log.warning("scene_fk:{}, Custom Entities are not found in the scene".format(scene_fk))
                        continue

                    if sequence_params[AdditionalAttr.MIN_TAGS_OF_ENTITY] == 0:
                        continue

                    seq = Sequence(self.data_provider, custom_matches)
                    sequence_res = seq.calculate_sequence(population, location, sequence_params, adj_config['report_label'])
                    result_count = len(sequence_res)
                    result = 1 if result_count > 0 else 0
                    score = result
                    if result > 0:
                        if is_debug:
                            Log.warning("scene_fk:{}, report_label:{}, result={}".format(scene_fk,
                                                                                         adj_config['report_label'],
                                                                                         result))

                    self.common.write_to_db_result(fk=kpi_level_2_fk,
                                                   numerator_id=custom_entity_pk,
                                                   denominator_id=template_fk,
                                                   context_id=self.store_id,
                                                   numerator_result=result_count,
                                                   denominator_result=scene_fk,
                                                   result=result,
                                                   score=score,
                                                   target=sequence_params[AdditionalAttr.MIN_TAGS_OF_ENTITY],
                                                   by_scene=True)
                else:
                    Log.warning("Invalid entity:{}".format(adj_config['entity']))

    def calculate_facings_in_cell_per_product(self):
        kpi_db = self.kpi_static_data[
            (self.kpi_static_data[Consts.KPI_FAMILY] == Consts.PS_KPI_FAMILY)
            & (self.kpi_static_data[Consts.KPI_NAME_DB] == Consts.FACINGS_IN_CELL_PER_PRODUCT)
            & (self.kpi_static_data['delete_time'].isnull())]

        if kpi_db.empty:
            print("KPI Name:{} not found in DB".format(Consts.FACINGS_IN_CELL_PER_PRODUCT))
        else:
            print("KPI Name:{} found in DB".format(Consts.FACINGS_IN_CELL_PER_PRODUCT))
            kpi_fk = kpi_db.pk.values[0]
            match_prod_scene_data = self.match_product_in_scene.merge(
                self.products, how='left', on='product_fk', suffixes=('', '_prod'))
            grouped_data = match_prod_scene_data.query(
                '(stacking_layer==1) or (product_type=="POS")'
            ).groupby(
                ['scene_fk', 'bay_number', 'shelf_number', 'product_fk']
            )
            for data_tup, scene_data_df in grouped_data:
                scene_fk, bay_number, shelf_number, product_fk = data_tup
                facings_count_in_cell = len(scene_data_df)
                cur_template_fk = int(self.scene_info[self.scene_info['scene_fk'] == scene_fk].get('template_fk'))
                self.common.write_to_db_result(fk=kpi_fk,
                                               numerator_id=product_fk,
                                               denominator_id=self.store_id,
                                               context_id=cur_template_fk,
                                               numerator_result=bay_number,
                                               denominator_result=shelf_number,
                                               result=facings_count_in_cell,
                                               score=facings_count_in_cell,
                                               by_scene=True)
