import os
import pandas as pd

from KPIUtils_v2.Calculations.SequenceCalculationsV2 import Sequence
from KPIUtils_v2.Calculations.CalculationsUtils.Constants import AdditionalAttr
from KPIUtils_v2.DB.PsProjectConnector import PSProjectConnector
from KPIUtils_v2.DB.CommonV2 import Common
from KPIUtils_v2.GlobalDataProvider.PsDataProvider import PsDataProvider

from Trax.Cloud.Services.Connector.Keys import DbUsers
from Trax.Algo.Calculations.Core.DataProvider import Data
from Trax.Utils.Logging.Logger import Log
from Projects.LIONJP_SAND.Data.Consts import Consts


__author__ = 'satya'


class LIONJP_SANDToolBox:
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
        self.store_id = self.data_provider[Data.STORE_FK]
        self.scif = self.data_provider[Data.SCENE_ITEM_FACTS]
        self.rds_conn = PSProjectConnector(self.project_name, DbUsers.CalculationEng)
        self.kpi_static_data = self.common.get_kpi_static_data()
        self.kpi_results_queries = []
        self.ps_data_provider = PsDataProvider(self.data_provider, self.output)
        self.targets = self.ps_data_provider.get_kpi_external_targets()
        self.setup_file = "setup.xlsx"
        self.kpi_sheet = self.get_setup(Consts.KPI_SHEET_NAME)

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

    def main_calculation(self, *args, **kwargs):
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
                    self.main_adjacency_calculations(kpis)
                else:
                    Log.warning("KPI_TYPE:{kt} not found in setup=>kpi_list sheet.".format(kt=kpi_type))
                    continue
            self.common.commit_results_data()
            return
        except Exception as err:
            Log.error("LionJP KPI calculation failed due to the following error: {}".format(err))

    def main_sos_calculations(self, kpis):
        for row_number, row_data in kpis.iterrows():
            if row_data[Consts.KPI_NAME] == Consts.FACINGS_IN_CELL_PER_PRODUCT:
                self.calculate_facings_in_cell_per_product()

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
                                               score=scene_fk)

    def main_adjacency_calculations(self, kpis):
        for row_number, row_data in kpis.iterrows():
            if row_data[Consts.KPI_NAME] == Consts.ADJACENCY_PRODUCT_GROUP_IN_SCENE_TYPE:
                kpi_level_2_fk = self.get_kpi_level_2_fk(Consts.ADJACENCY_PRODUCT_GROUP_IN_SCENE_TYPE)
                self.calculate_adjacency_per_scene(kpi_level_2_fk)
            else:
                Log.warning("KPI_NAME:{kn} not found in setup=>kpi_list sheet.".format(kn=row_data[Consts.KPI_NAME]))

    @staticmethod
    def prepare_data_adjacency(kpi_config):
        adj_config_list = []
        for row_number, row_data in kpi_config.iterrows():
            adj_config = {"custom_entity_fk": row_data['custom_entity_fk'],
                          "template_fks": row_data['template_fks'],
                          "group_entity": row_data['group_entity'],
                          "orientation": row_data['orientation'],
                          "minimum_tagging": row_data['minimum_tagging'],
                          "sequence_mode": row_data['sequence_mode'],
                          "include_stacking": row_data['include_stacking'],
                          "include_empty": row_data['include_empty'],
                          "include_others": row_data['include_others'],
                          "include_irrelevant": row_data['include_irrelevant'],
                          "include_pos": row_data['include_pos'],
                          "entity": row_data['entity'],
                          "entity_values": row_data['entity_values']}
            adj_config_list.append(adj_config)
        return adj_config_list

    @staticmethod
    def exclude_and_include_filter(adj_config):
        allowed_filters = []
        exclude_filters = []

        include_empty = adj_config['include_empty']
        include_others = adj_config['include_others']
        include_irrelevant = adj_config['include_irrelevant']
        include_pos = adj_config['include_pos']

        if include_empty == 0:
            allowed_filters.append("Empty")
        else:
            exclude_filters.append("Empty")

        if include_others == 0:
            allowed_filters.append("Other")
        else:
            exclude_filters.append("Other")

        if include_irrelevant == 0:
            allowed_filters.append("Irrelevant")
        else:
            exclude_filters.append("Irrelevant")

        if include_pos == 0:
            allowed_filters.append("POS")
        else:
            exclude_filters.append("POS")

        if len(allowed_filters) == 0:
            allowed_products_filters = None
        else:
            allowed_products_filters = {"product_type": allowed_filters}

        if len(exclude_filters) == 0:
            exclude_products_filters = None
        else:
            exclude_products_filters = {"product_type": exclude_filters}

        return exclude_products_filters, allowed_products_filters

    def build_ean_groups(self, adj_config):
        group_entities = adj_config['group_entity']
        entity_values = adj_config['entity_values']
        if group_entities.upper() == "Y":
            entity_values = [[u"{}".format(g) for g in group] for group in entity_values]
        entities = {}
        df_entities = pd.DataFrame()
        if group_entities.upper() == "Y":
            for entity_key, entity_value in enumerate(entity_values):
                if isinstance(entity_value, list):
                    entities["entity_{}".format(entity_key)] = entity_value
                else:
                    Log.warning("ean_codes are not in list of lists format")
                    return
        else:
            for entity_key, entity_value in enumerate(entity_values):
                entities["entity_{}".format(entity_key)] = entity_value

        for entity_key, entity_value in entities.items():
            df_entity = self.data_provider.all_products[['product_fk', 'product_ean_code']].copy()

            df_entity = df_entity[['product_fk']][df_entity['product_ean_code'].isin(entity_value)].copy()
            df_entity['entity'] = entity_key
            df_entities = df_entities.append(df_entity)

        df_entities = df_entities.reset_index(drop=True)
        df_custom_matches = self.data_provider.matches.copy()
        df_custom_matches = df_custom_matches.merge(df_entities, on="product_fk")

        population = {'entity': entities.keys()}
        exclude_filter, allowed_products_filter = self.exclude_and_include_filter(adj_config)

        if adj_config['orientation'].upper() == "VERTICALLY":
            direction = "DOWN"
        elif adj_config['orientation'].upper() == "HORIZONTALLY":
            direction = "RIGHT"
        else:
            Log.warning("Invalid direction:{}. Resetting to default orientation RIGHT".format(adj_config['orientation']))
            direction = "RIGHT"

        sequence_params = {AdditionalAttr.DIRECTION: direction,
                           AdditionalAttr.EXCLUDE_FILTER: exclude_filter,
                           AdditionalAttr.CHECK_ALL_SEQUENCES: False,
                           AdditionalAttr.STRICT_MODE: True if adj_config['sequence_mode'] == 1 else False,
                           AdditionalAttr.REPEATING_OCCURRENCES: True,
                           AdditionalAttr.INCLUDE_STACKING: True if adj_config['include_stacking'] == 1 else False,
                           AdditionalAttr.ALLOWED_PRODUCTS_FILTERS: allowed_products_filter,
                           AdditionalAttr.MIN_TAGS_OF_ENTITY: adj_config['minimum_tagging']}
        return population, sequence_params, df_custom_matches

    def calculate_adjacency_per_scene(self, kpi_level_2_fk):
        kpi_config = self.targets[self.targets["kpi_fk"] == kpi_level_2_fk]

        if kpi_config.empty:
            kpi_name = self.get_kpi_name(kpi_level_2_fk)
            message = "kpi_level_2_fk:{}, kpi_name:{} ".format(kpi_level_2_fk, kpi_name)
            message += " not found in static.kpi_external_targets table"
            Log.warning(message)
            return

        for idx, adj_config in kpi_config.iterrows():
            df_scene_template = self.scif[['scene_fk', 'template_fk']][self.scif['template_fk'].isin(adj_config['template_fks'])]
            df_scene_template = df_scene_template.drop_duplicates()
            for row_num, row_data in df_scene_template.iterrows():
                scene_fk = row_data['scene_fk']
                template_fk = row_data['template_fk']
                if adj_config['entity'] == "ean_code":
                    location = {"scene_fk": scene_fk}

                    population, sequence_params, custom_matches = self.build_ean_groups(adj_config)

                    if custom_matches.empty:
                        print("scene_fk:{}, Custom Entities are not found in the scene.".format(scene_fk))
                        continue

                    seq = Sequence(self.data_provider, custom_matches)
                    sequence_res = seq.calculate_sequence(population, location, sequence_params)
                    result_count = len(sequence_res)
                    result = 1 if result_count > 0 else 0
                    score = result
                    self.common.write_to_db_result(fk=kpi_level_2_fk,
                                                   numerator_id=adj_config['custom_entity_fk'],
                                                   denominator_id=template_fk,
                                                   context_id=scene_fk,
                                                   numerator_result=result_count,
                                                   denominator_result=0,
                                                   result=result,
                                                   score=score)
                else:
                    Log.warning("Invalid entity:{}".format(adj_config['entity']))
