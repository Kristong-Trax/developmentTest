# coding=utf-8

import pandas as pd
import json

from Trax.Algo.Calculations.Core.KPI.UnifiedKpiSingleton import UnifiedKPISingleton
from Projects.PNGJP_SAND2.Utils.KPIToolBox import PNGJP_SAND2ToolBox
from Projects.PNGJP_SAND2.Utils.KpiQualitative import PNGJP_SAND2KpiQualitative_ToolBox
from Projects.PNGJP_SAND2.Utils.KPIToolBox import log_runtime
from KPIUtils_v2.Utils.Parsers.ParseInputKPI import filter_df
from Trax.Utils.Logging.Logger import Log
from KPIUtils_v2.DB.CommonV2 import Common
from Trax.Algo.Calculations.Core.DataProvider import Data
from Trax.Cloud.Services.Connector.Keys import DbUsers
from KPIUtils_v2.DB.PsProjectConnector import PSProjectConnector
from Projects.PNGJP_SAND2.Utils.Fetcher import PNGJP_SAND2Queries
from Trax.Data.ProfessionalServices.PsConsts.DataProvider import MatchesConsts, ScifConsts, ProductsConsts, SceneInfoConsts
from pathlib import Path
from Projects.PNGJP_SAND2.Data.LocalConsts import Consts

class PNGJP_SAND2Util(UnifiedKPISingleton):
    MAX_SHELF = 'max_shelf'
    POS_FCOUNT_NORMAL = 'PGJAPAN_POSM_FCOUNT_NORMAL'
    POSM_FCOUNT_SCENE = 'PGJAPAN_POSM_FCOUNT_SCENE'
    LOCATION = 'location'
    POPULATION = 'population'
    INCLUDE = 'include'
    EXCLUDE = 'exclude'

    def __init__(self, output, data_provider):
        super(PNGJP_SAND2Util, self).__init__(data_provider)
        self.output = output
        self.data_provider = data_provider
        self.project_name = data_provider.project_name
        self.session_uid = self.data_provider.session_uid

        self.scif = self.data_provider[Data.SCENE_ITEM_FACTS]
        self.all_products = self.data_provider[Data.ALL_PRODUCTS]
        self.match_product_in_scene = self.data_provider[Data.MATCHES]
        self.scene_info = self.data_provider[Data.SCENES_INFO]
        self.all_templates = self.data_provider[Data.ALL_TEMPLATES]
        self.match_product_in_scene['count'] = 1
        self.matches_product = self.match_product_in_scene.merge(self.scif, on=[ScifConsts.PRODUCT_FK,
                                                                                ScifConsts.SCENE_FK],
                                                                 how='left')
        self.rds_conn = PSProjectConnector(self.project_name, DbUsers.CalculationEng)
        self.match_display_in_scene = self.get_match_display_in_scene()
        self.visit_date = self.data_provider[Data.VISIT_DATE]
        self.common = Common(self.data_provider)
        self.kpi_static_data = self.common.get_kpi_static_data()
        self.external_targets = self.get_all_kpi_external_targets()
        self.all_targets_unpacked = self.unpack_all_external_targets()
        self.bay_shelf_info = self.get_bay_shelf_combination_from_custom_entity()
        self.bay_shelf_entity_type_fk = self.get_bay_shelf_entity_type_fk()
        shelf_range_template_path = Path(__file__).parent / '../Data/shelf_range_template.xlsx'
        self.shelf_range_template = pd.read_excel(shelf_range_template_path)
        self.session_info = self.data_provider[Data.SESSION_INFO]
        self.store_fk = self.session_info['store_fk'].values[0]

    def get_match_display_in_scene(self):
        query = PNGJP_SAND2Queries.get_match_display(self.session_uid)
        match_display = pd.read_sql_query(query, self.rds_conn.db)
        match_display = match_display[match_display[SceneInfoConsts.SCENE_FK].\
                                                isin(self.scene_info[SceneInfoConsts.SCENE_FK].values)]
        match_display = match_display.merge(self.scene_info[[SceneInfoConsts.SCENE_FK, SceneInfoConsts.TEMPLATE_FK]],
                                            on=SceneInfoConsts.SCENE_FK, how='left')
        match_display = match_display.merge(self.all_templates, on=SceneInfoConsts.TEMPLATE_FK, how='left',
                                            suffixes=['', '_y'])
        return match_display

    def get_all_kpi_external_targets(self):
        query = PNGJP_SAND2Queries.get_kpi_external_targets(self.visit_date)
        external_targets = pd.read_sql_query(query, self.rds_conn.db)
        return external_targets

    def unpack_all_external_targets(self):
        targets_df = self.external_targets.drop_duplicates(subset=['operation_type', 'kpi_level_2_fk', 'key_json',
                                                                   'data_json'])
        output_targets = pd.DataFrame(columns=targets_df.columns.values.tolist())
        if not targets_df.empty:
            keys_df = self.unpack_external_targets_json_fields_to_df(targets_df, field_name='key_json')
            data_df = self.unpack_external_targets_json_fields_to_df(targets_df, field_name='data_json')
            targets_df = targets_df.merge(keys_df, on='pk', how='left')
            targets_df = targets_df.merge(data_df, on='pk', how='left')
            kpi_data = self.kpi_static_data[['pk', 'type']]
            kpi_data.rename(columns={'pk': 'kpi_level_2_fk'}, inplace=True)
            output_targets = targets_df.merge(kpi_data, on='kpi_level_2_fk', how='left')
        if output_targets.empty:
            Log.warning('KPI External Targets Results are empty')
        return output_targets

    @staticmethod
    def unpack_external_targets_json_fields_to_df(input_df, field_name):
        data_list = []
        for i, row in input_df.iterrows():
            data_item = json.loads(row[field_name]) if row[field_name] else {}
            data_item.update({'pk': row.pk})
            data_list.append(data_item)
        output_df = pd.DataFrame(data_list)
        return output_df

    def filter_matches_for_scene_kpis(self, params):
        matches = self.matches_product.copy()
        filters = self.get_scene_kpi_filters(params)
        filtered_matches = filter_df(filters, matches)
        return filtered_matches

    def get_target_by_kpi_type(self, kpi_type):
        if not self.all_targets_unpacked.empty:
            ext_target = self.all_targets_unpacked[self.all_targets_unpacked['type'] == kpi_type]
        else:
            ext_target = pd.DataFrame()
        return ext_target

    def get_scene_kpi_filters(self, param_row, exclude_filters=None):
        filters = {}
        location_filters = {ScifConsts.TEMPLATE_NAME: param_row[ScifConsts.TEMPLATE_NAME]}
        filters.update({self.LOCATION: location_filters})

        population_filters = {self.POPULATION: {}}
        include_filters = {ProductsConsts.PRODUCT_TYPE: ['SKU']}
        if param_row['Include Irrelevant']:
            include_filters[ProductsConsts.PRODUCT_TYPE].append('Irrelevant')
        if param_row['Include Empty']:
            include_filters[ProductsConsts.PRODUCT_TYPE].append('Empty')
        if param_row['Include Other']:
            include_filters[ProductsConsts.PRODUCT_TYPE].append('Other')

        population_filters[self.POPULATION].update({self.INCLUDE: [include_filters]})
        if exclude_filters:
            population_filters[self.POPULATION].update({self.EXCLUDE: exclude_filters})

        filters.update(population_filters)
        return filters

    def get_bay_shelf_combination_from_custom_entity(self):
        # self.rds_conn = PSProjectConnector(self.project_name, DbUsers.CalculationEng)
        query = PNGJP_SAND2Queries.get_all_custom_entities_query()
        bay_shelf_number_info = pd.read_sql_query(query, self.rds_conn.db)
        return bay_shelf_number_info

    def get_bay_shelf_entity_type_fk(self):
        # self.rds_conn = PSProjectConnector(self.project_name, DbUsers.CalculationEng)
        # entity_type_fk = 1601
        entity_type_fk = pd.read_sql_query(
            PNGJP_SAND2Queries.get_entity_type_fk_query(), self.rds_conn.db
        )['pk'].iloc[0]
        return entity_type_fk

    def sync_bay_shelf_combination_to_custom_entity(self, bay_no, shelf_no):
        # self.rds_conn = PSProjectConnector(self.project_name, DbUsers.CalculationEng)
        # entity_type_fk = 1601
        insert_query = PNGJP_SAND2Queries.insert_into_custom_entity_query(bay_no, shelf_no,
                                                                          self.bay_shelf_entity_type_fk)
        cur = self.rds_conn.db.cursor()
        cur.execute(insert_query)
        custom_entity_fk = cur.lastrowid
        self.rds_conn.db.commit()
        Log.info("Saved a record in custom_entity for bay:{} shelf:{} - pk = {}".format(bay_no, shelf_no,
                                                                                        custom_entity_fk))
        return custom_entity_fk

    def get_context_fk_from_custom_entity(self, bay_no, shelf_no):
        bay_shelf_comb_df = self.bay_shelf_info[
            self.bay_shelf_info.bay_shelf_combination == '{}:{}'.format(bay_no, shelf_no)
            ]
        if bay_shelf_comb_df.empty:
            Log.info("bay_shelf combination not found in custom entity bay:{} shelf:{}".format(bay_no, shelf_no))
            custom_entity_pk = self.sync_bay_shelf_combination_to_custom_entity(bay_no, shelf_no)
            # load again
            self.bay_shelf_info = self.get_bay_shelf_combination_from_custom_entity()
        else:
            custom_entity_pk = bay_shelf_comb_df.iloc[0].pk
        return custom_entity_pk

    # filters = {'location': {'scene_fk': [27898, 27896, 27894, 27859, 27892, 27854]},
    #            'population': {'exclude': {'template_name': 'Checkout Chocolate'},
    #                           'include': [{'category_fk': 6, 'manufacturer_fk': 3}]}}

    def get_product_position_code(self, product_fk, shelf_number):

        def to_list(x):
            x = str(x).strip()
            values = [v.strip() for v in x.split("-")]
            if len(values) == 1:
                return [int(o) for o in values]
            else:
                start_i, end_i = [int(o) for o in values][:2]
                return list(range(start_i, end_i+1))

        rel_sku = self.all_products[self.all_products['product_fk'] == product_fk]
        rel_template = self.shelf_range_template[
            self.shelf_range_template['Category'] == rel_sku['category'].iloc[0]
            ].copy()
        rel_template['Shelf_Range_lst'] = rel_template['Shelf Range'].apply(to_list)
        rel_template['Top_lst'] = rel_template['Top'].apply(to_list)
        rel_template['GoldenZone_lst'] = rel_template['GoldenZone'].apply(to_list)
        rel_template['Bottom_lst'] = rel_template['Bottom'].apply(to_list)

        bins = []
        for idx, row in rel_template.iterrows():
            if shelf_number in row['Shelf_Range_lst']:
                if shelf_number in row['Top_lst']:
                    bins.append(Consts.CODE_TOP)
                if shelf_number in row['GoldenZone_lst']:
                    bins.append(Consts.CODE_GOLDEN_ZONE)
                if shelf_number in row['Bottom_lst']:
                    bins.append(Consts.CODE_BOTTOM)
                break

        if len(bins) > 1:
            final_result = Consts.CODE_TOP_GZ
        elif len(bins) == 1:
            final_result = bins[0]
        else:
            Log.info("For Product {} in shelf_number {}, No matching shelf_template info".format(product_fk, shelf_number))
            final_result = 0

        return final_result
