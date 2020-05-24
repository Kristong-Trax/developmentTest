import os
from Trax.Algo.Calculations.Core.KPI.UnifiedKpiSingleton import UnifiedKPISingleton
from KPIUtils_v2.GlobalDataProvider.PsDataProvider import PsDataProvider
from KPIUtils_v2.DB.CommonV2 import Common
from Projects.STRAUSSFRITOLAYIL.Data.LocalConsts import Consts
from KPIUtils_v2.Calculations.AssortmentCalculations import Assortment
from Trax.Algo.Calculations.Core.DataProvider import Data
from Trax.Cloud.Services.Connector.Keys import DbUsers
from KPIUtils_v2.DB.PsProjectConnector import PSProjectConnector
from KPIUtils_v2.Calculations.CalculationsUtils.GENERALToolBoxCalculations import GENERALToolBox


class StraussfritolayilUtil(UnifiedKPISingleton):

    def __init__(self, output, data_provider):
        super(StraussfritolayilUtil, self).__init__(data_provider)
        self.output = output
        self.common = Common(self.data_provider)
        self.project_name = self.data_provider.project_name
        self.session_uid = self.data_provider.session_uid
        self.products = self.data_provider[Data.PRODUCTS]
        self.all_products = self.data_provider[Data.ALL_PRODUCTS]
        self.match_product_in_scene = self.data_provider[Data.MATCHES]
        self.scif = self.data_provider[Data.SCENE_ITEM_FACTS]
        self.visit_date = self.data_provider[Data.VISIT_DATE]
        self.session_info = self.data_provider[Data.SESSION_INFO]
        self.scene_info = self.data_provider[Data.SCENES_INFO]
        self.store_id = self.data_provider[Data.STORE_FK] if self.data_provider[Data.STORE_FK] is not None \
            else self.session_info['store_fk'].values[0]
        self.rds_conn = PSProjectConnector(self.project_name, DbUsers.CalculationEng)
        self.toolbox = GENERALToolBox(self.data_provider)
        self.ps_data = PsDataProvider(self.data_provider, self.output)
        self.kpi_external_targets = self.ps_data.get_kpi_external_targets(key_fields=Consts.KEY_FIELDS,
                                                                          data_fields=Consts.DATA_FIELDS)
        self.add_sub_brand_to_scif()
        self.assortment = Assortment(self.data_provider, self.output)
        self.lvl3_assortment = self.assortment.get_lvl3_relevant_ass()
        self.own_manuf_fk = int(self.data_provider.own_manufacturer.param_value.values[0])

    def add_sub_brand_to_scif(self):
        sub_brand_df = self.ps_data.get_custom_entities_df(entity_type_name='sub_brand')
        sub_brand_df = sub_brand_df[['entity_name', 'entity_fk']]
        # sub_brand_df['entity_name'] = sub_brand_df['entity_name'].str.lower()
        sub_brand_df.rename({'entity_fk': 'sub_brand_fk'}, axis='columns', inplace=True)
        # delete duplicates by name and entity_type_fk to avoid recognition duplicates.
        sub_brand_df.drop_duplicates(subset=['entity_name'], keep='first', inplace=True)
        self.scif['sub_brand'] = self.scif['sub_brand'].fillna('no value')
        self.scif = self.scif.merge(sub_brand_df, left_on="sub_brand", right_on="entity_name", how="left")

    # def calculate_sos(self, sos_filters, **general_filters):
    #     numerator_linear = self.calculate_share_space(**dict(sos_filters, **general_filters))
    #     denominator_linear = self.calculate_share_space(**general_filters)
    #     return float(numerator_linear), float(denominator_linear)
    #
    # def calculate_share_space(self, **filters):
    #     filtered_scif = self.filtered_scif[self.toolbox.get_filter_condition(self.filtered_scif, **filters)]
    #     space_length = filtered_scif['updated_gross_length'].sum()
    #     return space_length
    #
    # def add_kpi_result_to_kpi_results_df(self, result_list):
    #     self.kpi_results_check.loc[len(self.kpi_results_check)] = result_list
    #
    # def get_results_of_scene_level_kpis(self):
    #     scene_kpi_results = pd.DataFrame()
    #     if not self.scene_info.empty:
    #         scene_kpi_results = self.ps_data.get_scene_results(self.scene_info['scene_fk'].drop_duplicates().values)
    #     return scene_kpi_results
    #
    # def get_store_data_by_store_id(self):
    #     store_id = self.store_id if self.store_id else self.session_info['store_fk'].values[0]
    #     query = PEPSICOUK_Queries.get_store_data_by_store_id(store_id)
    #     query_result = pd.read_sql_query(query, self.rds_conn.db)
    #     return query_result
    #
    # def get_facings_scene_bay_shelf_product(self):
    #     self.filtered_matches['count'] = 1
    #     aggregate_df = self.filtered_matches.groupby(['scene_fk', 'bay_number', 'shelf_number', 'product_fk'],
    #                                                  as_index=False).agg({'count': np.sum})
    #     return aggregate_df
    #
    # def get_lvl3_relevant_assortment_result(self):
    #     assortment_result = self.assortment.get_lvl3_relevant_ass()
    #     # if assortment_result.empty:
    #     #     return assortment_result
    #     # products_in_session = self.filtered_scif.loc[self.filtered_scif['facings'] > 0]['product_fk'].values
    #     # assortment_result.loc[assortment_result['product_fk'].isin(products_in_session), 'in_store'] = 1
    #     return assortment_result
    #
    # @staticmethod
    # def get_block_and_adjacency_filters(target_series):
    #     filters = {target_series['Parameter 1']: target_series['Value 1']}
    #     if target_series['Parameter 2']:
    #        filters.update({target_series['Parameter 2']: target_series['Value 2']})
    #
    #     if target_series['Parameter 3']:
    #         filters.update({target_series['Parameter 3']: target_series['Value 3']})
    #     return filters
    #
    # @staticmethod
    # def get_block_filters(target_series):
    #     if isinstance(target_series['Value 1'], list):
    #         filters = {target_series['Parameter 1']: target_series['Value 1']}
    #     else:
    #         filters = {target_series['Parameter 1']: [target_series['Value 1']]}
    #
    #     if target_series['Parameter 2']:
    #         if isinstance(target_series['Value 2'], list):
    #             filters.update({target_series['Parameter 2']: target_series['Value 2']})
    #         else:
    #             filters.update({target_series['Parameter 2']: [target_series['Value 2']]})
    #
    #     if target_series['Parameter 3']:
    #         if isinstance(target_series['Value 2'], list):
    #             filters.update({target_series['Parameter 3']: target_series['Value 3']})
    #         else:
    #             filters.update({target_series['Parameter 3']: [target_series['Value 3']]})
    #     return filters
    #
    # def reset_filtered_scif_and_matches_to_exclusion_all_state(self):
    #     self.filtered_scif = self.commontools.filtered_scif.copy()
    #     self.filtered_matches = self.commontools.filtered_matches.copy()
    #
    # def get_available_hero_sku_list(self, dependencies_df):
    #     hero_list = dependencies_df[(dependencies_df['kpi_type'] == self.HERO_SKU_AVAILABILITY_SKU) &
    #                                 (dependencies_df['numerator_result'] == 1)]['numerator_id'].unique().tolist()
    #     return hero_list
    #
    # def get_unavailable_hero_sku_list(self, dependencies_df):
    #     hero_list = dependencies_df[(dependencies_df['kpi_type'] == self.HERO_SKU_AVAILABILITY_SKU) &
    #                                 (dependencies_df['numerator_result'] == 0)]['numerator_id'].unique().tolist()
    #     return hero_list
    #
    # def get_hero_type_custom_entity_df(self):
    #     hero_type_df = self.custom_entities[self.custom_entities['entity_type'] == self.HERO_TYPE]
    #     hero_type_df.rename(columns={'pk': 'entity_fk'}, inplace=True)
    #     return hero_type_df