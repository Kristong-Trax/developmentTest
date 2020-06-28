from Trax.Algo.Calculations.Core.KPI.UnifiedKpiSingleton import UnifiedKPISingleton
from KPIUtils_v2.GlobalDataProvider.PsDataProvider import PsDataProvider
from KPIUtils_v2.DB.CommonV2 import Common
from Projects.STRAUSSFRITOLAYIL.Data.LocalConsts import Consts
from KPIUtils_v2.Calculations.AssortmentCalculations import Assortment
from Trax.Algo.Calculations.Core.DataProvider import Data
from Trax.Cloud.Services.Connector.Keys import DbUsers
from KPIUtils_v2.DB.PsProjectConnector import PSProjectConnector
from KPIUtils_v2.Calculations.CalculationsUtils.GENERALToolBoxCalculations import GENERALToolBox
import pandas as pd
import json
from pandas.io.json import json_normalize


class StraussfritolayilUtil(UnifiedKPISingleton):
    def __init__(self, output, data_provider):
        super(StraussfritolayilUtil, self).__init__(data_provider)
        self.output = output
        self.common = Common(self.data_provider)
        self.project_name = self.data_provider.project_name
        self.session_uid = self.data_provider.session_uid
        self.ps_data = PsDataProvider(self.data_provider, self.output)
        self.products = self.data_provider[Data.PRODUCTS]
        self.all_products = self.data_provider[Data.ALL_PRODUCTS]
        self.scif = self.data_provider[Data.SCENE_ITEM_FACTS]
        self.add_sub_brand_to_scif()
        self.add_brand_mix_to_scif()
        self.match_probe_in_scene = self.ps_data.get_product_special_attribute_data(self.session_uid)
        self.match_product_in_scene = self.data_provider[Data.MATCHES]
        if not self.match_product_in_scene.empty:
            self.match_product_in_scene = self.match_product_in_scene.merge(self.scif[Consts.RELEVENT_FIELDS],
                                                                            on=["scene_fk", "product_fk"], how="left")
            self.filter_scif_and_mpis_to_contain_only_primary_shelf()
        else:
            unique_fields = [ele for ele in Consts.RELEVENT_FIELDS if ele not in ["product_fk", "scene_fk"]]
            self.match_product_in_scene = pd.concat([self.match_product_in_scene,
                                                     pd.DataFrame(columns=unique_fields)], axis=1)
        self.match_product_in_scene_wo_hangers = self.exclude_special_attribute_products(df=self.match_product_in_scene)
        self.visit_date = self.data_provider[Data.VISIT_DATE]
        self.session_info = self.data_provider[Data.SESSION_INFO]
        self.scene_info = self.data_provider[Data.SCENES_INFO]
        self.store_info = self.data_provider[Data.STORE_INFO]
        self.additional_attribute_2 = self.store_info[Consts.ADDITIONAL_ATTRIBUTE_2].values[0]
        self.additional_attribute_3 = self.store_info[Consts.ADDITIONAL_ATTRIBUTE_3].values[0]
        self.additional_attribute_4 = self.store_info[Consts.ADDITIONAL_ATTRIBUTE_4].values[0]
        self.store_id = self.store_info['store_fk'].values[0] if self.store_info['store_fk'] is not None else 0
        self.rds_conn = PSProjectConnector(self.project_name, DbUsers.CalculationEng)
        self.toolbox = GENERALToolBox(self.data_provider)
        self.kpi_external_targets = self.ps_data.get_kpi_external_targets(key_fields=Consts.KEY_FIELDS,
                                                                          data_fields=Consts.DATA_FIELDS)
        self.filter_external_targets()
        self.assortment = Assortment(self.data_provider, self.output)
        self.lvl3_assortment = self.set_updated_assortment()
        self.own_manuf_fk = int(self.data_provider.own_manufacturer.param_value.values[0])
        self.own_manufacturer_matches_wo_hangers = self.match_product_in_scene_wo_hangers[
            self.match_product_in_scene_wo_hangers['manufacturer_fk'] == self.own_manuf_fk]

    def set_updated_assortment(self):
        assortment_result = self.assortment.get_lvl3_relevant_ass()
        assortment_result = self.calculate_lvl3_assortment(assortment_result)
        replacement_eans_df = pd.DataFrame([json_normalize(json.loads(js)).values[0] for js
                                            in assortment_result['additional_attributes']])
        replacement_eans_df.columns = [Consts.REPLACMENT_EAN_CODES]
        replacement_eans_df = replacement_eans_df[Consts.REPLACMENT_EAN_CODES].apply(lambda row: [
            x.strip() for x in str(row).split(",")] if row else None)
        assortment_result = assortment_result.join(replacement_eans_df)
        assortment_result['facings_all_products'] = assortment_result['facings'].copy()
        assortment_result['facings_all_products_wo_hangers'] = assortment_result['facings_wo_hangers'].copy()
        assortment_result = self.handle_replacment_products_row(assortment_result)
        return assortment_result

    def filter_external_targets(self):
        self.kpi_external_targets = self.kpi_external_targets[
            (self.kpi_external_targets[Consts.ADDITIONAL_ATTRIBUTE_2].str.encode("utf8").isin(
                [None, self.additional_attribute_2.encode("utf8")])) &
            (self.kpi_external_targets[Consts.ADDITIONAL_ATTRIBUTE_3].str.encode("utf8").isin(
                [None, self.additional_attribute_3.encode("utf8")])) &
            (self.kpi_external_targets[Consts.ADDITIONAL_ATTRIBUTE_4].str.encode("utf8").isin(
                [None, self.additional_attribute_4.encode("utf8")]))]

    def calculate_lvl3_assortment(self, assortment_result):
        """
        :return: data frame on the sku level with the following fields:
        ['assortment_group_fk', 'assortment_fk', 'target', 'product_fk', 'in_store', 'kpi_fk_lvl1',
        'kpi_fk_lvl2', 'kpi_fk_lvl3', 'group_target_date', 'assortment_super_group_fk',
         'super_group_target', 'additional_attributes']. Indicates whether the product was in the store (1) or not (0).
        """
        if assortment_result.empty:
            return assortment_result
        assortment_result['in_store_wo_hangers'] = assortment_result['in_store'].copy()
        products_in_session = list(self.match_product_in_scene['product_fk'].values)
        products_in_session_wo_hangers = list(self.match_product_in_scene_wo_hangers['product_fk'].values)
        assortment_result.loc[assortment_result['product_fk'].isin(products_in_session), 'in_store'] = 1
        assortment_result.loc[assortment_result['product_fk'].isin(products_in_session_wo_hangers),
                              'in_store_wo_hangers'] = 1
        assortment_result['facings'] = 0
        assortment_result['facings_wo_hangers'] = 0
        product_assort = assortment_result['product_fk'].unique()
        for sku in product_assort:
            assortment_result.loc[assortment_result['product_fk'] == sku, 'facings'] = \
                len(self.match_product_in_scene[self.match_product_in_scene['product_fk'] == sku])
            assortment_result.loc[assortment_result['product_fk'] == sku, 'facings_wo_hangers'] = \
                len(self.match_product_in_scene_wo_hangers[self.match_product_in_scene_wo_hangers['product_fk'] == sku])
        return assortment_result

    def handle_replacment_products_row(self, assortment_result):
        additional_products_df = assortment_result[~assortment_result[Consts.REPLACMENT_EAN_CODES].isnull()]
        products_in_session = set(self.match_product_in_scene['product_ean_code'].values)
        products_in_session_wo_hangers = set(self.match_product_in_scene_wo_hangers['product_ean_code'].values)
        for i, row in additional_products_df.iterrows():
            replacement_products = row[Consts.REPLACMENT_EAN_CODES]
            facings = len(self.match_product_in_scene[self.match_product_in_scene[
                'product_ean_code'].isin(replacement_products)])
            facings_wo_hangers = len(self.match_product_in_scene_wo_hangers[self.match_product_in_scene_wo_hangers[
                'product_ean_code'].isin(replacement_products)])
            assortment_result.loc[i, 'facings_all_products'] = facings + row['facings']
            assortment_result.loc[i, 'facings_all_products_wo_hangers'] = facings_wo_hangers + row['facings_wo_hangers']
            if row['in_store'] != 1:
                for sku in replacement_products:
                    if sku in products_in_session:
                        product_df = self.all_products[self.all_products['product_ean_code'] == sku]['product_fk']
                        assortment_result.loc[i, 'product_fk'] = product_df.values[0]
                        assortment_result.loc[i, 'in_store'] = 1
                        break
            if row['in_store_wo_hangers'] != 1:
                for sku in replacement_products:
                    if sku in products_in_session_wo_hangers:
                        product_df = self.all_products[self.all_products['product_ean_code'] == sku]['product_fk']
                        assortment_result.loc[i, 'product_fk'] = product_df.values[0]
                        assortment_result.loc[i, 'in_store_wo_hangers'] = 1
                        break
        return assortment_result

    def filter_scif_and_mpis_to_contain_only_primary_shelf(self):
        self.scif = self.scif[self.scif.location_type == Consts.PRIMARY_SHELF]
        self.match_product_in_scene = self.match_product_in_scene[self.match_product_in_scene.location_type ==
                                                                  Consts.PRIMARY_SHELF]

    def add_sub_brand_to_scif(self):
        sub_brand_df = self.ps_data.get_custom_entities_df(entity_type_name='Sub_Brand_Local')
        sub_brand_df = sub_brand_df[['entity_name', 'entity_fk']].copy()
        # sub_brand_df['entity_name'] = sub_brand_df['entity_name'].str.lower()
        sub_brand_df.rename({'entity_fk': 'sub_brand_fk'}, axis='columns', inplace=True)
        # delete duplicates by name and entity_type_fk to avoid recognition duplicates.
        sub_brand_df.drop_duplicates(subset=['entity_name'], keep='first', inplace=True)
        self.scif['Sub_Brand_Local'] = self.scif['Sub_Brand_Local'].fillna('no value')
        self.scif = self.scif.merge(sub_brand_df, left_on='Sub_Brand_Local', right_on="entity_name", how="left")
        self.scif['sub_brand_fk'].fillna(Consts.SUB_BRAND_NO_VALUE, inplace=True)

    def add_brand_mix_to_scif(self):
        brand_mix_df = self.ps_data.get_custom_entities_df(entity_type_name='Brand_Mix')
        brand_mix_df = brand_mix_df[['entity_name', 'entity_fk']].copy()
        brand_mix_df.rename({'entity_fk': 'brand_mix_fk'}, axis='columns', inplace=True)
        # delete duplicates by name and entity_type_fk to avoid recognition duplicates.
        brand_mix_df.drop_duplicates(subset=['entity_name'], keep='first', inplace=True)
        self.scif['Brand_Mix'] = self.scif['Brand_Mix'].fillna('no value')
        self.scif = self.scif.merge(brand_mix_df, left_on='Brand_Mix', right_on="entity_name", how="left")
        self.scif['brand_mix_fk'].fillna(Consts.BRAND_MIX_NO_VALUE, inplace=True)

    @staticmethod
    def calculate_sos_result(numerator, denominator):
        if denominator == 0:
            return 0
        result = round((numerator / float(denominator)), 3)
        return result

    def exclude_special_attribute_products(self, df):
        """
        Helper to exclude smart_attribute products
        :return: filtered df without smart_attribute products
        """
        if self.match_probe_in_scene.empty:
            return df
        smart_attribute_df = self.match_probe_in_scene[self.match_probe_in_scene['name'] == Consts.ADDITIONAL_DISPLAY]
        if smart_attribute_df.empty:
            return df
        match_product_in_probe_fks = smart_attribute_df['match_product_in_probe_fk'].tolist()
        df = df[~df['probe_match_fk'].isin(match_product_in_probe_fks)]
        return df

