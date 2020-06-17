from Trax.Algo.Calculations.Core.DataProvider import Data
from Trax.Utils.Logging.Logger import Log
from KPIUtils_v2.Utils.GlobalScripts.Scripts import GlobalSessionToolBox
import pandas as pd
from Projects.RISPARKWINEDE_SAND.Utils.SOS import BrandOutManufacturerOutCategorySecondaryFacingsSOS, \
    CategorySecondaryFacingsSOSInStore, ManufacturerSecondaryFacingsSOSPerCategory, \
    BrandOutManufacturerOutCategorySecondaryFacingsSOS, OwnManufacturerPrimaryFacingsSOSInStore, \
    CategoryPrimaryFacingsSOSInStore, ManufacturerPrimaryFacingsSOSPerCategory, \
    BrandOutManufacturerOutCategoryPrimaryFacingsSOS, OwnManufacturerSecondaryFacingsSOSInStore
from KPIUtils_v2.GlobalDataProvider.PsDataProvider import PsDataProvider

from Projects.RISPARKWINEDE_SAND.Data.LocalConsts import Consts

# from KPIUtils_v2.Utils.Consts.DataProvider import 
# from KPIUtils_v2.Utils.Consts.DB import 
# from KPIUtils_v2.Utils.Consts.PS import 
# from KPIUtils_v2.Utils.Consts.GlobalConsts import 


from KPIUtils_v2.Calculations.AssortmentCalculations import Assortment


__author__ = 'limorc'


class ToolBox(GlobalSessionToolBox):

    def __init__(self, data_provider, output):
        GlobalSessionToolBox.__init__(self, data_provider, output)
        self.assortment = Assortment(self.data_provider)
        self.ps_data = PsDataProvider(self.data_provider, self.output)
        self.result_values = self.ps_data.get_result_values()

    def main_calculation(self):
        sos_res_secondary = self.sos_calculation_secondary()
        self.common.save_json_to_new_tables(sos_res_secondary)
        sos_res_primary = self.sos_calculation_primary()
        self.common.save_json_to_new_tables(sos_res_primary)
        self.common.commit_results_data()

    def sos_calculation_secondary(self):
        sos_results = []
        filtered_categories = ['Sparkling Pure', 'Sparkling Mix']

        manu_from_store_fk = self.common.get_kpi_fk_by_kpi_type('SOS OWN MANUFACTURER OUT OF STORE - SECONDARY SHELF')
        manu_from_store_res = OwnManufacturerSecondaryFacingsSOSInStore(data_provider=self.data_provider,
                                                                        kpi_definition_fk=manu_from_store_fk,
                                                                        categories_include=filtered_categories).calculate()
        manu_store_df = self.prepare_table_for_res(manu_from_store_res, False)
        manu_store_df = self.assign_parent_child_identifiers(manu_store_df, result_identifier={'val': manu_from_store_fk})
        sos_results.extend(manu_store_df.to_dict('records'))

        category_from_store_fk = self.common.get_kpi_fk_by_kpi_type('SOS CATEGORY OUT OF STORE - SECONDARY SHELF')
        category_from_store_res = CategorySecondaryFacingsSOSInStore(data_provider=self.data_provider,
                                                                     kpi_definition_fk=category_from_store_fk,
                                                                     categories_include=filtered_categories).calculate()

        category_from_store_df = self.prepare_table_for_res(category_from_store_res, True)

        category_from_store_df = self.assign_parent_child_identifiers(category_from_store_df, parent_identifier={
            'val': manu_from_store_fk}, result_identifier={'col': ['numerator_id', 'denominator_id'], 'val':
            category_from_store_fk})
        sos_results.extend(category_from_store_df.to_dict('records'))

        manu_from_cat_fk = self.common.get_kpi_fk_by_kpi_type('SOS MANUFACTURER OUT OF CATEGORY - SECONDARY SHELF')
        manu_from_cat_res = ManufacturerSecondaryFacingsSOSPerCategory(data_provider=self.data_provider,
                                                                       kpi_definition_fk=manu_from_cat_fk,
                                                                       categories_include=filtered_categories).calculate()
        manu_from_cat_df = self.prepare_table_for_res(manu_from_cat_res, True)

        manu_from_cat_df = self.assign_parent_child_identifiers(manu_from_cat_df,
                                                                parent_identifier={'col': ['denominator_id',
                                                                                           'context_id'], 'val':
                                                                    category_from_store_fk},
                                                                result_identifier={'col': ['numerator_id',
                                                                                            'denominator_id'], 'val':
                                                                    manu_from_cat_fk})

        sos_results.extend(manu_from_cat_df.to_dict('records'))

        barnd_out_manu_fk = self.common.get_kpi_fk_by_kpi_type(
            'SOS BRAND OUT OF MANUFACTURER OUT OF CATEGORY - SECONDARY SHELF')
        brand_out_manu_res = BrandOutManufacturerOutCategorySecondaryFacingsSOS(data_provider=self.data_provider,
                                                                                kpi_definition_fk=barnd_out_manu_fk,
                                                                                categories_include=filtered_categories).calculate()
        brand_out_manu_df = self.prepare_table_for_res(brand_out_manu_res, True)
        brand_out_manu_df = self.assign_parent_child_identifiers(brand_out_manu_df,
                                                                 parent_identifier={'col':
                                                                                        ['denominator_id', 'context_id'],
                                                                                    'val': manu_from_cat_fk})
        sos_results.extend(brand_out_manu_df.to_dict('records'))

        return sos_results

    def sos_calculation_primary(self):
        sos_results =[]
        filtered_categories = ['Sparkling Pure', 'Sparkling Mix']

        manu_from_store_fk = self.common.get_kpi_fk_by_kpi_type(
            'SOS OWN MANUFACTURER OUT OF STORE - PRIMARY SHELF')
        manu_from_store_res = OwnManufacturerPrimaryFacingsSOSInStore(data_provider=self.data_provider,
                                                                      kpi_definition_fk=manu_from_store_fk,
                                                                      categories_include=filtered_categories).calculate()
        manu_store_df = self.prepare_table_for_res(manu_from_store_res, False)
        manu_store_df = self.assign_parent_child_identifiers(manu_store_df,
                                                             result_identifier={'val': manu_from_store_fk})
        sos_results.extend(manu_store_df.to_dict('records'))

        category_from_store_fk = self.common.get_kpi_fk_by_kpi_type('SOS CATEGORY OUT OF STORE - PRIMARY SHELF')
        category_from_store_res = CategoryPrimaryFacingsSOSInStore(data_provider=self.data_provider,
                                                                   kpi_definition_fk=category_from_store_fk,
                                                                   categories_include=filtered_categories).calculate()
        category_from_store_df = self.prepare_table_for_res(category_from_store_res, True)
        category_from_store_df = self.assign_parent_child_identifiers(category_from_store_df, parent_identifier={
            'val': manu_from_store_fk}, result_identifier={'col': ['numerator_id', 'denominator_id'], 'val':
            category_from_store_fk})
        sos_results.extend(category_from_store_df.to_dict('records'))

        manu_from_cat_fk = self.common.get_kpi_fk_by_kpi_type('SOS MANUFACTURER OUT OF CATEGORY - PRIMARY SHELF')
        manu_from_cat_res = ManufacturerPrimaryFacingsSOSPerCategory(data_provider=self.data_provider,
                                                                     kpi_definition_fk=manu_from_cat_fk,
                                                                     categories_include=filtered_categories).calculate()
        manu_from_cat_df = self.prepare_table_for_res(manu_from_cat_res, True)
        manu_from_cat_df = self.assign_parent_child_identifiers(manu_from_cat_df,
                                                                parent_identifier={'col': ['denominator_id',
                                                                                           'context_id'], 'val':
                                                                                       category_from_store_fk},
                                                                result_identifier={'col': ['numerator_id',
                                                                                           'denominator_id'], 'val':
                                                                                       manu_from_cat_fk})
        sos_results.extend(manu_from_cat_df.to_dict('records'))

        barnd_out_manu_fk = self.common.get_kpi_fk_by_kpi_type(
            'SOS BRAND OUT OF MANUFACTURER OUT OF CATEGORY - PRIMARY SHELF')
        brand_out_manu_res = BrandOutManufacturerOutCategoryPrimaryFacingsSOS(data_provider=self.data_provider,
                                                                              kpi_definition_fk=barnd_out_manu_fk,
                                                                              categories_include=filtered_categories).calculate()
        brand_out_manu_df = self.prepare_table_for_res(brand_out_manu_res, True)
        brand_out_manu_df = self.assign_parent_child_identifiers(brand_out_manu_df,
                                                                 parent_identifier={'col':
                                                                                        ['denominator_id',
                                                                                         'context_id'],
                                                                                    'val': manu_from_cat_fk})
        sos_results.extend(brand_out_manu_df.to_dict('records'))
        return sos_results

    def assign_parent_child_identifiers(self, df, parent_identifier=None, result_identifier=None):
        if parent_identifier:
            df = self.assign_cols(df, 'identifier_parent', parent_identifier.get('col', None), parent_identifier.get('val',
                                                                                                                ''))
        if result_identifier:
            df = self.assign_cols(df, 'identifier_result', result_identifier.get('col', None),
                                 result_identifier.get('val', ''))
        return df

    def assign_cols(self, df, identifier, col_to_assign, val_to_assign):
        if col_to_assign:
            df.loc[:, identifier] = df[col_to_assign].apply(lambda x: ' '.join(x.astype(str)), axis=1) + ' '+str(val_to_assign)
        else:
            df.loc[:, identifier] = str(val_to_assign)

        return df

    def prepare_table_for_res(self, res_dict, should_enter):

        df = pd.DataFrame(
            [res.to_dict if type(res) != dict else res for res in res_dict])  # need to add parent identifer
        df.loc[:, 'session_fk'] = df['fk']
        df.loc[:, 'fk'] = df['kpi_definition_fk']
        df.loc[:, 'should_enter'] = should_enter

        return df

    def assortment_calculation(self):

        sku_result = self.assortment.calculate_lvl3_assortment(False)

        category_result = self.calculate_lvl2_assortment(sku_result, Consts.LVL2_CATEGORY_HEADERS)

        group_result= self.calculate_lvl2_assortment(sku_result, Consts.LVL2_CATEGORY_HEADERS)
        #
        # store_result =  self.assortment.calculate_lvl1_assortment()


    def distribution_sku_level(self, lvl_3_result):
        """ This function receive df = lvl_3_result assortment with data regarding the assortment products
            This function turn the sku_assortment_results to be in a shape of db result.
            return distribution_db_results df
        """
        lvl_3_result.rename(columns={'product_fk': 'numerator_id', 'assortment_group_fk': 'denominator_id',
                                     'in_store': 'result', 'kpi_fk_lvl3': 'kpi_level_2_fk'}, inplace=True)
        lvl_3_result.loc[:, 'result'] = lvl_3_result.apply(lambda row: self.kpi_result_value(row.result), axis=1)
        lvl_3_result = lvl_3_result.assign(numerator_result=lvl_3_result['result'],
                                           denominator_result=lvl_3_result['result'],
                                           score=lvl_3_result['result'])
        lvl_3_result = self.filter_df_by_col(lvl_3_result, Consts.SKU_LEVEL)
        Log.info('Distribution sku level is done ')
        return lvl_3_result

    def filter_df_by_col(self, df, level):
        """

        :param df: df results lvl2 / lvl3 assortment results
        :param level: sku /  group level
        :return:filtered df
        """
        if level == Consts.SKU_LEVEL:
            return df[Consts.LVL3_SESSION_RESULTS_COL]

        if level == Consts.GROUPS_LEVEL:
            return df[Consts.LVL2_SESSION_RESULTS_COL]

    def kpi_result_value(self, value):
        """
        :param value:  availability kpi result 0 for oos and 1 for distrbution
         Function retrieve the kpi_result_value needed for Availability KPIS
        (kpi result value match to mobile report signs) , according to the kpi result.
        :return pk of kpi_result_value
         """
        value = 'NO' if value == 0 else 'YES'
        value_info = self.result_values[self.result_values['value'] == value]
        if value_info.empty:
            return
        return value_info.pk.iloc[0]

    def calculate_lvl2_assortment(self, lvl3_assortment, group_by_cols):
        """
        :param lvl3_assortment: return value of 'calculate_lvl3_assortment' func.
        :return: data frame on the assortment group level with the following fields:
        ['assortment_super_group_fk', 'assortment_group_fk', 'assortment_fk', 'target', 'passes',
        'total', 'kpi_fk_lvl1', 'kpi_fk_lvl2', 'group_target_date', 'super_group_target'].
        Indicates for each assortment group how many products were in the store (passes) out of the total\ target
        (total\ target).
        """
        cat_df = self.all_products[['product_fk', 'category_fk']]
        lvl3_with_cat = lvl3_assortment.merge(cat_df, on='product_fk', how='left')
        lvl3_with_cat = lvl3_with_cat[lvl3_with_cat['category_fk'].notnull()]

        if lvl3_with_cat.empty:
            return pd.DataFrame(columns=group_by_cols)

        lvl3_res = lvl3_assortment.copy()
        lvl3_res = lvl3_res.fillna(Consts.EMPTY_VAL)
        lvl2_res = lvl3_res.groupby(group_by_cols)['in_store'].agg([('total', 'count'), ('passes', 'sum')]).reset_index()
        return lvl2_res

