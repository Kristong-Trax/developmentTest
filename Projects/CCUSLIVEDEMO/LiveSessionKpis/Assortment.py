import pandas as pd
from datetime import datetime
from Projects.CCUSLIVEDEMO.LiveSessionKpis.PSAssortmentProvider import LiveAssortmentDataProvider


class LiveAssortmentCalculation:
    LVL3_HEADERS = ['assortment_group_fk', 'assortment_fk', 'target', 'product_fk', 'kpi_fk_lvl1',
                    'kpi_fk_lvl2', 'kpi_fk_lvl3', 'group_target_date', 'assortment_super_group_fk',
                    'super_group_target', 'additional_attributes']
    ASSORTMENT_FK = 'assortment_fk'
    ASSORTMENT_GROUP_FK = 'assortment_group_fk'
    ASSORTMENT_SUPER_GROUP_FK = 'assortment_super_group_fk'
    LVL2_HEADERS = ['assortment_super_group_fk', 'assortment_group_fk', 'assortment_fk', 'target',
                    'kpi_fk_lvl1', 'kpi_fk_lvl2', 'group_target_date', 'super_group_target']
    EMPTY_VAL = -1
    LVL1_HEADERS = ['assortment_super_group_fk', 'assortment_group_fk', 'super_group_target', 'kpi_fk_lvl1']

    def __init__(self, data_provider):
        self.store_assortment = LiveAssortmentDataProvider(data_provider).execute()
        self.current_date = datetime.now()
        self.match_product_in_scene = data_provider.matches

    def get_lvl3_relevant_ass(self):
        """

        :return: data frame with all products in the specific store assortment.
        """
        if self.store_assortment.empty:
            return pd.DataFrame()
        assortment_result = self.store_assortment.loc[self.store_assortment['start_date'] <=
                                                      self.current_date][self.LVL3_HEADERS]
        assortment_result.loc[:, 'in_store'] = 0
        return assortment_result.drop_duplicates(subset=[self.ASSORTMENT_GROUP_FK, self.ASSORTMENT_FK, 'product_fk'],
                                                 keep='last')

    def calculate_lvl3_assortment(self, stacking=True):
        """
        :return: data frame on the sku level with the following fields:
        ['assortment_group_fk', 'assortment_fk', 'target', 'product_fk', 'in_store', 'kpi_fk_lvl1',
        'kpi_fk_lvl2', 'kpi_fk_lvl3', 'group_target_date', 'assortment_super_group_fk',
         'super_group_target', 'additional_attributes']. Indicates whether the product was in the store (1) or not (0).
        """
        assortment_result = self.get_lvl3_relevant_ass()

        if assortment_result.empty or self.match_product_in_scene.empty:
            return assortment_result
        mpi_filtered = self.match_product_in_scene.copy() if not stacking else\
            self.match_product_in_scene.loc[self.match_product_in_scene.stacking_layer == 1]
        assortment_result.loc[assortment_result.product_fk.isin(mpi_filtered.product_fk.values), 'in_store'] = 1
        mpi_filtered = mpi_filtered[['product_fk']].groupby('product_fk').size().reset_index(name='facings')
        assortment_result = assortment_result.merge(mpi_filtered, on='product_fk')
        return assortment_result

    def calculate_lvl2_assortment(self, lvl3_assortment):
        """
        :param lvl3_assortment: return value of 'calculate_lvl3_assortment' func.
        :return: data frame on the assortment group level with the following fields:
        ['assortment_super_group_fk', 'assortment_group_fk', 'assortment_fk', 'target', 'passes',
        'total', 'kpi_fk_lvl1', 'kpi_fk_lvl2', 'group_target_date', 'super_group_target'].
        Indicates for each assortment group how many products were in the store (passes) out of the total\ target
        (total\ target).
        """
        lvl3_res = lvl3_assortment.copy()
        lvl3_res = lvl3_res.fillna(self.EMPTY_VAL)
        lvl2_res = lvl3_res.groupby(self.LVL2_HEADERS)['in_store'].agg([('total', 'sum'), ('passes', 'count')]).reset_index()
        return lvl2_res

    def calculate_lvl_1_assortment(self, lvl2_assortment):
        ass_super = lvl2_assortment[~lvl2_assortment['kpi_fk_lvl1'] == self.EMPTY_VAL]
        ass_super = ass_super.groupby(self.LVL1_HEADERS).agg({'total': 'count', 'passes': 'sum'}).reset_index()
        if ass_super.empty:
            return ass_super
        ass_super.loc[:, 'passes'] = ass_super.apply(lambda row: 1 if row.passes > row.super_group_target else 0, axis=1)
        return ass_super






