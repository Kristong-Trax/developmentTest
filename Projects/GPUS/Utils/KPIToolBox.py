
from Trax.Algo.Calculations.Core.DataProvider import Data
from Trax.Cloud.Services.Connector.Keys import DbUsers
from KPIUtils_v2.DB.PsProjectConnector import PSProjectConnector
from KPIUtils_v2.Calculations.AssortmentCalculations import Assortment

from Trax.Algo.Calculations.Core.GraphicalModel.AdjacencyGraphs import AdjacencyGraph

# from Trax.Utils.Logging.Logger import Log
import pandas as pd
import os

from KPIUtils_v2.DB.CommonV2 import Common
# from KPIUtils_v2.Calculations.AssortmentCalculations import Assortment
# from KPIUtils_v2.Calculations.AvailabilityCalculations import Availability
# from KPIUtils_v2.Calculations.NumberOfScenesCalculations import NumberOfScenes
# from KPIUtils_v2.Calculations.PositionGraphsCalculations import PositionGraphs
# from KPIUtils_v2.Calculations.SOSCalculations import SOS
# from KPIUtils_v2.Calculations.SequenceCalculations import Sequence
# from KPIUtils_v2.Calculations.SurveyCalculations import Survey

# from KPIUtils_v2.Calculations.CalculationsUtils import GENERALToolBoxCalculations

from Projects.GPUS.Utils.Const import Const

__author__ = 'sam, nicolaske'


class GPUSToolBox:

    def __init__(self, data_provider, common, output):
        self.output = output
        self.data_provider = data_provider
        self.common = common
        self.project_name = self.data_provider.project_name
        self.session_uid = self.data_provider.session_uid
        self.session_id = self.data_provider.session_id
        self.session_info = self.data_provider[Data.SESSION_INFO]
        self.scene_info = self.data_provider[Data.SCENES_INFO]
        self.templates = self.data_provider[Data.TEMPLATES]
        self.products = self.data_provider[Data.PRODUCTS]
        self.all_products = self.data_provider[Data.ALL_PRODUCTS]
        self.match_product_in_scene = self.data_provider[Data.MATCHES]
        self.mpis = self.make_mpis()
        self.visit_date = self.data_provider[Data.VISIT_DATE]
        self.store_id = self.data_provider[Data.STORE_FK]
        self.scif = self.data_provider[Data.SCENE_ITEM_FACTS]
        self.scif = self.scif[~(self.scif['product_type'] == 'Irrelevant')]
        self.rds_conn = PSProjectConnector(self.project_name, DbUsers.CalculationEng)
        self.kpi_static_data = self.common.get_kpi_static_data()
        self.manufacturer_fk = int(self.data_provider[Data.OWN_MANUFACTURER].iloc[0, 1])
        self.gp_manufacturer = self.get_gp_manufacturer()
        self.gp_categories = self.get_gp_categories()
        self.gp_brands = self.get_gp_brands()
        self.all_man = self.scif[['manufacturer_name', 'manufacturer_fk']].set_index('manufacturer_name')\
                                                                          ['manufacturer_fk'].to_dict()
        self.all_brands = self.scif[['brand_name', 'brand_fk']].set_index('brand_name')['brand_fk'].to_dict()
        self.man_fk_filter = {'manufacturer_name': list(self.gp_manufacturer.keys())}
        self.cat_filter = {'category': list(self.gp_categories.keys())}
        self.brand_filter = {'brand_name': list(self.gp_brands.keys())}
        self.all_brands_filter = {'brand_name': list(self.all_brands.keys())}
        self.all_man_filter = {'manufacturer_name': list(self.all_man.keys())}
        self.kpi_results = []

        self.assortment = Assortment(self.data_provider, self.output)


    def main_calculation(self, *args, **kwargs):
        """
        This function calculates the KPI results.
        """
        if not self.filter_df(self.scif, self.brand_filter).empty:
            ''' This is a bad set-up, will fix tomorrow. Just need to make some data right now
                it's still not tomorrow... '''
            self.calculate_manufacturer_facings_sos('Manufacturer out of Category Facings SOS')
            self.calculate_brand_facings_sos('Brand out of Category Facings SOS')
            self.calculate_manufacturer_linear_sos('Manufacturer out of Category Linear SOS')
            self.calculate_brand_linear_sos('Brand out of Category Linear SOS')

            self.calculate_assortment('Distribution')

        if not self.filter_df(self.scif, self.cat_filter).empty:
            self.calculate_share_of_empty('Share of Empty out of Category')
            pass

        for result in self.kpi_results:
            self.write_to_db(**result)
        return

    def calculate_manufacturer_facings_sos(self, kpi):
        self.calculate_sos(kpi, nums=self.all_man_filter, dens=self.cat_filter,
                           num_fks=self.all_man, den_fks=self.gp_categories,
                           sum_col=Const.FACINGS)


    def calculate_brand_facings_sos(self, kpi):
        self.calculate_sos(kpi, nums=self.all_brands_filter, dens=self.cat_filter,
                           num_fks=self.all_brands, den_fks=self.gp_categories,
                           sum_col=Const.FACINGS)

    def calculate_manufacturer_linear_sos(self, kpi):
        self.calculate_sos(kpi, nums=self.all_man_filter, dens=self.cat_filter,
                           num_fks=self.all_man, den_fks=self.gp_categories,
                           sum_col=Const.LINEAR_FACINGS)


    def calculate_brand_linear_sos(self, kpi):
        self.calculate_sos(kpi, nums=self.all_brands_filter, dens=self.cat_filter,
                           num_fks=self.all_brands, den_fks=self.gp_categories,
                           sum_col=Const.LINEAR_FACINGS)

    def calculate_share_of_empty(self, kpi):
        # self.calculate_sos(kpi, nums=Const.EMPTY_FILTER, dens=self.cat_filter,
        #                    num_fks=Const.EMPTY_FKS, den_fks=self.gp_categories,
        #                    sum_col=Const.FACINGS)
        self.calculate_sos(kpi, nums=Const.EMPTY_FILTER, dens={'session_id': self.session_id},
                           num_fks=Const.EMPTY_FKS, den_fks={self.session_id: 0},
                           sum_col=Const.FACINGS)

    def calculate_sos(self, kpi, nums, dens, num_fks, den_fks, sum_col):
        den_groups = self.grouper(dens, self.scif)
        for den_name, den_df in den_groups:
            num_groups = self.grouper(nums, df=den_df)
            for num_name, num_df in num_groups:
                if num_df.empty:
                    continue
                num = num_df[sum_col].sum()
                den = den_df[sum_col].sum()
                res = self.safe_divide(num, den)
                kpi_res = {'kpi_name': kpi,
                           'numerator_id': num_fks[num_name],
                           'numerator_result': num,
                           'denominator_id': den_fks[den_name],
                           'denominator_result': den,
                           'score': 1,
                           'result': res}
                self.kpi_results.append(kpi_res)

    def grouper(self, filter, df):
        return self.filter_df(df, filter).groupby(filter.keys()[0])

    def calculate_assortment(self, kpi):
        lvl3_result = self.assortment.calculate_lvl3_assortment()
        if not lvl3_result.empty:
            lvl3_result['target'] = 1
            lvl2_result = self.assortment.calculate_lvl2_assortment(lvl3_result)
            lvl1_result = self.assortment.calculate_lvl1_assortment(lvl2_result)
            lvl1_result['total'] = lvl2_result['total'].sum()
            lvl1_result['passes'] = lvl2_result['passes'].sum()

            self.parse_assortment_results(lvl3_result, 'kpi_fk_lvl3', 'product_fk', 'in_store', 'assortment_fk',
                                          'target', None, 'assortment_group_fk')
            self.parse_assortment_results(lvl2_result, 'kpi_fk_lvl2', 'assortment_group_fk', 'passes', 'assortment_fk',
                                          'total', 'assortment_group_fk', 'assortment_super_group_fk')
            self.parse_assortment_results(lvl1_result, 'kpi_fk_lvl1', 'assortment_super_group_fk', 'passes',
                                          'assortment_super_group_fk', 'total', 'assortment_super_group_fk', None)

    def parse_assortment_results(self, df, kpi_col, num_id_col, num_col, den_id_col, den_col, self_id, parent):
        for i, row in df.iterrows():
            kpi_res = {'kpi_fk': row[kpi_col],
                       'numerator_id': row[num_id_col],
                       'numerator_result': row[num_col],
                       'denominator_id': row[den_id_col],
                       'denominator_result': row[den_col],
                       'score': 1,
                       'result': self.safe_divide(row[num_col], row[den_col]),
                       'ident_result': row[self_id] if self_id else None,
                       'ident_parent': row[parent] if parent else None}
            self.kpi_results.append(kpi_res)

    def safe_divide(self, num, den):
        return (float(num) / den) * 100 if num else 0

    @staticmethod
    def filter_df(df, filters, exclude=0):
        cols = set(df.columns)
        for key, val in filters.items():
            if key not in cols:
                return pd.DataFrame()
            if not isinstance(val, list):
                val = [val]
            if exclude:
                df = df[~df[key].isin(val)]
            else:
                df = df[df[key].isin(val)]
        return df

    def get_gp_categories(self):
        cats = self.products[self.products['manufacturer_fk'] == self.manufacturer_fk][['category', 'category_fk']]\
                              .drop_duplicates().set_index('category')['category_fk'].to_dict()
        return cats

    def get_gp_brands(self):
        brands = self.products[self.products['manufacturer_fk'] == self.manufacturer_fk][['brand_name', 'brand_fk']]\
                              .drop_duplicates().set_index('brand_name')['brand_fk'].to_dict()
        return brands

    def get_gp_manufacturer(self):
        name = self.products[self.products['manufacturer_fk'] == self.manufacturer_fk].reset_index()
        name = '' if name.empty else name.loc[0, 'manufacturer_name']
        return {name: self.manufacturer_fk}

    def make_mpis(self):
        mpis = self.match_product_in_scene.merge(self.products, on='product_fk', suffixes=['', '_p']) \
            .merge(self.scene_info, on='scene_fk', suffixes=['', '_s']) \
            .merge(self.templates, on='template_fk', suffixes=['', '_t'])
        return mpis

    def write_to_db(self, kpi_name=None, score=0, result=None, target=None, numerator_result=None, denominator_result=None,
                    numerator_id=999, denominator_id=999, ident_result=None, ident_parent=None, kpi_fk = None):
        """
        writes result in the DB
        :param kpi_name: str
        :param score: float
        :param display_text: str
        :param result: str
        :param threshold: int
        """
        if not kpi_fk:
            kpi_fk = self.common.get_kpi_fk_by_kpi_type(kpi_name)
        self.common.write_to_db_result(fk=kpi_fk, score=score, result=result, should_enter=True, target=target,
                                       numerator_result=numerator_result, denominator_result=denominator_result,
                                       numerator_id=numerator_id, denominator_id=denominator_id,
                                       identifier_result=ident_result, identifier_parent=ident_parent)

