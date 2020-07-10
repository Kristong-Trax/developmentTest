from __future__ import division

import operator as op
import pandas as pd

from KPIUtils_v2.Utils.GlobalScripts.Scripts import GlobalSessionToolBox

from Trax.Algo.Calculations.Core.DataProvider import Data

from Projects.CCBOTTLERSUS.CONTACT_CENTER.Const import *

__author__ = "trevaris"

COLUMNS = ['scene_match_fk', 'template_fk', 'template_name', 'scene_fk', 'manufacturer_fk', 'category', 'brand_fk',
           'brand_name', 'product_fk', 'United Deliver', 'att4']


class ContactCenterToolBox(GlobalSessionToolBox):
    def __init__(self, data_provider, output, common):
        GlobalSessionToolBox.__init__(self, data_provider, output, common)

        self.own_manufacturer = self.data_provider[Data.OWN_MANUFACTURER].iloc[0]['param_value']

        self.mpis = self.matches.merge(self.all_products, on='product_fk') \
            .merge(self.scene_info, on='scene_fk') \
            .merge(self.templates, on='template_fk')[COLUMNS]

        self.results_df = pd.DataFrame(
            columns=['fk', 'kpi_name', 'numerator_id', 'denominator_id', 'result']
        )

        self.calculations = {
            'availability': self.calculate_availability,
            'cooler_purity': self.calculate_purity,
            'results_analysis': self.calculate_results_analysis
        }

    def main_calculation(self):
        for kpi in KPIs[COMPONENT_KPI]:
            calculation = self.calculations.get(kpi[KPI_TYPE])
            calculation(kpi)

        for kpi in KPIs[PARENT_KPI]:
            calculation = self.calculations.get(kpi[KPI_TYPE])
            calculation(kpi)

        self.save_to_db()

    def calculate_purity(self, kpi):
        """
        :param kpi:
        """

        kpi_id = self.common.get_kpi_fk_by_kpi_name(kpi['name'])
        den_df = self._filter_df(self.mpis, kpi['den_filters']) \
            .rename(columns={'scene_match_fk': 'den_count'})
        num_df = self._filter_df(den_df, kpi['num_filters']) \
            .rename(columns={'den_count': 'num_count'})

        den_df = den_df.groupby(by='scene_fk', as_index=False).count()
        num_df = num_df.groupby(by='scene_fk', as_index=False).count()
        merged_df = num_df.merge(den_df[['scene_fk', 'den_count']], on=['scene_fk'])

        def calc_purity_result(row):
            purity = row['num_count'] / row['den_count'] * 100
            result = int(purity >= kpi['purity_threshold'])
            return pd.Series([purity, result])

        merged_df[['purity', 'result']] = merged_df.apply(calc_purity_result, axis=1)
        min_scenes = merged_df[merged_df['num_count'] / merged_df['den_count'] >= kpi['minimum_threshold']]

        for scene in min_scenes.iterrows():
            self.results_df = self.results_df.append({
                'fk': kpi_id,
                'kpi_name': kpi[NAME],
                'numerator_id': self.own_manufacturer,
                'denominator_id': self.store_id,
                'result': scene['result']
            }, ignore_index=True)

    def calculate_availability(self, kpi):
        kpi_id = self.common.get_kpi_fk_by_kpi_name(kpi[NAME])

        dataset_a = self._filter_df(self.mpis, filters=kpi[DATASET_A])
        result = all(bool(dataset_a[col].unique().shape[0] >= val) for col, val in kpi[TEST_A].items())

        if DATASET_B in kpi and result:
            dataset_b = self.mpis
            for func, filters in kpi[DATASET_B].items():
                dataset_b = self._filter_df(dataset_b, filters=filters, func=func)
            result = all(bool(dataset_b[col].unique().shape[0] >= val) for col, val in kpi[TEST_B].items())

        self.results_df = self.results_df.append({
            FK: kpi_id,
            'kpi_name': kpi[NAME],
            'numerator_id': self.own_manufacturer,
            'denominator_id': self.store_id,
            RESULT: int(result)
        }, ignore_index=True)

    def calculate_results_analysis(self, kpi):
        kpi_id = self.common.get_kpi_fk_by_kpi_name(kpi[NAME])

        component_df = self._filter_df(self.results_df, filters={'kpi_name': kpi[COMPONENT_KPI]})
        passed = component_df['result'].sum() or 0
        total = component_df.shape[0]

        result = passed / total * 100 if total > 0 else 0

        self.results_df = self.results_df.append({
            FK: kpi_id,
            KPI_NAME: kpi[NAME],
            NUMERATOR_ID: self.own_manufacturer,
            DENOMINATOR_ID: self.store_id,
            RESULT: result
        }, ignore_index=True)

    def save_to_db(self):
        for _, row in self.results_df.iterrows():
            self.write_to_db(
                fk=row[FK],
                numerator_id=row[NUMERATOR_ID],
                denominator_id=row[DENOMINATOR_ID],
                result=row[RESULT]
            )

    @staticmethod
    def _filter_df(df, filters, exclude=False, func='isin'):
        """
        :param df: DataFrame to filter.
        :param filters: Dictionary of column-value list pairs to filter by.
        :param exclude:
        :param func: Function to determine inclusion.
        :return: Filtered DataFrame.
        """

        funcs = {
            'eq': pd.Series.eq,  # inapplicable
            'isin': pd.Series.isin,
            'not isin': pd.Series.isin
        }

        vert = op.inv if exclude or 'not' in func else op.pos
        func = funcs.get(func, func)

        for col, val in filters.items():
            if not hasattr(val, '__iter__') or isinstance(val, str):
                val = [val]
            try:
                if (isinstance(val, pd.Series) and val.any()) or pd.notna(val[0]):
                    df = df[vert(func(df[col], val))]
            except TypeError:
                df = df[vert(func(df[col]))]
        return df

    def _get_template_fks(self, templates):
        return self._filter_df(self.templates, filters={'template_name': templates})['template_fk']

    def _get_brand_fks(self, brands):
        brands = [brand.lower() for brand in brands]
        products = self.all_products[['brand_fk', 'brand_name']] \
            .str.lower() \
            .drop_duplicates()
        return self._filter_df(products, filters={'brand_name': brands})['brand_fk'].unique()
