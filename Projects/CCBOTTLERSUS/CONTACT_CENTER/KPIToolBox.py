from __future__ import division

import operator as op
import pandas as pd

from KPIUtils_v2.Utils.GlobalScripts.Scripts import GlobalSessionToolBox

from Trax.Algo.Calculations.Core.DataProvider import Data

from Projects.CCBOTTLERSUS.CONTACT_CENTER.Const import *

__author__ = "trevaris"

COLUMNS = ['scene_match_fk', TEMPLATE_FK, 'template_name', SCENE_FK, MANUFACTURER_FK, CATEGORY, BRAND_FK,
           BRAND_NAME, PRODUCT_FK, UNITED_DELIVER, 'att4']


class ContactCenterToolBox(GlobalSessionToolBox):
    def __init__(self, data_provider, output, common):
        GlobalSessionToolBox.__init__(self, data_provider, output, common)

        self.own_manufacturer = self.data_provider[Data.OWN_MANUFACTURER].iloc[0]['param_value']

        self.mpis = self.matches.merge(self.all_products, on=PRODUCT_FK) \
            .merge(self.scene_info, on=SCENE_FK) \
            .merge(self.templates, on=TEMPLATE_FK)[COLUMNS]

        self.results_df = pd.DataFrame(
            columns=[FK, KPI_NAME, NUMERATOR_ID, DENOMINATOR_ID, CONTEXT_ID, SCORE, RESULT,
                     IDENTIFIER_RESULT, IDENTIFIER_PARENT, SHOULD_ENTER]
        )

        self.calculations = {
            AVAILABILITY: self.calculate_availability,
            COOLER_PURITY: self.calculate_purity_by_scene,
            RESULTS_ANALYSIS: self.calculate_results_analysis
        }

    def main_calculation(self):
        for kpi in KPIs[COMPONENT_KPI]:
            calculation = self.calculations.get(kpi[KPI_TYPE])
            calculation(kpi)

        for kpi in KPIs[PARENT_KPI]:
            calculation = self.calculations.get(kpi[KPI_TYPE])
            calculation(kpi)

        self.save_to_db()

    def calculate_purity_by_scene(self, kpi):
        kpi_id = self.common.get_kpi_fk_by_kpi_name(kpi['name'])
        den_df = self._filter_df(self.mpis, kpi['den_filters']) \
            .rename(columns={'scene_match_fk': 'den_count'})
        num_df = self._filter_df(den_df, kpi['num_filters']) \
            .rename(columns={'den_count': 'num_count'})

        den_df = den_df.groupby(by='scene_fk', as_index=False).count()
        num_df = num_df.groupby(by='scene_fk', as_index=False).count()
        merged_df = num_df.merge(den_df[['scene_fk', 'den_count']], on=['scene_fk'])

        def calc_purity_result(row):
            purity = round(row['num_count'] / row['den_count'], 2)
            result = int(purity >= kpi['purity_threshold']) * 100
            return pd.Series([purity, result])

        merged_df[['purity', RESULT]] = merged_df.apply(calc_purity_result, axis=1)
        min_scenes = merged_df[merged_df['num_count'] / merged_df['den_count'] * 100 >= kpi['minimum_threshold']]

        for _, scene in min_scenes.iterrows():
            self.save_to_results([kpi_id, kpi[NAME], self.own_manufacturer, self.store_id, scene[SCENE_FK],
                                  scene['purity'], scene[RESULT], scene[SCENE_FK], kpi[IDENTIFIER_PARENT],  True])

        # for _, scene in min_scenes.iterrows():
        #     self.results_df = self.results_df.append({
        #         FK: kpi_id,
        #         KPI_NAME: kpi[NAME],
        #         NUMERATOR_ID: self.own_manufacturer,
        #         DENOMINATOR_ID: self.store_id,
        #         SCORE: round(scene['purity'], 2),
        #         RESULT: scene[RESULT],
        #         IDENTIFIER_RESULT: scene[SCENE_FK],
        #         IDENTIFIER_PARENT: kpi[IDENTIFIER_PARENT],
        #         'should_enter': True
        #     }, ignore_index=True)

    def calculate_availability(self, kpi):
        kpi_id = self.common.get_kpi_fk_by_kpi_name(kpi[NAME])

        dataset_a = self._filter_df(self.mpis, filters=kpi[DATASET_A])
        # checks if the number of unique values in `col` is greater than or equal to `min_val`
        result = all(bool(dataset_a[col].unique().shape[0] >= min_val) for col, min_val in kpi[TEST_A].items())

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
            RESULT: int(result) * 100,
            IDENTIFIER_RESULT: kpi_id,
            IDENTIFIER_PARENT: kpi[IDENTIFIER_PARENT],
            SHOULD_ENTER: True
        }, ignore_index=True)

    def calculate_results_analysis(self, kpi):
        kpi_id = self.common.get_kpi_fk_by_kpi_name(kpi[NAME])

        component_df = self._filter_df(self.results_df, filters={'kpi_name': kpi[COMPONENT_KPI]})
        passed = component_df['result'].sum() or 0
        total = component_df.shape[0] * 100

        result = passed / total if total > 0 else 0

        self.results_df = self.results_df.append({
            FK: kpi_id,
            KPI_NAME: kpi[NAME],
            NUMERATOR_ID: self.own_manufacturer,
            DENOMINATOR_ID: self.store_id,
            RESULT: result,
            IDENTIFIER_RESULT: kpi[NAME],
            'should_enter': True
        }, ignore_index=True)

    def save_to_results(self, values):
        self.results_df.loc[self.results_df.shape[0], self.results_df.columns.tolist()] = values

    def save_to_db(self):
        """
        Writes values in `self.results_df` to database.
        """

        for row in self.results_df.itertuples():
            self.write_to_db(
                fk=getattr(row, FK),
                numerator_id=getattr(row, NUMERATOR_ID),
                denominator_id=getattr(row, DENOMINATOR_ID),
                score=getattr(row, SCORE),
                result=getattr(row, RESULT),
                identifier_parent=getattr(row, IDENTIFIER_PARENT, None),
                identifier_result=getattr(row, IDENTIFIER_RESULT),
                should_enter=getattr(row, SHOULD_ENTER, False)
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
        return self._filter_df(self.templates, filters={'template_name': templates})[TEMPLATE_FK]
