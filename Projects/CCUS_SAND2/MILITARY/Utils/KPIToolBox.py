from __future__ import division
import os
import pandas as pd

from KPIUtils_v2.DB.CommonV2 import Common
from Trax.Algo.Calculations.Core.DataProvider import Data

from Projects.CCUS.MILITARY.Utils import Const

__author__ = 'trevaris'

PATH = os.path.dirname(__file__)
COLUMNS = ['scene_match_fk', 'scene_fk', 'bay_number', 'manufacturer_fk', 'product_fk', 'category_fk', 'template_fk']


class MilitaryToolBox:
    def __init__(self, data_provider, output):
        self.data_provider = data_provider
        self.output = output
        self.common = Common(self.data_provider)
        self.session_uid = self.data_provider.session_uid
        self.own_manufacturer = self.data_provider[Data.OWN_MANUFACTURER].iloc[0]['param_value']
        self.store_info = data_provider[Data.STORE_INFO]
        self.templates = self.data_provider[Data.ALL_TEMPLATES]
        self.products = self.data_provider[Data.ALL_PRODUCTS]

        self.match_product_in_scene = data_provider[Data.MATCHES]
        self.products = data_provider[Data.PRODUCTS]
        self.scene_info = data_provider[Data.SCENES_INFO]

        self.mpis = self.match_product_in_scene \
            .merge(self.products, on='product_fk', suffixes=['', '_p']) \
            .merge(self.scene_info, on='scene_fk', suffixes=['', '_s'])[COLUMNS]

        with open(os.path.join(PATH, "mock_data.csv"), 'rb') as f:
            self.mpis = pd.read_csv(f)

    def main_calculation(self):
        # if self.filter_df(self.store_info, {'region_name': Const.REGION}).empty:
        #     return

        self.calculate_compliant_bay_kpi()
        # self.calculate_scene_availability_kpi()
        self.calculate_facings_sos_kpi()
        self.calculate_share_of_scenes_kpi()

    def calculate_compliant_bay_kpi(self):
        """Iterates through Compliant Bay type KPI."""
        for kpi in Const.KPIs[Const.COMPLIANT_BAY_COUNT]:
            self.calculate_compliant_bay(
                kpi['name'], kpi['template'], kpi['manufacturer'], kpi['exclude_manufacturers'])

    def calculate_compliant_bay(self, kpi, templates, manufacturers, exclude_manufacturers=False, sos_threshold=.501):
        """
        Calculates the something I've already forgotten.

        :param kpi: Name of KPI.
        :param templates:
        :param manufacturers:
        :param exclude_manufacturers: If True, exclues `manufacturers` in filter.
        :param sos_threshold: Minimum percentage to determine display branding.
        """

        kpi_id = self.common.get_kpi_fk_by_kpi_name(kpi)
        template_ids = self.get_template_fks(templates)
        manufacturer_ids = self.get_manufacturer_fks(manufacturers)

        template_df = self.filter_df(self.mpis, filters={'template_fk': template_ids})

        num_df = self.filter_df(template_df, {'manufacturer_fk': manufacturer_ids}, exclude=exclude_manufacturers) \
            .groupby(by=['scene_fk', 'bay_number'], as_index=False) \
            .count() \
            .rename(columns={'scene_match_fk': 'count'})

        if num_df.empty:
            return

        den_df = template_df.groupby(by=['scene_fk', 'bay_number'], as_index=False) \
            .count() \
            .rename(columns={'scene_match_fk': 'count'})

        result_df = num_df.merge(den_df, how='left', on=['scene_fk', 'bay_number'], suffixes=['_num', '_den'])
        result_df['sos'] = result_df['count_num'] / result_df['count_den']
        result_df['result'] = result_df.apply(lambda row: 1 if row['sos'] >= sos_threshold else 0, axis=1)
        result = sum(result_df['result'])

        self.common.write_to_db_result(
            fk=kpi_id,
            numerator_id=self.own_manufacturer,
            numerator_result=result,
            denominator_id=self.store_info['store_fk'][0],
            denominator_result=1,
            result=result
        )

    def calculate_scene_availability_kpi(self):
        """Iterates through Scene Availability type KPI"""

        for kpi in Const.KPIs[Const.SCENE_AVAILABILITY]:
            self.calculate_scene_availability(
                kpi=kpi['name'],
                template=kpi['template']
            )

    def calculate_scene_availability(self, kpi, template):
        """

        """

        kpi_id = self.common.get_kpi_fk_by_kpi_name(kpi)
        template_ids = self.get_template_fks(template)

        filtered_df = self.filter_df(self.mpis,
                                     filters={
                                         'template_fk': template_ids
                                     })

        self.common.write_to_db_result(
            fk=kpi_id,
        )

    def calculate_facings_sos_kpi(self):
        """Iterates through Facings SOS type KPI."""

        for kpi in Const.KPIs[Const.FACINGS_SOS]:
            self.calculate_facings_sos(
                kpi[Const.NAME],
                kpi[Const.TEMPLATE],
                kpi[Const.NUMERATOR],
                kpi[Const.DENOMINATOR],
                kpi[Const.CONTEXT]
            )

    def calculate_facings_sos(self, kpi, templates, numerator, denominator, context):
        """
        Calculates the SOS for manufacturer in category.

        :param kpi: The KPI name.
        :param templates: The list of templates to filter by.
        :param numerator:
        :param denominator:
        :param context:
        """

        kpi_id = self.common.get_kpi_fk_by_kpi_name(kpi)
        template_ids = self.get_template_fks(templates)

        template_df = self.filter_df(self.mpis, filters={'template_fk': template_ids})
        empty_df = self.filter_df(template_df, filters={'product_fk': 0}) \
            .groupby(by=['category_fk', 'manufacturer_fk'], as_index=False) \
            .count()[['category_fk', 'manufacturer_fk', 'scene_match_fk']] \
            .rename(columns={'scene_match_fk': 'empty_count'})
        cat_man = template_df.groupby(by=['category_fk', 'manufacturer_fk'], as_index=False) \
            .count() \
            .rename(columns={'scene_match_fk': 'manufacturer_count'})
        cat_count = cat_man.groupby('category_fk', as_index=False) \
            .sum() \
            .rename(columns={'manufacturer_count': 'category_count'})[['category_fk', 'category_count']]
        cat_man = cat_man.merge(empty_df, on=['category_fk', 'manufacturer_fk'])
        cat_man_count = cat_man.merge(cat_count, on='category_fk')

        for _, row in cat_man_count.iterrows():
            self.common.write_to_db_result(
                fk=kpi_id,
                numerator_id=row[numerator+'_fk'],
                numerator_result=row[numerator+'_count'],
                denominator_id=row[denominator+'_fk'],
                denominator_result=row[denominator+'_count'],
                context_id=row.get(context+'_fk'),
                result=row[numerator+'_count'] / row[denominator+'_count'],
            )

    def calculate_share_of_scenes_kpi(self):
        """Iterates through Share of Scenes type KPI."""

        for kpi in Const.KPIs[Const.SHARE_OF_SCENES]:
            self.calculate_share_of_scenes(
                kpi['name'],
                kpi['template']
            )

    def calculate_share_of_scenes(self, kpi, templates):
        """
        Calculates the number manufacturer-plurality scenes for manufacturer in category.

        :param kpi: Name of KPI.
        :param templates: List of templates to filter by.
        """

        kpi_id = self.common.get_kpi_fk_by_kpi_name(kpi)
        template_ids = self.get_template_fks(templates)

        template_df = self.filter_df(self.mpis, {'template_fk': template_ids})

        category_df = template_df.groupby(by=['category_fk', 'scene_fk'], as_index=False).count()
        category_df['scene_count'] = 1
        category_df = category_df.groupby(by=['category_fk'], as_index=False).count()[['category_fk', 'scene_count']]

        product_count = template_df.groupby(by=['category_fk', 'scene_fk', 'manufacturer_fk'], as_index=False) \
            .count() \
            .rename(columns={'scene_match_fk': 'count'})
        scene_max = product_count[
            product_count
                .groupby(by=['category_fk', 'scene_fk'])['count']
                .transform(max) == product_count['count']]
        scene_max['count'] = 1
        scene_max_count = scene_max.groupby(by=['category_fk', 'manufacturer_fk'], as_index=False) \
            .count() \
            .merge(category_df, on='category_fk')

        for manufacturer_category in scene_max_count.itertuples():
            self.common.write_to_db_result(
                fk=kpi_id,
                numerator_id=manufacturer_category['manufacturer_fk'],
                numerator_result=manufacturer_category['count'],
                denominator_id=manufacturer_category['category_fk'],
                denominator_result=manufacturer_category['scene_count'],
                result=manufacturer_category['count'] / manufacturer_category['scene_count']
            )

    def get_template_fks(self, template_names):
        if not hasattr(template_names, '__iter__'):
            template_names = [template_names]
        return self.templates[self.templates['template_name'].isin(template_names)]['template_fk']  # unique?

    def get_manufacturer_fks(self, manufacturer_names):
        if not hasattr(manufacturer_names, '__iter__'):
            manufacturer_names = [manufacturer_names]
        return self.products[self.products['manufacturer_name'].isin(manufacturer_names)]['manufacturer_fk'].unique()

    def get_product_fks(self, product_names):
        if not hasattr(product_names, '__iter__'):
            product_names = [product_names]
        return self.products[self.products['product_name'].isin(product_names)]['product_fk']  # unique?

    @staticmethod
    def filter_df(df, filters, exclude=False):
        """
        Filters dataframe based on `filters`.

        :param df: Dataframe to be filtered.
        :param filters: Dictionary of column-value pairs to filter by.
        :param exclude: If true, filter excludes values in `filters`.
        :return: Returns filtered dataframe.
        """

        for key, val in filters.items():
            if not hasattr(val, '__iter__'):
                val = [val]

            if exclude:
                df = df[~df[key].isin(val)]
            else:
                df = df[df[key].isin(val)]
        return df

    def commit_results(self):
        self.common.commit_results_data()
