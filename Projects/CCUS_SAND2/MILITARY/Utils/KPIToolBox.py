from __future__ import division
from datetime import datetime
import glob
import operator as op
import os
import pandas as pd
import re

from KPIUtils_v2.Utils.GlobalScripts.Scripts import GlobalSessionToolBox
from KPIUtils_v2.GlobalDataProvider.PsDataProvider import PsDataProvider

from Trax.Algo.Calculations.Core.DataProvider import Data

from Projects.CCUS_SAND2.MILITARY.Utils.Const import *

__author__ = 'trevaris'

PATH = os.path.dirname(__file__)
SHEETS = [KPI, SCENE_AVAILABILITY, COMPLIANT_BAY_COUNT, FACINGS_SOS, SHARE_OF_SCENES]
MPIS_COLUMNS = ['scene_match_fk',  TEMPLATE_FK, TEMPLATE_NAME, SCENE_FK, LOCATION, 'bay_number',
                'manufacturer_fk', 'brand_fk', 'product_fk', PRODUCT_TYPE, 'size', 'category_fk', FACINGS, 'SKUs', KEY_PACKAGE,
                'survey_question_fk', 'question_text', 'scene_survey_response']


def not_and(x, y): return x and not y


LOGIC = {
    '': op.pos,
    'AND': op.and_,
    'NEVER': not_and,
    'OR': op.or_,
}


class MilitaryToolBox(GlobalSessionToolBox):
    def __init__(self, data_provider, output, common):
        GlobalSessionToolBox.__init__(self, data_provider, output, common)
        self.ps_data_provider = PsDataProvider(self.data_provider, self.output)
        self.own_manufacturer = self.data_provider[Data.OWN_MANUFACTURER].iloc[0]['param_value']
        self.products = data_provider[Data.PRODUCTS]

        store_areas = self.ps_data_provider.get_store_area_df() \
            .rename(columns={"store_area_name": LOCATION})
        key_packages = self.scif[[PRODUCT_FK, 'Key Package']] \
            .drop_duplicates()
        product_facings = self.scif[[SCENE_FK, 'location_type_fk', PRODUCT_FK, FACINGS, 'facings_ign_stack']] \
            .rename(columns={'facings_ign_stack': 'SKUs', 'location_type_fk': LOCATION})
        survey_response = self.ps_data_provider.get_scene_surveys_responses() \
            .rename(columns={'question_fk': 'survey_question_fk', 'selected_option_text': 'scene_survey_response'})

        try:
            self.mpis = self.matches \
                .merge(self.products, how='left', on=PRODUCT_FK, suffixes=['', '_p']) \
                .merge(self.scene_info, how='left', on=SCENE_FK, suffixes=['', '_s']) \
                .merge(self.templates, how='left', on=TEMPLATE_FK, suffixes=['', '_t']) \
                .merge(store_areas, how='left', on=SCENE_FK, suffixes=['', '_sa']) \
                .merge(key_packages, how='left', on=PRODUCT_FK, suffixes=['', '_kp']) \
                .merge(product_facings, how='left', on=[SCENE_FK, PRODUCT_FK], suffixes=['', '_pf']) \
                .merge(survey_response, how='left', on=SCENE_FK, suffixes=['', '_sr'])[MPIS_COLUMNS]
            self.mpis.loc[:, 'Own-Manufacturer'] = self.own_manufacturer
        except ValueError:
            self.mpis = pd.DataFrame()

        self.kpi_templates = self.get_kpi_template(PATH, 'Data')
        self.results_df = pd.DataFrame(
            columns=[FK, KPI_NAME, SCENE_FK, NUMERATOR_ID, NUMERATOR_RESULT, DENOMINATOR_ID, DENOMINATOR_RESULT, RESULT,
                     IDENTIFIER_RESULT, IDENTIFIER_PARENT, SHOULD_ENTER])

        self.calculations = {
            SCENE_AVAILABILITY: self.calculate_scene_availability,
            SHARE_OF_SCENES: self.calculate_share_of_scenes,
            FACINGS_SOS: self.calculate_facings_sos2,
        }

    def main_calculation(self):
        if self.mpis.empty or self.filter_df(self.store_info, {'region_name': REGION}).empty:
            return

        parent_kpi = self.filter_df(self.kpi_templates[KPI], filters=[KPI_PARENT_ID], func='isna')
        for _, row in parent_kpi.iterrows():
            if row[KPI_TYPE] in self.calculations:
                kpi = self.filter_df(self.kpi_templates[row[KPI_TYPE]], filters={KPI_NAME: row[KPI_NAME]}).iloc[0]
                self.calculations.get(row[KPI_TYPE])(kpi)

        child_kpi = self.filter_df(self.kpi_templates[KPI], filters=[KPI_PARENT_ID], func='notna')
        for _, row in child_kpi.iterrows():
            if row[KPI_TYPE] in self.calculations:
                kpi = self.filter_df(self.kpi_templates[row[KPI_TYPE]], filters={KPI_NAME: row[KPI_NAME]}).iloc[0]
                self.calculations.get(row[KPI_TYPE])(kpi)

        self.calculate_compliant_bay_kpi()
        self.calculate_facings_sos_kpi()
        self.save_results()

    def calculate_compliant_bay_kpi(self):
        """Iterates through Compliant Bay type KPI."""

        for kpi in KPIs[COMPLIANT_BAY_COUNT]:
            self.calculate_compliant_bay(
                kpi[NAME], kpi[TEMPLATE], kpi[MANUFACTURER], kpi['exclude_manufacturers'])

    def calculate_compliant_bay(self, kpi_name, templates, manufacturers, exclude_manufacturers=False, sos_threshold=.501):
        """
        Calculates the something I've already forgotten.

        :param kpi_name: Name of KPI.
        :param templates:
        :param manufacturers:
        :param exclude_manufacturers: If True, excludes `manufacturers` in filter.
        :param sos_threshold: Minimum percentage to determine display branding.
        """

        kpi_id = self.common.get_kpi_fk_by_kpi_name(kpi_name)
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

        self.append_results({
            FK: kpi_id,
            KPI_NAME: kpi_name,
            NUMERATOR_ID: self.own_manufacturer,
            NUMERATOR_RESULT: result,
            DENOMINATOR_ID: self.store_id,
            DENOMINATOR_RESULT: 1,
            RESULT: result})

    def calculate_scene_availability(self, kpi):
        filters = {col: kpi[col] for col in MPIS_COLUMNS if col in kpi.index and kpi[col]}
        filtered_df = self.filter_df(self.mpis, filters=filters)

        for scene in filtered_df[SCENE_FK].unique():
            if pd.notna(kpi[KPI_PARENT_ID]):
                parent = self.filter_df(self.results_df,
                                    filters={IDENTIFIER_RESULT: kpi[KPI_PARENT_ID], SCENE_FK: scene})[RESULT].any()
                if not parent:
                    return

            scene_df = self.filter_df(filtered_df, filters={SCENE_FK: scene})
            datasets = dict.fromkeys(['a', 'b', 'c'], pd.DataFrame())
            for dataset in datasets.keys():
                for i in range(1, 3):
                    try:
                        if pd.notna(kpi[dataset+"_filter_"+str(i)]):
                            filters = [kpi[index] for index in kpi.index if dataset+'_filter' in index and pd.notna(kpi[index])]
                            values = [kpi[index] for index in kpi.index if dataset+'_value' in index and pd.notna(kpi[index])]
                            df = self.filter_df(
                                scene_df,
                                filters={col: val for col, val in zip(filters, values) if pd.notna(col)})
                            datasets[dataset] = df
                    except KeyError:
                        pass

            dataset_a_total = datasets['a'].groupby(PRODUCT_FK).count()[kpi['a_test']].sum()
            result = dataset_a_total >= kpi['a_threshold']
            for dataset, data in datasets.items()[1:]:
                if not data.empty:
                    conjunction = LOGIC.get(dataset+'_logic')
                    test = data[kpi[dataset+'_test']].sum() >= kpi[dataset+'_threshold']
                    result = conjunction(result, test)

            if kpi['no_failures'] and not result:
                return

            if kpi[KPI_NAME] == "Where is the 24 Pack 12 Ounce Coca-Cola CSD Brands Display Located?":
                self.append_results({
                    FK: None,
                    KPI_NAME: kpi[KPI_NAME],
                    SCENE_FK: scene,
                    NUMERATOR_ID: scene_df.iloc[0][LOCATION],
                    NUMERATOR_RESULT: None,  # ???
                    DENOMINATOR_ID: self.store_id,
                    DENOMINATOR_RESULT: scene,
                    RESULT: int(result),
                    IDENTIFIER_RESULT: kpi[KPI_ID],
                    IDENTIFIER_PARENT: kpi[KPI_PARENT_ID],
                    SHOULD_ENTER: True
                })
            else:
                self.append_results({
                    FK: None,
                    KPI_NAME: kpi[KPI_NAME],
                    SCENE_FK: scene,
                    NUMERATOR_ID: self.own_manufacturer,
                    NUMERATOR_RESULT: int(result),
                    DENOMINATOR_ID: self.store_id,
                    DENOMINATOR_RESULT: kpi[DENOMINATOR_RESULT],
                    RESULT: int(result),
                    IDENTIFIER_RESULT: kpi[KPI_ID],
                    IDENTIFIER_PARENT: kpi[KPI_PARENT_ID],
                    SHOULD_ENTER: True
                })

    def calculate_facings_sos_kpi(self):
        """Iterates through Facings SOS type KPI."""

        for kpi in KPIs[FACINGS_SOS]:
            self.calculate_facings_sos(
                kpi[NAME],
                kpi[TEMPLATE],
                kpi[NUMERATOR],
                kpi[DENOMINATOR],
                kpi[CONTEXT]
            )

    def calculate_facings_sos(self, kpi):
        """

        """

        templates = kpi[TEMPLATE_NAME].split(",")
        template_df = self.filter_df(self.mpis, filters={TEMPLATE_NAME: templates})
        den_df = self.filter_df(template_df, filters={kpi['b_filter_1']: kpi['b_value_1'].split(',')}) \
            .rename(columns={'scene_match_fk': 'total'}) \
            .groupby(by=['category_fk'], as_index=False) \
            .count()
        num_df = self.filter_df(template_df, filters={kpi['a_filter_1']: kpi['a_value_1'].split(',')}) \
            .rename(columns={'scene_match_fk': 'count'}) \
            .groupby(by=[CATEGORY_FK, MANUFACTURER_FK], as_index=False) \
            .count()
        num_df = num_df.merge(den_df[[CATEGORY_FK, 'total']], how='left', on=[CATEGORY_FK])

        num_entity = kpi[NUMERATOR_ENTITY]
        den_entity = kpi[DENOMINATOR_ENTITY]
        for _, row in num_df.iterrows():
            try:
                result = row['count'] / row['total'] * 100
            except ZeroDivisionError:
                result = 0

            self.append_results({
                FK: None,
                KPI_NAME: kpi[KPI_NAME],
                NUMERATOR_ID: row[num_entity],
                NUMERATOR_RESULT: row['count'],
                DENOMINATOR_ID: row[den_entity],
                DENOMINATOR_RESULT: row['total'],
                CONTEXT_ID: getattr(row, 'Context Entity', None),
                RESULT: result
            })

    def calculate_share_of_scenes(self, kpi):
        """
        Calculates the number manufacturer-plurality scenes for manufacturer in category.
        """

        templates = kpi[TEMPLATE_NAME].split(",")
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
        scene_max.loc[:, 'count'] = 1
        scene_max_count = scene_max.groupby(by=['category_fk', 'manufacturer_fk'], as_index=False) \
            .count() \
            .merge(category_df, on='category_fk')

        for manufacturer_category in scene_max_count.itertuples():
            self.append_results({
                FK: None,
                KPI_NAME: kpi[KPI_NAME],
                NUMERATOR_ID: manufacturer_category.manufacturer_fk,
                NUMERATOR_RESULT: manufacturer_category.count,
                DENOMINATOR_ID: manufacturer_category.category_fk,
                DENOMINATOR_RESULT: manufacturer_category.scene_count,
                RESULT: manufacturer_category.count / manufacturer_category.scene_count})

    # TODO
    def get_values(self, key):
        d = {
            'Own Manufacturer': lambda _: self.own_manufacturer
        }

        d.get(get)

        return

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
    def filter_df(df, filters, exclude=False, func='isin'):
        """
        Filters `df`` based on `filters`.

        :param df: DataFrame to be filtered.
        :param filters: Dictionary of column-value pairs to filter by.
        :param exclude: If true, filter excludes values in `filters`.
        :return: Returns filtered DataFrame.
        """

        funcs = {
            'isin': pd.Series.isin,
            'isna': pd.Series.isna,
            'notna': pd.Series.notna
        }

        vert = op.inv if exclude or 'not ' in func else op.pos
        func = funcs.get(func, func)


        if not hasattr(filters, '__iter__') or isinstance(filters, str):
            filters = [filters]

        for col in filters:
            try:
                val = filters.get(col)
                if not hasattr(val, '__iter__') or isinstance(val, str):
                    val = [val]
                df = df[vert(func(df[col], val))]
            except AttributeError:
                df = df[vert(func(df[col]))]
        return df

    @staticmethod
    def get_kpi_template(dir_path, data='Data'):
        data_path = os.path.join(os.path.dirname(dir_path), data)
        data_files = glob.glob1(data_path, "*.xlsx")
        dates = {datetime.strptime(re.search("\d{6}", filename).group(), "%m%d%y"): filename for filename in data_files}
        most_recent = dates[max(dates.keys())]
        template_path = os.path.join(data_path, most_recent)
        kpi_templates = {sheet: pd.read_excel(template_path, sheet_name=sheet) for sheet in SHEETS}

        return kpi_templates

    def append_results(self, results):
        """
        Organizes `results` according to the columns in `results_df` then appends them.

        :param results: Dictionary containing column-value pairs to add to results DataFrame.
        """

        if isinstance(results, dict):
            results = [results.get(col) for col in self.results_df.columns]
        self.results_df.loc[self.results_df.shape[0], self.results_df.columns.tolist()] = results

    def save_results(self):
        """
        Writes results to database.
        """

        for _, result in self.results_df.iterrows():
            result[FK] = self.common.get_kpi_fk_by_kpi_name(result[KPI_NAME])
            self.write_to_db(**result.to_dict())

    def commit_results(self):
        self.common.commit_results_data()