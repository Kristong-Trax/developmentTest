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

from Projects.CCUS.MILITARY.Utils.Const import *

__author__ = 'trevaris'

PATH = os.path.dirname(__file__)
SHEETS = [KPI, SCENE_AVAILABILITY, COMPLIANT_BAY_COUNT, FACINGS_SOS, SHARE_OF_SCENES]
MPIS_COLUMNS = [SCENE_MATCH_FK,  TEMPLATE_FK, TEMPLATE_NAME, SCENE_FK, LOCATION, BAY_NUMBER, MANUFACTURER_FK,
                BRAND_FK, PRODUCT_FK, PRODUCT_TYPE, 'size', 'category_fk', FACINGS, 'SKUs', KEY_PACKAGE,
                'survey_question_fk', 'question_text', 'scene_survey_response']

LOGIC = {
    '': op.pos,
    'AND': op.and_,
    'NEVER': lambda x, y: x and not y,
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
        key_packages = self.scif[[PRODUCT_FK, KEY_PACKAGE]] \
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
            COMPLIANT_BAY_COUNT: self.calculate_compliant_bay,
            SCENE_AVAILABILITY: self.calculate_scene_availability,
            SHARE_OF_SCENES: self.calculate_share_of_scenes,
            FACINGS_SOS: self.calculate_facings_sos,
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

        self.save_results()

    def calculate_compliant_bay(self, kpi):
        templates = kpi[TEMPLATE_NAME].split(",")
        template_df = self.filter_df(self.mpis, filters={TEMPLATE_NAME: templates})
        filters = {
            kpi['a_filter_1']: self.sanitize(kpi['a_value_1']),
            kpi['a_exclusion_filter']: self.sanitize(kpi['a_exclusion_value'])}

        num_df = self.filter_df(template_df, filters=filters) \
            .groupby(by=[SCENE_FK, BAY_NUMBER], as_index=False) \
            .count() \
            .rename(columns={SCENE_MATCH_FK: COUNT})

        den_df = template_df.groupby(by=[SCENE_FK, BAY_NUMBER]) \
            .count() \
            .rename(columns={SCENE_MATCH_FK: COUNT})

        result_df = num_df.merge(den_df, how='left', on=[SCENE_FK, BAY_NUMBER], suffixes=['_num', '_den'])
        result_df['sos'] = result_df['count_num'] / result_df['count_den']
        result_df[RESULT] = result_df.apply(lambda row: int(row['sos'] >= 0.5), result_type='reduce', axis=1)
        result = sum(result_df[RESULT])

        self.append_results({
            FK: None,
            KPI_NAME: kpi[KPI_NAME],
            NUMERATOR_ID: self.own_manufacturer,
            NUMERATOR_RESULT: result,
            DENOMINATOR_ID: self.store_id,
            DENOMINATOR_RESULT: 1,
            RESULT: result
        })

    def calculate_scene_availability(self, kpi):
        filters = {col: self.sanitize(kpi[col]) for col in MPIS_COLUMNS if col in kpi.index and kpi[col]}
        filtered_df = self.filter_df(self.mpis, filters=filters)
        scenes = filtered_df[SCENE_FK].unique()
        for scene in scenes:
            if pd.notna(kpi[KPI_PARENT_ID]):
                parent = self.filter_df(self.results_df,
                                        filters={IDENTIFIER_RESULT: kpi[KPI_PARENT_ID], SCENE_FK: scene})[RESULT].any()
                if not parent:
                    continue

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
                    conjunction = LOGIC.get(kpi[dataset+'_logic'])
                    test = data[kpi[dataset+'_test']].sum() >= kpi[dataset+'_threshold']
                    result = conjunction(result, test)

            if pd.notna(kpi['no_failures']) and not result:
                return

            if kpi[KPI_NAME] == "Where is the 24 Pack 12 Ounce Coca-Cola CSD Brands Display Located?":
                self.append_results({
                    FK: None,
                    KPI_NAME: kpi[KPI_NAME],
                    SCENE_FK: scene,
                    NUMERATOR_ID: scene_df.iloc[0][LOCATION],
                    NUMERATOR_RESULT: self.get_scene_location(scene),
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
        template_df = self.filter_df(self.mpis, {TEMPLATE_NAME: templates})

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

    def get_scene_location(self, scene):
        return self.filter_df(self.mpis, filters={SCENE_FK: scene})[LOCATION].iloc[0]

    @staticmethod
    def filter_df(df, filters, exclude=False, func='isin'):
        """
        Filters `df`` based on `filters`.

        :param df: DataFrame to be filtered.
        :param filters: Dictionary of column-value pairs to filter by.
        :param exclude: If true, filter excludes values in `filters`.
        :param func:
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
            except KeyError:
                pass
        return df

    @staticmethod
    def get_kpi_template(dir_path, data='Data'):
        """Extracts KPI design from accompanying template"""

        data_path = os.path.join(os.path.dirname(dir_path), data)
        data_files = glob.glob1(data_path, "*.xlsx")
        dates = {datetime.strptime(re.search("\d{6}", filename).group(), "%m%d%y"): filename for filename in data_files}
        most_recent = dates[max(dates.keys())]
        template_path = os.path.join(data_path, most_recent)
        kpi_templates = {sheet: pd.read_excel(template_path, sheet_name=sheet) for sheet in SHEETS}

        return kpi_templates

    @staticmethod
    def sanitize(data, separator=","):
        sanitized = data
        try:
            sanitized = int(data)
        except (TypeError, ValueError):
            if isinstance(data, (str, unicode)):
                split = data.split(separator)
                if len(split) > 1:
                    sanitized = [MilitaryToolBox.sanitize(datum) for datum in split]
            elif hasattr(data, "__iter__"):
                sanitized = [MilitaryToolBox.sanitize(datum) for datum in data]

        return sanitized

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
