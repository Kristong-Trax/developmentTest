import operator as op
import pandas as pd

from KPIUtils_v2.Utils.GlobalScripts.Scripts import GlobalSessionToolBox

from Projects.CCBOTTLERSUS.FACINGS_BY_SHELF.Const import *

__author__ = 'trevaris'

COLUMNS = ['scene_match_fk', 'template_fk', 'scene_fk', 'shelf_number', 'product_fk']


class FacingsToolBox(GlobalSessionToolBox):
    def __init__(self, data_provider, output, common):
        GlobalSessionToolBox.__init__(self, data_provider, output, common)

        self.mpis = self.matches.merge(self.scene_info, on='scene_fk')[COLUMNS]
        self.results_df = pd.DataFrame(
            columns=[FK, NUMERATOR_ID, DENOMINATOR_ID, CONTEXT_ID, RESULT, SCORE]
        )

    def main_calculation(self):
        self.calculate_facings_by_shelf()
        self.save_to_db()

    def calculate_facings_by_shelf(self):
        kpi_name = 'Facings by Shelf in Location'
        kpi_id = self.common.get_kpi_fk_by_kpi_name(kpi_name)

        df = self.mpis.groupby(by=[TEMPLATE_FK, SHELF_NUMBER, PRODUCT_FK], as_index=False) \
            .count() \
            .rename(columns={'scene_match_fk': FACINGS})

        for _, row in df.iterrows():
            self.save_to_results({
                FK: kpi_id,
                NUMERATOR_ID: row[PRODUCT_FK],
                DENOMINATOR_ID: row[SHELF_NUMBER],
                CONTEXT_ID: row[TEMPLATE_FK],
                RESULT: row[FACINGS],
                SCORE: row[SHELF_NUMBER]})

    def save_to_results(self, values):
        if isinstance(values, dict):
            values = [values.get(col) for col in self.results_df.columns]
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
                context_id=getattr(row, CONTEXT_ID),
                result=getattr(row, RESULT),
                score=row.score
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
