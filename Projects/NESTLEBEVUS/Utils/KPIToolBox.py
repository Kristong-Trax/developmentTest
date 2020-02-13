from Trax.Algo.Calculations.Core.DataProvider import Data
from Trax.Utils.Logging.Logger import Log
from KPIUtils_v2.Utils.GlobalScripts.Scripts import GlobalSessionToolBox
import pandas as pd
import os

from datetime import datetime
from Projects.NESTLEBEVUS.Data.LocalConsts import Consts

# from KPIUtils_v2.Utils.Consts.DataProvider import 
# from KPIUtils_v2.Utils.Consts.DB import 
# from KPIUtils_v2.Utils.Consts.PS import 
# from KPIUtils_v2.Utils.Consts.GlobalConsts import 
# from KPIUtils_v2.Utils.Consts.Messages import 
# from KPIUtils_v2.Utils.Consts.Custom import 
# from KPIUtils_v2.Utils.Consts.OldDB import 

# from KPIUtils_v2.Calculations.AssortmentCalculations import Assortment
# from KPIUtils_v2.Calculations.AvailabilityCalculations import Availability
# from KPIUtils_v2.Calculations.NumberOfScenesCalculations import NumberOfScenes
# from KPIUtils_v2.Calculations.PositionGraphsCalculations import PositionGraphs
# from KPIUtils_v2.Calculations.SOSCalculations import SOS
# from KPIUtils_v2.Calculations.SequenceCalculations import Sequence
# from KPIUtils_v2.Calculations.SurveyCalculations import Survey

# from KPIUtils_v2.Calculations.CalculationsUtils import GENERALToolBoxCalculations

__author__ = 'krishnat'

TEMPLATE_PATH = os.path.join(os.path.dirname(os.path.realpath(__file__)), '..', 'Data',
                             'NestleRTD_Template_v1.1.xlsx')


def log_runtime(description, log_start=False):
    def decorator(func):
        def wrapper(*args, **kwargs):
            calc_start_time = datetime.utcnow()
            if log_start:
                Log.info('{} started at {}'.format(description, calc_start_time))
            result = func(*args, **kwargs)
            calc_end_time = datetime.utcnow()
            Log.info('{} took {}'.format(description, calc_end_time - calc_start_time))
            return result

        return wrapper

    return decorator


SHEETS = [Consts.KPIS, Consts.SOS, Consts.DISTRIBUTION]


class ToolBox(GlobalSessionToolBox):
    def __init__(self, data_provider, output):
        GlobalSessionToolBox.__init__(self, data_provider, output)
        self.templates = {}
        self.parse_template()
        self.match_product_in_scene = self.data_provider[Data.MATCHES]
        self.own_manufacturer_fk = int(self.data_provider.own_manufacturer.param_value.values[0])
        self.results_df = pd.DataFrame(columns=['kpi_name', 'kpi_fk', 'numerator_id', 'numerator_result',
                                                'denominator_id', 'denominator_result', 'result'])

    def parse_template(self):
        for sheet in SHEETS:
            self.templates[sheet] = pd.read_excel(TEMPLATE_PATH, sheet_name=sheet)

    def main_calculation(self):
        relevant_kpi_template = self.templates[Consts.KPIS]
        foundation_kpi_types = [Consts.SOS, Consts.DISTRIBUTION]
        foundation_kpi_template = relevant_kpi_template[
            relevant_kpi_template[Consts.KPI_TYPE].isin(foundation_kpi_types)]

        self._calculate_kpis_from_template(foundation_kpi_template)
        self.save_results_to_db()
        return

    def save_results_to_db(self):
        self.results_df.drop(columns=['kpi_name'], inplace=True)
        self.results_df.rename(columns={'kpi_fk': 'fk'}, inplace=True)
        self.results_df.fillna(0, inplace=True)
        results = self.results_df.to_dict('records')
        for result in results:
            self.write_to_db(**result)


    def _calculate_kpis_from_template(self, template_df):
        for i, row in template_df.iterrows():
            calculation_function = self._get_calculation_function_by_kpi_type(row[Consts.KPI_TYPE])
            try:
                kpi_row = self.templates[row[Consts.KPI_TYPE]][
                    self.templates[row[Consts.KPI_TYPE]][Consts.KPI_NAME].str.encode('utf-8') == row[
                        Consts.KPI_NAME].encode('utf-8')].iloc[
                    0]
            except IndexError:
                pass
            result_data = calculation_function(kpi_row)
            for result in result_data:
                if result['result'] <= 1:
                    result['result'] = result['result'] * 100
                self.results_df.loc[len(self.results_df), result.keys()] = result

    def _get_calculation_function_by_kpi_type(self, kpi_type):
        if kpi_type == Consts.SOS:
            return self.calculate_sos
        elif kpi_type == Consts.DISTRIBUTION:
            return self.calculate_distribution

    def calculate_distribution(self, row):
        kpi_name = row[Consts.KPI_NAME]
        kpi_fk = self.get_kpi_fk_by_kpi_type(kpi_name)
        relevant_product_fk = self.sanitize_values(row.product_list_pk)

        bool_array_present_products_fk_in_session = pd.np.in1d(relevant_product_fk,
                                                               self.scif.product_fk.unique().tolist())
        present_products_fk_in_session_index = pd.np.flatnonzero(bool_array_present_products_fk_in_session)
        present_products_fk_in_session = pd.np.array(relevant_product_fk).ravel()[present_products_fk_in_session_index]
        absent_products_fk_in_session_index = pd.np.flatnonzero(~ bool_array_present_products_fk_in_session)
        absent_products_fk_in_session = pd.np.array(relevant_product_fk).ravel()[absent_products_fk_in_session_index]

        result_dict_list = []
        for present_product_fk in present_products_fk_in_session:
            result = self.scif[self.scif.product_fk.isin([present_product_fk])].facings.iat[0]
            result_dict = {'kpi_name': kpi_name, 'kpi_fk': kpi_fk, 'numerator_id': present_product_fk, 'denominator_id': self.store_id,
                           'result': result}
            result_dict_list.append(result_dict)

        for absent_products_fk_in_session in absent_products_fk_in_session:
            result = 0
            result_dict = {'kpi_name': kpi_name, 'kpi_fk': kpi_fk, 'numerator_id': absent_products_fk_in_session, 'denominator_id': self.store_id,
                           'result': result}
            result_dict_list.append(result_dict)
        return result_dict_list

    def calculate_sos(self, row):
        kpi_name = row[Consts.KPI_NAME]
        kpi_fk = self.get_kpi_fk_by_kpi_type(kpi_name)
        iterate_by = self.sanitize_values(row.iterate_by)
        numerator_id = self.own_manufacturer_fk
        denominator_id = self.store_id

        # Have to save the sos by sku. So each sku will have its result (sos) saved
        # The skus relevant are saved in the iterate(in the template)
        sku_relevelant_scif = self.scif[self.scif.product_fk.isin(iterate_by)]

        result_dict_list = []
        for unique_product_fk in set(sku_relevelant_scif.product_fk):
            # The logic for denominator result: The denominator scif is filter by category_fk.
            # The tricky part is the category_fk is determined by the product_fk.
            # So if the category_fk is 1 for product_fk 99. Then the denominator scif is filtered by the
            # category fk 1.
            denominator_relevant_scif = \
                self.scif[self.scif.category_fk.isin(
                    self.scif.category_fk[self.scif.product_fk == unique_product_fk].to_numpy())]
            denominator_result = denominator_relevant_scif[
                row[Consts.OUTPUT]].sum() if not denominator_relevant_scif.empty else 1

            relevant_numerator_scif = self.scif[self.scif.product_fk.isin([unique_product_fk])]
            numerator_result = relevant_numerator_scif[
                row[Consts.OUTPUT]].sum() if not relevant_numerator_scif.empty else 0

            result = float(numerator_result) / denominator_result
            result_dict = {'kpi_name': kpi_name, 'kpi_fk': kpi_fk, 'numerator_id': numerator_id,
                           'denominator_id': denominator_id,
                           'result': result}
            result_dict_list.append(result_dict)
        return result_dict_list

    @staticmethod
    def sanitize_values(item):
        if pd.isna(item):
            return item
        else:
            items = [x.strip() for x in item.split(',')]
            return items
