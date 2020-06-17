from Trax.Algo.Calculations.Core.DataProvider import Data
from Trax.Utils.Logging.Logger import Log
from KPIUtils_v2.Utils.GlobalScripts.Scripts import GlobalSessionToolBox
from KPIUtils_v2.GlobalDataProvider.PsDataProvider import PsDataProvider
from KPIUtils_v2.Utils.Parsers import ParseInputKPI

from collections import OrderedDict
import pandas as pd
import simplejson
import numpy as np
import re

from Projects.MONDELEZUSPS.Data.LocalConsts import Consts

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


class ToolBox(GlobalSessionToolBox):

    def __init__(self, data_provider, output):
        GlobalSessionToolBox.__init__(self, data_provider, output)
        self.ps_data_provider = PsDataProvider(self.data_provider, self.output)
        self.targets = self.ps_data_provider.get_kpi_external_targets()
        self.results_df = pd.DataFrame(columns=['kpi_name', 'kpi_fk', 'numerator_id', 'numerator_result', 'context_id',
                                                'denominator_id', 'denominator_result', 'result', 'score'])

    def save_results_to_db(self):
        self.results_df.drop(columns=['kpi_name'], inplace=True)
        self.results_df.rename(columns={'kpi_fk': 'fk'}, inplace=True)
        self.results_df[['result']].fillna(0, inplace=True)
        results = self.results_df.to_dict('records')
        for result in results:
            result = simplejson.loads(simplejson.dumps(result, ignore_nan=True))
            self.write_to_db(**result)

    def main_calculation(self):
        relevant_kpi_types = [Consts.SHARE_OF_SCENES]
        targets = self.targets[self.targets[Consts.KPI_TYPE].isin(relevant_kpi_types)]

        self._calculate_kpis_from_template(targets)
        self.save_results_to_db()
        return

    def _calculate_kpis_from_template(self, template_df):
        for i, row in template_df.iterrows():
            calculation_function = self._get_calculation_function_by_kpi_type(row[Consts.KPI_TYPE])
            # row[row.index.str.contains('JSON')].apply(self.parse_json_row)
            row = self.apply_json_parser(row)
            result_data = calculation_function(row)
            if result_data and isinstance(result_data, list):
                for result in result_data:
                    self.results_df.loc[len(self.results_df), result.keys()] = result
            elif isinstance(result_data, dict):
                self.results_df.loc[len(self.results_df), result_data.keys()] = result_data

    def _get_calculation_function_by_kpi_type(self, kpi_type):
        if kpi_type == Consts.SHARE_OF_SCENES:
            return self.calculate_share_of_scenes

    def calculate_share_of_scenes(self, row):
        return_holder = self._get_kpi_name_and_fk(row)
        facings_threshold = row['Config Params: JSON'].get('facings_threshold')[0]
        numerator_type, denominator_type, context_type = self._get_numerator_and_denominator_type(
            row['Config Params: JSON'], context_relevant=True)

        relevant_scif = ParseInputKPI.filter_df(row['Location: JSON'], self.scif)
        result_dict_list = self.logic_of_sos(return_holder, relevant_scif, numerator_type, denominator_type,
                                             context_type, facings_threshold)
        if not result_dict_list:
            result_dict_list = [{'kpi_name': return_holder[0], 'kpi_fk': return_holder[1],'result': 0}]

        return result_dict_list

    @staticmethod
    def logic_of_sos(return_holder, relevant_scif, numerator_type, denominator_type, context_type,
                     facings_threshold):
        result_dict_list = []

        for unique_template_fk in set(relevant_scif[context_type]):
            template_unique_scif = relevant_scif[relevant_scif[context_type].isin([unique_template_fk])]
            for unique_category_fk in set(template_unique_scif[denominator_type]):
                denominator_result = 0
                category_unique_scif = template_unique_scif[
                    template_unique_scif[denominator_type].isin([unique_category_fk])]
                for unique_scene in set(category_unique_scif.scene_fk):
                    numerator_result = 0
                    scene_unique_scif = category_unique_scif[category_unique_scif.scene_fk.isin([unique_scene])]
                    for unique_manufacturer_fk in set(scene_unique_scif[numerator_type]):
                        manufacturer_unique_scif = scene_unique_scif[
                            scene_unique_scif[numerator_type].isin([unique_manufacturer_fk])]
                        if manufacturer_unique_scif.facings.sum() >= int(facings_threshold):
                            numerator_result = numerator_result + 1
                            denominator_result = denominator_result + 1
                if denominator_result != 0:
                    result = float(numerator_result) / denominator_result
                    result_dict = {'kpi_name': return_holder[0], 'kpi_fk': return_holder[1],
                                   'numerator_id': unique_manufacturer_fk,
                                   'numerator_result': numerator_result,
                                   'denominator_id': unique_category_fk, 'denominator_result': denominator_result,
                                   'result': result}
                    result_dict_list.append(result_dict)
        return result_dict_list

    def apply_json_parser(self, row):
        json_relevent_rows_with_parse_logic = row[row.index.str.contains('JSON')].apply(self.parse_json_row)
        row = row[~ row.index.isin(json_relevent_rows_with_parse_logic.index)].append(
            json_relevent_rows_with_parse_logic)
        return row

    def parse_json_row(self, item):
        '''
        :param item: improper json value (formatted incorrectly)
        :return: properly formatted json dictionary

        The function will be in conjunction with apply. The function will applied on the row(pandas series). This is
            meant to convert the json comprised of improper format of strings and lists to a proper dictionary value.
        '''
        if item:
            container = self.prereq_parse_json_row(item)
        else:
            container = None
        return container

    def _get_kpi_name_and_fk(self, row):
        kpi_name = row[Consts.KPI_NAME]
        kpi_fk = self.common.get_kpi_fk_by_kpi_type(kpi_name)
        output = [kpi_name, kpi_fk]
        return output

    @staticmethod
    def prereq_parse_json_row(item):
        '''
        primarly logic for formatting the value of the json
        '''

        if isinstance(item, list):
            container = OrderedDict()
            for it in item:
                value = re.findall("[0-9a-zA-Z_]+", it)
                if len(value) == 2:
                    for i in range(0, len(value), 2):
                        container[value[i]] = [value[i + 1]]
                else:
                    if len(container.items()) == 0:
                        print('issue')  # delete later
                        # raise error
                        # haven't encountered an this. So should raise an issue.
                        pass
                    else:
                        last_inserted_value_key = container.items()[-1][0]
                        container.get(last_inserted_value_key).append(value[0])
        else:
            print('need to write logic for prereq parse json')  # delete later
            container = 1
        return container

    @staticmethod
    def _get_numerator_and_denominator_type(config_param, context_relevant=False):
        numerator_type = config_param['numerator_type'][0]
        denominator_type = config_param['denominator_type'][0]
        if context_relevant:
            context_type = config_param['context_type'][0]
            return numerator_type, denominator_type, context_type
        return numerator_type, denominator_type
