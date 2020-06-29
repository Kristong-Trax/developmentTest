from Trax.Algo.Calculations.Core.DataProvider import Data
from Trax.Utils.Logging.Logger import Log
from KPIUtils_v2.Utils.GlobalScripts.Scripts import GlobalSceneToolBox
from KPIUtils_v2.GlobalDataProvider.PsDataProvider import PsDataProvider
from KPIUtils_v2.DB.PsProjectConnector import PSProjectConnector
from Trax.Cloud.Services.Connector.Keys import DbUsers

import pandas as pd
import numpy as np
from collections import OrderedDict
import re
import ast

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


class SceneToolBox(GlobalSceneToolBox):

    def __init__(self, data_provider, output):
        GlobalSceneToolBox.__init__(self, data_provider, output)
        self.ps_data_provider = PsDataProvider(self.data_provider, self.output)
        self.rds_conn = PSProjectConnector(self.project_name, DbUsers.CalculationEng)
        self.targets = self.ps_data_provider.get_kpi_external_targets()
        self.store_area = self.get_store_area_df()
        self.gold_zone_scene_location_kpi = ['Lobby/Entrance', 'Main Alley/Hot Zone', 'Gold Zone End Cap',
                                             'Lobby/Main Entrance']

    def main_function(self):
        self.calculate_scene_location()
        return

    def calculate_scene_location(self):
        scene_location_kpi_template = self.targets[self.targets[Consts.KPI_TYPE].isin(['Scene Location'])]
        for i, row in scene_location_kpi_template.iterrows():
            row = self.apply_json_parser(row)
            return_holder = self._get_kpi_name_and_fk(row)
            scene_store_area_df = self.store_area
            # scene_store_area_df['result'] = scene_store_area_df.name.apply(
            #     lambda x: 1 if x in self.gold_zone_scene_location_kpi else 0)
            scene_store_area_df['result'] = np.in1d(scene_store_area_df.name.values,
                                                    self.gold_zone_scene_location_kpi) * 1

            for store_area_row in scene_store_area_df.itertuples():
                # result_dict = {'kpi_name': return_holder[0], 'kpi_fk': return_holder[1],
                #                'numerator_id': store_area_row.pk,
                #                'numerator_result': store_area_row.result,
                #                'denominator_id': self.store_id, 'denominator_result': 1,
                #                'result': store_area_row.result}
                self.common.write_to_db_result(fk=return_holder[1], numerator_id=store_area_row.pk,
                                               numerator_result=store_area_row.result, result=store_area_row.result,
                                               denominator_id=self.store_id, denominator_result=1,
                                               should_enter=True, by_scene=True)

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

    @staticmethod
    def prereq_parse_json_row(item):
        '''
        primarly logic for formatting the value of the json
        '''

        if isinstance(item, list):
            container = OrderedDict()
            for it in item:
                # value = re.findall("[0-9a-zA-Z_]+", it)
                value = re.findall("'([^']*)'", it)
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
            container = ast.literal_eval(item)
        return container

    def get_store_area_df(self):
        query = """
                 select st.pk, sst.scene_fk, st.name, sc.session_uid 
                 from probedata.scene_store_task_area_group_items sst
                 join static.store_task_area_group_items st on st.pk=sst.store_task_area_group_item_fk
                 join probedata.scene sc on sc.pk=sst.scene_fk
                 where sc.delete_time is null and sc.session_uid = '{}' and sst.scene_fk = '{}';
                 """.format(self.session_uid, self.scene_info.scene_fk.iat[0])

        df = pd.read_sql_query(query, self.rds_conn.db)
        return df

    def _get_kpi_name_and_fk(self, row):
        kpi_name = row[Consts.KPI_NAME]
        kpi_fk = self.common.get_kpi_fk_by_kpi_type(kpi_name)
        output = [kpi_name, kpi_fk]
        return output
