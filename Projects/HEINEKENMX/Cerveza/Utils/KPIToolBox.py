
from Trax.Algo.Calculations.Core.DataProvider import Data
from Trax.Utils.Logging.Logger import Log
from KPIUtils_v2.Utils.GlobalScripts.Scripts import GlobalSessionToolBox
import pandas as pd

from Projects.HEINEKENMX.Cerveza.Data.LocalConsts import Consts

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

__author__ = 'huntery'


class CervezaToolBox(GlobalSessionToolBox):

    def __init__(self, data_provider, output, common):
        GlobalSessionToolBox.__init__(self, data_provider, output, common)
        self.state = self.store_info['state']
        self.city = self.store_info['city']
        self.relevant_targets = self._get_relevant_external_targets()

    def main_calculation(self):
        score = 1
        return score

    def _get_relevant_external_targets(self):
        template_df = pd.read_excel(Consts.TEMPLATE_PATH, sheetname='Planogram_cerveza', header=1)
        template_df = template_df[(template_df['GZ'] == self.state) &
                                  (template_df['Ciudad'] == self.city)]

        template_df['Puertas'] = template_df['Puertas'].fillna(1)
        return template_df

class CervezaRealogram(object):
    def __init__(self, mpis, scene_fk, planogram_data):
        self.scene_fk = scene_fk
        self.mpis = mpis[mpis['scene_fk'] == self.scene_fk]


