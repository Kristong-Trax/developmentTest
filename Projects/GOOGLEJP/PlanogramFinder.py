from Trax.Apps.Services.KEngine.Handlers.Utils.Scripts import PlanogramFinderBaseClass
import pandas as pd
from Trax.Apps.Services.KEngine.PRE.Resources.Constants import Keys
from Trax.Utils.Logging.Logger import Log

__author__ = 'Shivi'


class PlanogramCompliance(PlanogramFinderBaseClass):

    def get_planogram_id(self):
        project_name = self._data_provider._project_name

    def get_planogram_id_by_policies(self, project_name, data_provider):
        policies_table = pd.DataFrame()
        if policies_table.empty:
            Log.error("There is no Planogram that matches this scene.")
            return None
        else:
            return policies_table.iloc[0][Keys.PLANOGRAM_FK]
