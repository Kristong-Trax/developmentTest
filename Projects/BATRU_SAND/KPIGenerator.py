
from Trax.Utils.Logging.Logger import Log

from Projects.BATRU_SAND.Utils.KPIToolBox import BATRU_SANDToolBox

__author__ = 'uri'


class BATRU_SANDGenerator:

    def __init__(self, data_provider, output):
        self.data_provider = data_provider
        self.output = output
        self.project_name = data_provider.project_name
        self.session_uid = self.data_provider.session_uid
        self.tool_box = BATRU_SANDToolBox(self.data_provider, self.output)
        # # # upload assortment for p1
        # # assortment_file_path = '/home/idanr/Desktop/StoreAssortment.csv'
        # self.tool_box.upload_store_assortment_file_for_p1(assortment_file_path)

    def main_function(self):
        """
        This is the main KPI calculation function.
        It calculates the score for every KPI set and saves it to the DB.
        """
        if self.tool_box.scif.empty:
            Log.warning('Scene item facts is empty for session {}'.format(self.tool_box.session_uid))
            return
        self.tool_box.main_calculation()
        self.tool_box.commit_results_data()
