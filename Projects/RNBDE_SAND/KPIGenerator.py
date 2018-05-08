from Trax.Utils.Logging.Logger import Log

from Projects.RNBDE_SAND.Utils.KPIToolBox import RNBDE_SANDToolBox, log_runtime

__author__ = 'uri'


class RNBDE_SANDGenerator:
    def __init__(self, data_provider, output):
        self.data_provider = data_provider
        self.output = output
        self.project_name = data_provider.project_name
        self.session_uid = self.data_provider.session_uid
        self.tool_box = RNBDE_SANDToolBox(self.data_provider, self.output)

    @log_runtime('Total Calculations', log_start=True)
    def main_function(self):
        """
        This is the main KPI calculation function.
        It calculates the score for every KPI set and saves it to the DB.
        """
        if self.tool_box.scif.empty:
            Log.warning('Scene item facts is empty for this session')
        set_names = self.tool_box.kpi_static_data['kpi_set_name'].unique().tolist()
        # self.tool_box.tools.update_templates()
        # set_names = ['Footcare - Refill', 'Footcare - Tights', 'Footcare - Insoles', 'Footcare - Gadgets',
        #              'Aircare - Refill', 'Aircare - Candles & Waxmelts', 'Aircare - Gadgets', 'Aircare - Spray',
        #              'ADW - Brand Group', 'ADW - Products', 'SWB - Products', 'SWB - Brand Group', 'MPC - Sagrotan',
        #              'MPC - Bath, Kitchen & Liquid', 'MPC - Wipes', 'MPC - Cillit', 'Displays', 'Gondola Ends',
        #              'Second Placement', 'Location']
        # set_names = ['Footcare', 'Aircare']
        for kpi_set_name in set_names:
            self.tool_box.main_calculation(set_name=kpi_set_name)
        Log.info('Downloading templates took {}'.format(self.tool_box.download_time))
        self.tool_box.commit_results_data()
