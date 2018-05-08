import os
from Trax.Utils.Logging.Logger import Log
from Projects.INBEVBR.Utils.ParseTemplates import parse_template
from Projects.INBEVBR.Utils.KPIToolBox import INBEVBRToolBox, log_runtime

__author__ = 'Yasmin'

TEMPLATE_PATH = os.path.join('/home/Yasmin/dev/trax_ace_factory/Projects/INBEVBR/', 'Data',
                             'Setupfile_current.xlsx')
class INBEVBRGenerator:

    def __init__(self, data_provider, output):
        self.data_provider = data_provider
        self.output = output
        self.project_name = data_provider.project_name
        self.session_uid = self.data_provider.session_uid
        self.tool_box = INBEVBRToolBox(self.data_provider, self.output)

    @log_runtime('Total Calculations', log_start=True)
    def main_function(self):
        """
        This is the main KPI calculation function.
        It calculates the score for every KPI set and saves it to the DB.
        """
        if self.tool_box.scif.empty:
            Log.warning('Scene item facts is empty for this session')
        # for kpi_set_fk in self.tool_box.kpi_static_data['kpi_set_fk'].unique().tolist():

        kpi_template = parse_template(TEMPLATE_PATH, sheet_name='KPIs')
        kpi_sets = filter(None, kpi_template['Tested KPI Group'].unique().tolist())
        for kpi_set_name in kpi_sets:
            kpi_set_fk = self.tool_box.kpi_static_data.loc[
                self.tool_box.kpi_static_data['kpi_set_name'] == kpi_set_name]['kpi_set_fk'].iloc[0]
            score = self.tool_box.main_calculation(kpi_set_fk=kpi_set_fk)
            self.tool_box.write_to_db_result(kpi_set_fk, score, self.tool_box.LEVEL1)
        self.tool_box.commit_results_data()