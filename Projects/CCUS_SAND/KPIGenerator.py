
from Trax.Utils.Logging.Logger import Log
from Projects.CCUS_SAND.Utils.CCUSToolBox import log_runtime, CCUSToolBox
from Projects.CCUS_SAND.DISPLAYS.KPIToolBox import DISPLAYSToolBox
from Projects.CCUS_SAND.OBBO.KPIToolBox import OBBOToolBox
from Projects.CCUS_SAND.MSC.Utils.KPIToolBox import MSCToolBox


__author__ = 'ortal'


class CCUSGenerator:
    def __init__(self, data_provider, output):
        self.data_provider = data_provider
        self.output = output

    @log_runtime('Total Calculations', log_start=True)
    def main_function(self):
        """
        This is the main KPI calculation function.
        It calculates the score for every KPI set and saves it to the DB.
        """
        self.calculate_manufacturer_displays()
        self.calculate_obbo()
        self.calculate_dunkin_donuts()
        self.calculate_msc()

    @log_runtime('Manufacturer Displays Calculations')
    def calculate_msc(self):
        tool_box = MSCToolBox(self.data_provider, self.output)
        kpi_sets = tool_box.kpi_static_data['kpi_set_name'].unique().tolist()
        for set in kpi_sets:
            if set == 'MSC':
                tool_box.main_calculation()
                set_fk = \
                    tool_box.kpi_static_data[tool_box.kpi_static_data['kpi_set_name'] == set]['kpi_set_fk'].values[0]
                tool_box.commit_results_data(kpi_set_fk=set_fk)

    @log_runtime('Manufacturer Displays Calculations')
    def calculate_manufacturer_displays(self):
        tool_box = DISPLAYSToolBox(self.data_provider, self.output)
        tool_box.main_calculation()
        tool_box.commit_results_data()

    @log_runtime('OBBO Calculations')
    def calculate_obbo(self):
        tool_box = OBBOToolBox(self.data_provider, self.output)
        tool_box.main_calculation()
        tool_box.commit_results_data()

    @log_runtime('Dunkin Donuts Calculations')
    def calculate_dunkin_donuts(self):
        set_name = 'Dunkin Donuts'
        tool_box = CCUSToolBox(self.data_provider, self.output)
        if tool_box.scif.empty:
            Log.warning('Scene item facts is empty for this session')
        tool_box.tools.update_templates()
        tool_box.main_calculation(set_name=set_name)
        tool_box.save_level1(set_name=set_name, score=100)
        set_fk = tool_box.kpi_static_data[tool_box.kpi_static_data['kpi_set_name'] == set_name]['kpi_set_fk'].values[0]
        tool_box.commit_results_data(kpi_set_fk=set_fk)
        Log.info('Dunkin Donuts: Downloading templates took {}'.format(tool_box.download_time))
