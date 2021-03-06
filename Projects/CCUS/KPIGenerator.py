from Projects.CCUS.MONSTER.Utils.KPIToolBox import MONSTERToolBox
from Projects.CCUS.FSOP.Utils.KPIToolBox import FSOPToolBox
from Projects.CCUS.Utils.CCUSToolBox import log_runtime
from Projects.CCUS.DISPLAYS.KPIToolBox import DISPLAYSToolBox
from Projects.CCUS.Programs.Utils.KPIToolBox import PROGRAMSToolBox
from Projects.CCUS.Holiday.Utils.KPIToolBox import HOLIDAYToolBox
from Projects.CCUS.SpecialPrograms.Utils.KPIToolBox import SpecialProgramsToolBox
from Projects.CCUS.Pillars.Utils.KPIToolBox import PillarsPROGRAMSToolBox
from Projects.CCUS.Validation.Utils.KPIToolBox import VALIDATIONToolBox
from Projects.CCUS.JEFF_DEMO.Utils.KPIToolBox import JEFFToolBox
from Projects.CCUS.MILITARY.Utils.KPIToolBox import MilitaryToolBox
from KPIUtils_v2.DB.CommonV2 import Common


__author__ = 'ortal'


class CCUSGenerator:
    def __init__(self, data_provider, output):
        self.data_provider = data_provider
        self.output = output
        self.common = Common(self.data_provider)

    @log_runtime('Total Calculations', log_start=True)
    def main_function(self):
        """
        This is the main KPI calculation function.
        It calculates the score for every KPI set and saves it to the DB.
        """
        # self.calculate_fsop()

        # self.calculate_manufacturer_displays()
        # # self.calculate_obbo()
        # # self.calculate_dunkin_donuts()
        # self.calculate_monster()
        # self.calculate_programs()
        # self.calculate_holiday_programs()
        # # self.calculate_msc_new()
        # # self.calculate_gold_peak_block()
        # self.calculate_special_programs()
        # self.calculate_validation()
        self.calculate_pillars_programs()
        # self.calculate_jeff()
        self.calculate_military()

        self.common.commit_results_data()

    @log_runtime('Manufacturer Displays Calculations')
    def calculate_manufacturer_displays(self):
        tool_box = DISPLAYSToolBox(self.data_provider, self.output)
        tool_box.main_calculation()
        tool_box.commit_results_data()
        del tool_box

    @log_runtime('MONSTER Calculations')
    def calculate_monster(self):
        tool_box = MONSTERToolBox(self.data_provider, self.output)
        tool_box.main_calculation()
        tool_box.commit_results_data(kpi_set_fk=27)
        del tool_box

    @log_runtime('FSOP Calculations')
    def calculate_fsop(self):
        tool_box = FSOPToolBox(self.data_provider, self.output, self.common)
        tool_box.main_calculation()
        del tool_box

    @log_runtime('JEFF Calculations')
    def calculate_jeff(self):
        tool_box = JEFFToolBox(self.data_provider, self.output, self.common)
        tool_box.main_calculation()
        del tool_box

    # @log_runtime('OBBO Calculations')
    # def calculate_obbo(self):
    #     tool_box = OBBOToolBox(self.data_provider, self.output)
    #     tool_box.main_calculation()
    #     tool_box.commit_results_data()
    #
    # @log_runtime('Dunkin Donuts Calculations')
    # def calculate_dunkin_donuts(self):
    #     set_name = 'Dunkin Donuts'
    #     tool_box = CCUSToolBox(self.data_provider, self.output)
    #     if tool_box.scif.empty:
    #         Log.warning('Scene item facts is empty for this session')
    #     tool_box.tools.update_templates()
    #     tool_box.main_calculation(set_name=set_name)
    #     tool_box.save_level1(set_name=set_name, score=100)
    #     set_fk = tool_box.kpi_static_data[tool_box.kpi_static_data['kpi_set_name'] == set_name]['kpi_set_fk'].values[0]
    #     tool_box.commit_results_data(kpi_set_fk=set_fk)
    #     Log.info('Dunkin Donuts: Downloading templates took {}'.format(tool_box.download_time))

    @log_runtime('Programs Calculations')
    def calculate_programs(self):
        tool_box = PROGRAMSToolBox(self.data_provider, self.output)
        tool_box.main_calculation()
        tool_box.commit_results_data(kpi_set_fk=28)
        del tool_box

    @log_runtime('Pillars Programs Calculations')
    def calculate_pillars_programs(self):
        tool_box = PillarsPROGRAMSToolBox(self.data_provider, self.output, self.common)
        tool_box.main_calculation()
        del tool_box

    # @log_runtime('MSC New Calculations')
    # def calculate_msc_new(self):
    #     tool_box = MSC_NEWToolBox(self.data_provider, self.output)
    #     tool_box.main_calculation()
    #     tool_box.commit_results_data(kpi_set_fk=29)

    @log_runtime('Holiday Programs Calculations')
    def calculate_holiday_programs(self):
        tool_box = HOLIDAYToolBox(self.data_provider, self.output)
        tool_box.main_calculation()
        tool_box.commit_results_data(kpi_set_fk=31)
        del tool_box

    # @log_runtime('Programs Calculations')
    # def calculate_gold_peak_block(self):
    #     tool_box = GOLD_PEAK_BLOCKToolBox(self.data_provider, self.output)
    #     tool_box.main_calculation()
    #     tool_box.commit_results_data(kpi_set_fk = 30)

    @log_runtime('Special Programs Calculations')
    def calculate_special_programs(self):
        tool_box = SpecialProgramsToolBox(self.data_provider, self.output)
        tool_box.main_calculation()
        tool_box.commit_results_data(kpi_set_fk=32)
        del tool_box

    @log_runtime('Special Programs Calculations')
    def calculate_validation(self):
        tool_box = VALIDATIONToolBox(self.data_provider, self.output, kpi_set_fk=34)
        tool_box.main_calculation()
        tool_box.commit_results_data()
        del tool_box

    @log_runtime('Military Calculations')
    def calculate_military(self):
        tool_box = MilitaryToolBox(self.data_provider, self.output, self.common)
        tool_box.main_calculation()
        del tool_box
