from Trax.Utils.Logging.Logger import Log

# from KPIUtils.GlobalProjects.HEINZ.Utils.KPIToolBox import GOOGLEToolBox, log_runtime
from Projects.GOOGLEKR_SAND.Utils.KPIToolBox import GOOGLEToolBox, log_runtime

__author__ = 'Eli'


class GOOGLEGenerator:

    def __init__(self, data_provider, output, common):
        self.data_provider = data_provider
        self.output = output
        self.common = common
        self.project_name = data_provider.project_name
        self.session_uid = self.data_provider.session_uid
        self.tool_box = GOOGLEToolBox(self.data_provider, self.output, self.common)

    @log_runtime('Total Calculations', log_start=True)
    def main_function(self):
        """
        This is the main KPI calculation function.
        It calculates the score for every KPI set and saves it to the DB.
        """
        if self.tool_box.scif.empty:
            Log.warning('Scene item facts is empty for this session')
        for kpi_set_fk in self.tool_box.kpi_static_data['kpi_set_fk'].unique().tolist():
            score = self.tool_box.main_calculation(kpi_set_fk=kpi_set_fk)
            self.tool_box.write_to_db_result(kpi_set_fk, score, self.tool_box.LEVEL1)
        self.tool_box.commit_results_data()

    @log_runtime('Total Calculations', log_start=True)
    def google_global_SOS(self):
        try:
            # Log.info('In KPI generator GOOGLE-SAND')
            if self.tool_box.scif.empty:
                Log.warning('Distribution is empty for this session')
            self.tool_box.google_global_SOS()
        except Exception as e:
            Log.error('{}'.format(e))

    @log_runtime('Total Calculations', log_start=True)
    def heinz_global_distribution_per_category(self):
        try:
            # Log.info('In KPI generator DIAGEOTW-SAND')
            if self.tool_box.scif.empty:
                Log.warning('Distribution is empty for this session')
            self.tool_box.heinz_global_distribution_per_category()
        except Exception as e:
            Log.error('{}'.format(e))

    @log_runtime('Total Calculations', log_start=True)
    def heinz_global_share_of_shelf_function(self):
        """
        This is the main KPI calculation function.
        It calculates the score for every KPI set and saves it to the DB.
        """
        try:
            # Log.info('In KPI generator DIAGEOTW-SAND')
            if self.tool_box.scif.empty:
                Log.warning('Scene item facts is empty for this session')
            self.tool_box.main_sos_calculation()
        except Exception as e:
            Log.error('{}'.format(e))

    @log_runtime('Total Calculations', log_start=True)
    def heinz_global_price_adherence(self, config_df):
        """

        :return:
        """
        try:
            # Log.info('In KPI generator DIAGEOTW-SAND')
            if self.tool_box.scif.empty:
                Log.warning('Scene item facts is empty for this session')
            self.tool_box.heinz_global_price_adherence(config_df)
        except Exception as e:
            Log.error('{}'.format(e))


    @log_runtime('Total Calculations', log_start=True)
    def heinz_global_extra_spaces(self):
        """

        :return:
        """
        try:
            # Log.info('In KPI generator DIAGEOTW-SAND')
            if self.tool_box.scif.empty:
                Log.warning('Scene item facts is empty for this session')
            self.tool_box.heinz_global_extra_spaces()
        except Exception as e:
            Log.error('{}'.format(e))
