
from Trax.Utils.Logging.Logger import Log

from Projects.CCZA_SAND.Utils.KPIToolBox import CCZAToolBox
from Projects.CCZA_SAND.Utils.Const import Const
from KPIUtils.Utils.Helpers.LogHandler import log_handler


__author__ = 'Elyashiv'


class CCZAGenerator:

    def __init__(self, data_provider, output):
        self.data_provider = data_provider
        self.output = output
        self.project_name = data_provider.project_name
        self.session_uid = self.data_provider.session_uid
        self.tool_box = CCZAToolBox(self.data_provider, self.output)

    @log_handler.log_runtime('Total Calculations', log_start=True)
    def main_function(self):
        """
        This is the main KPI calculation function.
        It calculates the score for every KPI set and saves it to the DB.
        """
        if self.tool_box.scif.empty:
            Log.error('Scene item facts is empty for this session')
            return
        self.tool_box.main_calculation_red_score()
        # set_score = 0
        # try:
        #     set_name = self.tool_box.kpi_sheets[Const.KPIS].iloc[len(self.tool_box.kpi_sheets[Const.KPIS]) - 1][
        #         Const.KPI_NAME]
        #     kpi_fk = self.tool_box.common_v2.get_kpi_fk_by_kpi_type(set_name)
        #     set_identifier_res = self.tool_box.common_v2.get_dictionary(kpi_fk=kpi_fk)
        #     if self.tool_box.store_type in self.tool_box.kpi_sheets[Const.KPIS].keys().tolist():
        #         for i in xrange(len(self.tool_box.kpi_sheets[Const.KPIS]) - 1):
        #             params = self.tool_box.kpi_sheets[Const.KPIS].iloc[i]
        #             percent = self.tool_box.get_percent(params[self.tool_box.store_type])
        #             if percent == 0:
        #                 continue
        #             kpi_score = self.tool_box.main_calculation(identifier_parent=set_identifier_res, params=params)
        #             set_score += kpi_score * percent
        #     else:
        #         Log.warning('The store-type "{}" is not recognized in the template'.format(self.tool_box.store_type))
        #         return
        #     # set_name = self.tool_box.kpi_sheets[Const.KPIS].iloc[len(self.tool_box.kpi_sheets[Const.KPIS]) - 1][
        #     #     Const.KPI_NAME]
        #     kpi_names = {Const.column_name1: set_name}
        #     set_fk = self.tool_box.get_kpi_fk_by_kpi_path(self.tool_box.common.LEVEL1, kpi_names)
        #     if set_fk:
        #         try:
        #             self.tool_box.common.write_to_db_result(score=set_score,
        #                                                     level=self.tool_box.common.LEVEL1, fk=set_fk)
        #         except Exception as exception:
        #             Log.error('Exception in the set {} writing to DB: {}'.format(set_name, exception.message))
        #     self.tool_box.common_v2.write_to_db_result(fk=kpi_fk, numerator_id=self.tool_box.own_manuf_fk,
        #                                                denominator_id=self.tool_box.store_id, score=set_score,
        #                                                result=set_score, identifier_result= set_identifier_res,
        #                                                should_enter=True)
        # except Exception as exception:
        #     Log.error('Exception in the kpi-set calculating: {}'.format(exception.message))
        #     pass
        self.tool_box.sos_main_calculation()
        self.tool_box.common.commit_results_data()
        self.tool_box.common_v2.commit_results_data()
