import os

from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
# from Trax.Algo.Calculations.Core.DataProvider import ACEDataProvider, Output, KEngineDataProvider
#
# from Trax.Utils.Conf.Configuration import Config
# from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
# from mock import MagicMock

from Projects.CARREFOURAR.Utils.KPIToolBox import CarrefourArKpiToolBox
from Projects.CARREFOURAR.Utils.CARREFOUR_ARJSON import CARREFOUR_ARJsonGenerator

__author__ = 'ortal'


class CARREFOUR_ARalculations(BaseCalculationsScript):
    def run_project_calculations(self):
        pass
        # self.timer.start()  # use log.time_message
        # tool_box = CarrefourArKpiToolBox(self.data_provider, self.output)
        # if tool_box.scif.empty:
        #     self.timer.stop('carrefouraralculations.run_project_calculations')
        # else:
        #     self.base_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'Data/')
        #     if str(tool_box.visit_date) >= '17-07-07':
        #         jg = CARREFOUR_ARJsonGenerator('carrefour_ar', tool_box.visit_date, tool_box.store_number, self.base_path)
        #         file = jg.create_json()
        #         if not file.empty:
        #             tool_box.calculate_kpi(file)
        #         self.timer.stop('carrefouraralculations.run_project_calculations')
        #     else:
        #         self.timer.stop('carrefouraralculations.run_project_calculations')

# if __name__ == '__main__':
#     LoggerInitializer.init('carrefourar calculations')
#     Config.init()
#     project_name = 'carrefourar'
#     session_uids = ['e24fc830-f8f2-4a19-b25f-ef06e785fdf0']
#     data_provider = KEngineDataProvider(project_name, monitor=MagicMock())
#     output = Output()
#     for session in session_uids:
#         data_provider.load_session_data(session)
#         CARREFOUR_ARalculations(data_provider, output).run_project_calculations()
        #     #
        # jg = JsonGenerator('ccru')
        # jg.create_json('Hypermarket 220117.xlsx', 'Hypermarket')
        # tb = KPIToolBox(data_provider, self.output, 'Hypermarket')
        # tb.insert_new_kpis_old(project_name, jg.project_kpi_dict.get('kpi_data')[0])
        #
