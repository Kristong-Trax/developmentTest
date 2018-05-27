import pandas as pd
# from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
# from Trax.Utils.Conf.Configuration import Config
# from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
# from Trax.Algo.Calculations.Core.DataProvider import ACEDataProvider, Output
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
from Projects.MARSRU2_SAND.Utils.MARSRUToolBox import MARSRU2_SANDMARSRUKPIToolBox
from Projects.MARSRU2_SAND.Utils.MARSRUJSON import MARSRU2_SANDMARSRUJsonGenerator

__author__ = 'urid'


class MARSRU2_SANDMARSRUCalculations(BaseCalculationsScript):
    def run_project_calculations(self):
        return
        self.timer.start()  # use log.time_message
        tool_box = MARSRU2_SANDMARSRUKPIToolBox(self.data_provider, self.output, 'MARS KPIs 2017')
        jg = MARSRU2_SANDMARSRUJsonGenerator('marsru-sand')
        jg.create_json('KPI MARS 06.03.17.xlsx')
        # tool_box.check_availability(jg.project_kpi_dict.get('kpi_data')[0])
        # tool_box.check_survey_answer(jg.project_kpi_dict.get('kpi_data')[0])
        tool_box.custom_linear_sos(jg.project_kpi_dict.get('kpi_data')[0])

        attributes_for_table1 = pd.DataFrame([(tool_box.set_name, tool_box.session_uid,
                                               tool_box.store_id, tool_box.visit_date.isoformat()
                                               , 100, 3)], columns=['kps_name', 'session_uid', 'store_fk',
                                                                       'visit_date', 'score_1',
                                                                       'kpi_set_fk'])

        tool_box.write_to_db_result(attributes_for_table1, 'level1', tool_box.set_name)
        self.timer.stop('MARSRU2_SANDMARSRUCalculations.run_project_calculations')


# if __name__ == '__main__':
#     LoggerInitializer.init('MARSRU_PROD calculations')
#     Config.init()
#     project_name = 'marsru-sand'
#     # session_uids = ['e85aad1b-ec24-4c2e-9eae-5cac69445f98']
#     session_uids = ['39150053-177f-44f8-ae66-bcbc5d4b4455']
#     data_provider = ACEDataProvider(project_name)
#     output = Output()
#     for session in session_uids:
#         data_provider.load_session_data(session)
#         MARSRU2_SANDMARSRUCalculations(data_provider, output).run_project_calculations()

    # jg = MARSRU2_SANDMARSRUJsonGenerator('marsru-prod')
    # jg.create_json('KPI MARS 21.02.17.xlsx')
    # # tb = MARSRU2_SANDMARSRUKPIToolBox(data_provider, self.output, 'Hypermarket')
    # tb = MARSRU2_SANDMARSRUKPIToolBox(data_provider, output, 'MARS KPIs 2017')
    # tb.insert_new_kpis_old(project_name, jg.project_kpi_dict.get('kpi_data')[0])
    # tb.insert_new_kpis_old(project_name)

