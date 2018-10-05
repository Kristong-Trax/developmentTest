from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
# from Trax.Apps.Services.Simon.Jobs.Reports.TESCO_TPN_Report.TPN import TPNReportMobile
# from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
# from Trax.Utils.Conf.Configuration import Config
# from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
# from Trax.Utils.Logging.Logger import Log

__author__ = 'yacovm'

PROJECT = 'tescouk'


class TESCOUKCalculations(BaseCalculationsScript):
    def run_project_calculations(self):
        pass
#         self.timer.start()
#         Log.info('Starting project calculations for tescouk')
#         tpn_report = TPNReportMobile(self.data_provider.project_name)
#         tpn_report.connect()
#         tpn_report.generate_report(session_uid=self.data_provider.session_uid)
#         self.timer.stop('KPIGenerator.run_project_calculations')


# if __name__ == '__main__':
#     LoggerInitializer.init('{} calculations'.format(PROJECT))
#     Config.init()
#     project_name = PROJECT
#     data_provider = KEngineDataProvider(project_name)
#     session = '962B1999-0100-40E1-A90C-E6DED7CAA03D'
#     data_provider.load_session_data(session)
#     output = Output()
#     TESCOUKCalculations(data_provider, output).run_project_calculations()
