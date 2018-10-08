from DebuggingToolsSDK.DataHandling import DataHandler
from DebuggingToolsSDK.PersistentLocals import log_locals
from DebuggingToolsSDK.Singleton import OnlyOne
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Trax.Utils.Conf.Configuration import Config
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
import importlib


class Debugger:

    def __init__(self, project_name, sessions, checker_dict):
        self.project = project_name
        self.data_provider = KEngineDataProvider(project_name)
        self.sessions = sessions
        self.singleton = OnlyOne()
        self.singleton.simplify_filter_dict(checker_dict)
        self.singleton.is_active = True

    def execute(self):
        self.log_variables()
        dh = DataHandler(project_name, sessions, db.singleton.data).execute()

    @log_locals()
    def log_variables(self):
        upper_project_name = self.project.replace("-", "_").upper()
        words = self.project.upper().split('-')
        class_path = "Projects.{}.Calculations".format(upper_project_name)
        class_name = "{}Calculations".format(words[0])
        for session in self.sessions:
            self.data_provider.load_session_data(session)
            output = Output()
            # TODO: Use reflection to get the Calculations Class
            module = importlib.import_module(class_path)
            report_class = getattr(module, class_name)
            api_object = report_class(self.data_provider, output).run_project_calculations()


# if __name__ == '__main__':
#     LoggerInitializer.init('Self Checker')
#     Config.init()
#     sessions = ['7D2A86BC-CC37-42EB-B79D-06A48FD7FEF7']
#     project_name = 'heinzcr-sand'
#     checker_dict = dict(
#         {
#             "Files": [
#                 {
#                     "name": "KPIToolBox",
#                     "Methods": [
#                         {
#                             "name": "heinz_global_distribution_per_category",
#                             "groups": [
#                                 {
#                                     "name": "Denominator",
#                                     "variables": [
#                                         "denominator_val",
#                                         "denominator_key",
#                                         "numerator_key",
#                                         "df"
#                                     ]
#                                 },
#                                 {
#                                     "name": "Numerator",
#                                     "variables": [
#                                         "numerator_id",
#                                         "numerator_val",
#                                         "numerator_key",
#                                         "df_1"
#                                     ]
#                                 },
#                                 {
#                                     "name": "KPI Level 2",
#                                     "variables": [
#                                         "kpi_fk",
#                                         "target",
#                                         "stores"
#                                     ]
#                                 }
#                             ]
#                         },
#                         {
#                             "name": "main_sos_calculation",
#                             "groups": [
#                                 {
#                                     "name": "SOS_Denominator",
#                                     "variables": [
#                                         "denominator",
#                                         "denominator_id",
#                                         "denominator_key",
#                                         "denominator_val"
#                                     ]
#                                 },
#                                 {
#                                     "name": "SOS_Numerator",
#                                     "variables": [
#                                         "numerator",
#                                         "numerator_id",
#                                         "numerator_key",
#                                         "numerator_val"
#                                     ]
#                                 },
#                                 {
#                                     "name": "Others",
#                                     "variables": [
#                                         "json_policy",
#                                         "key",
#                                         "kpi_fk",
#                                         "manufacturer",
#                                         "score",
#                                         "sos_policy",
#                                         "stores",
#                                         "value"
#                                     ]
#                                 }
#                             ]
#                         }
#                     ]
#                 }
#                 # {
#                 #     "name": "Calculations",
#                 #     "Methods": [
#                 #         {
#                 #             "name": "heinz_global_distribution_per_category",
#                 #             "groups": [
#                 #                 {
#                 #                     "name": "Numerators",
#                 #                     "variables": [
#                 #                         "aa",
#                 #                         "bb"
#                 #                     ]
#                 #                 },
#                 #                 {
#                 #                     "name": "Denominators",
#                 #                     "variables": [
#                 #                         "lala",
#                 #                         "kakaroto"
#                 #                     ]
#                 #                 }
#                 #             ]
#                 #         },
#                 #         {
#                 #             "name": "main_sos_calculation",
#                 #             "groups": [
#                 #                 {
#                 #                     "name": "SOS_Numerators",
#                 #                     "variables": [
#                 #                         "aa",
#                 #                         "bb"
#                 #                     ]
#                 #                 },
#                 #                 {
#                 #                     "name": "SOS_Denominators",
#                 #                     "variables": [
#                 #                         "lala",
#                 #                         "kakaroto"
#                 #                     ]
#                 #                 }
#                 #             ]
#                 #         }
#                 #     ]
#                 # }
#             ]
#         }
#     )
#
#     db = Debugger(project_name, sessions, checker_dict)
#     db.execute()


