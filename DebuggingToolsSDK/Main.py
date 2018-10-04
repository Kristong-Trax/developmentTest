from DebuggingToolsSDK.DataHandling import DataHandler
from DebuggingToolsSDK.PersistentLocals import log_locals
from DebuggingToolsSDK.Singleton import OnlyOne
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Trax.Utils.Conf.Configuration import Config
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Projects.HEINZCR_SAND.Calculations import HEINZCRCalculations


class Debugger:

    def __init__(self, project_name, sessions, checker_dict):
        self.project = project_name
        self.data_provider = KEngineDataProvider(project_name)
        self.sessions = sessions
        self.singleton = OnlyOne()
        self.singleton.simplify_filter_dict(checker_dict)
        self.singleton.is_active = True

    @log_locals()
    def execute(self):
        for session in self.sessions:
            self.data_provider.load_session_data(session)
            output = Output()
            # TODO: Use reflection to get the Calculations Class
            HEINZCRCalculations(self.data_provider, output).run_project_calculations()



if __name__ == '__main__':
    LoggerInitializer.init('Self Checker')
    Config.init()
    sessions = ['7D2A86BC-CC37-42EB-B79D-06A48FD7FEF7']
    project_name = 'heinzcr-sand'
    checker_dict = dict(
        {
            "Files": [
                {
                    "name": "KPIToolBox",
                    "Methods": [
                        {
                            "name": "heinz_global_distribution_per_category",
                            "groups": [
                                {
                                    "name": "Denominator",
                                    "variables": [
                                        "denominator_val",
                                        "denominator_key",
                                        "numerator_key"
                                    ]
                                },
                                {
                                    "name": "Numerator",
                                    "variables": [
                                        "numerator_id",
                                        "numerator_val",
                                        "numerator_key"
                                    ]
                                },
                                {
                                    "name": "KPI Level 2",
                                    "variables": [
                                        "kpi_fk",
                                        "target",
                                    ]
                                }
                            ]
                        }
                        # {
                        #     "name": "main_sos_calculation",
                        #     "groups": [
                        #         {
                        #             "name": "SOS_Numerators",
                        #             "variables": [
                        #                 "aa",
                        #                 "bb"
                        #             ]
                        #         },
                        #         {
                        #             "name": "SOS_Denominators",
                        #             "variables": [
                        #                 "lala",
                        #                 "kakaroto"
                        #             ]
                        #         }
                        #     ]
                        # }
                    ]
                }
                # {
                #     "name": "Calculations",
                #     "Methods": [
                #         {
                #             "name": "heinz_global_distribution_per_category",
                #             "groups": [
                #                 {
                #                     "name": "Numerators",
                #                     "variables": [
                #                         "aa",
                #                         "bb"
                #                     ]
                #                 },
                #                 {
                #                     "name": "Denominators",
                #                     "variables": [
                #                         "lala",
                #                         "kakaroto"
                #                     ]
                #                 }
                #             ]
                #         },
                #         {
                #             "name": "main_sos_calculation",
                #             "groups": [
                #                 {
                #                     "name": "SOS_Numerators",
                #                     "variables": [
                #                         "aa",
                #                         "bb"
                #                     ]
                #                 },
                #                 {
                #                     "name": "SOS_Denominators",
                #                     "variables": [
                #                         "lala",
                #                         "kakaroto"
                #                     ]
                #                 }
                #             ]
                #         }
                #     ]
                # }
            ]
        }
    )

    db = Debugger(project_name, sessions, checker_dict)
    db.execute()
    dh = DataHandler(project_name, sessions, db.singleton.data).execute()

    print "a"
