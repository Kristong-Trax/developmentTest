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
        self.singleton.is_active = True
        self.singleton.checker = checker_dict

    @log_locals()
    def execute(self):
        for session in self.sessions:
            self.data_provider.load_session_data(session)
            output = Output()
            # TODO: Use reflection to get the Calculations Class
            HEINZCRCalculations(self.data_provider, output).run_project_calculations()

    # def logger(self):


if __name__ == '__main__':
    LoggerInitializer.init('Self Checker')
    Config.init()
    sessions = ['7D2A86BC-CC37-42EB-B79D-06A48FD7FEF7']
    project_name = 'heinzcr-sand'
    checker_dict = dict()
    checker_dict['KPIToolBox'] = ['heinz_global_distribution_per_category']
    # checker_dict['KPIToolBox1'] = ['heinz_global_distribution_per_category']
    db = Debugger(project_name, sessions, checker_dict)
    db.execute()

    print "a"
