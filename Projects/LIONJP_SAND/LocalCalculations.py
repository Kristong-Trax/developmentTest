
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Projects.LIONJP_SAND.Calculations import Calculations


if __name__ == '__main__':
    import pickle
    LoggerInitializer.init('lionjp-sand2 calculations')
    Config.init()
    project_name = 'psapac-sand2'
    data_provider = KEngineDataProvider(project_name)

    sessions = ["2A940FF8-08C9-4C06-B7AC-40420DCA7305",
                "5F484D8D-801F-411D-A9B9-5996F0C3A517",
                "FF16C669-4FEF-4192-8F3E-33447C2EA78D",
                "FFF533BD-83E3-4350-B3D4-2EA253C207F3",
                "FF422538-0462-455A-82FF-6F33FCAE60AD"]

    for session in sessions:
        # f = open(session, "wb")
        print "Running for {}".format(session)
        data_provider.load_session_data(session)
        # pickle.dump(data_provider, f)
        # f.close()
        output = Output()
        Calculations(data_provider, output).run_project_calculations()
