
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Projects.LIONJP_SAND.Calculations import Calculations


if __name__ == '__main__':
    LoggerInitializer.init('lionjp-sand2 calculations')
    Config.init()
    project_name = 'psapac-sand2'
    data_provider = KEngineDataProvider(project_name)

    sessions = [
                "2A940FF8-08C9-4C06-B7AC-40420DCA7305",
                "5F484D8D-801F-411D-A9B9-5996F0C3A517",
                "FF16C669-4FEF-4192-8F3E-33447C2EA78D",
                "FFF533BD-83E3-4350-B3D4-2EA253C207F3",
                "FF422538-0462-455A-82FF-6F33FCAE60AD",
                "FFE0BB8C-E780-4772-9048-FA8BE67E58FF",
                "AD7C1744-52F5-416B-A57C-1FE7A90EE981",
                "D90183A6-8A99-4D70-BA4F-8537CAD7845C",
                "9ED2A057-FF56-4272-AABD-D2FFC01AD0CE",
                "B382594A-C6AA-4B28-B478-65256CC3E5DF"
               ]

    for session in sessions:
        print "Running for {}".format(session)
        data_provider.load_session_data(session)
        output = Output()
        Calculations(data_provider, output).run_project_calculations()
