from Trax.Utils.Conf.Configuration import Config
from Projects.TWEGAU.Calculations import Calculations
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output

if __name__ == '__main__':
    LoggerInitializer.init('twegau calculations')
    project_name = 'twegau'
    data_provider = KEngineDataProvider(project_name)
    sessions = [
        "000E4051-2111-4220-9991-3D3FA1102540",  # store number 1 => '556032'
        "00B36414-40EE-4F43-A277-D17969F88567",  # store number 1 => '604644'
        "03CA5819-063C-452D-9F14-99370BCE9279",  # store number 1 => '602949'
        "04899E1E-8714-433D-A9AA-8B665412E75B",  # store number 1 => '523570'
        "0510B1BD-409A-4ACD-A406-7BFD9C4F7897",  # store number 1 => '519567'
        "094AE49F-81AD-4E37-A247-F9F0641BFBDF",  # store number 1 => '516121'
        "0A14CB41-C8DA-458F-A6D9-630FEE94EE84",  # store number 1 => '603147'
        "0CE8A970-A014-457C-AF5D-81387D0688F7",  # store number 1 => '556032'
        "0D09EC05-AC0F-4538-A293-10F5A501B9AB",  # store number 1 => '522119'
        "109E18B6-BDE5-48D9-892B-B6D5C6085F8B",  # store number 1 => '505620'

    ]

    for session in sessions:
        print "Running session >>", session
        data_provider.load_session_data(session)
        output = Output()
        Calculations(data_provider, output).run_project_calculations()
        print "*******************************"
