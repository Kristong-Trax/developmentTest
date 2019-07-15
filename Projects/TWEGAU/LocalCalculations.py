#
# from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
# from Trax.Utils.Conf.Configuration import Config
# from Projects.TWEGAU.Calculations import Calculations
#
# if __name__ == '__main__':
#     Config.init()
#     project_name = 'twegau'
#     data_provider = KEngineDataProvider(project_name)
#     sessions = [
#         'F8AF5B91-7CA1-4134-AA0D-DEE08547933C',
#         '4138394A-F64F-421D-AC10-0876F5EEE4FE',
#         '184741F1-93ED-4A37-8217-77338210F009'
#     ]
#
#     for session in sessions:
#         print "Running session >>", session
#         data_provider.load_session_data(session)
#         output = Output()
#         Calculations(data_provider, output).run_project_calculations()
#         print "*******************************"
