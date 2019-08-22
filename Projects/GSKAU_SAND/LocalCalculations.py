#
# from Trax.Utils.Logging.Logger import Log
# from Trax.Utils.Conf.Configuration import Config
# from Projects.GSKAU_SAND.Calculations import Calculations
# from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
# from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
# from Projects.GSKAU_SAND.SceneKpis.SceneCalculations import SceneCalculations
#
#
# if __name__ == '__main__':
#     LoggerInitializer.init('gskau-sand calculations')
#     Config.init()
#     project_name = 'gskau-sand'
#     data_provider = KEngineDataProvider(project_name)
#     # RUN for scene level KPIs
#     session_scene_map = {
#         '6DF2E0C8-8AE0-432A-AD3B-C2BE8F086E3B': [
#             '12697', '12753', '12742'
#         ],
#         '6520B138-780D-4AD1-95CF-8DA1727C4580': [
#             '14779', '14810', '14818', '14824', '14833', '14837', '14849',
#         ],
#         '6944F6F5-79D7-43FB-BE32-E3FAE237FA63': [
#             '10561', '10564', '10566', '10576', '10651', '10654', '10658', '10663', '10695', '10723', '10727', '10732',
#             '10756', '10819', '10846', '10854',
#         ],
#         '6FDB6757-E3DA-4297-941A-8C1A40DD2E90': [
#             '13881', '13885', '13888', '13890', '13903', '22526', '22537', '22543',
#         ],
#         '9DC66118-F981-4D01-A6DD-1E181FA05507': [
#             '2762', '2770', '2774', '2780', '2782', '2806', '2807', '2819', '2821', '2823', '2825'
#         ],
#         'E0BE9853-B6C8-4B36-919C-5293BF52EF5B': [
#             '23519', '23521', '23523', '11997', '12018', '12023', '12031', '12037', '12039', '12115', '12118', '12120',
#             '12138', '12142', '12144', '12146', '12148', '12150', '12152'
#         ]
#     }
#     for session, scenes in session_scene_map.iteritems():
#         for e_scene in scenes:
#             data_provider.load_scene_data(session, e_scene)
#             SceneCalculations(data_provider).calculate_kpis()
#     sessions = [
#         '6DF2E0C8-8AE0-432A-AD3B-C2BE8F086E3B',
#         '6520B138-780D-4AD1-95CF-8DA1727C4580',
#         '6944F6F5-79D7-43FB-BE32-E3FAE237FA63',
#         '6FDB6757-E3DA-4297-941A-8C1A40DD2E90',
#         '9DC66118-F981-4D01-A6DD-1E181FA05507',
#         'E0BE9853-B6C8-4B36-919C-5293BF52EF5B'
#     ]
#     # RUN FOR Session level KPIs
#     for sess in sessions:
#         Log.info("[Session level] Running for session: {}".format(sess))
#         data_provider.load_session_data(sess)
#         output = Output()
#         Calculations(data_provider, output).run_project_calculations()
