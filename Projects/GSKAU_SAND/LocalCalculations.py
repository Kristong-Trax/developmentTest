
from Trax.Utils.Logging.Logger import Log
from Trax.Utils.Conf.Configuration import Config
from Projects.GSKAU_SAND.Calculations import Calculations
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Projects.GSKAU_SAND.SceneKpis.SceneCalculations import SceneCalculations


if __name__ == '__main__':
    LoggerInitializer.init('gskau-sand calculations')
    Config.init()
    project_name = 'gskau-sand'
    data_provider = KEngineDataProvider(project_name)
    # RUN for scene level KPIs
    session_scene_map = {
        # '6DF2E0C8-8AE0-432A-AD3B-C2BE8F086E3B': ['12697', '12753', '12742'],
        '6520B138-780D-4AD1-95CF-8DA1727C4580': ['14779', '14810', '14818', '14824', '14833', '14837', '14849',],
        # '01006BB4-3E04-4C34-9C2E-FE24B557C74D': ['15173', '15186', '15188', '15215', '15234'],
        # '02E8F2A3-391E-4BC5-8EFA-09E59570D9DF': ['12523', '12546', '12582', '12593'],
        # '7DDEA72B-9DCC-48DA-AC68-3D3E61612875': ['15157', '15162', '15166'],
        # '1CAEDC02-EFD4-4F83-81B0-95764B2A5A9A': ['16219', '16234'],
        # '1F137EBF-9724-4890-A69A-15F72EDE38E7': ['13492', '13512', '13515', '13518'],
        '0BDDC8E9-2F39-4572-99BA-63D505A97968': ['12894', '12899', '12919'],
        # '0A5A3F52-06F2-4B7F-905D-5E8938AC4404': ['12907', '12928', '12932', '12960'],
        '0618D9AB-BD10-4917-8A86-35EFE993E7CA': ['12941', '12966', '12974', '13010', '13159', '13162'],
        # '10004FDB-964D-424D-A4F8-086F1F0F020E': ['15854', '15856', '15868', '14177', '14183', '14187', '14191', '14199',
        #                                          '14206']

    }
    for session, scenes in session_scene_map.iteritems():
        for e_scene in scenes:
            data_provider.load_scene_data(session, e_scene)
            SceneCalculations(data_provider).calculate_kpis()
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
