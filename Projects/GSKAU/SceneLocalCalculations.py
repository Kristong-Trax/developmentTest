# import pandas as pd
# from collections import OrderedDict
#
# from Trax.Utils.Logging.Logger import Log
# from Trax.Utils.Conf.Configuration import Config
# from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
# from Trax.Algo.Calculations.Core.Vanilla.Output import VanillaOutput
# from Projects.GSKAU.SceneKpis.SceneCalculations import SceneCalculations
# from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
# from Trax.Algo.Calculations.Core.Vanilla.Calculations import SceneVanillaCalculations
# from Trax.Algo.Calculations.Core.Constants import Keys, Fields, SCENE_ITEM_FACTS_COLUMNS
#
#
# def save_scene_item_facts_to_data_provider(data_provider, output):
#     scene_item_facts_obj = output.get_facts()
#     if scene_item_facts_obj:
#         scene_item_facts = scene_item_facts_obj[Keys.SCENE_ITEM_FACTS][Keys.SCENE_ITEM_FACTS].fact_df
#     else:
#         scene_item_facts = pd.DataFrame(columns=SCENE_ITEM_FACTS_COLUMNS)
#     scene_item_facts.rename(columns={Fields.PRODUCT_FK: 'item_id',
#                                      Fields.SCENE_FK: 'scene_id'}, inplace=True)
#     data_provider.set_scene_item_facts(scene_item_facts)
#
#
# if __name__ == '__main__':
#     LoggerInitializer.init('gskau Scene Calculations')
#     Config.init()
#     project_name = 'gskau'
#     # RUN for scene level KPIs
#     session_scene_map = OrderedDict([
#         # ('0BD1A441-BEFD-4688-9350-64D6412B2ABB', ['5C3E76DA-0896-4098-8382-558F182F56B1']),
#         ('BE989AAD-40D1-44AC-8B7B-B94D95150C07', ['D32EB976-2309-46B0-B8B9-92C0CC4AE949'])
#     ])
#
#     for session, scenes in session_scene_map.iteritems():
#         for e_scene in scenes:
#             print "\n"
#             data_provider = KEngineDataProvider(project_name)
#             data_provider.load_scene_data(session, scene_uid=e_scene)
#             Log.info("**********************************")
#             Log.info('*** Starting session: {sess}: scene: {scene}. ***'.format(sess=session, scene=e_scene))
#             Log.info("**********************************")
#             output = VanillaOutput()
#             SceneVanillaCalculations(data_provider, output).run_project_calculations()
#             save_scene_item_facts_to_data_provider(data_provider, output)
#             SceneCalculations(data_provider).calculate_kpis()
