from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
# from Trax.Algo.Calculations.Core.DataProvider import ACEDataProvider, Output
# from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
# from Trax.Utils.Conf.Configuration import Config
from Trax.Utils.Logging.Logger import Log

from Projects.INTEG4.Sets.Canteen import INTEG4CanteenCalculations
from Projects.INTEG4.Sets.FT import INTEG4FTCalculations
from Projects.INTEG4.Sets.FastFood import INTEG4FastFoodCalculations
from Projects.INTEG4.Sets.HoReCa import INTEG4HoReCaCalculations
from Projects.INTEG4.Sets.Hypermarket import INTEG4HypermarketCalculations
from Projects.INTEG4.Sets.Petrol import INTEG4PetrolCalculations
from Projects.INTEG4.Sets.Superette import INTEG4SuperetteCalculations
from Projects.INTEG4.Sets.Supermarket import INTEG4SupermarketCalculations
from Projects.INTEG4.Utils.ToolBox import INTEG4KPIToolBox

__author__ = 'urid'

HYPERMARKET_MARKETS = ['Hypermarket', 'Cash & Carry']
SUPERMARKET_MARKETS = ['Supermarket']
SUPERETTE_EXCLUSIONS = ['Hypermarket', 'Cash & Carry', 'Supermarket']

CANTEEN = 'PoS 2017 - Canteen'
PETROL = 'PoS 2017 - Petroleum'
HORECA = 'PoS 2017 - HoReCa'
FT = 'PoS 2017 - FT'
HYPERMARKET = 'PoS 2017 - MT - Hypermarket'
SUPERMARKET = 'PoS 2017 - MT - Supermarket'
SUPERETTE = 'PoS 2017 - MT - Superette'
FAST_FOOD = 'PoS 2017 - QSR'


class INTEG4Calculations(BaseCalculationsScript):
    def run_project_calculations(self):
        self.timer.start()  # use log.time_message
        tool_box = INTEG4KPIToolBox(self.data_provider, self.output)
        kpi_set_name = tool_box.set_name
        if kpi_set_name == CANTEEN:
            INTEG4CanteenCalculations(self.data_provider, self.output).main_function()
        elif kpi_set_name == PETROL:
            INTEG4PetrolCalculations(self.data_provider, self.output).main_function()
        elif kpi_set_name == HORECA:
            INTEG4HoReCaCalculations(self.data_provider, self.output).main_function()
        elif kpi_set_name == FT:
            INTEG4FTCalculations(self.data_provider, self.output).main_function()
        elif kpi_set_name == HYPERMARKET:
            INTEG4HypermarketCalculations(self.data_provider, self.output).main_function()
        elif kpi_set_name == SUPERMARKET:
            INTEG4SupermarketCalculations(self.data_provider, self.output).main_function()
        elif kpi_set_name == SUPERETTE:
            INTEG4SuperetteCalculations(self.data_provider, self.output).main_function()
        elif kpi_set_name == FAST_FOOD:
            INTEG4FastFoodCalculations(self.data_provider, self.output).main_function()
        else:
            Log.error('Session store "{}" is not set to calculation'.format(
                tool_box.session_info.store_type))  # todo add all supported store types

        self.timer.stop('INTEG4Calculations.run_project_calculations')


# if __name__ == '__main__':
#     LoggerInitializer.init('CCRU calculations')
#     Config.init()
#     project_name = 'integ4'
#     # session_uids = [
#     # # session_uids = [
#     #     # 'e6de968a-89f9-4891-9094-78446d5b7596', #FT
#     #     # 'dec164ea-4569-4cc0-a4c2-ed0a1be0365c', #FT
#     #
#     #     # '22be4539-e892-4ec2-95cf-91ac66ecdcc5',  #FT
#     #     # 'e360d946-ddc6-44c6-83bc-55f5e47ac310',  #FT
#     #
#     #     # 'be9ece03-65c2-4ee0-9c6f-192df6a836a1',  #FT   send Y
#     #     # 'c9ec8a28-881c-4528-a276-f5cbb0f95950',  #Hypermarket     send Y
#     #
#     #     # '21bedc59-3734-4258-825b-72b22bd6b893',  #Hypermarket
#     #     # 'cb6210a6-6077-41ec-b678-4e859832cbdf',  #Hypermarket
#     #     # '535d0656-a3d1-467d-88bc-225ecaf8ca5e',  #Hypermarket
#     #     # '16432124-4016-42b4-8bb3-603b3e12e67b'  #Hypermarket
#     #     #Superette
#     #     # '7b494d9e-3850-412a-a06a-f1b51f80d0ca',
#     #     # '31e594c7-39dd-48ed-8704-6116bf1b05ee',
#     #     # 'b2118f18-0323-44fd-b923-5617af863b93',
#     #     # '7cedf12b-5bd1-424d-8f3a-f23687a91f21',
#     #     # 'df98c0d6-c045-485e-b020-761becdc7c70',
#     #     # 'de5ede0b-67af-4e1d-b74a-8416e1a6bea8',
#     #     # '678f7949-2c70-4d1e-b85d-3d188f5d45dd',
#     #     # '2938ecc6-9535-467f-b8c6-507bd4b3b99f',
#     #     # 'e38c100a-5b7e-4234-9b58-c64a945e7eb5',
#     #     # '688996b4-3c8c-4945-9d57-fdbbe5491ee3'
#     #     #HoReCa
#     #     # 'b110b362-adad-43bd-ae4a-3bd24af0e7b4',
#     #     # 'ccb588da-20e6-493b-9af3-881f600fd9af',
#     #     # 'ef315df0-cf64-4124-ad36-c684810d7284',
#     #     # 'c0cf3d88-c80a-4ea4-8151-f2d7ac3eff9a',  #This session has many bugs - no scif + bug in sos
#     #     # 'b1d3e44d-d8ba-44a1-b944-5a4e4b0974d2',
#     #     # '383a0b9d-79ba-4e23-938f-7ab51cdcef91',
#     #     # '1e2286f2-6d8e-4341-bfcb-2b0c04ec2214',
#     #     # '2f949d51-9440-42d1-ba68-2967eb7f7484'
#     #     #Fast Food
#     #     # 'a4b16192-6fb0-4fd8-add1-136914c3b44e',
#     #     # '2d543b76-9112-4335-b5df-a3b45f9f5ce8',
#     #     # 'abb1e6d6-e62a-44d6-bfb7-bc016907db7d',
#     #     # 'f10f3a5c-c94e-4c5b-8953-76f4e83cebaa',
#     #     # '4c8f4461-bd89-4814-9bb1-778b741f60f3'
#     #     #Canteen
#     #     # '9acb597c-f212-4b09-882e-3c7cbc4b6e63',
#     #     # 'fd77e75e-574e-4a05-b8a1-87498e09bae3',
#     #     # '408dba7e-5b4c-467c-a2b2-d19cb7e8d661',
#     #     # 'dacbbb27-6b68-43e5-b0fe-a34a1f7dc13a'
#     #     #Petrol
#     #     'c14026f0-05f1-4f77-b420-65de2e583139',
#     #     'd58c76f0-0057-4d5b-9ff4-dbd259f8511f',
#     #     '18244983-900b-4f34-a1d0-70661ed905bb',
#     #     'f3635391-e65f-4ba6-87e2-0f6720351e93', #This session had a session which was not defined in scif
#     #     '3589ab93-9a56-448f-9e88-19fd534c5d58'
#     # ]
#
#     session_uids = ['00281E09-BE0F-4C8C-95EC-0D5F49E95C9C'] #HoReCa
#     data_provider = ACEDataProvider(project_name)
#     # data_provider.load_session_data('66d26d27-12b4-4082-af64-3d5ba2bae835')
#     # data_provider.load_session_data('8d0ecaae-a1eb-4fd5-a06b-0fb25ed503a8')
#     # data_provider.load_session_data('9b616f29-584b-4dfa-8085-b36282f8e2d8')
#     # data_provider.load_session_data('6d710fc8-6f37-43cb-94d1-2e13459094b4')
#     # data_provider.load_session_data('4d40f299-d24d-46b0-ae80-c53732b06688')
#     # data_provider.load_session_data('5d0b5080-84af-4892-b02f-d170001e911f')
#     # data_provider.load_session_data('06472ae6-f5a4-4ca0-a224-01c45f057da4')
#     # data_provider.load_session_data('f8c66ec2-d802-4515-8b49-811f5fe6ca4d')
#     # data_provider.load_session_data('fe6f585c-81e6-41df-8eb2-38a64c7436ad')
#     # data_provider.load_session_data('00310f67-e826-46f0-94b9-78f91b7686a5')
#     # data_provider.load_session_data('6cb560bf-1118-43c7-a326-7e871b07ffe4')
#     # data_provider.load_session_data('10378a12-3ba8-422c-a48f-ec5490701ad2')  # Supermarket
#     # data_provider.load_session_data('481753ea-82a8-4cd2-ac6b-ab8e2944283f')  # Supermarket
#     # data_provider.load_session_data('ee0eb23d-c519-404f-bdda-32eac0b6c028')  # Supermarket
#     # data_provider.load_session_data('974c4aee-f777-4895-84c3-88687c2dc571')  # Supermarket
#     # data_provider.load_session_data('40c8eed3-cc4b-4138-8af9-d89d24dec724')  # Supermarket
#     # data_provider.load_kpis_hierarchy()
#     output = Output()
#     for session in session_uids:
#         data_provider.load_session_data(session)
#         INTEG4Calculations(data_provider, output).run_project_calculations()
#     # SessionVanillaCalculations(data_provider, self.output).run_project_calculations()
#     # INTEG4Calculations(data_provider, self.output).insert_new_kpis(project_name)
#     # data_provider.load_session_data('0069b8bf-1908-4656-b921-58796cebf5c2')
#     # INTEG4Calculations(data_provider, self.output).run_project_calculations()
#
#     # data_provider.export_session_data(output)
#
#     # kpi_level_1_hierarchy = pd.DataFrame(data=[('FT', None, None, 'WEIGHTED_AVERAGE',
#     #                                             1, '2016-11-28', None, None)],
#     #                                      columns=['name', 'short_name', 'eng_name', 'operator',
#     #                                               'version', 'valid_from', 'valid_until', 'delete_date'])
#     # output.add_kpi_hierarchy(Keys.KPI_LEVEL_1, kpi_level_1_hierarchy)
#     # kpi_level_2_hierarchy = pd.DataFrame(data=[
#     #     (1, 'gdm_50%_visivel', None, None, None, None, 0.25, 1, '2016-11-28', None, None),
#     #     (1, 'INV GDM CSD', None, None, None, 'BINARY_SCORE', 0.75, 1, '2016-11-28', None, None)],
#     #                              columns=['kpi_level_1_fk', 'name', 'short_name', 'eng_name', 'operator',
#     #                                       'score_func', 'original_weight', 'version', 'valid_from', 'valid_until',
#     #                                       'delete_date'])
#     # output.add_kpi_hierarchy(Keys.KPI_LEVEL_2, kpi_level_2_hierarchy)
#     # kpi_level_3_hierarchy = pd.DataFrame(data=[(1, 'gdm_50%_visivel atomic', None, None, 'TB.SINGLE_SURVEY',
#     #                                            'BINARY_SCORE', None, 1, '2016-11-28', None, None),
#     #                                            (2, 'atomic 1', None, None, 'TB.CHECK_SCENES', 'BINARY_SCORE', None, 1,
#     #                                            '2016-11-28', None, None),
#     #                                            (2, 'atomic 2', None, None, 'TB.CHECK_SCENES', 'BINARY_SCORE', None, 1,
#     #                                            '2016-11-28', None, None),
#     #                                            (2, 'atomic 3', None, None, 'TB.CHECK_SCENES', 'PROPORTIONAL_SCORE',
#     #                                            None, 1, '2016-11-28', None, None)],
#     #                                      columns=['kpi_level_2_fk', 'name', 'short_name', 'eng_name', 'operator',
#     #                                               'score_func', 'original_weight', 'version', 'valid_from',
#     #                                               'valid_until', 'delete_date'])
#     # output.add_kpi_hierarchy(Keys.KPI_LEVEL_3, kpi_level_3_hierarchy)
#     # data_provider.export_kpis_hierarchy(output)
#     # jg = INTEG4JsonGenerator('integ4')
#     # jg.create_json('Hypermarket 220117.xlsx', 'Hypermarket')
#     # tb = INTEG4KPIToolBox(data_provider, self.output, 'Hypermarket')
#     # # tb.insert_new_kpis(project_name, jg.project_kpi_dict.get('kpi_data')[0])
#     # tb.insert_new_kpis_old(project_name, jg.project_kpi_dict.get('kpi_data')[0])
