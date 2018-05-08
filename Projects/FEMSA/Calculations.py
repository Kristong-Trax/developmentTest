from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
# from Trax.Algo.Calculations.Core.DataProvider import ACEDataProvider, Output
from Trax.Algo.Calculations.Core.Vanilla import SessionVanillaCalculations
from Trax.Algo.Calculations.FEMSA.Sets.ActivationProducts import ActivationProductsCalculations
from Trax.Algo.Calculations.FEMSA.Sets.Combo import ComboCalculations
from Trax.Algo.Calculations.FEMSA.Sets.Geladeria import GeladeriaCalculations
from Trax.Algo.Calculations.FEMSA.Sets.Invasao import InvasaoCalculations
from Trax.Algo.Calculations.FEMSA.Sets.MaterialAtivacao import MaterialAtivacaoCalculations
from Trax.Algo.Calculations.FEMSA.Sets.Padoca import PadocaCalculations
from Trax.Algo.Calculations.FEMSA.Sets.PontosExtra import PontosExtraCalculations
from Trax.Utils.Conf.Configuration import Config
#from Trax.Cloud.Services.Connector.Logger import LoggerInitializer

__author__ = 'zeevs'


class FEMSACalculations(BaseCalculationsScript):
    def run_project_calculations(self):
        self.timer.start()
        ActivationProductsCalculations(self.data_provider, self.output).run_calculations()
        PontosExtraCalculations(self.data_provider, self.output).run_calculations()
        ComboCalculations(self.data_provider, self.output).run_calculations()
        PadocaCalculations(self.data_provider, self.output).run_calculations()
        MaterialAtivacaoCalculations(self.data_provider, self.output).run_calculations()
        GeladeriaCalculations(self.data_provider, self.output).run_calculations()
        InvasaoCalculations(self.data_provider, self.output).run_calculations()
        self.timer.stop('FEMSACalculations.run_project_calculations')

    def run_project_calculations_test_1(self):
        self.timer.start()
        GeladeriaCalculations(self.data_provider, self.output).run_calculations_test_1()
        self.timer.stop('FEMSACalculations.run_project_calculations_test')

    def run_project_calculations_test_with_atomics(self):
        self.timer.start()
        GeladeriaCalculations(self.data_provider, self.output).run_calculations_test_2()
        self.timer.stop('FEMSACalculations.run_project_calculations_test')


# if __name__ == '__main__':
#     LoggerInitializer.init('FEMSA calculations')
#     Config.init()
#     project_name = 'dev2'
#     data_provider = ACEDataProvider(project_name)
#     data_provider.load_session_data('dc4d5e85-2479-4dd6-9d23-4cf2dda3f272')  # '119b3830-a550-4396-a82b-049b859e8be8')
#     output = Output()
#
#     # kpi_level_1_hierarchy = pd.DataFrame(data=[('KPI_LEVEL_1 name', None, None, 'WEIGHTED_AVERAGE_EXCLUDE_WEIGHTS',
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
#
#     data_provider.load_kpis_hierarchy()
#
#     SessionVanillaCalculations(data_provider, output).run_project_calculations()
#     FEMSACalculations(data_provider, output).run_project_calculations_test_with_atomics()
#     data_provider.export_session_calculations_data(output)
#
#
# if __name__ == '__main__':
#     LoggerInitializer.init('FEMSA calculations')
#     Config.init()
#     project_name = 'ccbr-prod'
#     data_provider = ACEDataProvider(project_name)
#     data_provider.load_session_data('119b3830-a550-4396-a82b-049b859e8be8')
#     output = Output()
#     SessionVanillaCalculations(data_provider, output).run_project_calculations()
#     FEMSACalculations(data_provider, output).run_project_calculations()
#     data_provider.export(output)
