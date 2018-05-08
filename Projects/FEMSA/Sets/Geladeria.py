# -*- coding: utf-8 -*-
from datetime import date

import pandas as pd

from Trax.Algo.Calculations.Core.Constants import Fields as Fd
from Trax.Algo.Calculations.Core.DataProvider import Keys, Data
from Trax.Algo.Calculations.Core.Utils import Predicates as Pred, WeightedAverage, ScoreFunc
from Trax.Algo.Calculations.FEMSA.Constants import PossibleAnswers as Pa
from Trax.Algo.Calculations.FEMSA.Constants import Products as Pr
from Trax.Algo.Calculations.FEMSA.NewConstants import StoreTypes as St
from Trax.Algo.Calculations.FEMSA.Constants import Templates as Tm
from Trax.Algo.Calculations.FEMSA.NewConstants import Manufacturers as Ma
from Trax.Algo.Calculations.FEMSA.NewConstants import Products as Prd
# from Trax.Algo.Calculations.FEMSA.NewConstants import Brands as Br
from Trax.Algo.Calculations.FEMSA.NewConstants import TemplateGroups as Tg
from Trax.Algo.Calculations.FEMSA.Sets.Base import FEMSACalculationsGroup
from Trax.Utils.Logging.Logger import Log
from Trax.Algo.Calculations.Core.Utils import ToolBox as TBox


class GeladeriaCalculations(FEMSACalculationsGroup):
    def convert_kpi_level_1(self, kpi_level_1):
        kpi_level_1_df = self.data_provider[Data.KPI_LEVEL_1]
        kpi_name = kpi_level_1.fact_name
        kpi_fk = kpi_level_1_df[kpi_level_1_df['name'] == kpi_name].reset_index()['pk'][0]
        kpi_result = kpi_level_1.fact_df['result'][0]
        kpi_level_1_results = pd.DataFrame(columns=self.output.KPI_LEVEL_1_RESULTS_COLS)
        kpi_level_1_results = kpi_level_1_results.append({'session_fk': self.session_pk, 'kpi_level_1_fk': kpi_fk,
                                                          'result': kpi_result}, ignore_index=True)
        kpi_level_1_results = self.data_provider.add_session_fields_old_tables(kpi_level_1_results)
        return kpi_level_1_results

    def convert_kpi_level_2(self, level_2_kpis):
        kpi_level_2_results = pd.DataFrame(columns=self.output.KPI_LEVEL_2_RESULTS_COLS)
        kpi_level_2_df = self.data_provider[Data.KPI_LEVEL_2]
        for kpi in level_2_kpis:
            kpi_name = kpi.fact_name
            kpi_fk = kpi_level_2_df[kpi_level_2_df['name'] == kpi_name].reset_index()['pk'][0]
            kpi_result = kpi.fact_df['result'][0]
            kpi_score = kpi.fact_df['score'][0]
            kpi_weight = kpi.fact_df['original_weight'][0]
            kpi_level_2_results = kpi_level_2_results.append({'session_fk': self.session_pk, 'kpi_level_2_fk': kpi_fk,
                                                              'result': kpi_result, 'score': kpi_score,
                                                              'weight': kpi_weight}, ignore_index=True)
        kpi_level_2_results = self.data_provider.add_session_fields_old_tables(kpi_level_2_results)
        return kpi_level_2_results

    def convert_kpi_level_3(self, level_3_kpis):
        kpi_level_3_results = pd.DataFrame(columns=self.output.KPI_LEVEL_3_RESULTS_COLS)
        kpi_level_3_df = self.data_provider[Data.KPI_LEVEL_3]
        for level_3_kpi in level_3_kpis:
            kpi_name = level_3_kpi.fact_name
            kpi_fk = kpi_level_3_df[kpi_level_3_df['name'] == kpi_name].reset_index()['pk'][0]
            kpi_result = level_3_kpi.fact_df['result'][0]
            kpi_score = level_3_kpi.fact_df['score'][0]
            kpi_weight = level_3_kpi.fact_df['original_weight'][0]
            kpi_target = level_3_kpi.fact_df['target'][0]
            kpi_level_3_results = kpi_level_3_results.append({'session_fk': self.session_pk,
                                                              'kpi_level_3_fk': kpi_fk,
                                                              'result': kpi_result, 'score': kpi_score,
                                                              'weight': kpi_weight, 'target': kpi_target},
                                                             ignore_index=True)
        kpi_level_3_results = self.data_provider.add_session_fields_old_tables(kpi_level_3_results)
        return kpi_level_3_results

    def run_calculations_test_1(self):
        kpi_a = self.geladeria_kpi_15(self.scif)
        kpi_b = self.geladeria_kpi_16()

        kpis_in_set = [kpi_a, kpi_b]
        kpi_set = self.add_weighted_average_fact_hierarchy(kpis_in_set, 'KPI_LEVEL_1 name', 'KPI_LEVEL_1 desc',
                                                           weighted_average_method=WeightedAverage.EXCLUDE_WEIGHTS)

        self.output.add_kpi_results(Keys.KPI_LEVEL_1_RESULTS, self.convert_kpi_level_1(kpi_set))
        self.output.add_kpi_results(Keys.KPI_LEVEL_2_RESULTS, self.convert_kpi_level_2(kpis_in_set))
        # self.output.add_kpi_results(Keys.ATOMIC_KPI_RESULTS,
        #                             self.convert_kpi_level_3(kpis_in_set, self.get_atomic_kpis_by_name()))

    def run_calculations_test_2(self):
        kpi_a = self.geladeria_kpi_15_with_atomics(self.scif)
        kpi_b = self.geladeria_kpi_16_with_atomics()

        kpis_in_set = [kpi_a, kpi_b]
        kpi_set = self.add_weighted_average_fact_hierarchy(kpis_in_set, 'KPI_LEVEL_1 name', 'KPI_LEVEL_1 desc',
                                                           weighted_average_method=WeightedAverage.EXCLUDE_WEIGHTS)

        self.output.add_kpi_results(Keys.KPI_LEVEL_1_RESULTS, self.convert_kpi_level_1(kpi_set))
        self.output.add_kpi_results(Keys.KPI_LEVEL_2_RESULTS, self.convert_kpi_level_2(kpis_in_set))
        # self.output.add_kpi_results(Keys.ATOMIC_KPI_RESULTS,
        #                             self.convert_kpi_level_3(kpis_in_set, self.get_atomic_kpis_by_name()))

    def run_calculations(self):
        Log.info('Starting Geladeria calculation for session_pk: {}.'.format(self.session_pk))
        if self.visit_date >= date(2016, 6, 1):
            self.run_june_2016_calculations()
        else:
            Log.info('No Geladeria calculations were executed.')

    def run_june_2016_calculations(self):
        store_type = self.session_info.store_type

        if store_type in [St.ST_MERCEARIA, St.ST_PADARIA, St.ST_BARLAN]:
            self.run_june_2016_group1()
            self.run_june_2016_group2()
            self.run_june_2016_group3()
            self.run_june_2016_group4()
            self.run_june_2016_restaurante()
            self.run_june_2016_group5()
        elif store_type in [St.ST_AS14, St.ST_AS5, St.ST_ATACADISTA]:
            self.run_june_2016_group2()
            self.run_june_2016_group1()
            if store_type != St.ST_ATACADISTA:
                self.run_june_2016_group5()
        elif store_type == St.ST_RESTAURANTE:
            self.run_june_2016_group3()
            self.run_june_2016_group5()
            self.run_june_2016_group1()
            self.run_june_2016_restaurante()
        elif store_type == St.ST_CPQ:
            self.run_june_2016_cpq()
            self.run_june_2016_group2()
        elif store_type == St.ST_AS5:
            self.run_june_2016_as5()
        elif store_type == St.ST_AS14:
            self.run_june_2016_group4()
        elif store_type == St.ST_BARLAN:
            self.run_june_2016_barlan()
        elif store_type == St.ST_CONVENIENCIA:
            self.run_june_2016_conveniencia()
            self.run_june_2016_group1()

    def run_june_2016_restaurante(self):
        scif = self.scif
        # Femsa Geladeria (24) - Minimal Total Distribution
        target_name_cko = [Tm.TN_GDM_KOF_NCB_NP_1A_POS, Tm.TN_GDM_KOF_NCB_P_1A_POS]
        self.check_distinct_product_count(fact_name='gdm_ncb_1_pos',
                                          fact_desc='GDM NCB 1 POS',
                                          eng_desc='Has non carbonated in refrigerator equipment in first position',
                                          pop_filter=((scif[Fd.T_NAME].isin(target_name_cko)) &
                                                      (scif[Fd.DIST_SC] == 1)))

    def run_june_2016_restaurante___uri(self):
        scif = self.scif
        # Femsa Geladeria (24) - Minimal Total Distribution
        any_cooler_location_type = ["Cooler", "Other Coolers", "Cold Shelf"]
        self.check_distinct_product_count(fact_name='test 1',
                                          fact_desc='test 1 desc',
                                          eng_desc='dfgdfdfg',
                                          pop_filter=((scif[Fd.LOCATION_TYPE].isin(any_cooler_location_type)) &
                                                      (scif[Fd.PRODUCT_FK] == Prd.P_AGUA_FEMSA_6_PACK_CRYSTAL_1L)))

    def run_june_2016_barlan(self):
        # Femsa Geladeria (1) - Survey Question
        self.check_single_survey_response_number(fact_name='50%colas_i',
                                                 fact_desc='50%COLAS I',
                                                 eng_desc='Amount of GDMs (Refrigeretors) '
                                                          '50% full of Cokes for Immediate Consume',
                                                 question="50% Colas CONS IMEDIATO",
                                                 expected_answer=1)

    def run_june_2016_group4(self):
        scif = self.scif
        # Femsa Geladeria (22) - Minimal Total Distribution
        target_name2 = [Tm.TN_GDM_KOF_CSD_P_POS_LIDERENCA, Tm.TN_GDM_KOF_NCB_NP_POS_LIDERENCA,
                        Tm.TN_GDM_KOF_NCB_P_POS_LIDERENCA, Tm.TN_GDM_KOF_CSD_NP_POS_LIDERENCA,
                        Tm.TN_GDM_KOF_CERV_NP_POS_LIDERENCA, Tm.TN_GDM_KOF_CERV_P_POS_LIDERENCA]
        self.check_distinct_product_count(fact_name='gdm_kof_em_pos_de_lideranca',
                                          fact_desc='GDM KOF EM POS DE LIDERANÇA',
                                          pop_filter=((scif[Fd.T_NAME].isin(target_name2)) &
                                                      (scif[Fd.DIST_SC] == 1)))
        # Femsa Geladeria (23) - Survey Question
        self.add_single_survey_response(fact_name='gdm_kof_minimo_80%_abast',
                                        fact_desc='GDM KOF MINIMO 80% ABAST',
                                        eng_desc='Refrigerator must be filled with at '
                                                 'least 80% of its capacity',
                                        question="GDM KOF MINIMO 80% ABAST",
                                        expected_answer=Pa.YES_ANSWER)

    def run_june_2016_group5(self):
        # Femsa Geladeria (25) - Survey Question
        self.add_single_survey_response(fact_name='porta_gdm_cerveja_femsa',
                                        fact_desc='PORTA GDM CERVEJA FEMSA',
                                        eng_desc='Number of GDM doors with Femsas Beer facings',
                                        question="PORTA GDM CERVEJA FEMSA",
                                        expected_answer=Pa.YES_ANSWER)

        # Femsa Geladeria (26) - Survey Question
        self.check_single_survey_response_number(fact_name='porta_gdm_csd_femsa',
                                                 fact_desc='PORTA GDM CSD FEMSA',
                                                 eng_desc='Number of GDM doors with Femsas CSD facings',
                                                 question="PORTA GDM CSD FEMSA",
                                                 expected_answer=0)

        # Femsa Geladeria (27) - Survey Question
        self.check_single_survey_response_number(fact_name='porta_gdm_ncb_femsa',
                                                 fact_desc='PORTA GDM NCB FEMSA',
                                                 eng_desc='Number of GDM doors with Femsas NCARB. facings',
                                                 question="PORTA GDM NCB FEMSA",
                                                 expected_answer=0)

    def run_june_2016_as5(self):
        # Femsa Geladeria (12) - Survey Question
        self.add_single_survey_response(fact_name='gdn_kof_csd_no_caixa_normal_com_50_csd_individual',
                                        fact_desc='GDM KOF CSD NO CAIXA NORMAL COM 50% CSD INDIVIDUAL',
                                        eng_desc='GDM (Refrigerator) Coca-Cola,'
                                                 'filled with, at least, 50% of CSD for immediate consume,'
                                                 'in the fast cashier',
                                        question="GDM KOF CSD NO CAIXA NORMAL COM 50% CSD INDIVIDUAL",
                                        expected_answer=Pa.YES_ANSWER)
        # Femsa Geladeria (13) - Survey Question
        self.add_single_survey_response(fact_name='gdn_kof_csd_no_caixa_rapido',
                                        fact_desc='GDM KOF CSD NO CAIXA RÁPIDO',
                                        eng_desc=None,
                                        question="GDM KOF CSD NO CAIXA RÁPIDO	",
                                        expected_answer=Pa.YES_ANSWER)
        # Femsa Geladeria (14) - Survey Question
        self.add_single_survey_response(fact_name='gdn_kof_csd_no_caixa_rapido_com_50_csd_futuro',
                                        fact_desc='GDM KOF CSD NO CAIXA RÁPIDO COM 50% CSD FUTURO',
                                        eng_desc='GDM (Refrigerator) Coca-Cola, filled with, at least, '
                                                 '50% of CSD for future consume, in the fast cashier',
                                        question="GDM KOF CSD NO CAIXA RÁPIDO COM 50% CSD FUTURO",
                                        expected_answer=Pa.YES_ANSWER)

        # Femsa Geladeria (19) - Survey Question
        self.add_single_survey_response(fact_name='gdn_femsa_no_ckout_min_80%_csd',
                                        fact_desc='GDM FEMSA NO CKOUT MIN 80% CSD',
                                        eng_desc='Has Refrigerator Femsa in Checkout',
                                        question="GDM FEMSA NO CKOUT MIN 80% CSD",
                                        expected_answer=Pa.YES_ANSWER)
        # Femsa Geladeria (20) - Survey Question
        self.add_single_survey_response(fact_name='gdn_kof_csd_no_caixa_normal_min_80%_abast',
                                        fact_desc='GDM KOF CSD NO CAIXA NORMAL MIN 80% ABAST',
                                        eng_desc='Coca-Cola Refrigerator with at least 80% full of CSD,'
                                                 ' in the normal cashier',
                                        question="GDM KOF CSD NO CAIXA NORMAL MIN 80% ABAST",
                                        expected_answer=Pa.YES_ANSWER)
        # Femsa Geladeria (21) - Survey Question
        self.add_single_survey_response(fact_name='gdn_kof_csd_no_caixa_rapido_min_80%_abast',
                                        fact_desc='GDM KOF CSD NO CAIXA RÁPIDO MIN 80% ABAST',
                                        eng_desc='Coca-Cola Refrigerator with at least 80% full of CSD,'
                                                 'in the fast cashier',
                                        question="GDM KOF CSD NO CAIXA RÁPIDO MIN 80% ABAST",
                                        expected_answer=Pa.YES_ANSWER)

        # Femsa Geladeria (28) - Survey Question
        self.add_single_survey_response(fact_name='possui_caixa_rapido',
                                        fact_desc='POSSUI CAIXA RÁPIDO?',
                                        eng_desc='Has PDV in the fast cashier',
                                        question="POSSUI CAIXA RÁPIDO?",
                                        expected_answer=Pa.YES_ANSWER)
        scif = self.scif
        # Femsa Geladeria (22) - Minimal Total Distribution
        target_name3 = [Tm.TN_GDM_KOF_CSD_P_POS_LIDERENCA, Tm.TN_GDM_KOF_NCB_NP_POS_LIDERENCA,
                        Tm.TN_GDM_KOF_NCB_P_POS_LIDERENCA, Tm.TN_GDM_KOF_CSD_NP_POS_LIDERENCA,
                        Tm.TN_GDM_KOF_CERV_NP_POS_LIDERENCA, Tm.TN_GDM_KOF_CERV_P_POS_LIDERENCA]
        self.check_distinct_product_count(fact_name='gdm_kof_em_pos.de_lideranca',
                                          fact_desc='GDM KOF EM POS DE LIDERANÇA',
                                          eng_desc=None,
                                          pop_filter=((scif[Fd.T_NAME].isin(target_name3)) &
                                                      (scif[Fd.DIST_SC] == 1)))

    def run_june_2016_conveniencia(self):
        scif = self.scif
        # Femsa Geladeria (18) - Minimal Total Distribution
        target_name4 = [Tm.TN_GDM_KOF_CSD_P_CKO, Tm.TN_GDM_KOF_NCB_NP_CKO, Tm.TN_GDM_KOF_NCB_P_CKO,
                        Tm.TN_GDM_KOF_CSD_NP_CKO, Tm.TN_GDM_KOF_CERV_NP_CKO, Tm.TN_GDM_KOF_CERV_P_CKO]
        self.check_distinct_product_count(fact_name='gdm_femsa_no_ckout',
                                          fact_desc='GDM FEMSA NO CKOUT',
                                          pop_filter=((scif[Fd.T_NAME].isin(target_name4)) &
                                                      (scif[Fd.DIST_SC] == 1)))

    def run_june_2016_cpq(self):
        self.run_june_2016_kpis2()

        self.geladeria_kpi_16()

    def geladeria_kpi_16(self):
        # Femsa Geladeria (16) - Survey Question
        return self.add_single_survey_response(fact_name='gdm_50%_visivel',
                                               fact_desc='GDM 50% VISIVEL?',
                                               question="GDM 50% VISIVEL?",
                                               expected_answer=Pa.YES_ANSWER,
                                               original_weight=0.25)

    def geladeria_kpi_16_with_atomics(self):
        # Femsa Geladeria (16) - Survey Question
        atomic_fact = self.add_single_survey_response(fact_name='gdm_50%_visivel atomic',
                                                      fact_desc='check if answer for survey "GDM 50% VISIVEL?" is Yes',
                                                      question="GDM 50% VISIVEL?",
                                                      expected_answer=Pa.YES_ANSWER)
        self.output.add_kpi_results(Keys.KPI_LEVEL_3_RESULTS, self.convert_kpi_level_3([atomic_fact]))

        return TBox.add_fact('GDM 50% VISIVEL?', 'gdm_50%_visivel', atomic_fact.fact_df, self.output,
                             self.data_provider, original_weight=0.25)

    def run_june_2016_group3(self):
        scif = self.scif
        # Femsa Geladeria (17) - Minimal Total Distribution
        template_name1 = [Tm.TN_GDM_KOF_CSD_P_1A_POS, Tm.TN_GDM_KOF_CSD_NP_1A_POS]
        self.check_distinct_product_count(fact_name='gdm_csd_1_pos',
                                          fact_desc='GDM CSD 1 POS',
                                          eng_desc='Has CSD in refrigerator equipment in first position',
                                          pop_filter=((scif[Fd.T_NAME].isin(template_name1)) &
                                                      (scif[Fd.DIST_SC] == 1) &
                                                      (scif[Fd.MAN_FK] == Ma.M_FEMSA)))

    def run_june_2016_group2(self):
        # Femsa Geladeria (2) - Survey Question
        self.check_single_survey_response_number(fact_name='50%_Colas_tt',
                                                 fact_desc='50%COLAS TT',
                                                 eng_desc='Amount of GDMs (Refrigeretors) 50% '
                                                          'full of Cokes for Immediate Consume',
                                                 question="50% Colas Totais",
                                                 expected_answer=1)

    def run_june_2016_group1(self):
        self.run_june_2016_kpis1()

    def run_june_2016_kpis1(self):
        # Femsa Geladeria (3) - Survey Question
        self.check_single_survey_response_number(fact_name='abs_gdm_csd_c_csd',
                                                 fact_desc='ABS GDM CSD C CSD',
                                                 eng_desc='Amount of GDMs with CSD sticker,'
                                                          ' filled with 80% of CSD',
                                                 question="ABS GDM CSD C CSD",
                                                 expected_answer=1)

        # Femsa Geladeria (4) - Survey Question
        self.check_single_survey_response_number(fact_name='abs_gdm_csd_c_hnk',
                                                 fact_desc='ABS GDM CSD C HNK',
                                                 eng_desc='Amount of GDMs with CSD sticker, '
                                                          'filled with 80% of Beer',
                                                 question="ABS GDM CSD C HNK",
                                                 expected_answer=1)

        # Femsa Geladeria (5) - Survey Question
        self.check_single_survey_response_number(fact_name='abs_gdm_csd_c_ncat',
                                                 fact_desc='ABS GDM CSD C NCAT',
                                                 eng_desc='Amount of GDMs with CSD sticker, '
                                                          'filled with 80% of Non carbonated beverages',
                                                 question="ABS GDM CSD C NCAT",
                                                 expected_answer=1)

        # Femsa Geladeria (6) - Survey Question
        self.check_single_survey_response_number(fact_name='abs_gdm_hnk_c_csd',
                                                 fact_desc='ABS GDM HNK C CSD',
                                                 eng_desc='Amount of GDMs with Beer sticker, '
                                                          'filled with 80% of CSD',
                                                 question="ABS GDM HNK C CSD",
                                                 expected_answer=1)

        # Femsa Geladeria (7) - Survey Question
        self.check_single_survey_response_number(fact_name='abs_gdm_hnk_c_hnk',
                                                 fact_desc='ABS GDM HNK C HNK',
                                                 eng_desc='Amount of GDMs with Beer sticker, '
                                                          'filled with 80% of Beer',
                                                 question="ABS GDM HNK C HNK")

        # Femsa Geladeria (8) - Survey Question
        self.check_single_survey_response_number(fact_name='abs_gdm_hnk_c_ncat',
                                                 fact_desc='ABS GDM HNK C NCAT',
                                                 eng_desc='Amount of GDMs with Beer sticker,'
                                                          ' filled with 80% of Non carbonated beverages',
                                                 question="ABS GDM HNK C NCAT")

        # Femsa Geladeria (9) - Survey Question
        self.check_single_survey_response_number(fact_name='abs_gdm_ncat_c_csd',
                                                 fact_desc='ABS GDM NCAT C CSD',
                                                 eng_desc='Amount of GDMs with Non carbonated sticker,'
                                                          ' filled with 80% of CSD',
                                                 question="ABS GDM NCAT C CSD")

        # Femsa Geladeria (10) - Survey Question
        self.check_single_survey_response_number(fact_name='abs_gdm_ncat_c_hnk',
                                                 fact_desc='ABS GDM NCAT C HNK',
                                                 eng_desc='Amount of GDMs with Non carbonated sticker, '
                                                          'filled with 80% of Beer',
                                                 question="ABS GDM NCAT C HNK")

        # Femsa Geladeria (11) - Survey Question
        self.check_single_survey_response_number(fact_name='abs_gdm_ncat_c_ncat',
                                                 fact_desc='ABS GDM NCAT C NCAT',
                                                 eng_desc='Amount of GDMs with Non carbonated sticker, '
                                                          'filled with 80% of Non carbonated beverages',
                                                 question="ABS GDM NCAT C NCAT")

    def run_june_2016_kpis2(self):
        # Femsa Geladeria (29) - Survey Question
        self.check_single_survey_response_number(fact_name='qtd_quantidade_de_gdm_abastecidas',
                                                 fact_desc='QTD – Quantidade de GDM Abastecidas',
                                                 eng_desc='Amount of filled GDMs',
                                                 question="QTD – Quantidade de Blocos de GDM CSD",
                                                 expected_answer=0)

        scif = self.scif
        # Femsa Geladeria (30) - Count SCENE
        temp_name_group2 = [Tg.TG_GDM_KOF_CSD, Tg.TG_GDM_KOF_CSD_NP]
        self.check_scenes(fact_name='qtd_gdm_csd',
                          fact_desc='QTD GDM CSD',
                          eng_desc='Amount of GDM KOF of CSD',
                          pop_filter=(scif[Fd.T_GROUP].isin(temp_name_group2)),
                          target=0)

        # Femsa Geladeria (31) - Count SCENE
        temp_name_group3 = [Tg.TG_GDM_KOF_CERV, Tg.TG_GDM_KOF_CERV_NP]
        self.check_scenes(fact_name='qtd_gdm_hnk',
                          fact_desc='QTD GDM HNK',
                          eng_desc='Amount of GDM KOF of Heineken',
                          pop_filter=(scif[Fd.T_GROUP].isin(temp_name_group3)),
                          target=0)

        # Femsa Geladeria (32) - Count SCENE
        temp_name_group4 = [Tg.TG_GDM_KOF_NCB, Tg.TG_GDM_KOF_NCB_NP]
        self.check_scenes(fact_name='qtd_gdm_ncat',
                          fact_desc='QTD GDM NCAT',
                          eng_desc='Amount of GDM KOF of NCB',
                          pop_filter=(scif[Fd.T_GROUP].isin(temp_name_group4)),
                          target=0)

        # Femsa Geladeria (33) - Survey Question
        self.add_single_survey_response(fact_name='ha_alguna_gdm_kof_invadida',
                                        fact_desc='HÁ ALGUMA GDM KOF INVADIDA?',
                                        question="HÁ ALGUMA GDM KOF INVADIDA?",
                                        expected_answer=Pa.YES_ANSWER)

        self.geladeria_kpi_15(scif)

    def geladeria_kpi_15(self, scif):
        # Femsa Geladeria (15) - Count SCENE
        brand_group = [Pr.B_AMSTEL_PULSE_CERVEJA, Pr.B_BAVARIA_CERVEJA, Pr.B_HEINEKEN_CERVEJA, Pr.B_KAISER_CERVEJA,
                       Pr.B_FEMSA_CERVEJA, Pr.B_SOL_CERVEJA, Pr.B_SOL_PREMIUM_CERVEJA, Pr.B_XINGU_CERVEJA,
                       Pr.B_GOLD_CERVEJA, Pr.B_BIRRA_MORETTI_CERVEJA, Pr.B_DESPERADOS_CERVEJA, Pr.B_DOS_EQUIS_CERVEJA,
                       Pr.B_EDELWEISS_CERVEJA, Pr.B_MURPHYS_IRISH_STOUT, Pr.B_MURPHYS_IRISH_RED]
        temp_name_group5 = [Tg.TG_GDM_KOF_CSD, Tg.TG_GDM_KOF_CSD_NP]
        fact = 'INV GDM CSD'
        description = 'inv_gdm_csd'
        calc1 = self.check_scenes_for_operator(pop_filter=((scif[Fd.B_NAME].isin(brand_group)) &
                                                           (scif[Fd.T_GROUP].isin(temp_name_group5))))
        calc2 = self.check_scenes_for_operator(pop_filter=((scif[Fd.MAN_FK] != Ma.M_FEMSA) &
                                                           (scif[Fd.T_GROUP].isin(temp_name_group5))))
        calc3 = self.check_scenes_for_operator(pop_filter=((scif[Fd.P_NAME] == Pr.P_IRRELEVENT) &
                                                           (scif[Fd.T_GROUP].isin(temp_name_group5))))
        return self.add_or_fact([calc1, calc2, calc3], description, fact, original_weight=0.75)

    def geladeria_kpi_15_with_atomics(self, scif):
        # Femsa Geladeria (15) - Count SCENE
        brand_group = [Pr.B_AMSTEL_PULSE_CERVEJA, Pr.B_BAVARIA_CERVEJA, Pr.B_HEINEKEN_CERVEJA, Pr.B_KAISER_CERVEJA,
                       Pr.B_FEMSA_CERVEJA, Pr.B_SOL_CERVEJA, Pr.B_SOL_PREMIUM_CERVEJA, Pr.B_XINGU_CERVEJA,
                       Pr.B_GOLD_CERVEJA, Pr.B_BIRRA_MORETTI_CERVEJA, Pr.B_DESPERADOS_CERVEJA, Pr.B_DOS_EQUIS_CERVEJA,
                       Pr.B_EDELWEISS_CERVEJA, Pr.B_MURPHYS_IRISH_STOUT, Pr.B_MURPHYS_IRISH_RED]
        temp_name_group5 = [Tg.TG_GDM_KOF_CSD, Tg.TG_GDM_KOF_CSD_NP]
        fact = 'INV GDM CSD'
        description = 'inv_gdm_csd'
        atomic_1 = self.check_scenes(pop_filter=((scif[Fd.B_NAME].isin(brand_group)) &
                                                 (scif[Fd.T_GROUP].isin(temp_name_group5))),
                                     fact_name='atomic 1', fact_desc='atomic_1 desc')
        atomic_2 = self.check_scenes(pop_filter=((scif[Fd.MAN_FK] != Ma.M_FEMSA) &
                                                 (scif[Fd.T_GROUP].isin(temp_name_group5))),
                                     fact_name='atomic 2', fact_desc='atomic_2 desc')
        atomic_3 = self.check_scenes(pop_filter=((scif[Fd.P_NAME] == Pr.P_IRRELEVENT) &
                                                 (scif[Fd.T_GROUP].isin(temp_name_group5))),
                                     target=2, fact_name='atomic 3', fact_desc='atomic_3 desc',
                                     score_func=ScoreFunc.PROPORTIONAL)
        atomic_kpis = [atomic_1, atomic_2, atomic_3]
        self.output.add_kpi_results(Keys.KPI_LEVEL_3_RESULTS,
                                    self.convert_kpi_level_3(atomic_kpis))
        return self.add_or_fact_by_facts(atomic_kpis, description, fact, original_weight=0.75)
