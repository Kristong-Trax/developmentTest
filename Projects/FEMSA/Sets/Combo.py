# -*- coding: utf-8 -*-

from datetime import date

from Trax.Algo.Calculations.FEMSA.Constants import PossibleAnswers as Po
from Trax.Algo.Calculations.FEMSA.NewConstants import StoreTypes as St
from Trax.Algo.Calculations.FEMSA.Sets.Base import FEMSACalculationsGroup
from Trax.Utils.Logging.Logger import Log

__author__ = 'Dudi_s'


class ComboCalculations(FEMSACalculationsGroup):

    def run_calculations(self):
        Log.info('Starting Combo calculation for session_pk: {}.'.format(self.session_pk))
        if self.session_info.visit_date >= date(2015, 6, 1):
            self.run_june_2016_calculations()
        else:
            Log.info('No Combo calculations were executed.')

    def run_june_2016_calculations(self):
        store_type = self.session_info.store_type

        if store_type in [St.ST_CONVENIENCIA, St.ST_ATACADISTA, St.ST_AS14, St.ST_AS5, St.ST_PADARIA, St.ST_BARLAN,
                          St.ST_MERCEARIA, St.ST_RESTAURANTE, St.ST_CPQ]:
            self.run_june_2016_group1()
        if store_type in [St.ST_BARLAN, St.ST_PADARIA]:
                self.run_june_2016_group2()
                if store_type == St.ST_BARLAN:
                    self.run_june_2016_barlan()
                else:
                    self.run_june_2016_padaria()
        if store_type == [St.ST_CONVENIENCIA]:
            self.run_june_2016_conveniencia()

    def run_june_2016_group1(self):
        # Femsa Combo (1) - Survey Question
        self.add_single_survey_response(fact_name='ha_mat_com_combo_kof',
                                        fact_desc='HA MAT COM COMBO KOF?',
                                        eng_desc=None,
                                        question="HA MAT COM COMBO KOF?",
                                        expected_answer=Po.YES_ANSWER)

        # Femsa Combo (8) - Survey Question
        self.add_single_survey_response(fact_name='ha_presenca_de_combo_kof',
                                        fact_desc='HA PRESENÇA DE COMBO KOF',
                                        question="HA PRESENÇA DE COMBO KOF",
                                        expected_answer=Po.YES_ANSWER)

    def run_june_2016_group2(self):
        # Femsa Combo (2) - Survey Question
        self.add_single_survey_response(fact_name='combo_csd_no_balcao',
                                        fact_desc='COMBO CSD NO BALCÃO',
                                        eng_desc=None,
                                        question="COMBO CSD NO BALCÃO",
                                        expected_answer=Po.YES_ANSWER)

        # Femsa Combo (7) - Survey Question
        self.add_single_survey_response(fact_name='menuboard_com_combo_csd',
                                        fact_desc='MENUBOARD COM COMBO CSD',
                                        question="MENUBOARD COM COMBO CSD",
                                        eng_desc=None,
                                        expected_answer=Po.YES_ANSWER)

    def run_june_2016_barlan(self):
        # Femsa Combo (6) - Survey Question
        self.add_single_survey_response(fact_name='ha_menuboard_ou_mat_de_combo',
                                        fact_desc='HA MENUBOARD OU MAT DE COMBO',
                                        eng_desc='Has menuboard or Combo material',
                                        question="HA MENUBOARD OU MAT DE COMBO",
                                        expected_answer=Po.YES_ANSWER)

    def run_june_2016_padaria(self):
        # Femsa Combo (5) - Survey Question
        self.add_single_survey_response(fact_name='combo_kof_no_balcao',
                                        fact_desc='COMBO SUCO NO BALCÃO',
                                        eng_desc=None,
                                        question="COMBO SUCO NO BALCÃO",
                                        expected_answer=Po.YES_ANSWER)

    def run_june_2016_conveniencia(self):
        # Femsa Combo (3) - Survey Question
        self.add_single_survey_response(fact_name='combo_kof_exclusivo',
                                        fact_desc='COMBO KOF (EXCLUSIVO)',
                                        eng_desc='Has CSD Combo in Exclusive Material',
                                        question="COMBO KOF (EXCLUSIVO)",
                                        expected_answer=Po.YES_ANSWER)
        # Femsa Combo (4) - Survey Question
        self.add_single_survey_response(fact_name='combo_ncb',
                                        fact_desc='COMBO NCB',
                                        eng_desc='Has Non carbonated beverages Combo',
                                        question="COMBO NCB",
                                        expected_answer=Po.YES_ANSWER)
