# -*- coding: utf-8 -*-
from datetime import date

from Trax.Algo.Calculations.FEMSA.Constants import PossibleAnswers as Po
from Trax.Algo.Calculations.FEMSA.NewConstants import StoreTypes as St
from Trax.Algo.Calculations.FEMSA.Sets.Base import FEMSACalculationsGroup
from Trax.Utils.Logging.Logger import Log


class PadocaCalculations(FEMSACalculationsGroup):
    def run_calculations(self):
        Log.info('Starting Padocacalculation for session_pk: {}.'.format(self.session_pk))
        if self.session_info.visit_date >= date(2015, 5, 1):
            self.run_june_2016_calculations()
        else:
            Log.info('No Padoca calculations were executed.')

    def run_june_2016_calculations(self):
        store_type = self.session_info.store_type

        if store_type == St.ST_PADARIA:
            self.run_may_2016_padaria()

    def run_may_2016_padaria(self):
        # Femsa Padoca  (1) - Survey Question
        self.add_single_survey_response(fact_name='gdm_kof_1_porta_csd_ncb',
                                        fact_desc='GDM KOF 1 PORTA CSD/NCB',
                                        eng_desc=None,
                                        question="GDM KOF 1 PORTA CSD/NCB",
                                        expected_answer=Po.YES_ANSWER)

        # Femsa Padoca  (2) - Survey Question
        self.add_single_survey_response(fact_name='gdm_kof_csd_ncb_minimo_75%_abast',
                                        fact_desc='GDM KOF CSD/NCB MINIMO 75% ABAST',
                                        eng_desc=None,
                                        question="GDM KOF CSD/NCB MINIMO 75% ABAST",
                                        expected_answer=Po.YES_ANSWER)

        # Femsa Padoca  (3) - Survey Question
        self.add_single_survey_response(fact_name='rack_com_coca_cola_com_del_valle_ou_crystal',
                                        fact_desc='RACK COM COCA-COLA COM DEL VALLE OU CRYSTAL',
                                        question="RACK COM COCA-COLA COM DEL VALLE OU CRYSTAL",
                                        expected_answer=Po.YES_ANSWER)

        # Femsa Padoca  (4) - Survey Question
        self.add_single_survey_response(fact_name='rack_com_minimo_75%_da_abast',
                                        fact_desc='RACK COM MÍNIMO 75% DA ABAST',
                                        question="RACK COM MÍNIMO 75% DA ABAST",
                                        expected_answer=Po.YES_ANSWER)

        # Femsa Padoca  (5) - Survey Question
        self.add_single_survey_response(fact_name='rack_multicategoria_ref_pet',
                                        fact_desc='RACK MULTICATEGORIA / REF-PET',
                                        question="RACK MULTICATEGORIA / REF-PET",
                                        expected_answer=Po.YES_ANSWER)
