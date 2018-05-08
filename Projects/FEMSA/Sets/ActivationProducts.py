# -*- coding: utf-8 -*-
from datetime import date

from Trax.Algo.Calculations.Core.Constants import Fields as Fd
# from Trax.Algo.Calculations.FEMSA.NewConstants import Products as Pr, Templates as Tm
from Trax.Algo.Calculations.FEMSA.Constants import PossibleAnswers as Po
from Trax.Algo.Calculations.FEMSA.NewConstants import StoreTypes as St
from Trax.Algo.Calculations.FEMSA.NewConstants import Manufacturers as Ma
from Trax.Algo.Calculations.FEMSA.NewConstants import Categories as Ca
from Trax.Algo.Calculations.FEMSA.NewConstants import TemplateGroups as Tg
from Trax.Algo.Calculations.FEMSA.Sets.Base import FEMSACalculationsGroup
from Trax.Utils.Logging.Logger import Log


class ActivationProductsCalculations(FEMSACalculationsGroup):
    def run_calculations(self):
        Log.info('Starting Activation Products calculation for session_pk: {}.'.format(self.session_pk))
        if self.visit_date >= date(2016, 6, 1):
            self.run_june_2016_calculations()
        else:
            Log.info('No Activation Products calculations were executed.')

    # @trace
    def run_june_2016_calculations(self):
        store_type = self.session_info.store_type
        if store_type in [St.ST_AS14, St.ST_AS5, St.ST_BARLAN,
                          St.ST_CONVENIENCIA, St.ST_PADARIA, St.ST_RESTAURANTE, St.ST_MERCEARIA]:
            self.run_june_2016_group1()

        if store_type in [St.ST_AS14, St.ST_PADARIA, St.ST_MERCEARIA]:
            self.run_june_2016_group2()

        if store_type in [St.ST_PADARIA, St.ST_MERCEARIA, St.ST_CPQ, St.ST_BARLAN]:
            self.run_june_2016_group3()

        if store_type == St.ST_CPQ:
            self.run_june_2016_cpq()

        if store_type == St.ST_CONVENIENCIA:
            self.run_june_2016_conveniencia()

        if store_type == St.ST_ADEGA:
            self.run_june_2016_adega()

    # @trace
    def run_june_2016_adega(self):
        # Femsa ActivationProducts (9) - Survey Question
        categories = [Ca.C_AGUA, Ca.C_SUCO, Ca.C_CHA, Ca.C_ENERG, Ca.C_LACTEO, Ca.C_ISO, Ca.C_REFRES, Ca.C_OTHER,
                      Ca.C_CSD, Ca.C_CERVEJA]
        fact = '3 FRENTES CERV NA GDM'
        description = 'three_frentes_cerv_na_gdm'
        eng_desc = 'None exposed product (SOVI) in the client store',
        calc1 = self.check_single_survey_response(question="INV CC 2,5L", expected_answer=Po.AC_ANSWER)
        scif = self.scif
        calc2 = self.check_scenes_for_operator(pop_filter=(scif[Fd.CAT_FK].isin(categories)), target=0)
        self.add_or_fact([calc1, calc2], description, fact, eng_desc)

    def run_june_2016_group1(self):

        # Femsa ActivationProducts (1) - Survey Question
        self.add_single_survey_response(fact_name='inv_cc_2_and_a_half_l',
                                        fact_desc='INV CC 2,5L?',
                                        eng_desc='Franchise Invasion for the Coca-cola 2,5L',
                                        question="INV CC 2,5L",
                                        expected_answer=Po.AC_ANSWER)

        # Femsa ActivationProducts (2) - Survey Question
        self.add_single_survey_response(fact_name='inv_cc_2_l',
                                        fact_desc='INV CC 2L?',
                                        eng_desc='Franchise Invasion for the Coca-cola 2,0L',
                                        question="INV CC 2,0L ",
                                        expected_answer=Po.AC_ANSWER)

        # Femsa ActivationProducts (3) - Survey Question
        self.add_single_survey_response(fact_name='inv_cc_LT',
                                        fact_desc='INV CC LT?',
                                        eng_desc='Franchise Invasion for the Coca-cola can',
                                        question="INV CC LT",
                                        expected_answer=Po.AC_ANSWER)

    def run_june_2016_group2(self):
        # Femsa ActivationProducts (6) - Survey Question
        self.add_single_survey_response(fact_name='5_csd_ret_cons_fut_gelados',
                                        fact_desc='5 CSD RET CONS FUT GELADOS',
                                        eng_desc='Has it least 5 SKUs of CSD for future consume (up to 600 ml) '
                                                 'returnable botles, in refrigerated equipment ',
                                        question="5 CSD RET CONS FUT GELADOS.",
                                        expected_answer=Po.YES_ANSWER)

        # Femsa ActivationProducts (8) - Survey Question
        self.add_single_survey_response(fact_name='ha_ret_cons_fut_ao_lado_de_tubainas',
                                        fact_desc='HA RET CONS FUT AO LADO DE TUBAINAS',
                                        eng_desc='Returnable future consume drink side to a Tubaina drink',
                                        question="HA RET CONS FUT AO LADO DE TUBAINAS",
                                        expected_answer=Po.YES_ANSWER)

        # Femsa ActivationProducts (10) - Survey Question
        self.add_single_survey_response(fact_name='refpet_ao_lado_de_coca_cola_2l_descartavel',
                                        fact_desc='REFPET AO LADO DE COCA COLA 2L DESCARTAVEL?',
                                        eng_desc='Is there Refpet product side with Coca Cola Pet 2l ',
                                        question="REFPET AO LADO DE COCA COLA 2L DESCARTAVEL",
                                        expected_answer=Po.YES_ANSWER)

    def run_june_2016_group3(self):
        # Femsa ActivationProducts (7) - Survey Question
        self.add_single_survey_response(fact_name='contem_min_2_categ_no_menu',
                                        fact_desc='CONTEM MIN 2 CATEG NO MENU?',
                                        eng_desc=None,
                                        question="CONTEM MIN 2 CATEG NO MENU?",
                                        expected_answer=Po.YES_ANSWER)

    def run_june_2016_conveniencia(self):
        scif = self.scif
        # Femsa ActivationProducts (4) - Count SKU
        template_group_list1 = [Tg.TG_GDM_KOF_CERV, Tg.TG_GDM_KOF_CERV_NP, Tg.TG_GDM_KOF_CSD,
                                Tg.TG_GDM_KOF_CSD_NP, Tg.TG_GDM_KOF_NCB, Tg.TG_GDM_KOF_NCB_NP,
                                Tg.TG_GDM_CONC_PROPRIO, Tg.TG_GDM_CONC_PROPRIO_NP]
        self.check_facings(fact_name='three_frentes_cerv_na_gdm',
                           fact_desc='3 FRENTES CERV NA GDM',
                           eng_desc='Does it has 3 or more facings of beer in a Beer GDM Femsa',
                           pop_filter=((scif[Fd.CAT_FK] == Ca.C_CERVEJA) &
                                       (scif[Fd.T_GROUP].isin(template_group_list1)) &
                                       (scif[Fd.MAN_FK] == Ma.M_FEMSA)),
                           target=3)

        # Femsa ActivationProducts (5) - Count SKU
        template_group_list2 = [Tg.TG_GDM_KOF_CERV, Tg.TG_GDM_KOF_CERV_NP, Tg.TG_GDM_KOF_CSD,
                                Tg.TG_GDM_KOF_CSD_NP, Tg.TG_GDM_KOF_NCB, Tg.TG_GDM_KOF_NCB_NP,
                                Tg.TG_GDM_CONC_PROPRIO, Tg.TG_GDM_CONC_PROPRIO_NP]
        self.check_facings(fact_name='four_frentes_cerv_na_gdm',
                           fact_desc='4 FRENTES CERV NA GDM',
                           eng_desc='Does it has 4 or more facings of beer in a GDM Femsa',
                           pop_filter=((scif[Fd.CAT_FK] == Ca.C_CERVEJA) &
                                       (scif[Fd.T_GROUP].isin(template_group_list2)) &
                                       (scif[Fd.MAN_FK] == Ma.M_FEMSA)),
                           target=4)

    # @trace
    def run_june_2016_cpq(self):
        scif = self.scif
        # Femsa ActivationProducts (11) - Minimal Total Distribution
        self.check_distinct_product_count(fact_name='trabalha_com_agua_kof',
                                          fact_desc='TRABALHA COM AGUA KOF?',
                                          pop_filter=((scif[Fd.CAT_FK] == Ca.C_AGUA) &
                                                      (scif[Fd.DIST_SC] == 1) &
                                                      (scif[Fd.MAN_FK] == Ma.M_FEMSA)))

        # Femsa ActivationProducts (12) - Minimal Total Distribution
        self.check_distinct_product_count(fact_name='trabalha_com_cha_kof',
                                          fact_desc='TRABALHA COM CHA KOF?',
                                          pop_filter=((scif[Fd.CAT_FK] == Ca.C_CHA) &
                                                      (scif[Fd.DIST_SC] == 1) &
                                                      (scif[Fd.MAN_FK] == Ma.M_FEMSA)))

        # Femsa ActivationProducts (13) - Minimal Total Distribution
        self.check_distinct_product_count(fact_name='trabalha_com_refrig_kof',
                                          fact_desc='TRABALHA COM REFRIG KOF?',
                                          pop_filter=((scif[Fd.CAT_FK] == Ca.C_CSD) &
                                                      (scif[Fd.DIST_SC] == 1) &
                                                      (scif[Fd.MAN_FK] == Ma.M_FEMSA)))

    # @trace
    def run_jan_2016_calculation(self):
        pass
