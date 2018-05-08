# -*- coding: utf-8 -*-
from datetime import date

from Trax.Algo.Calculations.Core.Constants import Fields as Fd
from Trax.Algo.Calculations.Core.Utils import Predicates as Pred
from Trax.Algo.Calculations.FEMSA.Constants import Templates as Tm, \
    PossibleAnswers as Po, Products as Pr
from Trax.Algo.Calculations.FEMSA.NewConstants import StoreTypes as St
from Trax.Algo.Calculations.FEMSA.NewConstants import Manufacturers as Ma
from Trax.Algo.Calculations.FEMSA.NewConstants import Categories as Ca
# from Trax.Algo.Calculations.FEMSA.NewConstants import Brands as Br
from Trax.Algo.Calculations.FEMSA.NewConstants import TemplateGroups as Tg
from Trax.Algo.Calculations.FEMSA.Sets.Base import FEMSACalculationsGroup
from Trax.Utils.Logging.Logger import Log


class PontosExtraCalculations(FEMSACalculationsGroup):
    def run_calculations(self):
        Log.info('Starting Pontos Extra calculation for session_pk: {}.'.format(self.session_pk))
        if self.session_info.visit_date >= date(2015, 5, 1):
            self.run_june_2016_calculations()
        else:
            Log.info('No Pontos Extra calculations were executed.')

    def run_june_2016_calculations(self):
        store_type = self.session_info.store_type

        if store_type == St.ST_CONVENIENCIA:
            self.run_june_2016_conveniencia()

        if store_type == St.ST_ATACADISTA:
            self.run_june_2016_atacadista()

        if store_type == St.ST_AS5:
            self.run_june_2016_as5()

        if store_type == St.ST_AS14:
            self.run_june_2016_as14()

        if store_type == St.ST_CPQ:
            self.run_june_2016_cpq()

        if store_type == St.ST_MERCEARIA:
            self.run_june_2016_mercearia()

        if store_type == St.ST_PADARIA:
            self.run_june_2016_padaria()

    def run_june_2016_as5(self):
        scif = self.scif
        # Femsa PontosExtra (5) - Count SKU
        self.ha_pe_csd_20_frentes_precif()

        # Femsa PontosExtra (13) - Count SKU
        temp_name_group = [Tm.TN_EXIBICAO_ACOU, Tm.TN_EXIBICAO_NP_ACOU, Tm.TN_PG_ACOU, Tm.TN_PG_NP_ACOU,
                           Tm.TN_ILHA_NP_ACOU, Tm.TN_ILHA_ACOU]
        size_group = [Pr.S_1, Pr.S_1_25, Pr.S_1_45, Pr.S_1_50, Pr.S_2, Pr.S_2_25, Pr.S_2_50, Pr.S_3, Pr.S_3_3,
                      Pr.S_3_50, Pr.S_4, Pr.S_5, Pr.S_6, Pr.S_6_25, Pr.S_6_30, Pr.S_9, Pr.S_1500, Pr.S_1000,
                      Pr.S_2000, Pr.S_2250]

        fact = 'PE CSD CONS FUT OU MPACK CONS IMED ACOU (ATIV 40 FRENTES)'
        description = 'pe csd cons fut ou mpack cons imed acou )ativ 40 frentes)'
        eng_desc = 'Has at least 40 facings of CSD future consume, or packs for immediate consume, ' \
                   'in the buchery section, out of an Cooler '
        calc1 = self.check_facings_for_operator(pop_filter=((scif[Fd.CAT_FK] == Ca.C_CSD) &
                                                            (scif[Fd.T_GROUP].isin(temp_name_group)) &
                                                            (scif[Fd.MAN_FK] == Ma.M_FEMSA)),
                                                target=40,
                                                predicate=Pred.GREATER_THAN_EQUAL)

        calc2 = self.check_facings_for_operator(pop_filter=((scif[Fd.SIZE].isin(size_group)) &
                                                            (scif[Fd.CAT_FK] == Ca.C_CSD) &
                                                            (scif[Fd.MAN_FK] == Ma.M_FEMSA) &
                                                            (scif[Fd.T_GROUP].isin(temp_name_group))),
                                                target=40,
                                                predicate=Pred.GREATER_THAN_EQUAL)
        self.add_or_fact([calc1, calc2], description, fact, eng_desc)

        # Femsa A PontosExtra (14) - Survey Question
        self.add_single_survey_response(fact_name='pe_csd_cons_fut_ou_mpack_cons_imed_acou_(equip_kof_70%)',
                                        fact_desc='PE CSD CONS FUT OU MPACK CONS IMED ACOU (EQUIP KOF 70%)',
                                        eng_desc='Extra Point in the buchery, KOF cooler with at least 70% '
                                                 'filled with KOF CSD for future consume, or multipack'
                                                 ' for immediate consume',
                                        question="PE CSD CONS FUT OU MPACK CONS IMED ACOU (EQUIP KOF 70%)",
                                        expected_answer=Po.YES_ANSWER)

        # Femsa A PontosExtra (15) - Survey Question
        self.add_single_survey_response(fact_name='pe_csd_cons_fut_ou_mpack_cons_imed_acou_(gdm_kof_80%)',
                                        fact_desc='PE CSD CONS FUT OU MPACK CONS IMED ACOU (GDM KOF 80%)',
                                        eng_desc='Coca-cola cooler with at least 80% filled CSD or multipack '
                                                 'for immediate consume in the buchery',
                                        question="PE CSD CONS FUT OU MPACK CONS IMED ACOU (GDM KOF 80%)",
                                        expected_answer=Po.YES_ANSWER)

        # Femsa PontosExtra (16) - Count SKU
        temp_name_group2 = [Tm.TN_PG_SEC_BEB, Tm.TN_PG_NP_SEC_BEB]
        size_group1 = [Pr.S_1, Pr.S_1_25, Pr.S_1_45, Pr.S_1_50, Pr.S_2, Pr.S_2_25, Pr.S_2_50, Pr.S_3, Pr.S_3_50,
                       Pr.S_4, Pr.S_5, Pr.S_6, Pr.S_6_25, Pr.S_6_30, Pr.S_9, Pr.S_1000, Pr.S_2000, Pr.S_2250]
        fact = 'PE CSD CONS FUT OU MPACK PG SB 40 FRENTES?'
        description = 'pe csd cons fut ou mpack pg sb 40 frentes'
        eng_desc = 'Extra Point of CSD for future consume or multipack 40 facings not on kof cooler'
        calc1 = self.check_facings_for_operator(pop_filter=((scif[Fd.SIZE].isin(size_group1)) &
                                                            (scif[Fd.CAT_FK] == Ca.C_CSD) &
                                                            (scif[Fd.NUMBER_OF_SUB_PACKAGES] >= 2) &
                                                            (scif[Fd.T_GROUP].isin(temp_name_group2))),
                                                target=40,
                                                predicate=Pred.GREATER_THAN_EQUAL)

        calc2 = self.check_facings_for_operator(pop_filter=((scif[Fd.CAT_FK] == Ca.C_CSD) &
                                                            (scif[Fd.T_GROUP].isin(temp_name_group2))),
                                                target=40,
                                                predicate=Pred.GREATER_THAN_EQUAL)
        self.add_or_fact([calc1, calc2], description, fact, eng_desc)

        # Femsa A PontosExtra (17) - Survey Question
        self.add_single_survey_response(fact_name='pe_csd_con_ fut_ou_mpack_pg_sb_(equip_kof_70%)',
                                        fact_desc='PE CSD CONS FUT OU MPACK PG SB (EQUIP KOF 70%)',
                                        eng_desc='Extra Point of CSD for future consume or multipack in cooler cc'
                                                 ' or gondola point in the beverages section, 70% filled',
                                        question="PE CSD CONS FUT OU MPACK CONS IMED ACOU (GDM KOF 80%)",
                                        expected_answer=Po.YES_ANSWER)

        # Femsa A PontosExtra (18) - Survey Question
        self.add_single_survey_response(fact_name='pe_csd_cons_fut_ou_mpack_pg_sb_(gdm_kof_80%)',
                                        fact_desc='PE CSD CONS FUT OU MPACK PG SB (GDM KOF 80%)',
                                        eng_desc='Extra Point of CSD for future consume or multipack in cooler cc or '
                                                 'gondola point in the beverages section, 80% filled',
                                        question="PE CSD CONS FUT OU MPACK PG SB (GDM KOF 80%)",
                                        expected_answer=Po.YES_ANSWER)

        # Femsa PontosExtra (19) - Count SKU
        temp_name_group3 = [Tm.TN_EXIBICAO_BISC, Tm.TN_ILHA_BISC, Tm.TN_EXIBICAO_NP_BISC, Tm.TN_ILHA_NP_BISC,
                            Tm.TN_PG_BISC, Tm.TN_PG_NP_BISC]
        fact = 'PE CSD E KAPO SEÇÃO DE BISC (CSD 30 FRENTES + KAPO 30 FRENTES)'
        description = 'pe_csd_e_kapo_secao_de_bisc_csd_30_frentes_kapo_30_frentes'
        eng_desc = 'Extra point of CSD and KAPO (juice brand) out of cooler in the cookies ' \
                   'section with 30 facings csd+30 facings kapo'
        calc1 = self.check_facings_for_operator(pop_filter=((scif[Fd.CAT_FK] == Ca.C_CSD) &
                                                            (scif[Fd.MAN_FK] == Ma.M_FEMSA) &
                                                            (scif[Fd.T_GROUP].isin(temp_name_group3))),
                                                target=30,
                                                predicate=Pred.GREATER_THAN_EQUAL)

        calc2 = self.check_facings_for_operator(pop_filter=((scif[Fd.CAT_FK] == Ca.C_CSD) &
                                                            (scif[Fd.B_NAME].isin([Pr.B_KAPO_SUCO,
                                                                                   Pr.B_KAPO_CHOCOLATE_LACTEO])) &
                                                            (scif[Fd.T_GROUP].isin(temp_name_group3))), target=30,
                                                predicate=Pred.GREATER_THAN_EQUAL)
        self.check_and([calc1, calc2], description, fact, eng_desc)

        # Femsa A PontosExtra (20) - Survey Question
        self.add_single_survey_response(fact_name='pe_csd_e_kapo_sec_biscoitos_equip_kof_70%(50%_de_csd_e_50%_kapo)',
                                        fact_desc='PE CSD E KAPO SEC BISCOITOS EQUIP KOF 70% (50% DE CSD E 50% KAPO)',
                                        eng_desc='Extra Point of CSD and KAPO (Juice brand) in KOF cooler in the'
                                                 'cookies section 70% filled (50% csd, 50% kapo)',
                                        question="PE CSD E KAPO SEC BISCOITOS EQUIP KOF 70% (50% DE CSD E 50% KAPO)",
                                        expected_answer=Po.YES_ANSWER)

        # Femsa A PontosExtra (21) - Survey Question
        self.add_single_survey_response(fact_name='pe_csd_e_kapo_sec_biscoitos_gdm_kof_80%(50%_de_csd_e_50%_kapo)',
                                        fact_desc='PE CSD E KAPO SEC BISCOITOS GDM KOF 80% (50% DE CSD E 50% KAPO)',
                                        eng_desc='KOF Cooler with at least 80% filled in the cookies'
                                                 ' section with 50% CSD and 50% Kapo',
                                        question="PE CSD E KAPO SEC BISCOITOS GDM KOF 80% (50% DE CSD E 50% KAPO)",
                                        expected_answer=Po.YES_ANSWER)

        # Femsa A PontosExtra (24) - Survey Question
        self.add_single_survey_response(fact_name='pe_mcateg_hort_equip_kof_70%(50%_de_csd_e_50%_ncb)',
                                        fact_desc='PE MCATEG HORT EQUIP KOF 70% (50% DE CSD E 50% NCB)',
                                        eng_desc='Extra Point Multicategorie in the grocery section Cooler'
                                                 ' KOF w70% filled (50% CSD, 50% NCARB)',
                                        question="PE MCATEG HORT EQUIP KOF 70% (50% DE CSD E 50% NCB)",
                                        expected_answer=Po.YES_ANSWER)

        # # Femsa PontosExtra (25) - Count SKU
        temp_name_group3 = [Tm.TN_EXIBICAO_HORT, Tm.TN_ILHA_HORT, Tm.TN_EXIBICAO_NP_HORT, Tm.TN_Ilha_NP_HORT,
                            Tm.TN_PG_HORT, Tm.TN_PG_NP_HORT]
        size_group2 = [Pr.S_1, Pr.S_1_25, Pr.S_1_45, Pr.S_1_50, Pr.S_2, Pr.S_2_25, Pr.S_2_50, Pr.S_3, Pr.S_3_3,
                       Pr.S_3_50, Pr.S_4, Pr.S_5, Pr.S_6, Pr.S_6_25, Pr.S_6_30, Pr.S_9, Pr.S_1000, Pr.S_2000, Pr.S_2250]
        cat_group = [Ca.C_AGUA, Ca.C_SUCO, Ca.C_CHA, Ca.C_ENERG, Ca.C_LACTEO, Ca.C_ISO, Ca.C_REFRES]
        fact = 'PE MCATEG HORT FORA EQUIP 30 FRENTES CSD + 30 FRENTES NCB'
        description = 'pe_mcateg_hort_fora_equip_30_frentes_csd_30_frentes_ncb'
        eng_desc = 'Extra Point mmulticategory in grocery section 30 facings of CSD future consume+3- ' \
                   'facings NCARB future consume'
        calc1 = self.check_facings_for_operator(pop_filter=((scif[Fd.SIZE].isin(size_group2)) &
                                                            (scif[Fd.CAT_FK] == Ca.C_CSD) &
                                                            (scif[Fd.MAN_FK] == Ma.M_FEMSA) &
                                                            (scif[Fd.T_GROUP].isin(temp_name_group3))),
                                                target=30,
                                                predicate=Pred.GREATER_THAN_EQUAL)

        calc2 = self.check_facings_for_operator(pop_filter=((scif[Fd.SIZE].isin(size_group2)) &
                                                            (scif[Fd.CAT_FK].isin(cat_group)) &
                                                            (scif[Fd.MAN_FK] == Ma.M_FEMSA) &
                                                            (scif[Fd.T_GROUP].isin(temp_name_group3))),
                                                target=30,
                                                predicate=Pred.GREATER_THAN_EQUAL)
        self.check_and([calc1, calc2], description, fact, eng_desc)

        # Femsa A PontosExtra (26) - Survey Question
        self.add_single_survey_response(fact_name='pe_mcateg_hort_gdm_kof_80%(50%_de_csd_e_50%_ncb)',
                                        fact_desc='PE MCATEG HORT GDM KOF 80% (50% DE CSD E 50% NCB)',
                                        eng_desc='KOF Cooler ate least 80% filled in the grocery '
                                                 'section with multicategory',
                                        question="PE MCATEG HORT GDM KOF 80% (50% DE CSD E 50% NCB)",
                                        expected_answer=Po.YES_ANSWER)

        # Femsa A PontosExtra (27) - Survey Question
        self.add_single_survey_response(fact_name='pe_mcateg_pad_equip_kof_70%(50%_de_csd_e_50%_ncb)',
                                        fact_desc='PE MCATEG PAD EQUIP KOF 70% (50% DE CSD E 50% NCB)',
                                        eng_desc='Extra Point Multicategory in the bakery cooler Kof 70% filled '
                                                 '(50% csd, 50% Ncarb)',
                                        question="PE MCATEG PAD EQUIP KOF 70% (50% DE CSD E 50% NCB)",
                                        expected_answer=Po.YES_ANSWER)

        # # Femsa PontosExtra (28) - Count SKU
        temp_name_group4 = [Tm.TN_EXIBICAO_PAD, Tm.TN_ILHA_PAD, Tm.TN_EXIBICAO_NP_PAD, Tm.TN_Ilha_NP_PAD,
                            Tm.TN_PG_PAD, Tm.TN_PG_NP_PAD]

        cat_group = [Ca.C_AGUA, Ca.C_SUCO, Ca.C_CHA, Ca.C_ENERG, Ca.C_LACTEO, Ca.C_ISO, Ca.C_REFRES]
        fact = 'PE MCATEG PAD FORA EQUIP 30 FRENTES CSD + 30 FRENTES NCB'
        description = 'pe_mcateg_pad_fora_equip_30_frentes_csd_30_frentes_ncb'
        eng_desc = 'Extra Point of Multicategory out of cooler Kof, with 30 facings ou CSD future ' \
                   'consume+30 facings ou Ncarb future consume'
        calc1 = self.check_facings_for_operator(pop_filter=(
            (scif[Fd.CAT_FK] == Ca.C_CSD) &
            (scif[Fd.MAN_FK] == Ma.M_FEMSA) &
            (scif[Fd.T_GROUP].isin(temp_name_group4))),
                                                target=30,
                                                predicate=Pred.GREATER_THAN_EQUAL)

        calc2 = self.check_facings_for_operator(pop_filter=((scif[Fd.CAT_FK].isin(cat_group)) &
                                                            (scif[Fd.MAN_FK] == Ma.M_FEMSA) &
                                                            (scif[Fd.T_GROUP].isin(temp_name_group4))),
                                                target=30,
                                                predicate=Pred.GREATER_THAN_EQUAL)
        self.check_and([calc1, calc2], description, fact, eng_desc)

        # Femsa A PontosExtra (29) - Survey Question
        self.add_single_survey_response(fact_name='pe_mcateg_pad_gdm_kof_80%(50%_de_csd_e_50%_ncb)',
                                        fact_desc='PE MCATEG PAD GDM KOF 80% (50% DE CSD E 50% NCB)',
                                        eng_desc='KOF Cooler of at least 80% abastecido '
                                                 '(50% csd and 50% NCARB) in the bakery section',
                                        question="PE MCATEG PAD GDM KOF 80% (50% DE CSD E 50% NCB)",
                                        expected_answer=Po.YES_ANSWER)

        # Femsa A PontosExtra (40) - Survey Question
        self.add_single_survey_response(fact_name='pe_cerv_acou',
                                        fact_desc='PE CERV ACOU',
                                        eng_desc='Is there an Extra point of Beer in the buchery section',
                                        question="PPE CERV ACOU",
                                        expected_answer=Po.YES_ANSWER)

        # Femsa A PontosExtra (41) - Survey Question
        self.add_single_survey_response(fact_name='poss._pg_sec_beb_nao_alcoolicos',
                                        fact_desc='POSS. PG SEC BEB NAO ALCOOLICOS',
                                        eng_desc='Is there possibility of having PG continuing to the non '
                                                 'alcoholic beverages section',
                                        question="PPOSS. PG SEC BEB NAO ALCOOLICOS",
                                        expected_answer=Po.YES_ANSWER)

    def run_june_2016_conveniencia(self):
        scif = self.scif
        # Femsa A PontosExtra (2) - Minimal Total Distribution
        target_name_cko = [Tm.TN_GDM_KOF_CSD_P_CKO, Tm.TN_GDM_KOF_NCB_NP_CKO, Tm.TN_GDM_KOF_NCB_P_CKO,
                           Tm.TN_GDM_KOF_CSD_NP_CKO]
        self.check_distinct_product_count(fact_name='per_equio_prox_ao_caixa_csdncat',
                                          fact_desc='PER EQUIP PROX AO CAIXA CSDNCAT',
                                          eng_desc='Has refrigerator of CSD ou non carbonated next to cashier',
                                          pop_filter=((scif[Fd.T_NAME].isin(target_name_cko)) &
                                                      (scif[Fd.DIST_SC] == 1) &
                                                      (scif[Fd.MAN_FK] == Ma.M_FEMSA)))

        # Femsa A PontosExtra (3) - Minimal Total Distribution
        target_name_serv = [Tm.TN_GDM_KOF_CSD_P_F_SERV, Tm.TN_GDM_KOF_NCB_NP_F_SERV, Tm.TN_GDM_KOF_NCB_P_F_SERV,
                            Tm.TN_GDM_KOF_CSD_NP_F_SERV]
        self.check_distinct_product_count(fact_name='per_equio_prox_ao_food_service_csdncat',
                                          fact_desc='PER EQUIP PROX AO FOOD SERVICE CSDNCAT',
                                          eng_desc='Has refrigerator of CSD ou non carbonated next to food service',
                                          pop_filter=((scif[Fd.T_NAME].isin(target_name_serv)) &
                                                      (scif[Fd.DIST_SC] == 1) &
                                                      (scif[Fd.MAN_FK] == Ma.M_FEMSA)))

        # Femsa PontosExtra (30) - Survey Question
        self.pe_mpack_cerv()

        # Femsa PontosExtra (9) - Minimal Total Distribution
        self.pe_cervejz()

        # Femsa PontosExtra (43) - Count Tagged
        self.perpeburn()

        # Femsa PontosExtra (44) - Count Tagged
        template_group_list1 = [Tg.TG_EXIBICAO, Tg.TG_PG, Tg.TG_ILHA, Tg.TG_RACK_KOF_NCB, Tg.TG_RACK_KOF_CERV,
                                Tg.TG_RACK_CONC, Tg.TG_RACK_KOF_CSD, Tg.TG_GDM_CONC_PROPRIO, Tg.TG_GDM_KOF_CERV,
                                Tg.TG_GDM_KOF_CSD, Tg.TG_GDM_KOF_NCB]
        cat_group1 = [Ca.C_CHA, Ca.C_AGUA, Ca.C_ENERG, Ca.C_SUCO, Ca.C_ISO, Ca.C_REFRES, Ca.C_LACTEO]
        size_group4 = [Pr.S_1, Pr.S_1_45, Pr.S_1_50, Pr.S_2, Pr.S_2_25, Pr.S_2_50, Pr.S_3, Pr.S_3_50,
                       Pr.S_1500, Pr.S_2000]
        scif = self.scif
        fact = 'PERPECONSFUTUROCSDNCAT'
        description = 'perpeconsfuturocsdmncat'
        eng_desc = 'Has Extra Point of CSD or NCARB. For future consume (packages bigger than 600 ml)',
        calc1 = self.check_tagged_for_operator(pop_filter=((scif[Fd.CAT_FK].isin(cat_group1)) &
                                                           (scif[Fd.SIZE].isin(size_group4)) &
                                                           (scif[Fd.T_GROUP].isin(template_group_list1)) &
                                                           (scif[Fd.MAN_FK] == Ma.M_FEMSA)),
                                               target=4)

        calc2 = self.check_tagged_for_operator(pop_filter=((scif[Fd.CAT_FK] == Ca.C_CSD) &
                                                           (scif[Fd.T_GROUP].isin(template_group_list1)) &
                                                           (scif[Fd.MAN_FK] == Ma.M_FEMSA)),
                                               target=4)
        self.add_or_fact([calc1, calc2], description, fact, eng_desc)

        size_group5 = [Pr.S_200, Pr.S_250, Pr.S_255, Pr.S_269, Pr.S_350, Pr.S_300, Pr.S_600, Pr.S_473,
                       Pr.S_500, Pr.S_355, Pr.S_450, Pr.S_343]

        # Femsa PontosExtra (45) - Count Tagged
        cat_group2 = [Ca.C_CHA, Ca.C_AGUA, Ca.C_CSD, Ca.C_SUCO, Ca.C_ISO, Ca.C_REFRES, Ca.C_LACTEO]
        self.check_tagged(fact_name='PERPECONSIMEDIATOEXCETOBURNECERV',
                          fact_desc='perpeconsimediatoexcetoburnecerv',
                          eng_desc='Has Extra Point of CSD or NCARB. For immediate consumer '
                                   '(packages  equal ou smaller of 600ml)',
                          pop_filter=((scif[Fd.CAT_FK].isin(cat_group2)) &
                                      (scif[Fd.SIZE].isin(size_group5)) &
                                      (scif[Fd.T_GROUP].isin(template_group_list1)) &
                                      (scif[Fd.MAN_FK] == Ma.M_FEMSA)),
                          target=4)

        # Femsa PontosExtra (46) - Count Tagged
        self.check_tagged(fact_name='PERPECONSIMEDIATONCAT',
                          fact_desc='perpeconsimediatoncat',
                          eng_desc='Has Extra Point of  NCARB. For immediate consumer '
                                   '(packages  equal ou smaller of 600ml)',
                          pop_filter=((scif[Fd.CAT_FK].isin(cat_group1)) &
                                      (scif[Fd.SIZE].isin(size_group5)) &
                                      (scif[Fd.T_GROUP].isin(template_group_list1)) &
                                      (scif[Fd.MAN_FK] == Ma.M_FEMSA)),
                          target=4)

        # Femsa PontosExtra (47) - Count Tagged
        cat_group3 = [Ca.C_CHA, Ca.C_AGUA, Ca.C_CSD, Ca.C_SUCO, Ca.C_ISO, Ca.C_REFRES, Ca.C_LACTEO, Ca.C_ENERG]
        self.check_tagged(fact_name='PERPEKOFCSDNCAT',
                          fact_desc='perpekofcsdncat',
                          eng_desc='Has Extra Point of CSD or NCARB',
                          pop_filter=((scif[Fd.CAT_FK].isin(cat_group3)) &
                                      (scif[Fd.T_GROUP].isin(template_group_list1)) &
                                      (scif[Fd.MAN_FK] == Ma.M_FEMSA)),
                          target=4)
        # Femsa PontosExtra (48) - Count Tagged
        self.check_tagged(fact_name='PERPEKOFEXCETOBURNECERV',
                          fact_desc='perpekofexcetoburnecerv',
                          eng_desc='Has Extra Point of Femsa products, that arent Burn (energy drink) or Beer',
                          pop_filter=((scif[Fd.CAT_FK].isin(cat_group2)) &
                                      (scif[Fd.T_GROUP].isin(template_group_list1)) &
                                      (scif[Fd.MAN_FK] == Ma.M_FEMSA)),
                          target=4)

    def run_june_2016_atacadista(self):
        # Femsa A PontosExtra (1) - Survey Question
        self.add_single_survey_response(fact_name='csd_mpack_ckouts',
                                        fact_desc='CSD MPACK CKOUTS',
                                        eng_desc='Has multipack of CSDs next to checkout',
                                        question="CSD MPACK CKOUTS",
                                        expected_answer=Po.YES_ANSWER)
        # Femsa PontosExtra (30) - Survey Question
        self.pe_mpack_cerv()

        # Femsa A PontosExtra (31) - Survey Question
        self.add_single_survey_response(fact_name='pe_mpack_ncb_sb',
                                        fact_desc='PE MPACK NCB SB',
                                        eng_desc='Has extra point of NCARB in the beverages section',
                                        question="PE MPACK NCB SB",
                                        expected_answer=Po.YES_ANSWER)

        # Femsa A PontosExtra (32) - Survey Question
        self.add_single_survey_response(fact_name='pe_mpack_sabores_sb',
                                        fact_desc='PE MPACK SABORES SB',
                                        eng_desc='Has extra Point of Multipacks of CSD not Coca-cola '
                                                 'in the beverages section',
                                        question="PE MPACK SABORES SB",
                                        expected_answer=Po.YES_ANSWER)

        # Femsa A PontosExtra (33) - Survey Question
        self.add_single_survey_response(fact_name='csd_ncat_mpack_fora_sb',
                                        fact_desc='PE NCAT MPACK FORA SB',
                                        eng_desc='Has Extra Point of NCARB multipacks in the beverages section',
                                        question="PE NCAT MPACK FORA SB",
                                        expected_answer=Po.YES_ANSWER)

        # Femsa A PontosExtra (38) - Survey Question
        self.add_single_survey_response(fact_name='pg_mpack_ncb_sb',
                                        fact_desc='PG MPACK NCB SB',
                                        eng_desc='Has NCARB. Multipack stacked in bevereges section',
                                        question="PG MPACK NCB SB",
                                        expected_answer=Po.YES_ANSWER)

        # Femsa A PontosExtra (39) - Survey Question
        self.add_single_survey_response(fact_name='pg_mpack_sabores_sb',
                                        fact_desc='PG MPACK SABORES SB',
                                        eng_desc='Has Gondola Spot of Multipacks of CSDs non Coca-Cola '
                                                 'in the beverages section',
                                        question="PG MPACK SABORES SB",
                                        expected_answer=Po.YES_ANSWER)

        # Femsa PontosExtra (4) - Count SKU
        self.ha_minimo_20_frentes_agua_crystal()
        # Femsa PontosExtra (5) - Count SKU
        self.ha_pe_csd_20_frentes_precif()
        # Femsa PontosExtra (6) - Count SKU
        self.ha_pe_ncb_20_frentes_precif()

    def run_june_2016_cpq(self):
        # Femsa A PontosExtra (9) - Minimal Total Distribution
        self.pe_cervejz()

    def run_june_2016_as14(self):
        # Femsa A PontosExtra (34) - Survey Question
        self.add_single_survey_response(fact_name='per+pack_csd_ret_futuro_entrada_loja_ou_ate_a_1ª_pg_50%_abast',
                                        fact_desc='PER RACK CSD RET FUTURO ENTRADA LOJA OU ATE A 1ª PG - 50% ABAST',
                                        eng_desc=None,
                                        question="RACK CSD RET FUTURO ENTRADA LOJA OU ATE A 1ª PG - 50% ABAST",
                                        expected_answer=Po.YES_ANSWER)

        # Femsa PontosExtra (4) - Count SKU
        self.ha_minimo_20_frentes_agua_crystal()
        # Femsa PontosExtra (5) - Count SKU
        self.ha_pe_csd_20_frentes_precif()
        # Femsa PontosExtra (6) - Count SKU
        self.ha_pe_ncb_20_frentes_precif()
        # Femsa A PontosExtra (7) - Survey Question
        self.ha_presencia_de_tubaina_no_pdv()
        # Femsa PontosExtra (30) - Survey Question
        self.pe_mpack_cerv()
        # Femsa PontosExtra (35) - Minimal Total Distribution
        self.pe_sucos_ativ_30_frentes()
        # Femsa PontosExtra (36) - Survey Question
        self.pe_sucos_equip_kof_70()
        # Femsa PontosExtra (37) - Survey Question
        self.pe_sucos_gdm_kof_80()

    def run_june_2016_mercearia(self):
        # Femsa A PontosExtra (7) - Survey Question
        self.ha_presencia_de_tubaina_no_pdv()

    def run_june_2016_padaria(self):
        # Femsa PontosExtra (35) - Minimal Total Distribution
        self.pe_sucos_ativ_30_frentes()
        # Femsa PontosExtra (36) - Survey Question
        self.pe_sucos_equip_kof_70()
        # Femsa PontosExtra (37) - Survey Question
        self.pe_sucos_gdm_kof_80()
        # Femsa A PontosExtra (7) - Survey Question
        self.ha_presencia_de_tubaina_no_pdv()

    def ha_minimo_20_frentes_agua_crystal(self):
        # Femsa PontosExtra (4) - Count SKU
        scif = self.scif
        self.check_facings(fact_name='ha_minimo_20_frentes_agua_crystal',
                           fact_desc='HA MINIMO 20 FRENTES AGUA CRYSTAL',
                           eng_desc=None,
                           pop_filter=(scif[Fd.B_NAME] == Pr.B_CRYSTAL_AGUA),  # TODO: Ortal
                           target=20)

    def ha_pe_csd_20_frentes_precif(self):
        # Femsa PontosExtra (5) - Count SKU
        scif = self.scif
        temp_name_group = [Tg.TG_EXIBICAO, Tg.TG_ILHA, Tg.TG_RACK_CONC, Tg.TG_RACK_KOF_CERV, Tg.TG_RACK_KOF_CSD,
                           Tg.TG_RACK_KOF_NCB, Tg.TG_GDM_CONC_PROPRIO, Tg.TG_GDM_KOF_CERV,
                           Tg.TG_GDM_KOF_CSD, Tg.TG_GDM_KOF_NCB]
        self.check_facings(fact_name='ha_pe_csd_20_frentes_precif',
                           fact_desc='HA PE CSD 20 FRENTES PRECIF',
                           eng_desc=None,
                           pop_filter=((scif[Fd.CAT_FK] == Ca.C_CSD) &
                                       (scif[Fd.T_GROUP].isin(temp_name_group)) &
                                       (scif[Fd.MAN_FK] == Ma.M_FEMSA)),
                           target=20)

    def ha_pe_ncb_20_frentes_precif(self):
        # Femsa PontosExtra (6) - Count SKU
        scif = self.scif
        temp_name_group = [Tg.TG_EXIBICAO, Tg.TG_ILHA, Tg.TG_RACK_CONC, Tg.TG_RACK_KOF_CERV, Tg.TG_RACK_KOF_CSD,
                           Tg.TG_RACK_KOF_NCB, Tg.TG_GDM_CONC_PROPRIO, Tg.TG_GDM_KOF_CERV,
                           Tg.TG_GDM_KOF_CSD, Tg.TG_GDM_KOF_NCB]
        cat_group = [Ca.C_AGUA, Ca.C_CHA, Ca.C_ENERG, Ca.C_ISO, Ca.C_LACTEO, Ca.C_REFRES, Ca.C_SUCO]
        self.check_facings(fact_name='ha_pe_ncb_20_frentes_precif',
                           fact_desc='HA PE NCB 20 FRENTES PRECIF',
                           eng_desc=None,
                           pop_filter=((scif[Fd.CAT_FK].isin(cat_group)) &
                                       (scif[Fd.T_GROUP].isin(temp_name_group)) &
                                       (scif[Fd.MAN_FK] == Ma.M_FEMSA)),
                           target=20)

    def ha_presencia_de_tubaina_no_pdv(self):
        # Femsa PontosExtra (7) - Survey Question
        self.add_single_survey_response(fact_name='ha_presencia_de_tubaina_no_pdv',
                                        fact_desc='HÁ PRESENÇA DE TUBAINA NO PDV?',
                                        eng_desc='Is there Itubaina drink at the store',
                                        question="HÁ PRESENÇA DE TUBAINA NO PDV?",
                                        expected_answer=Po.YES_ANSWER)

    def pe_mpack_cerv(self):
        # Femsa PontosExtra (30) - Survey Question
        self.add_single_survey_response(fact_name='pe_mpack_cerv',
                                        fact_desc='PE MPACK CERV',
                                        eng_desc='Has Extra point of Beer Multipacks',
                                        question="PE MPACK CERV",
                                        expected_answer=Po.YES_ANSWER)

    def pe_cervejz(self):
        # Femsa PontosExtra (9) - Minimal Total Distribution
        scif = self.scif
        template_group = [Tg.TG_EXIBICAO, Tg.TG_ILHA, Tg.TG_PG, Tg.TG_RACK_CONC, Tg.TG_RACK_KOF_CSD, Tg.TG_RACK_KOF_NCB,
                          Tg.TG_GDM_CONC_PROPRIO, Tg.TG_GDM_KOF_CERV, Tg.TG_GDM_KOF_CSD, Tg.TG_GDM_KOF_NCB]

        self.check_distinct_product_count(fact_name='per_equip_prox_ao_food_service_csdncat',
                                          fact_desc='PER EQUIP PROX AO FOOD SERVICE CSDNCAT',
                                          pop_filter=((scif[Fd.T_NAME].isin(template_group)) &
                                                      (scif[Fd.CAT_FK] == Ca.C_CERVEJA) &
                                                      (scif[Fd.DIST_SC] == 1) &
                                                      (scif[Fd.MAN_FK] == Ma.M_FEMSA)))

    def pha_pe_ncb_20_frentes_precif(self):
        # Femsa PontosExtra (6) - Count SKU
        scif = self.scif
        temp_name_group = [Tg.TG_EXIBICAO, Tg.TG_ILHA, Tg.TG_PG, Tm.TN_EXIBICAO_NP, Tm.TN_Ilha_NP, Tm.TN_PG_NP]
        self.check_facings(fact_name='ha_pe_ncb_20_frentes_precif',
                           fact_desc='HA PE NCB 20 FRENTES PRECIF',
                           eng_desc=None,
                           pop_filter=((scif[Fd.CAT_FK] == Ca.C_SUCO) &
                                       (scif[Fd.T_GROUP].isin(temp_name_group)) &
                                       (scif[Fd.MAN_FK] == Ma.M_FEMSA)),
                           target=30)

    def pe_sucos_ativ_30_frentes(self):
        # Femsa PontosExtra (35) - Minimal Total Distribution
        scif = self.scif
        template_group = [Tg.TG_EXIBICAO, Tg.TG_ILHA, Tg.TG_PG, Tg.TG_PG, Tm.TN_EXIBICAO_NP, Tm.TN_Ilha_NP]

        self.check_distinct_product_count(fact_name='pe_sucos_ativ_30_frentes',
                                          fact_desc='PE SUCOS ATIV 30 FRENTES',
                                          eng_desc='Extra Point of juices not in cooler '
                                                   'kof with at least 30 facings of jucie',
                                          pop_filter=((scif[Fd.T_NAME].isin(template_group)) &
                                                      (scif[Fd.CAT_FK] == Ca.C_SUCO) &
                                                      (scif[Fd.DIST_SC] == 1) &
                                                      (scif[Fd.MAN_FK] == Ma.M_FEMSA)),
                                          target=30)

    def pe_sucos_equip_kof_70(self):
        # Femsa PontosExtra (36) - Survey Question
        self.add_single_survey_response(fact_name='pe_sucos_equip_kof_70%',
                                        fact_desc='PE SUCOS EQUIP KOF 70%',
                                        eng_desc='Extra Point of jusices in cooler KOF with 70% filled',
                                        question="PE SUCOS EQUIP KOF 70%",
                                        expected_answer=Po.YES_ANSWER)

    def pe_sucos_gdm_kof_80(self):
        # Femsa PontosExtra (37) - Survey Question
        self.add_single_survey_response(fact_name='pe_sucos_gdm_kof_80%',
                                        fact_desc='PERPESUCOSGDMKOF80%',
                                        eng_desc='Has Extra Point of Juice GDM Kof 80% full',
                                        question="PE SUCOS GDM KOF 80%",
                                        expected_answer=Po.YES_ANSWER)

    # Femsa PontosExtra (43) - Count Tagged

    def perpeburn(self):
        template_group_list1 = [Tg.TG_EXIBICAO, Tg.TG_PG, Tg.TG_ILHA, Tg.TG_RACK_KOF_NCB, Tg.TG_RACK_KOF_CERV,
                                Tg.TG_RACK_CONC, Tg.TG_RACK_KOF_CSD, Tg.TG_GDM_CONC_PROPRIO, Tg.TG_GDM_KOF_CERV,
                                Tg.TG_GDM_KOF_CSD, Tg.TG_GDM_KOF_NCB]
        scif = self.scif
        self.check_tagged(fact_name='PERPEBURN',
                          fact_desc='perpeburn',
                          eng_desc='Has Extra point of Burn (energy drink)',
                          pop_filter=((scif[Fd.CAT_FK] == Ca.C_ENERG) &
                                      (scif[Fd.B_NAME] == Pr.B_BURN) &
                                      (scif[Fd.T_GROUP].isin(template_group_list1)) &
                                      (scif[Fd.MAN_FK] == Ma.M_FEMSA)),
                          target=4)

if __name__ == '__main__':
    members = [attr for attr in dir(Ca()) if not callable(attr) and not attr.startswith("__")]
    d = Ca.__dict__
    for member in members:
        print ("Name: " + member)
        print ("Value: " + str(d[member]))
        pass
