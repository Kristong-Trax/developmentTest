# -*- coding: utf-8 -*-
from datetime import date

from Trax.Algo.Calculations.Core.Constants import Fields as Fd
from Trax.Algo.Calculations.FEMSA.Constants import PossibleAnswers as Pa
from Trax.Algo.Calculations.FEMSA.NewConstants import StoreTypes as St
from Trax.Algo.Calculations.FEMSA.Constants import Templates as Tm
from Trax.Algo.Calculations.FEMSA.NewConstants import Manufacturers as Ma
from Trax.Algo.Calculations.FEMSA.NewConstants import Categories as Ca
from Trax.Algo.Calculations.FEMSA.NewConstants import TemplateGroups as Tg
from Trax.Algo.Calculations.FEMSA.Sets.Base import FEMSACalculationsGroup
from Trax.Utils.Logging.Logger import Log


class MaterialAtivacaoCalculations(FEMSACalculationsGroup):
    def run_calculations(self):
        Log.info('Starting Materiais de Ativacao calculation for session_pk: {}.'.format(self.session_pk))
        if self.visit_date >= date(2016, 6, 1):
            self.run_june_2016_calculations()
        else:
            Log.info('No Materiais de Ativacao calculations were executed.')

    def run_june_2016_calculations(self):
        store_type = self.session_info.store_type

        if store_type in [St.ST_MERCEARIA, St.ST_AS14]:
            self.run_june_2016_group1()

        if store_type in [St.ST_AS14, St.ST_ADEGA]:
            self.run_june_2016_as14()

        if store_type == St.ST_RESTAURANTE:
            self.run_june_2016_restaurante()

        if store_type == St.ST_CPQ:
            self.run_june_2016_cpq()

        if store_type == St.ST_BARLAN:
            self.run_june_2016_barlan()

        if store_type == St.ST_CONVENIENCIA:
            self.run_june_2016_conveniencia()

        if store_type == St.ST_AS5:
            self.run_june_2016_as5()

        if store_type == St.ST_ATACADISTA:
            self.run_june_2016_atacadista()

        if store_type == St.ST_PADARIA:
            self.run_june_2016_padaria()

    def run_june_2016_as5(self):
        # Femsa Materiais de Ativacao  (20) - Survey Question
        self.ha_material_de_preco_positivo()

        # Femsa Materiais de Ativacao  (21) - Survey Question
        self.ha_material_permanente()

        # Femsa Materiais de Ativacao  (22) - Survey Question
        self.ha_material_temporario()

        # Femsa Materiais de Ativacao  (23) - Survey Question
        self.ha_producto_precificado()

        # Femsa Materiais de Ativacao  (26) - Survey Question
        self.ha_vent_burn_abastec()
        # Femsa Materiais de Ativacao  (29) - Survey Question
        self.ha_material_das_olimpiadas()

    def run_june_2016_as14(self):
        # Femsa Materiais de Ativacao  (20) - Survey Question
        self.ha_material_de_preco_positivo()

    def run_june_2016_cpq(self):
        # Femsa Materiais de Ativacao  (20) - Survey Question
        self.ha_material_de_preco_positivo()

        # Femsa Materiais de Ativacao (2) - Survey Question
        self.add_single_survey_response(fact_name='ha_imagem_de_prod_kof',
                                        fact_desc='HA IMAGEM DE PROD KOF?',
                                        question="HA IMAGEM DE PROD KOF?",
                                        expected_answer=Pa.YES_ANSWER)
        # Femsa Materiais de Ativacao  (23) - Survey Question
        self.ha_producto_precificado()

        # Femsa Materiais de Ativacao  (29) - Survey Question
        self.ha_material_das_olimpiadas()

    def run_june_2016_conveniencia(self):
        # Femsa Materiais de Ativacao (1) - Survey Question
        self.add_single_survey_response(fact_name='ha_material_de_csd_ou_ncb_no_balco_mesa',
                                        fact_desc='HÁ MATERIAL DE CSD OU NCB NO BALCÃO/MESA',
                                        question="HÁ MATERIAL DE CSD OU NCB NO BALCÃO/MESA",
                                        expected_answer=Pa.YES_ANSWER)

        # Femsa Materiais de Ativacao (7) - Survey Question
        self.add_single_survey_response(fact_name='ativ_csd',
                                        fact_desc='ATIV CSD',
                                        eng_desc='Has CSD POS Material',
                                        question="ATIV CSD",
                                        expected_answer=Pa.YES_ANSWER)

        # Femsa Materiais de Ativacao (15) - Survey Question
        self.add_single_survey_response(fact_name='ativ_ncb',
                                        fact_desc='ATIV NCB',
                                        eng_desc='Has POS Material of Non cabonated beverages',
                                        question="ATIV NCB",
                                        expected_answer=Pa.YES_ANSWER)

        # Femsa Materiais de Ativacao  (18) - Survey Question
        self.ha_attivacao_interna_csd_ou_ncb()

        # Femsa Materiais de Ativacao  (21) - Survey Question
        self.ha_material_permanente()

        # Femsa Materiais de Ativacao  (20) - Survey Question
        self.ha_material_de_preco_positivo()
        # Femsa Materiais de Ativacao  (22) - Survey Question
        self.ha_material_temporario()

        # Femsa Materiais de Ativacao  (23) - Survey Question
        self.ha_producto_precificado()

        # Femsa Materiais de Ativacao  (26) - Survey Question
        self.ha_vent_burn_abastec()

        # Femsa Materiais de Ativacao  (29) - Survey Question
        self.ha_material_das_olimpiadas()

    def run_june_2016_barlan(self):
        # Femsa Materiais de Ativacao  (10) - Survey Question
        self.add_single_survey_response(fact_name='ativ_csd_cons_imed_ext_entrada_lj_tam_min_prod_preco',
                                        fact_desc='ATIV CSD CONS IMED EXT/ENTRADA LJ TAM MIN PROD+PREÇO',
                                        eng_desc=None,
                                        question="ATIV CSD CONS IMED EXT/ENTRADA LJ TAM MIN PROD+PREÇO",
                                        expected_answer=Pa.YES_ANSWER)

        # Femsa Materiais de Ativacao  (19) - Survey Question
        self.ha_balde_de_cerveja()
        # Femsa Materiais de Ativacao (24) - Survey Question
        self.per_ha_quadro_ou_cartaz_de_kai_bav_amstel()
        # Femsa Materiais de Ativacao (25) - Survey Question
        self.per_ha_quadro_ou_culminoso_hnk()
        # Femsa Materiais de Ativacao  (20) - Survey Question
        self.ha_material_de_preco_positivo()
        # Femsa Materiais de Ativacao  (21) - Survey Question
        self.ha_material_permanente()
        # Femsa Materiais de Ativacao  (22) - Survey Question
        self.ha_material_temporario()
        # Femsa Materiais de Ativacao  (23) - Survey Question
        self.ha_producto_precificado()
        # Femsa Materiais de Ativacao  (26) - Survey Question
        self.ha_vent_burn_abastec()
        # Femsa Materiais de Ativacao  (29) - Survey Question
        self.ha_material_das_olimpiadas()

    def run_june_2016_restaurante(self):
        # Femsa Materiais de Ativacao  (19) - Survey Question
        self.ha_balde_de_cerveja()
        # Femsa Materiais de Ativacao (16) - Survey Question
        self.possui_cervejela()
        # Femsa Materiais de Ativacao (24) - Survey Question
        self.per_ha_quadro_ou_cartaz_de_kai_bav_amstel()
        # Femsa Materiais de Ativacao (25) - Survey Question
        self.per_ha_quadro_ou_culminoso_hnk()
        # Femsa Materiais de Ativacao  (20) - Survey Question
        self.ha_material_de_preco_positivo()
        # Femsa Materiais de Ativacao  (21) - Survey Question
        self.ha_material_permanente()
        # Femsa Materiais de Ativacao  (22) - Survey Question
        self.ha_material_temporario()
        # Femsa Materiais de Ativacao  (23) - Survey Question
        self.ha_producto_precificado()
        # Femsa Materiais de Ativacao  (26) - Survey Question
        self.ha_vent_burn_abastec()

        # Femsa Materiais de Ativacao  (29) - Survey Question
        self.ha_material_das_olimpiadas()

    def run_june_2016_padaria(self):
        # Femsa Materiais de Ativacao  (4) - Survey Question
        self.add_single_survey_response(fact_name='ativ_balcao_ncb_tam_min_guardanudo',
                                        fact_desc='ATIV BALCÃO NCB TAM MIN GUARDANUDO',
                                        eng_desc=None,
                                        question="ATIV BALCÃO NCB TAM MIN GUARDANUDO",
                                        expected_answer=Pa.YES_ANSWER)
        # Femsa Materiais de Ativacao (24) - Survey Question
        self.per_ha_quadro_ou_cartaz_de_kai_bav_amstel()

        # Femsa Materiais de Ativacao  (11) - Survey Question
        self.ativ_csd_cons_ext_ou_ent_loja_tam_min_pop()

        # Femsa Materiais de Ativacao (25) - Survey Question
        self.per_ha_quadro_ou_culminoso_hnk()

        # Femsa Materiais de Ativacao  (17) - Survey Question
        self.ha_1_prod_kof_no_menuboard()
        # Femsa Materiais de Ativacao  (20) - Survey Question
        self.ha_material_de_preco_positivo()

        # Femsa Materiais de Ativacao  (21) - Survey Question
        self.ha_material_permanente()

        # Femsa Materiais de Ativacao  (23) - Survey Question
        self.ha_producto_precificado()

        # Femsa Materiais de Ativacao  (26) - Survey Question
        self.ha_vent_burn_abastec()

        # Femsa Materiais de Ativacao  (29) - Survey Question
        self.ha_material_das_olimpiadas()

    def run_june_2016_group1(self):

        # Femsa Materiais de Ativacao  (29) - Survey Question
        self.ha_material_das_olimpiadas()

        # Femsa Materiais de Ativacao  (26) - Survey Question
        self.ha_vent_burn_abastec()

        # Femsa Materiais de Ativacao  (22) - Survey Question
        self.ha_material_temporario()

        # Femsa Materiais de Ativacao  (21) - Survey Question
        self.ha_material_permanente()

        # Femsa Materiais de Ativacao  (18) - Survey Question
        self.ha_attivacao_interna_csd_ou_ncb()

        # Femsa Materiais de Ativacao  (11) - Survey Question
        self.ativ_csd_cons_ext_ou_ent_loja_tam_min_pop()

        # Femsa Materiais de Ativacao  (23) - Survey Question
        self.ha_producto_precificado()

        scif = self.scif
        # Femsa Materiais de Ativacao (27) - Count scenes
        self.check_scenes(fact_name='mat_pop_cerveja',
                          fact_desc='MAT POP CERVEJA',
                          eng_desc='Has Beer POS Material',
                          pop_filter=((scif[Fd.T_GROUP] == Tg.TG_MATERIAL) &
                                      (scif[Fd.CAT_FK] == Ca.C_CERVEJA) &
                                      (scif[Fd.P_TYPE] == Fd.P_TYPE_POS)),
                          target=1)

        # Femsa Materiais de Ativacao  (28) - Survey Question
        self.add_single_survey_response(fact_name='per_ativ_csd_ou_ncb_area_int_tam_min_prod_preco',
                                        fact_desc='PER ATIV CSD OU NCB AREA INT TAM MIN PROD+PREÇO',
                                        eng_desc=None,
                                        question="PER ATIV CSD OU NCB AREA INT TAM MIN PROD+PREÇO",
                                        expected_answer=Pa.YES_ANSWER)

        # Femsa Materiais de Ativacao  (8) - Survey Question
        self.add_single_survey_response(fact_name='per_ativ_csd_cons_fut_ext_ou_ent_da_loja_tam_min_pop',
                                        fact_desc='PER ATIV CSD CONS FUT EXT OU ENT DA LOJA TAM MIN POP',
                                        eng_desc=None,
                                        question="PER ATIV CSD CONS FUT EXT OU ENT DA LOJA TAM MIN POP",
                                        expected_answer=Pa.YES_ANSWER)

        # Femsa Materiais de Ativacao  (9) - Survey Question
        self.add_single_survey_response(fact_name='ativ_csd_cons_fut_ret_ext_ou_ent_da_loja_tam_min_pop',
                                        fact_desc='PER ATIV CSD CONS FUT RET EXT OU ENT DA LOJA TAM MIN POP',
                                        eng_desc=None,
                                        question="PER ATIV CSD CONS FUT RET EXT OU ENT DA LOJA TAM MIN POP",
                                        expected_answer=Pa.YES_ANSWER)

    def run_june_2016_atacadista(self):

        scif = self.scif
        # Femsa Materiais de Ativacao (12) - Survey Question
        self.add_single_survey_response(fact_name='ativ_csd_mpack_entrada_loja',
                                        fact_desc='ATIV CSD MPACK ENTRADA LOJA',
                                        eng_desc='Has POS Material of Multipack CSDs in the entrance of the store',
                                        question="ATIV CSD MPACK ENTRADA LOJA",
                                        expected_answer=Pa.YES_ANSWER)

        # Femsa Materiais de Ativacao (13) - Minimal Total Distribution
        self.check_distinct_product_count(fact_name='ATIV CSD SB	',
                                          fact_desc='ativ_csd_sb',
                                          eng_desc='Has POS Material of CSD in the bevereges section',
                                          pop_filter=((scif[Fd.T_NAME] == Tm.TN_MATERIAL_SEC_BEB) &
                                                      (scif[Fd.CAT_FK] == Ca.C_CSD) &
                                                      (scif[Fd.P_TYPE] == Fd.P_TYPE_POS) &
                                                      (scif[Fd.DIST_SC] == 1)))

        cat_group = [Ca.C_AGUA, Ca.C_CHA, Ca.C_ENERG, Ca.C_ISO, Ca.C_LACTEO, Ca.C_REFRES, Ca.C_SUCO]
        # Femsa Materiais de Ativacao (14) - Minimal Total Distribution
        self.check_distinct_product_count(fact_name='ATIV NCAT SB	',
                                          fact_desc='ativ_ncat_sb',
                                          eng_desc='Has POS Material for Non carbonated beverages '
                                                   'in the beverages section',
                                          pop_filter=((scif[Fd.T_NAME] == Tm.TN_MATERIAL_SEC_BEB) &
                                                      (scif[Fd.CAT_FK].isin(cat_group)) &
                                                      (scif[Fd.P_TYPE] == Fd.P_TYPE_POS) &
                                                      (scif[Fd.DIST_SC] == 1)))

        # Femsa Materiais de Ativacao  (23) - Survey Question
        self.ha_producto_precificado()

        # Femsa Materiais de Ativacao  (26) - Survey Question
        self.ha_vent_burn_abastec()

        # Femsa Materiais de Ativacao  (29) - Survey Question
        self.ha_material_das_olimpiadas()

    def ativ_balcao_csd_tam_min_guardanudo(self):
        # Femsa Materiais de Ativacao  (3) - Survey Question
        self.add_single_survey_response(fact_name='ativ_balcao_csd_tam_min_guardanudo',
                                        fact_desc='ATIV BALCÃO CSD TAM MIN GUARDANUDO',
                                        eng_desc=None,
                                        question="ATIV BALCÃO CSD TAM MIN GUARDANUDO",
                                        expected_answer=Pa.YES_ANSWER)

    def ativ_cerveja(self):
        scif = self.scif
        # Femsa Materiais de Ativacao (6) - Minimal Total Distribution (and barlan_padaria)
        self.check_distinct_product_count(fact_name='ATIV CERVEJA',
                                          fact_desc='ativ_cerveja',
                                          eng_desc='Has Beer POS Material',
                                          pop_filter=((scif[Fd.T_NAME] == Tg.TG_MATERIAL) &
                                                      (scif[Fd.CAT_FK] == Ca.C_CERVEJA) &
                                                      (scif[Fd.P_TYPE] == Fd.P_TYPE_POS) &
                                                      (scif[Fd.DIST_SC] == 1) &
                                                      (scif[Fd.MAN_FK] == Ma.M_FEMSA)))

    def ativ_csd_cons_ext_ou_ent_loja_tam_min_pop(self):
        # Femsa Materiais de Ativacao  (11) - Survey Question
        self.add_single_survey_response(fact_name='ativ_csd_cons_ext_ou_ent_loja_tam_min_pop',
                                        fact_desc='PER ATIV CSD EXT OU ENT DA LOJA TAM MIN POP',
                                        eng_desc='POS Material of CSD External or Internal to the store,'
                                                 'minimum size CPC',
                                        question="PER ATIV CSD EXT OU ENT DA LOJA TAM MIN POP",
                                        expected_answer=Pa.YES_ANSWER)

    def possui_cervejela(self):
        # Femsa Materiais de Ativacao (16) - Survey Question
        self.add_single_survey_response(fact_name='POSSUI CERVEJELA?',
                                        fact_desc='possui_cervejela',
                                        eng_desc='Has "BEER CASE"',
                                        question="POSSUI CERVEJELA?",
                                        expected_answer=Pa.YES_ANSWER)

    def ha_1_prod_kof_no_menuboard(self):
        # Femsa Materiais de Ativacao  (17) - Survey Question
        self.add_single_survey_response(fact_name='ha_1_prod_kof_no_mwnuboard',
                                        fact_desc='HA 1 PROD KOF NO MENUBOARD?',
                                        eng_desc=None,
                                        question="HA 1 PROD KOF NO MENUBOARD?",
                                        expected_answer=Pa.YES_ANSWER)

    def ha_attivacao_interna_csd_ou_ncb(self):
        # Femsa Materiais de Ativacao  (18) - Survey Question
        self.add_single_survey_response(fact_name='ha_attivacao_interna_csd_ou_ncb',
                                        fact_desc='HA ATIVACAO INTERNA CSD OU NCB',
                                        eng_desc='Internal POS Materal of CSD or NCARB',
                                        question="HA ATIVACAO INTERNA CSD OU NCB",
                                        expected_answer=Pa.YES_ANSWER)

    def ha_balde_de_cerveja(self):
        # Femsa Materiais de Ativacao  (19) - Survey Question
        self.add_single_survey_response(fact_name='ha_balde_de_cerveja',
                                        fact_desc='HA BALDE DE CERVEJA',
                                        eng_desc='Bucket of beer',
                                        question="HA BALDE DE CERVEJA",
                                        expected_answer=Pa.YES_ANSWER)

    def ha_material_de_preco_positivo(self):
        # Femsa Materiais de Ativacao  (20) - Survey Question
        self.add_single_survey_response(fact_name='ha_material_de_preco_positivo',
                                        fact_desc='HA MATERIAL DE PRECO POSITIVO?',
                                        eng_desc='Has Material of positive Price',
                                        question="HA MATERIAL DE PRECO POSITIVO?",
                                        expected_answer=Pa.YES_ANSWER)

    def ha_material_permanente(self):
        # Femsa Materiais de Ativacao  (21) - Survey Question
        self.add_single_survey_response(fact_name='ha_material_permanente',
                                        fact_desc='HA MATERIAL PERMANENTE',
                                        eng_desc='Has any Permanent Femsa POS Material',
                                        question="HA MATERIAL PERMANENTE",
                                        expected_answer=Pa.YES_ANSWER)

    def ha_material_temporario(self):
        # Femsa Materiais de Ativacao  (22) - Survey Question
        self.add_single_survey_response(fact_name='ha_material_temporario',
                                        fact_desc='HA MATERIAL TEMPORARIO',
                                        eng_desc='Has any temporary Femsa POS Material',
                                        question="HA MATERIAL TEMPORARIO",
                                        expected_answer=Pa.YES_ANSWER)

    def ha_producto_precificado(self):
        # Femsa Materiais de Ativacao  (23) - Survey Question
        self.add_single_survey_response(fact_name='ha_producto_precificado',
                                        fact_desc='HÁ PRODUTO PRECIFICADO',
                                        eng_desc='Is there any priced product in the store',
                                        question="HÁ PRODUTO PRECIFICADO",
                                        expected_answer=Pa.YES_ANSWER)

    def per_ha_quadro_ou_cartaz_de_kai_bav_amstel(self):
        # Femsa Materiais de Ativacao (24) - Survey Question (and barlan_padaria)
        self.add_single_survey_response(fact_name='per_ha_quadro_ou_cartaz_de_kai_bav_amstel',
                                        fact_desc='PER HA QUADRO OU CARTAZ DE KAI/BAV/AMSTEL',
                                        eng_desc='Is there Outdoor or board for Kaiser/Bavaria/Amstel brand',
                                        question="PER HA QUADRO OU CARTAZ DE KAI/BAV/AMSTEL",
                                        expected_answer=Pa.YES_ANSWER)

    def per_ha_quadro_ou_culminoso_hnk(self):
        # Femsa Materiais de Ativacao (25) - Survey Question (and barlan_padaria)
        self.add_single_survey_response(fact_name='per_ha_quadro_ou_luminoso_hnk',
                                        fact_desc='PER HA QUADRO OU LUMINOSO HNK',
                                        eng_desc='Is there luminous Heineken Outdoor',
                                        question="PER HA QUADRO OU CLUMINOSO HNK",
                                        expected_answer=Pa.YES_ANSWER)

    def ha_vent_burn_abastec(self):
        # Femsa Materiais de Ativacao  (26) - Survey Question
        self.add_single_survey_response(fact_name='ha_vent_burn_abastec',
                                        fact_desc='HA VENT BURN ABASTEC',
                                        eng_desc=None,
                                        question="HA VENT BURN ABASTEC",
                                        expected_answer=Pa.YES_ANSWER)

    def ha_material_das_olimpiadas(self):
        # Femsa Materiais de Ativacao  (29) - Survey Question
        self.add_single_survey_response(fact_name='ha_material_das_olimpiadas',
                                        fact_desc='HA MATERIAL DAS OLIMPÍADAS',
                                        eng_desc='Are there promotional material for the Olympics',
                                        question="HA MATERIAL DAS OLIMPÍADAS",
                                        expected_answer=Pa.YES_ANSWER)
