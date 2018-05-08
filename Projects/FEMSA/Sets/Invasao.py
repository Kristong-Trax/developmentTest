# -*- coding: utf-8 -*-
from datetime import date

from Trax.Algo.Calculations.Core.Constants import Fields as Fd
from Trax.Algo.Calculations.Core.Utils import Predicates as Pred
from Trax.Algo.Calculations.FEMSA.Constants import Products as Pr
from Trax.Algo.Calculations.FEMSA.NewConstants import StoreTypes as St
from Trax.Algo.Calculations.FEMSA.Constants import Templates as Tm
# from Trax.Algo.Calculations.FEMSA.Constants import TemplateGroups as Tg
from Trax.Algo.Calculations.FEMSA.NewConstants import Manufacturers as Ma
from Trax.Algo.Calculations.FEMSA.NewConstants import Categories as Ca
# from Trax.Algo.Calculations.FEMSA.NewConstants import Brands as Br
from Trax.Algo.Calculations.FEMSA.NewConstants import TemplateGroups as Tg
from Trax.Algo.Calculations.FEMSA.Sets.Base import FEMSACalculationsGroup
from Trax.Utils.Logging.Logger import Log


class InvasaoCalculations(FEMSACalculationsGroup):
    def run_calculations(self):
        Log.info('Starting Invasao calculation for session_pk: {}.'.format(self.session_pk))
        if self.visit_date >= date(2016, 6, 1):
            self.run_june_2016_calculations()
        else:
            Log.info('No Invasao calculations were executed.')

    def run_june_2016_calculations(self):
        store_type = self.session_info.store_type

        if store_type in [St.ST_MERCEARIA, St.ST_PADARIA, St.ST_BARLAN, St.ST_CONVENIENCIA, St.ST_AS5,
                          St.ST_AS14, St.ST_RESTAURANTE, St.ST_ATACADISTA]:
            self.run_june_2016_invasao()

    def run_june_2016_invasao(self):
        scif = self.scif
        brand_gdm_cerv_group = [Pr.B_FEMSA_ISO, Pr.B_FEMSA_CSD, Pr.B_FEMSA_CHA, Pr.B_FEMSA_AGUA, Pr.B_FEMSA_SUCO,
                                Pr.B_CRYSTAL_AGUA, Pr.B_COCA_COLA_CSD, Pr.B_COCA_COLA_LIGHT_CSD, Pr.B_KUTA_CSD,
                                Pr.B_COCA_COLA_ZERO_CSD, Pr.B_LEAO_CHA, Pr.B_FANTA_CSD, Pr.B_DEL_VALLE_SUCO,
                                Pr.B_SCHWEPPES_CSD, Pr.B_SPRITE_CSD, Pr.B_AQUARIUS_CSD, Pr.B_GUARANA_JESUS_CSD,
                                Pr.B_GUARAPAN_CSD, Pr.B_SIMBA_CSD, Pr.B_TAI_GUARANA_CSD, Pr.B_BURN_ENERG, Pr.B_I9_ISO,
                                Pr.B_POWERADA_ISO, Pr.B_KAPO_CHOCOLATE_LACTEO, Pr.B_TAI_GUARANA_CSD, Pr.B_MAIS_SUCO,
                                Pr.B_DEL_VALLE_REFRES, Pr.B_SCHWEPPES_CITRUS_OTHER_CSD, Pr.B_SCHWEPPES_TONIC_OTHER_CSD,
                                Pr.B_KAPO_SUCO, Pr.B_SCHWEPPES_SODA_OTHER_CSD]
        brand_ncb_cerv_group = [Pr.B_AMSTEL_PULSE_CERVEJA, Pr.B_BAVARIA_CERVEJA, Pr.B_HEINEKEN_CERVEJA,
                                Pr.B_KAISER_CERVEJA, Pr.B_FEMSA_CERVEJA, Pr.B_SOL_CERVEJA, Pr.B_SOL_PREMIUM_CERVEJA,
                                Pr.B_XINGU_CERVEJA, Pr.B_GOLD_CERVEJA, Pr.B_BIRRA_MORETTI_CERVEJA,
                                Pr.B_DESPERADOS_CERVEJA, Pr.B_DOS_EQUIS_CERVEJA, Pr.B_EDELWEISS_CERVEJA,
                                Pr.B_MURPHYS_IRISH_STOUT, Pr.B_MURPHYS_IRISH_RED]
        cat_csd_kof = [Ca.C_CERVEJA, Ca.C_AGUA, Ca.C_CHA, Ca.C_CSD, Ca.C_ENERG, Ca.C_ISO, Ca.C_LACTEO, Ca.C_REFRES,
                       Ca.C_SUCO]
        cat_ncb = [Ca.C_OTHER, Ca.C_AGUA, Ca.C_CHA, Ca.C_CSD, Ca.C_ENERG, Ca.C_ISO, Ca.C_LACTEO, Ca.C_REFRES, Ca.C_SUCO]
        cat_gdm_cerv = [Ca.C_AGUA, Ca.C_CHA, Ca.C_CSD, Ca.C_ENERG, Ca.C_ISO, Ca.C_LACTEO, Ca.C_REFRES, Ca.C_SUCO]
        temp_gdm_cerv_group = [Tg.TG_GDM_KOF_CERV_NP, Tg.TG_GDM_KOF_CERV]
        temp_gdm_ncb_group = [Tg.TG_GDM_KOF_NCB_NP, Tg.TG_GDM_KOF_NCB]
        temp_gdm_kof_csd_group = [Tg.TG_GDM_KOF_CSD, Tg.TG_GDM_KOF_CSD_NP]

        # Femsa Invasao (1) - Count SKU
        fact = 'INV GDM HNK'
        description = 'inv_gdm_hnk'
        eng_desc = 'Amount of GDMs KOF (Coca-cola Femsa) Heineken (Femsas beer) inavded'
        calc1 = self.check_scenes_for_operator(pop_filter=((scif[Fd.B_NAME].isin(brand_gdm_cerv_group)) &
                                                           (scif[Fd.T_GROUP].isin(temp_gdm_cerv_group))))

        calc2 = self.check_scenes_for_operator(pop_filter=((scif[Fd.MAN_FK] == Ma.M_FEMSA) &
                                                           (scif[Fd.T_GROUP].isin(temp_gdm_cerv_group))))

        calc3 = self.check_scenes_for_operator(pop_filter=((scif[Fd.P_NAME] == Pr.P_IRRELEVENT) &
                                                           (scif[Fd.T_GROUP].isin(temp_gdm_cerv_group))))

        self.add_or_fact([calc1, calc2, calc3], description, fact, eng_desc)

        # Femsa Invasao (2) - Count SCENE
        fact = 'INV GDM NCAT'
        description = 'inv_gdm_ncat'
        eng_desc = 'Amount of GDMs KOF (Coca-cola Femsa) NCB (non carbonated) invaded'
        calc1 = self.check_scenes_for_operator(pop_filter=((scif[Fd.B_NAME].isin(brand_ncb_cerv_group)) &
                                                           (scif[Fd.T_GROUP].isin(temp_gdm_ncb_group))))

        calc2 = self.check_scenes_for_operator(pop_filter=((scif[Fd.MAN_FK] == Ma.M_FEMSA) &
                                                           (scif[Fd.T_GROUP].isin(temp_gdm_cerv_group))))

        calc3 = self.check_scenes_for_operator(pop_filter=((scif[Fd.P_NAME] == Pr.P_IRRELEVENT) &
                                                           (scif[Fd.T_GROUP].isin(temp_gdm_cerv_group))))

        self.add_or_fact([calc1, calc2, calc3], description, fact, eng_desc)

        # Femsa Invasao (3) - Count SCENE
        self.check_scenes(fact_name='GDM CSD INVAD PELA CONC? ',
                          fact_desc='gdm_csd_invad_pela_conc',
                          pop_filter=((scif[Fd.B_NAME].isin(brand_gdm_cerv_group)) &
                                      (scif[Fd.T_GROUP].isin(temp_gdm_cerv_group))),
                          target=1)

        # Femsa Invasao (4) - Count SCENE

        fact = 'GDM CSD INVAD POR OUT PROD?'
        description = 'gdm_csd_invad_por_out_prod?'
        calc1 = self.check_scenes_for_operator(pop_filter=((scif[Fd.CAT_FK].isin(cat_csd_kof)) &
                                                           (scif[Fd.T_GROUP].isin(temp_gdm_kof_csd_group))) &
                                                          (scif[Fd.MAN_FK] != Ma.M_FEMSA))

        calc2 = self.check_scenes_for_operator(pop_filter=((scif[Fd.P_NAME] == Pr.P_IRRELEVENT) &
                                                           (scif[Fd.T_GROUP].isin(temp_gdm_kof_csd_group))))

        self.add_or_fact([calc1, calc2], description, fact)

        # Femsa Invasao (5) - Count SCENE
        self.check_scenes(fact_name='GDM CSD/NCAT INVAD POR CERV KOF?',
                          fact_desc='gdm_csd_ncat_invad_por_cerv_kof',
                          eng_desc=None,
                          pop_filter=((scif[Fd.CAT_FK] == Ca.C_CERVEJA) &
                                      (scif[Fd.T_GROUP].isin(temp_gdm_cerv_group and temp_gdm_ncb_group)) &
                                      (scif[Fd.MAN_FK] == Ma.M_FEMSA)),
                          target=1)

        # Femsa Invasao (6) - Count SCENE
        self.check_scenes(fact_name='GDM NCAT INVAD PELA CONC?',
                          fact_desc='gdm_ncat_invad_pela_conc',
                          eng_desc=None,
                          pop_filter=((scif[Fd.CAT_FK].isin(cat_ncb)) &
                                      (scif[Fd.T_GROUP].isin(temp_gdm_ncb_group)) &
                                      (scif[Fd.MAN_FK] != Ma.M_FEMSA)),
                          target=1)

        # Femsa Invasao (7) - Count SCENE
        fact = 'GDM NCAT INVAD POR OUT PROD?'
        description = 'gdm_ncat_invad_por_ot_prod'
        eng_desc = None
        calc1 = self.check_scenes_for_operator(pop_filter=((scif[Fd.CAT_FK].isin([Ca.C_CERVEJA, Ca.C_CSD])) &
                                                           (scif[Fd.T_GROUP].isin(temp_gdm_ncb_group)) &
                                                           (scif[Fd.MAN_FK] != Ma.M_FEMSA)))

        calc2 = self.check_scenes_for_operator(pop_filter=((scif[Fd.P_NAME] == Pr.P_IRRELEVENT) &
                                                           (scif[Fd.T_GROUP].isin(temp_gdm_ncb_group))))

        self.add_or_fact([calc1, calc2], description, fact, eng_desc)

        # Femsa Invasao (8) - Count SCENE
        self.check_scenes(fact_name='GDM CERV INVAD PELA CONC?',
                          fact_desc='gdm_cerv_invad_pela_conc',
                          eng_desc=None,
                          pop_filter=((scif[Fd.CAT_FK] == Ca.C_CERVEJA) &
                                      (scif[Fd.T_GROUP].isin(temp_gdm_cerv_group)) &
                                      (scif[Fd.MAN_FK] != Ma.M_FEMSA)),
                          target=1)

        # Femsa Invasao (9) - Count SCENE
        self.check_scenes(fact_name='GDM CERV INVAD POR CSD/NCAT KOF?',
                          fact_desc='gdm_cerv_invad_por_csd_ncat_kof',
                          eng_desc=None,
                          pop_filter=((scif[Fd.CAT_FK].isin(cat_gdm_cerv)) &
                                      (scif[Fd.T_GROUP].isin(temp_gdm_cerv_group)) &
                                      (scif[Fd.MAN_FK] == Ma.M_FEMSA)),
                          target=1)

        # Femsa Invasao (10) - Count SCENE
        fact = 'GDM CERV INVAD POR OUT PROD?'
        description = 'gdm_cerv_invad_por_ot_prod'
        eng_desc = None
        calc1 = self.check_scenes_for_operator(pop_filter=((scif[Fd.CAT_FK].isin(cat_ncb)) &
                                                           (scif[Fd.T_GROUP].isin(temp_gdm_cerv_group)) &
                                                           (scif[Fd.MAN_FK] != Ma.M_FEMSA)))

        calc2 = self.check_scenes_for_operator(pop_filter=((scif[Fd.P_NAME] == Pr.P_IRRELEVENT) &
                                                           (scif[Fd.T_GROUP].isin(temp_gdm_cerv_group))))
        self.add_or_fact([calc1, calc2], description, fact, eng_desc)
