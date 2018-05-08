# - *- coding: utf- 8 - *-

__author__ = 'davids'


class StoreTypes(object):

    ST_AS5 = "AS5"
    ST_AS14 = "AS14"
    ST_BARLAN = "BARLAN"
    ST_PADARIA = "PADARIA"
    ST_RESTAURANTE = "RESTAURANTE"
    ST_ADHOC = "ADHOC"
    ST_MERCEARIA = "MERCEARIA"
    ST_CONVENIENCIA = "CONVENIENCIA"
    ST_CPQ = "CPQ"
    ST_ATACADISTA = "ATACADISTA"
    ST_ADEGA = "ADEGA"


class Templates(object):

    TG_EXIBICAO = u"Exibição"
    TG_Ilha = "Ilha"
    TG_PG = "PG"
    TG_RACK_CONC = "RACK Conc"
    TG_RACK_KOF_CERV = "RACK KOF Cerv"
    TG_RACK_KOF_CSD = "RACK KOF CSD"
    TG_RACK_KOF_NCB = "RACK KOF NCB"
    TG_GDM_KOF_CERV = "GDM KOF Cerv"
    TG_GDM_KOF_CSD = "GDM KOF CSD"
    TG_GDM_KOF_NCB = "GDM KOF NCB"
    TG_GDM_CONC_Proprio = "GDM Conc/Proprio - NP"
    TG_GDM_KOF_CERV_NP = "GDM KOF Cerv - NP"
    TG_GDM_KOF_CSD_NP = "GDM KOF CSD - NP"
    TG_GDM_KOF_NCB_NP = "GDM KOF NCB"
    TG_GDM_CONC_Proprio_NP = "GDM KOF NCB - NP"
    TG_MATERIAL = "Material"


    TN_EXIBICAO_BISC = u"Exibição - Bisc"
    TN_EXIBICAO_ACOU = u"Exibição - Açou"
    TN_EXIBICAO_MERC = u"Exibição - Merc"
    TN_EXIBICAO_PAD = u"Exibição - Pad"
    TN_EXIBICAO_CKO = u"Exibição - Cko"
    TN_EXIBICAO_NP_CKO = u"Exibição NP - Cko"
    TN_EXIBICAO_NP_ACOU = u"Exibição NP - Açou"
    TN_EXIBICAO_NP_BISC = u"Exibição NP - Bisc"
    TN_PG_BISC = "PG - Bisc"
    TN_PG_ACOU = u"PG - Açou"
    TN_PG_MERC = u"PG - Merc"
    TN_PG_PAD = "PG - Pad"
    TN_PG_CKO = "PG - Cko"
    TN_PG_NP_CKO = "PG NP - Cko"
    TN_PG_NP_ACOU = u"PG NP - Açou"
    TN_PG_NP_BISC = u"PG NP - Bisc"
    TN_ILHA_ACOU = u"Ilha - Açou"
    TN_ILHA_BISC = "Ilha - Bisc"
    TN_ILHA_MERC = "Ilha - Merc"
    TN_ILHA_PAD = "Ilha - Pad"
    TN_ILHA_CKO = "Ilha - Cko"
    TN_ILHA_NP_CKO = "Ilha NP - Cko"
    TN_ILHA_NP_ACOU = u"Ilha NP - Açou"
    TN_ILHA_NP_BISC = u"Ilha NP - Bisc"
    TN_RACK_KOF_CERV_BISC = "RACK KOF Cerv - Bisc"
    TN_RACK_KOF_CERV_ACOU = u"RACK KOF Cerv - Açou"
    TN_RACK_KOF_CERV_MERC = "RACK KOF Cerv - Merc"
    TN_RACK_KOF_CERV_PAD = "RACK KOF Cerv - Pad"
    TN_RACK_CONC_BISC = "RACK Conc - Bisc"
    TN_RACK_CONC_ACOU = u"RACK Conc - Açou"
    TN_RACK_CONC_MERC = "RACK Conc - Merc"
    TN_RACK_CONC_PAD = "RACK Conc - Pad"
    TN_RACK_KOF_CSD_BISC = "RACK KOF CSD - Bisc"
    TN_RACK_KOF_CSD_ACOU = u"RACK KOF CSD - Açou"
    TN_RACK_KOF_CSD_MERC = "RACK KOF CSD - Merc"
    TN_RACK_KOF_CSD_PAD = "RACK KOF CSD - Pad"
    TN_RACK_KOF_NCB_BISC = "RACK KOF NCB - Bisc"
    TN_RACK_KOF_NCB_ACOU = u"RACK KOF CSD - Açou"
    TN_RACK_KOF_NCB_MERC = "RACK KOF NCB - Merc"
    TN_RACK_KOF_NCB_PAD = "RACK KOF NCB - Pad"
    TN_GDM_CONC_PROPRIO_BISC = "GDM Conc/Proprio - Bisc"
    TN_GDM_CONC_PROPRIO_ACOU = u"GDM Conc/Proprio - Açou"
    TN_GDM_CONC_PROPRIO_MERC = "GDM Conc/Proprio - Merc"
    TN_GDM_CONC_PROPRIO_PAD = "GDM Conc/Proprio - Pad"
    TN_GDM_CONC_PROPRIO_CKO = "GDM Conc/Proprio - Cko"
    TN_GDM_CONC_PROPRIO_NP_CKO = "GDM Conc/Proprio NP - Cko"
    TN_GDM_KOF_CERV_P_BISC = "GDM KOF Cerv P - Bisc"
    TN_GDM_KOF_CERV_P_ACOU = u"GDM KOF Cerv P - Açou"
    TN_GDM_KOF_CERV_P_MERC = "GDM KOF Cerv P - Merc"
    TN_GDM_KOF_CERV_P_PAD = "GDM KOF Cerv P - Pad"
    TN_GDM_KOF_CERV_P_CKO = "GDM KOF Cerv P - Cko"
    TN_GDM_KOF_CERV_NP_CKO = "GDM KOF Cerv NP - Cko"
    TN_GDM_KOF_CSD_P_BISC = "GDM KOF CSD P - Bisc"
    TN_GDM_KOF_CSD_P_MERC = "GDM KOF CSD P - Merc"
    TN_GDM_KOF_CSD_P_ACOU = u"GDM KOF CSD P - Açou"
    TN_GDM_KOF_CSD_P_PAD = "GDM KOF CSD P - Pad"
    TN_GDM_KOF_CSD_P_CKO = "GDM KOF CSD P - Cko"
    TN_GDM_KOF_CSD_P_1A_POS = "GDM KOF CSD P - 1a pos"
    TN_GDM_KOF_NCB_P_1A_POS = "GDM KOF NCB P - 1a pos"
    TN_GDM_KOF_CSD_P_F_SERV = "GDM KOF CSD P - F Serv"
    TN_GDM_KOF_CSD_NP_CKO = "GDM KOF CSD NP - Cko"
    TN_GDM_KOF_CSD_NP_1A_POS = "GDM KOF CSD NP - 1a pos"
    TN_GDM_KOF_NCB_NP_1A_POS = "GDM KOF NCB NP - 1a pos"
    TN_GDM_KOF_CSD_NP_F_SERV = "GDM KOF CSD NP - F Serv"
    TN_GDM_KOF_NCB_P_CKO = "GDM KOF NCB P - Cko"
    TN_GDM_KOF_NCB_P_F_SERV = "GDM KOF NCB P - F Serv"
    TN_GDM_KOF_NCB_P_BISC = "GDM KOF NCB P - Bisc"
    TN_GDM_KOF_NCB_P_ACOU = u"GDM KOF NCB P - Açou"
    TN_GDM_KOF_NCB_P_MERC = "GDM KOF NCB P - Merc"
    TN_GDM_KOF_NCB_P_PAD = "GDM KOF NCB P - Pad"
    TN_GDM_KOF_NCB_NP_CKO = "GDM KOF NCB NP - Cko"
    TN_GDM_KOF_NCB_NP_F_SERV = "GDM KOF NCB NP - F Serv"
    TN_MATERIAL_SEC_BEB = "Material - Sec Beb"
    TN_GDM_KOF_CERV_NP_POS_LIDERENCA = u"GDM KOF Cerv NP - POS. DE LIDERENÇA"
    TN_GDM_KOF_CERV_P_POS_LIDERENCA = U"GDM KOF Cerv P - POS. DE LIDERENÇA"
    TN_GDM_KOF_CSD_NP_POS_LIDERENCA = u"GDM KOF CSD NP - POS. DE LIDERENÇA"
    TN_GDM_KOF_CSD_P_POS_LIDERENCA = U"GDM KOF CSD P - POS. DE LIDERENÇA"
    TN_GDM_KOF_NCB_NP_POS_LIDERENCA = u"GDM KOF NCB NP - POS. DE LIDERENÇA"
    TN_GDM_KOF_NCB_P_POS_LIDERENCA = U"GDM KOF NCB P - POS. DE LIDERENÇA"
    TN_GDM_CSD_P_1A_POS = "GDM KOF CSD P - 1a pos"
    TN_GDM_NCB_P_1A_POS = "GDM KOF NCB P - 1a pos"
    TN_GDM_CSD_NP_1A_POS = "GDM KOF CSD NP - 1a pos"
    TN_GDM_NCB_NP_1A_POS = "GDM KOF NCB NP - 1a pos"
    TN_MATERIAL_INTERNO = "Material - Interno"
    TN_EXIBICAO_NP = u"Exibição - NP"
    TN_Ilha_NP = "Ilha - NP"
    TN_PG_NP = "PG - NP"
    TN_PG_SEC_BEB = "PG - Sec Beb"
    TN_PG_NP_SEC_BEB = "PG NP - Sec Beb"
    TN_EXIBICAO_NP_HORT = u"Exibição NP - Hort"
    TN_Ilha_NP_HORT = "Ilha NP - Hort"
    TN_PG_HORT = "PG - Hort"
    TN_PG_NP_HORT = "PG NP - Hort"
    TN_EXIBICAO_HORT = u"Exibição - Hort"
    TN_ILHA_HORT = "Ilha - Hort"
    TN_PG_NP_PAD = "PG NP - Pad"
    TN_EXIBICAO_NP_PAD = u"Exibição NP - Pad"
    TN_Ilha_NP_PAD = "Ilha NP - Pad"



class Products(object):

    P_LAC_FEMSA_KAPO_CHOCOLATE_200ML_TP = "LAC FEMSA KAPO CHOCOLATE 200ML TP"
    P_SUCO_FEMSA_DEL_VALLE_KAPO_NECTAR_200ML = "SUCO FEMSA DEL VALLE KAPO NECTAR 200ML"
    P_SUCO_FEMSA_KAPO_200ML = "SUCO FEMSA KAPO 200ML"
    P_LAC_FEMSA_KAPO_OUTROS_CHOCOLATE = "LAC FEMSA Kapo OUTROS CHOCOLATE"
    P_SUCO_FEMSA_KAPO_OUTROS = "SUCO FEMSA KAPO OUTROS"
    P_CERV_HEINEKEN_CERVEJELA1 = "CERV HEINEKEN CERVEJELA 1"
    P_CERV_HEINEKEN_CERVEJELA2 = "CERV HEINEKEN CERVEJELA 2"
    P_CERV_HEINEKEN_12PACK_HEINEKEN_350ML_LT = "CERV HEINEKEN 12-PACK HEINEKEN 350ML LT"
    P_CERV_HEINEKEN_6PACK_HEINEKEN_LN = "CERV HEINEKEN 6-PACK HEINEKEN LN"
    P_CERV_HEINEKEN_M_PACK_HEINEKEN_250ML_VD = "CERV HEINEKEN M-PACK HEINEKEN 250ML VD"
    P_CERV_HEINEKEN_12PACK_BAVARIA_350ML_LT = "CERV HEINEKEN 12-PACK BAVARIA 350ML LT"
    P_CERV_HEINEKEN_12PACK_KAISER_350ML_LT = "CERV HEINEKEN 12-PACK KAISER 350ML LT"
    P_CERV_HEINEKEN_12PACK_KAISER_RADLER_350ML_LT = "CERV HEINEKEN 12-PACK KAISER RADLER 350ML LT"
    P_IRRELEVENT = "irrelevent"

    # M_FEMSA = "FEMSA"
    # M_HEINEKEN_BRASIL = "HEINEKEN BRASIL"

    # C_AGUA = "Agua"
    # C_CERVEJA = "Cerveja"
    # C_CHA = "Cha"
    # C_CSD = "CSD"
    # C_ENERG = "Energ"
    # C_ISO = "Iso"
    # C_LACTEO = "Lacteo"
    # C_OTHER = "Other"
    # C_REFRES = "Refres"
    # C_SUCO = "Suco"

    B_COCA_COLA = "COCA-COLA"
    B_COCA_COLA_LIGHT = "COCA-COLA LIGHT"
    B_COCA_COLA_ZERO = "COCA-COLA ZERO"
    B_FEMSA_ISO = "FEMSA (Iso)"
    B_FEMSA_CSD = "FEMSA (CSD)"
    B_FEMSA_CHA = "FEMSA (Cha)"
    B_FEMSA_AGUA = "FEMSA (Agua)"
    B_FEMSA_SUCO = "FEMSA (Suco)"
    B_CRYSTAL_AGUA = "CRYSTAL (Agua)"
    B_COCA_COLA_CSD = "COCA-COLA (CSD)"
    B_COCA_COLA_LIGHT_CSD = "COCA-COLA LIGHT (CSD)"
    B_COCA_COLA_ZERO_CSD = "COCA-COLA ZERO (CSD)"
    B_LEAO_CHA ="LEÃO (Cha)"
    B_FANTA_CSD = "FANTA (CSD)"
    B_KUTA_CSD = "KUTA (CSD)"
    B_SCHWEPPES_CSD = "SCHWEPPES (CSD)"
    B_SPRITE_CSD = "SPRITE (CSD)"
    B_AQUARIUS_CSD = "AQUARIUS (CSD)"
    B_GUARANA_JESUS_CSD = "GUARANA_JESUS (CSD)"
    B_GUARAPAN_CSD = "GUARAPAN (CSD)"
    B_SIMBA_CSD = "SIMBA (CSD)"
    B_TAI_GUARANA_CSD = "TAI_GUARANA (CSD)"
    B_BURN_ENERG = "BURN (Energ)"
    B_BURN = "BURN"
    B_I9_ISO = "I9 (Iso)"
    B_POWERADA_ISO = "POWERADA (Iso)"
    B_KAPO_CHOCOLATE_LACTEO = "KAPO CHOCOLATE (Lacteo)"
    B_DEL_VALLE_SUCO = "DEL_VALLE (Suco)"
    B_DEL_VALLE_REFRES = "DEL_VALLE (Refres)"
    B_SCHWEPPES_CITRUS_OTHER_CSD = "Schweppes Citrus Other (CSD)"
    B_SCHWEPPES_SODA_OTHER_CSD = "Schweppes Soda Other (CSD)"
    B_SCHWEPPES_TONIC_OTHER_CSD = "Schweppes Tonic Other (CSD)"
    B_KAPO_SUCO = "KAPO (Suco)"
    B_MAIS_SUCO = "MAIS (Suco)"
    B_AMSTEL_PULSE_CERVEJA = "AMSTEL PULSE (Cerveja)"
    B_BAVARIA_CERVEJA = "BAVARIA (Cerveja)"
    B_HEINEKEN_CERVEJA = "HEINEKEN (Cerveja)"
    B_KAISER_CERVEJA = "KAISER (Cerveja)"
    B_FEMSA_CERVEJA = "FEMSA (Cerveja)"
    B_SOL_CERVEJA = "SOL (Cerveja)"
    B_SOL_PREMIUM_CERVEJA = "SOL PREMIUM (Cerveja)"
    B_XINGU_CERVEJA = "XINGU (Cerveja)"
    B_GOLD_CERVEJA = "GOLD (Cerveja)"
    B_BIRRA_MORETTI_CERVEJA = "BIRRA MORETTI (Cerveja)"
    B_DESPERADOS_CERVEJA = "DESPERADOS (Cerveja)"
    B_DOS_EQUIS_CERVEJA = "DOS EQUIS (Cerveja)"
    B_EDELWEISS_CERVEJA = "EDELWEISS (Cerveja)"
    B_MURPHYS_IRISH_STOUT = "MURPHY’S IRISH STOUT (Cerveja)"
    B_MURPHYS_IRISH_RED = "MURPHY’S IRISH RED (Cerveja)"

    S_1 = '1.0'
    S_1_25 = '1.25'
    S_1_45 = '1.45'
    S_1_50 = '1.5'
    S_2 = '2.0'
    S_2_25 = '2.25'
    S_2_50 = '2.5'
    S_3 = '3.0'
    S_3_3 = '3.3'
    S_3_50 = '3.5'
    S_4 = '4.0'
    S_5 = '5.0'
    S_6 = '6.0'
    S_6_25 = '6.25'
    S_6_30 = '6.3'
    S_9 = '9.0'
    S_1500 = '1500.0'
    S_1000 = '1000.0'
    S_2000 = '2000.0'
    S_2250 = '2250.0'
    S_200 = '200.0'
    S_250 = '250.0'
    S_255 = '255.0'
    S_269 = '269.0'
    S_350 = '350.0'
    S_300 = '300.0'
    S_600 = '600.0'
    S_473 = '473.0'
    S_355 = '355.0'
    S_450 = '450.0'
    S_343 = '343.0'
    S_500 = '500.0'


class PossibleAnswers(object):

    YES_ANSWER = u'Sim'
    AC_ANSWER = u'AC'
