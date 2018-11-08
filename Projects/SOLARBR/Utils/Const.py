#!/usr/bin/python
# -*- coding: utf-8 -*-

_author_ = 'NicolasKeeton'

class Const(object):

    #sheets Score
    DROGARIA_SHEET, LOJA_SHEET, RESTAURANTE_AB_SHEET ='DROGARIA', 'LOJA DE CONVENIÊNCIA', 'RESTAURANTE AB'
    RESTAURANTE_C_SHEET, BAR_BOTECO_AB_SHEET, BAR_BOTECO_C_SHEET ='RESTAURANTE C','BAR BOTECO AB','BAR BOTECO C'
    BAR_LANCHONETE_AB_SHEET, BAR_LANCHONETE_C_SHEET = 'BAR LANCHONETE AB','BAR LANCHONETE C'
    MERCERIA_SHEET, PANDARIA_AB_SHEET, PANDARIA_C_SHEET = 'MERCEARIA', 'PADARIA AB','PADARIA C'
    AS_14_SHEET, AS_5_SHEET = 'AS 1-4', 'AS 5+'
    ATACADO_SHEET = 'ATACADO'

    #Sheet main template
    KPIS = "KPIs"
    SOVI = "SOVI"


    SHEETS_SCORE = { DROGARIA_SHEET, LOJA_SHEET, RESTAURANTE_AB_SHEET, RESTAURANTE_C_SHEET, BAR_BOTECO_AB_SHEET, BAR_BOTECO_C_SHEET,
               BAR_LANCHONETE_AB_SHEET, BAR_LANCHONETE_C_SHEET, MERCERIA_SHEET, PANDARIA_AB_SHEET, PANDARIA_C_SHEET,
               AS_14_SHEET, AS_5_SHEET, ATACADO_SHEET}

    SHEETS_MAIN = {KPIS, SOVI}

    #generic columns
    KPI_NAME = "KPI NAME"

    #columns for Score_sheets
    KPI = 'Kpi'
    LOW_PERCENT= 'Low'
    HIGH_PERCENT ='High'
    ACCEPTANCE_PERCENT ='range'
    SCORE = 'Score'

    #column for KPIs
    Type = "Type"
    SCENE_TYPES = "Scene Types"
    STORE_TYPES = "STORE TYPES"
    TEMPLATE_NAME = "TEMPLATE_NAME"
    TEMPLATE_GROUP = "TEMPLATE GROUP"

    #column of SOVI:
    SOVI_NAME = "KPI NAME"
    KPI_RULE = "KPI RULE"
    DEN_TYPES_1 = "denominator param"
    DEN_VALUES_1 = "denominator value"
    NUM_TYPES_1 = "numerator param 1"
    NUM_VALUES_1 = "numerator value 1"
    NUM_TYPES_2 = "numerator param 2"
    NUM_VALUES_2 = "numerator value 2"
    NUM_TYPES_3 = "numerator param 3"
    NUM_VALUES_3 = "numerator value 3"

    SOS = "SOS"
    TOTAL = "total"