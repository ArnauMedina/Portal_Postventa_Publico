 #::::::::::::::::::::::::::::::::::::::::: Drive - Repsol  ::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
 
 #------------------------------------------------ LIBRERIAS -------------------------------------------------------------------- 

import typedframe
from typedframe import TypedDataFrame, UTC_DATE_TIME_DTYPE, DATE_TIME_DTYPE
import numpy as np
from google.colab import files
import sys


#------------------------------------------------ INDEXACIÓN HOJAS DOCUMENTO -------------------------------------------------------------------- 

#------------------------------------------------ HOJAS DOCUMENTO -------------------------------------------------------------------- 

###---------------- Hoja 360 ------------------###

class treseseinta(TypedDataFrame):
    
    schema = {
        'Acción Comercial': str,
        'Oportunidad: Nombre de la oportunidad':str,
        'Tipo de cliente':('Empresa', 'Residencial'),
        'NIF / CIF':str,
        'Cliente: Nombre / Razón Social':str,
        'PYME Razón Social':str,
        'Email':str,
        'Origen de carga':('Carga Masiva','carga masiva'),
        'Canal':('7 - Internet', 'otro'),
        'Subcanal': ('7 - Internet', 'otra'),
        'Agente: Nombre completo': str,
        'Fecha de creación':DATE_TIME_DTYPE,
        'Fecha Firma Solicitud Contratación': DATE_TIME_DTYPE,
        'Fecha de la última modificación': DATE_TIME_DTYPE,
        'Estado de carga': ('OK','KO'),
        'Detalle de carga': str
    } 
    
    
###---------------- Hoja Liquidacion ------------------###

class Liquidacion(TypedDataFrame):
    
    schema = {
        'Sol.Contratacion': str,
        'MES LIQUIDACION':str
    } 
 
 #------------------------------------------------ FUNCIÓN DICCIONARIO --------------------------------------------------------------------
 
 
def Error_to_dict(error):
    Actual = error[1].args[0].split('Actual:')[1].split("\nExpected: ")[0]
    Expected = error[1].args[0].split('Actual:')[1].split("\nExpected: ")[1].split("\nDifference: ")[0]
    Difference = error[1].args[0].split('Actual:')[1].split("\nExpected: ")[1].split("\nDifference: ")[1]

    columns_Schema = Difference.replace("{","").replace("}","").replace("<class ","").replace(">","").replace("'","").split('),')
    columns_Schema = [item.split(',',1) for item in columns_Schema]
    for item in columns_Schema:
        item[0] = item[0].replace('(',"").lstrip()
        item[1] = item[1].replace("))",")").lstrip()

    Difference_dict = dict()
    for sub in columns_Schema:
        Difference_dict[sub[0]] = sub[1]

    Actual_Schema = Actual.replace("{","").replace("}","").replace("),",")>,").replace("],","]>,").replace(" <class ","").replace("'","").split(">,")
    Actual_Schema = [item.split(':',1) for item in Actual_Schema]
    for item in columns_Schema:
        item[0] = item[0].lstrip()
        item[1] = item[1].replace(">","").lstrip()

    Actual_dict = dict()
    for sub in Actual_Schema:
        Actual_dict[sub[0]] = sub[1]

    Expected_Schema = Expected.replace("{","").replace("}","").replace("),",")>,").replace("],","]>,").replace(" <class ","").replace("'","").split(">,")
    Expected_Schema = [item.split(':',1) for item in Expected_Schema]
    for item in columns_Schema:
        item[0] = item[0].lstrip()
        item[1] = item[1].replace(">","").lstrip()

    Expected_dict = dict()
    for sub in Expected_Schema:
        Expected_dict[sub[0]] = sub[1]

    Result_dict = {
        "Actual":Actual_dict,
        "Expected":Expected_dict,
        "Difference":Difference_dict
    }
    return(Result_dict)
    
 
 
 
 #------------------------------------------------ FUNCIÓN COMPARACIÓN ESQUEMA -------------------------------------------------------------------- 
 
 
def check_schema(df, df_s):
  try:
      df2 = df_s.convert(df)
      Resultado="La hoja tiene la estructura correcta  ✓ "
  except:
      error1 = sys.exc_info()
      try:
        df2 = df_s(df)
        Resultado="Ahora si, Bien"
      except:
        error = sys.exc_info()
        Resultado = Error_to_dict(error)
        print("                                             ")
        print("El excel no tiene la estructura esperada ✘")
        print("--------------------------------------------------")
        print(error1)
  return(Resultado,df2)
  
  
  
  
  
