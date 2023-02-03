 #::::::::::::::::::::::::::::::::::::::::: Drive - Data Cetelem  ::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
 
 #------------------------------------------------ LIBRERIAS -------------------------------------------------------------------- 

import typedframe
from typedframe import TypedDataFrame, UTC_DATE_TIME_DTYPE, DATE_TIME_DTYPE
import numpy as np
from google.colab import files
import sys


#------------------------------------------------ INDEXACIÓN HOJAS DOCUMENTO -------------------------------------------------------------------- 

class Hoja:
    
    hoja = {'esquema_hoja':
        {
        'Liquidaciones': ('Fch Entrada', 'Cod Origen', 'Desc Origen',
        'Fch Decision Final Adq', 'Sw Cliente Conocido',
        'Fch Posicion Actual', 'Autorizacion', 'Nif', 'Producto Comercial',
        'Importe Contado', 'Estado Decision Final Adq', 'Estado Operacion',
        'Desc Destalle Operacion', 'Vendedor',
        'Usuario Decision Final Adq', 'Tipo Instancia', 'Nombre Vendedor',
        'NUEVO-CONOCIDO', 'Fch Firma', 'PREMIUM', 'CONSO', 'PREMIUM/GOLD',
        'FIRMADOS EN DIA', 'DELAI OPERACIÓN (desde autoriz hasta decidida',
        'AUXILIAR OPERACIONES delai operación', 'AUXILIAR FIRMADOS DIA',
        'Liquidación'),
        'Llamadas_Hist': ('FECHA', 'CAMPAÑA', 'ASESOR', 'EXTENSION', 'COMBO', 'MOTIVO',
        'CIERRE', 'Ident LLAMADA', 'Identf CLIENTE', 'Duracion Llamada',
        'Hora Llamada', 'Hora en segundos', 'Identf AUDIO', 'NIF',
        'TIEMPO AUDIO', 'AUTENTICADOS', 'DESTINO', 'AGENCIA', 'ORIGEN',
        'IVR'),
        'Llamadas Mes': ('FECHA', 'CAMPAÑA', 'ASESOR', 'EXTENSION', 'COMBO', 'MOTIVO',
        'CIERRE', 'Ident LLAMADA', 'Identf CLIENTE', 'Duracion Llamada',
        'Hora Llamada', 'Hora en segundos', 'Identf AUDIO', 'NIF',
        'TIEMPO AUDIO', 'AUTENTICADOS', 'DESTINO', 'AGENCIA', 'ORIGEN',
        'IVR'),
        'Datos_Hist': ('Fch Entrada', 'Cod Origen', 'Desc Origen',
        'Fch Decision Final Adq', 'Sw Cliente Conocido',
        'Fch Posicion Actual', 'Autorizacion', 'Nif', 'Producto Comercial',
        'Importe Contado', 'Estado Decision Final Adq', 'Estado Operacion',
        'Desc Destalle Operacion', 'Vendedor',
        'Usuario Decision Final Adq', 'Tipo Instancia', 'Nombre Vendedor',
        'NUEVO-CONOCIDO', 'Fch Firma', 'PREMIUM', 'CONSO', 'PREMIUM/GOLD',
        'FIRMADOS EN DIA', 'DELAI OPERACIÓN (desde autoriz hasta decidida',
        'AUXILIAR OPERACIONES delai operación', 'AUXILIAR FIRMADOS DIA'),
        'Datos Mes': ('Fch Entrada', 'Cod Origen', 'Desc Origen',
        'Fch Decision Final Adq', 'Sw Cliente Conocido',
        'Fch Posicion Actual', 'Autorizacion', 'Nif', 'Producto Comercial',
        'Importe Contado', 'Estado Decision Final Adq', 'Estado Operacion',
        'Desc Destalle Operacion', 'Vendedor',
        'Usuario Decision Final Adq', 'Tipo Instancia', 'Nombre Vendedor',
        'NUEVO-CONOCIDO', 'Fch Firma', 'PREMIUM', 'CONSO', 'PREMIUM/GOLD',
        'FIRMADOS EN DIA', 'DELAI OPERACIÓN (desde autoriz hasta decidida',
        'AUXILIAR OPERACIONES delai operación', 'AUXILIAR FIRMADOS DIA'),
        'Stock_Hist': ('Fch Entrada', 'Cod Origen', 'Desc Origen',
        'Fch Decision Final Adq', 'Sw Cliente Conocido',
        'Fch Posicion Actual', 'Autorizacion', 'Nif', 'Producto Comercial',
        'Importe Contado', 'Estado Decision Final Adq', 'Estado Operacion',
        'Desc Destalle Operacion', 'Vendedor',
        'Usuario Decision Final Adq', 'Tipo Instancia', 'Nombre Vendedor',
        'NUEVO-CONOCIDO', 'Fch Firma', 'PREMIUM', 'CONSO', 'PREMIUM/GOLD',
        'FIRMADOS EN DIA', 'DELAI OPERACIÓN (desde autoriz hasta decidida',
        'AUXILIAR OPERACIONES delai operación', 'AUXILIAR FIRMADOS DIA'),
        'Stock': ('Fch Entrada', 'Cod Origen', 'Desc Origen',
        'Fch Decision Final Adq', 'Sw Cliente Conocido',
        'Fch Posicion Actual', 'Autorizacion', 'Nif', 'Producto Comercial',
        'Importe Contado', 'Estado Decision Final Adq', 'Estado Operacion',
        'Desc Destalle Operacion', 'Vendedor',
        'Usuario Decision Final Adq', 'Tipo Instancia', 'Nombre Vendedor',
        'NUEVO-CONOCIDO', 'Fch Firma', 'PREMIUM', 'CONSO', 'PREMIUM/GOLD',
        'FIRMADOS EN DIA', 'DELAI OPERACIÓN (desde autoriz hasta decidida',
        'AUXILIAR OPERACIONES delai operación', 'AUXILIAR FIRMADOS DIA'),
        'Stock Equipo Accom histórico': ('Usuario Decision Final Adq', 'Fch Decision Final Adq',
        'Estado Primera Decision', 'Nombre Vendedor', 'Producto Comercial',
        'Sw Cliente Conocido', 'Importe Crto', 'Fch Entrada', 'EQUIPO',
        'Usuario Primera Decision', 'NÚMERO', 'Familia Producto',
        'Autorizacion', 'Desc Familia Producto', 'Nombre Producto',
        'Estado Operacion', 'Desc Estado Operacion',
        'Desc Destalle Operacion', 'Estado Decision Final Adq',
        'Fch Posicion Actual', 'Fch Primera Decision', 'Codigo Seguro'),
        'Stock Equipo Accom': ('Usuario Decision Final Adq', 'Fch Decision Final Adq',
        'Estado Primera Decision', 'Nombre Vendedor', 'Producto Comercial',
        'Sw Cliente Conocido', 'Importe Crto', 'Fch Entrada', 'EQUIPO',
        'Usuario Primera Decision', 'NÚMERO', 'Familia Producto',
        'Autorizacion', 'Desc Familia Producto', 'Nombre Producto',
        'Estado Operacion', 'Desc Estado Operacion',
        'Desc Destalle Operacion', 'Estado Decision Final Adq',
        'Fch Posicion Actual', 'Fch Primera Decision', 'Codigo Seguro'),
        'Conexiones_Hist': ('Mes', 'Semana', 'Fecha', 'soc', 'fase', 'team', 'Agente',
        'Ll_Tot', 'Ll_desc_aten', 'min_hrlogin', 'max_hrlogout', 'T_Login',
        'T_efectivo', 'tready', 'Tvideo', 'Taudio', 'Tacw', 'jornada',
        'aprov', 'GAP AUDIO-VIDEO'),
        'Conexiones': ('Mes', 'Semana', 'Fecha', 'soc', 'fase', 'team', 'Agente',
        'Ll_Tot', 'Ll_desc_aten', 'min_hrlogin', 'max_hrlogout', 'T_Login',
        'T_efectivo', 'tready', 'Tvideo', 'Taudio', 'Tacw', 'jornada',
        'aprov', 'GAP AUDIO-VIDEO'),
        'Operaciones PE Mes': ('Cliente', 'Nif', 'Autorizacion', 'Cod origen',
        'Producto comercial', 'Importe contado SUM', 'Fch entrada',
        'Fch primera decision', 'Estado primera decision',
        'Fch decision final adq', 'Estado decision final adq',
        'Usuario decision final adq', 'Fch posicion actual',
        'Estado operacion'),
        'Reporting FIDI/TPT': ('xº', 'Autorizacion', 'Nif', 'Contrato', 'Tipo Instancia',
        'Producto Comercial', 'Estado Operacion', 'Imp Cru Rev',
        'Imp Limite Credito Rev', 'Imp Cma Rev', 'Importe Crto',
        'PRODUCTO', 'Usuario Financia Operacion', 'Fch Financiacion',
        'Desc Tipo Instancia')},
        
        'nombre_hoja':{'Liquidaciones':'Liquidaciones',
        'Llamadas_Hist': 'Llamadas_Hist',
        'Llamadas_Mes':'Lamadas Mes',
        'Datos_Hist':'Datos_Hist',
        'Datos_Mes':'Datos Mes',
        'Stock_Hist':'Stock_Hist',
        'Stock': 'Stock',
        'Stock_Accom_historico':'Stock Equipo Accom histórico',
        'Stock_Equipo_Accom': 'Stock Equipo Accom',
        'Conexiones_Hist':'Conexiones_Hist',
        'Conexiones':'Coneixiones',
        'Operaciones_PE_Mes': 'Operaciones PE Mes',
        'Reporting_FIDI_TPT':'Reporting FIDI/TPT'
            
        }
    }





#------------------------------------------------ HOJAS DOCUMENTO -------------------------------------------------------------------- 
###---------------- Hoja Liquidaciones ------------------###

class Liquidaciones(TypedDataFrame):
    
    schema = {

        'Fch Entrada': UTC_DATE_TIME_DTYPE,
        'Cod Origen':('T','E'),
        'Desc Origen':('OCTROI','WEB ( EXTERNO )'),
        'Fch Decision Final Adq':UTC_DATE_TIME_DTYPE,
        'Sw Cliente Conocido':('S','s'),
        'Fch Posicion Actual':UTC_DATE_TIME_DTYPE,
        'Autorizacion':np.int64,
        'Nif':str,
        'Producto Comercial':('DM','DMABS','DMF','DR', 'DRABS', 'DRF', 'MD', 'MDABS','MDF'),
        'Importe Contado':np.int64,
        'Estado Decision Final Adq':('AU', 'au'),
        'Estado Operacion':('FF', 'FN'),
        'Desc Destalle Operacion': ('OK','ok'),
        'Vendedor': np.int64,
        'Usuario Decision Final Adq': str,
        'Tipo Instancia':('F', 'f'), 
        'Nombre Vendedor': str,
        'NUEVO-CONOCIDO': ('CONOCIDO', 'NUEVO'),
        'Fch Firma': UTC_DATE_TIME_DTYPE,
	    'PREMIUM': ('PREMIUM','NO PREMIUM'),
	    'CONSO': str, 
        'PREMIUM/GOLD': str,
        'FIRMADOS EN DIA': str,
        'DELAI OPERACIÓN (desde autoriz hasta decidida': str ,
	    'AUXILIAR OPERACIONES delai operación':str,
	    'AUXILIAR FIRMADOS DIA': str,
	    'Liquidación': str
    } 
 


###---------------- Hoja Llamadas_Hist ------------------###

class Llamadas_Hist(TypedDataFrame):
    
    schema = {
        
        'FECHA': str,
        'CAMPAÑA':str,
        'ASESOR':str,
        'EXTENSION':np.int64,
        'COMBO':str,
        'MOTIVO':str,
        'CIERRE':str,
        'Ident LLAMADA':np.int64,
        'Identf CLIENTE':str,
        'Duracion Llamada':np.int64,
        'Hora Llamada': np.int64,
        'Hora en segundos':np.int64,
        'Identf AUDIO': str,
        'NIF': str,
        'TIEMPO AUDIO': np.int64,
        'AUTENTICADOS':str,
        'DESTINO': str,
        'AGENCIA': str,
        'ORIGEN': str,
        'IVR':str
    }



###---------------- Hoja Llamadas_Mes ------------------###

class Llamadas_Mes(TypedDataFrame):
    
    schema = {
        'FECHA': UTC_DATE_TIME_DTYPE,
        'CAMPAÑA':str,
        'ASESOR':str,
        'EXTENSION':np.int64,
        'COMBO':str,
        'MOTIVO':str,
        'CIERRE':str,
        'Ident LLAMADA':np.int64,
        'Identf CLIENTE':str,
        'Duracion Llamada':np.int64,
        'Hora Llamada': np.int64,
        'Hora en segundos':np.int64,
        'Identf AUDIO': str,
        'NIF': str,
        'TIEMPO AUDIO': np.int64,
        'AUTENTICADOS':str,
        'DESTINO': str,
        'AGENCIA': str,
        'ORIGEN': str,
        'IVR':str
    }
    
###---------------- Hoja Stock ------------------###
 
class Stock(TypedDataFrame):
    
    schema = {
        'Fch Entrada': UTC_DATE_TIME_DTYPE,
        'Cod Origen':('T','E'),
        'Desc Origen':('OCTROI','WEB ( EXTERNO )'),
        'Fch Decision Final Adq':UTC_DATE_TIME_DTYPE,
        'Sw Cliente Conocido': ('N','S'),
        'Fch Posicion Actual':UTC_DATE_TIME_DTYPE,
        'Autorizacion':np.int64,
        'Nif':str,
        'Producto Comercial': str,
        'Importe Contado':np.int64,
        'Estado Decision Final Adq': str,
        'Estado Operacion': str,
        'Desc Destalle Operacion': str,
        'Vendedor': np.int64,
        'Usuario Decision Final Adq': str,
        'Tipo Instancia':('F', 'f'), 
        'Nombre Vendedor': str,
        'NUEVO-CONOCIDO': ('CONOCIDO', 'NUEVO'),
        'Fch Firma': str,
	    'PREMIUM': ('PREMIUM', 'NO PREMIUM')
    }



###---------------- Hoja Stock_Hist ------------------###

class Stock_Hist(TypedDataFrame):
    
    schema = {
        'Fch Entrada': UTC_DATE_TIME_DTYPE,
        'Cod Origen':('T','E'),
        'Desc Origen':('OCTROI','WEB ( EXTERNO )'),
        'Fch Decision Final Adq':UTC_DATE_TIME_DTYPE,
        'Sw Cliente Conocido':('S','N'),
        'Fch Posicion Actual':UTC_DATE_TIME_DTYPE,
        'Autorizacion':np.int64,
        'Nif':str,
        'Producto Comercial':str,
        'Importe Contado':np.int64,
        'Estado Decision Final Adq':str,
        'Estado Operacion':str,
        'Desc Destalle Operacion': str,
        'Vendedor': np.int64,
        'Usuario Decision Final Adq': str,
        'Tipo Instancia':str, 
        'Nombre Vendedor': np.int64,
        'NUEVO-CONOCIDO': ('CONOCIDO', 'NUEVO'),
        'Fch Firma': UTC_DATE_TIME_DTYPE,
	    'PREMIUM': ('PREMIUM', 'NO PREMIUM')
    }


###---------------- Hoja Datos_Hist ------------------###

class Datos_Hist(TypedDataFrame):
    
    schema = {
        'Fch Entrada': UTC_DATE_TIME_DTYPE,
        'Cod Origen':('T','E'),
        'Desc Origen':('OCTROI','WEB ( EXTERNO )'),
        'Fch Decision Final Adq':UTC_DATE_TIME_DTYPE,
        'Sw Cliente Conocido':('S','N'),
        'Fch Posicion Actual':UTC_DATE_TIME_DTYPE,
        'Autorizacion':np.int64,
        'Nif':str,
        'Producto Comercial':str,
        'Importe Contado':np.int64,
        'Estado Decision Final Adq':str,
        'Estado Operacion':str,
        'Desc Destalle Operacion': str,
        'Vendedor': np.int64,
        'Usuario Decision Final Adq': str,
        'Tipo Instancia':('F', 'f'), 
        'Nombre Vendedor': str,
        'NUEVO-CONOCIDO': ('CONOCIDO', 'NUEVO'),
        'Fch Firma': UTC_DATE_TIME_DTYPE,
	    'PREMIUM': ('PREMIUM', 'NO PREMIUM')
    }


###---------------- Hoja Datos Mes ------------------###

class Datos_Mes(TypedDataFrame):
    
    schema = {
        'Fch Entrada': UTC_DATE_TIME_DTYPE,
        'Cod Origen':('T','E'),
        'Desc Origen':('OCTROI','WEB ( EXTERNO )'),
        'Fch Decision Final Adq':UTC_DATE_TIME_DTYPE,
        'Sw Cliente Conocido':('S','N'),
        'Fch Posicion Actual':UTC_DATE_TIME_DTYPE,
        'Autorizacion':np.int64,
        'Nif':str,
        'Producto Comercial':str,
        'Importe Contado':np.int64,
        'Estado Decision Final Adq':str,
        'Estado Operacion':str,
        'Desc Destalle Operacion': str,
        'Vendedor': np.int64,
        'Usuario Decision Final Adq': str,
        'Tipo Instancia':str, 
        'Nombre Vendedor': str,
        'NUEVO-CONOCIDO': ('CONOCIDO', 'NUEVO'),
        'Fch Firma': UTC_DATE_TIME_DTYPE,
	    'PREMIUM': ('PREMIUM', 'NO PREMIUM')
    }
    
    
    
    
###---------------- Hoja Stock_Hist ------------------###

class Stock_Hist(TypedDataFrame):
    
    schema = {
        'Fch Entrada': UTC_DATE_TIME_DTYPE,
        'Cod Origen':str,
        'Desc Origen':('OCTROI','WEB ( EXTERNO )'),
        'Fch Decision Final Adq':UTC_DATE_TIME_DTYPE,
        'Sw Cliente Conocido':str,
        'Fch Posicion Actual':UTC_DATE_TIME_DTYPE,
        'Autorizacion':np.int64,
        'Nif':str,
        'Producto Comercial':str,
        'Importe Contado':np.int64,
        'Estado Decision Final Adq': str,
        'Estado Operacion':str,
        'Desc Destalle Operacion': str,
        'Vendedor': np.int64,
        'Usuario Decision Final Adq': str,
        'Tipo Instancia':str, 
        'Nombre Vendedor': str,
        'NUEVO-CONOCIDO': ('CONOCIDO', 'NUEVO'),
        'Fch Firma': UTC_DATE_TIME_DTYPE,
	    'PREMIUM': ('PREMIUM', 'NO PREMIUM')
    }    
    
 
###---------------- Hoja Stock ------------------###

class Stock(TypedDataFrame):
    
    schema = {
        'Fch Entrada': UTC_DATE_TIME_DTYPE,
        'Cod Origen':str,
        'Desc Origen':('OCTROI','WEB ( EXTERNO )'),
        'Fch Decision Final Adq':UTC_DATE_TIME_DTYPE,
        'Sw Cliente Conocido':str,
        'Fch Posicion Actual':UTC_DATE_TIME_DTYPE,
        'Autorizacion':np.int64,
        'Nif':str,
        'Producto Comercial':str,
        'Importe Contado':np.int64,
        'Estado Decision Final Adq':str,
        'Estado Operacion':str,
        'Desc Destalle Operacion': str,
        'Vendedor': np.int64,
        'Usuario Decision Final Adq': str,
        'Tipo Instancia':str, 
        'Nombre Vendedor': str,
        'NUEVO-CONOCIDO': ('CONOCIDO', 'NUEVO'),
        'Fch Firma': UTC_DATE_TIME_DTYPE,
	    'PREMIUM': ('PREMIUM', 'NO PREMIUM')
    }    
 
 
   

###---------------- Hoja Stock Equipo Accom Histórico ------------------###

class Stock_Equipo_Accom_historico(TypedDataFrame):

    schema = {
        'Usuario Decision Final Adq': str,
        'Fch Decision Final Adq': DATE_TIME_DTYPE,
        'Estado Primera Decision': str,
        'Nombre Vendedor':str,
        'Producto Comercial': str,
        'Sw Cliente Conocido': str,
        'Importe Crto': np.int64,
        'Fch Entrada':DATE_TIME_DTYPE,
        'EQUIPO': str,
        'Usuario Primera Decision': str,
        'NÚMERO': np.int64,
        'Familia Producto': str,
        'Autorizacion': np.int64,
        'Desc Familia Producto': str,
        'Nombre Producto': str,
        'Estado Operacion':str,
        'Desc Estado Operacion':str,
        'Desc Destalle Operacion': str,
        'Estado Decision Final Adq': str,
        'Fch Posicion Actual':DATE_TIME_DTYPE,
	    'Fch Primera Decision':DATE_TIME_DTYPE ,
	    'Codigo Seguro': str
    }

###---------------- Hoja Stock Equipo Accom  ------------------###

class Stock_Equipo_Accom(TypedDataFrame):

    schema = {
        'Usuario Decision Final Adq': str,
        'Fch Decision Final Adq': DATE_TIME_DTYPE,
        'Estado Primera Decision': str,
        'Nombre Vendedor':str,
        'Producto Comercial': str,
        'Sw Cliente Conocido': str,
        'Importe Crto': np.int64,
        'Fch Entrada':DATE_TIME_DTYPE,
        'EQUIPO': str,
        'Usuario Primera Decision': str,
        'NÚMERO': np.int64,
        'Familia Producto': str,
        'Autorizacion': np.int64,
        'Desc Familia Producto': str,
        'Nombre Producto': str,
        'Estado Operacion':str,
        'Desc Estado Operacion':str,
        'Desc Destalle Operacion': str,
        'Estado Decision Final Adq': str,
        'Fch Posicion Actual':DATE_TIME_DTYPE,
	    'Fch Primera Decision':DATE_TIME_DTYPE ,
	    'Codigo Seguro': str
    }

###---------------- Hoja Conexiones Hist  ------------------###

class Conexiones_Hist(TypedDataFrame):
    schema = {
        'Mes':str,
        'Semana':np.int64,
        'Fecha':str,
        'soc':np.int64,
        'fase':np.int64,
        'team': str,
        'Agente':str,
        'Ll_Tot':np.int64,
        'Ll_desc_aten':np.int64,
        'min_hrlogin':str,
        'max_hrlogout':str,
        'T_Login':str,
        'T_efectivo':str,
        'tready':str,
        'Tvideo':str,
        'Taudio':str,
        'Tacw':str,
        'jornada':np.int64,
        'aprov':np.int64,
        'GAP AUDIO-VIDEO':str
    }



###---------------- Hoja Conexiones Hist  ------------------###

class Conexiones(TypedDataFrame):
    schema = {
        'Mes':str,
        'Semana':np.int64,
        'Fecha':str,
        'soc':np.int64,
        'fase':np.int64,
        'team': str,
        'Agente':str,
        'Ll_Tot':np.int64,
        'Ll_desc_aten':np.int64,
        'min_hrlogin':str,
        'max_hrlogout':str,
        'T_Login':str,
        'T_efectivo':str,
        'tready':str,
        'Tvideo':str,
        'Taudio':str,
        'Tacw':str,
        'jornada':np.int64,
        'aprov':np.int64,
        'GAP AUDIO-VIDEO':str
    }


###---------------- Hoja Operaciones PE Mes  ------------------###

class Operaciones_PE_Mes(TypedDataFrame):
    schema = {
        'Cliente':np.int64,
        'Nif':str,
        'Autorizacion':np.int64,
        'Cod origen':str,
        'Producto comercial':str,
        'Importe contado SUM': np.int64,
        'Fch entrada':str,
        'Fch primera decision':str,
        'Estado primera decision':str,
        'Fch decision final adq':str,
        'Estado decision final adq':str,
        'Usuario decision final adq':str,
        'Fch posicion actual':str,
        'Estado operacion':str
    }


###---------------- Hoja Reporting FIDI/TPT  ------------------###

class Reporting_FIDI_TPT(TypedDataFrame):
    schema = {
        'xº':np.int64,
        'Autorizacion':np.int64,
        'Nif':str,
        'Contrato': np.int64,
        'Tipo Instancia':str,
        'Producto comercial': str,
        'Estado Operacion':str,
        'Imp Cru Rev':str,
        'Imp Limite Credito Rev':str,
        'Imp Cma Rev':str,
        'Importe Crto':str,
        'PRODUCTO':str,
        'Usuario Financia Operacion':str,
        'Fch Financiacion':str,
        'Desc Tipo Instancia':str
    }


 #------------------------------------------------ FUNCIONES Y TRANSFORMACIÓN -------------------------------------------------------------------- 
 
 ###---------------- Hoja Stock ------------------###
"""
def transform(df):
    if df = 
        df['Vendedor'] = df['Vendedor'].fillna(0) 
        #df['FIRMADOS EN DIA'] = df['FIRMADOS EN DIA'].fillna(0)
        #df['DELAI OPERACIÓN (desde autoriz hasta decidida'] = df['DELAI OPERACIÓN (desde autoriz hasta decidida'].fillna(0)
        #df['AUXILIAR OPERACIONES delai operación'] = df['AUXILIAR OPERACIONES delai operación'].fillna(0)
        #df['AUXILIAR FIRMADOS DIA'] = df['AUXILIAR FIRMADOS DIA'].fillna(0) 
    elif df ==
    else:


 
"""
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
  
  
  
  
  
