import os
import shutil
import time

import tabula
import logging
import pandas as pd
import psycopg2

filelog = "log/info_" + time.strftime("%d-%m-%Y") + "_" + time.strftime("%H-%M-%S") + ".txt"
logging.basicConfig(filename=filelog, level=logging.DEBUG, format='%(asctime)s %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p')

#Eliminar salidas si existen
if os.path.isdir("output"):
    shutil.rmtree("output")

#Crear directorio de salida
os.mkdir("output")
parametros = ["Arsénico Total*", "Bario Total (Ba)*", "BTEX* (Benceno. Tolueno.\rEtilbenceno. Xilenos)",
              "Cadmio Total *", "Cobre Total*", "Cromo Total*", "Fenoles totales", "Grasas y Aceites*",
              "HAPs* (Hidrocarburos Aromáticos\rPoliciclicos)", "Hidrocarburos totales (TPH)*", "Hierro Total*",
              "Mercurio Organico", "Niquel Total*", "Plomo Total*", "Selenio Total*", "Zinc Total*", "Mercurio Total*",
              "Humedad Natural*"]

parametrosJSON = {
  "Arsénico Total*": "arsenico_",
  "Bario Total (Ba)*": "bario_",
  "BTEX* (Benceno. Tolueno.\rEtilbenceno. Xilenos)": "btex",
  "Cadmio Total *": "cadmio_",
  "Cobre Total*": "cobre_tot",
  "Cromo Total*": "cromo",
  "Fenoles totales": "fenoles_tot",
  "Grasas y Aceites*": "gya_",
  "HAPs* (Hidrocarburos Aromáticos\rPoliciclicos)": "hap",
  "Hidrocarburos totales (TPH)*": "hidrocar_tot_",
  "Hierro Total*": "hierro_tot",
  "Mercurio Organico": "merc_org",
  "Niquel Total*": "niquel_tot",
  "Plomo Total*": "plomo",
  "Selenio Total*": "selenio_",
  "Zinc Total*": "zinc_",
  "Mercurio Total*": "mercurio_",
  "Humedad Natural*": "humedad"
}

conn = psycopg2.connect("dbname='segral' user='varichem' host='' password=''")
conn.autocommit = True
cur = conn.cursor()

#Recorrer los archivos de files
for x in os.listdir('files/'):
    if x == '.DS_Store': continue
    print("[INFO] Analizando Directorio: {}".format(x))
    os.mkdir("output/{}".format(x))

    #Recorer los PDF dentro del directorio.
    for file in os.listdir('files/'+x):
        if file == '.DS_Store': continue
        print ("[INFO] Revision archivo {}".format(file))
        pathPDF = 'files/{}/{}'.format(x,file)
        extraerNombre = file.split(".pdf")
        FOLDER = x
        FILE = file
        print ("[INFO] Path completo {}".format(pathPDF))

        columnasPuntoA = []
        valuesPuntoA = []

        columnasPuntoB = []
        valuesPuntoB = []

        columnasLimite = []
        valuesLimite = []

        df = tabula.read_pdf(pathPDF, pages=1, area=(99.833,26.393,198.518,578.723), lattice=True, spreadsheet=True, silent=True, pandas_options={'header': None})

        columnasPuntoA.append('"muestreo_date"')
        valuesPuntoA.append("'" +  df.loc[2,3]  + "'")

        columnasPuntoB.append('"muestreo_date"')
        valuesPuntoB.append("'" +  df.loc[2,3]  + "'")

        columnasLimite.append('"muestreo_date"')
        valuesLimite.append("'" +  df.loc[2,3]  + "'")


        columnasPuntoA.append('"muestreo_place"')
        valuesPuntoA.append("'" + df.loc[7,3] + "'")

        columnasPuntoB.append('"muestreo_place"')
        valuesPuntoB.append("'" + df.loc[7,3] + "'")

        columnasLimite.append('"muestreo_place"')
        valuesLimite.append("'" + df.loc[7,3] + "'")

        #print("[INFO] Fecha de Muestreo: "+ df.loc[2,3] )
        #print("[INFO] Lugar de Muestreo: "+ df.loc[7,3] )
        #print("[INFO] Finalizacion Muestreo")

        #Exportar Tabla a CSV.
        #tabula.convert_into(pathPDF, "output/{}/{}".format(x, extraerNombre[0]+".csv"), pages=1, output_format="csv", pandas_options={'header': None}, area=(215.348, 27.923, 476.978, 584.078), java_options="-Dfile.encoding=UTF8", lattice=True)
        #dfTable = tabula.read_pdf(pathPDF, pages=1, pandas_options={'header': None}, area=(215.348, 27.923, 476.978, 584.078), java_options="-Dfile.encoding=UTF8", lattice=True)
        dfTable = tabula.read_pdf(pathPDF, pages=1, pandas_options={'header': None}, area=(203.873,27.923,521.347,584.078), java_options="-Dfile.encoding=UTF8", lattice=True)
        #print(dfTable.shape)
        #logging.info(pathPDF)
        #logging.info(str(dfTable.shape))
        #logging.info(str(dfTable.loc[1, 7]) + " - P1 Nombre1")
        #logging.info(str(dfTable.loc[2 , 7]) + " - P1 Nombre2")
        #logging.info(str(dfTable.loc[1, 8]) + " - P2 Nombre1")
        #logging.info(str(dfTable.loc[2, 8]) + " - P2 Nombre2")#

        #Folder
        columnasPuntoA.append('"folder"')
        valuesPuntoA.append("'"+FOLDER+"'")

        columnasPuntoB.append('"folder"')
        valuesPuntoB.append("'"+FOLDER+"'")

        columnasLimite.append('"folder"')
        valuesLimite.append("'"+FOLDER+"'")

        # File
        columnasPuntoA.append('"file_"')
        valuesPuntoA.append("'"+FILE+"'")

        columnasPuntoB.append('"file_"')
        valuesPuntoB.append("'"+FILE+"'")

        columnasLimite.append('"file_"')
        valuesLimite.append("'"+FILE+"'")

        # point Name
        columnasPuntoA.append('"point_name"')
        valuesPuntoA.append("'"+str(dfTable.loc[2, 7]) +"'")

        columnasPuntoB.append('"point_name"')
        valuesPuntoB.append("'"+str(dfTable.loc[2, 8]) +"'")

        columnasLimite.append('"point_name"')
        valuesLimite.append("'Limite'")


        for i in range(0, dfTable.shape[0]):
            if dfTable.loc[i,2] in parametros:
                #Contruir arreglo de Punto A
                columnasPuntoA.append('"'+parametrosJSON[dfTable.loc[i,2]]+'"')
                valuesPuntoA.append("'"+dfTable.loc[i,7].strip("<")+"'")

                # Contruir arreglo de Punto B
                columnasPuntoB.append('"'+parametrosJSON[dfTable.loc[i, 2]]+'"')
                valuesPuntoB.append("'"+dfTable.loc[i, 8].strip("<")+"'")

                # Contruir arreglo de Limite
                columnasLimite.append('"'+parametrosJSON[dfTable.loc[i, 2]]+'"')
                valuesLimite.append("'"+dfTable.loc[i, 5].strip("<")+"'")

        InsertColumnPuntoA = ",".join(columnasPuntoA)
        ValuesColumnsPuntoA = ",".join(valuesPuntoA)

        InsertColumnPuntoB = ",".join(columnasPuntoB)
        ValuesColumsPuntoB = ",".join(valuesPuntoB)

        InserColumnLimite = ",".join(columnasLimite)
        ValuesColumnLimite = ",".join(valuesLimite)

        cur.execute("INSERT INTO public.saga_file_upload ("+InsertColumnPuntoA+") VALUES ("+ValuesColumnsPuntoA+")")
        cur.execute("INSERT INTO public.saga_file_upload ("+InsertColumnPuntoB+") VALUES ("+ValuesColumsPuntoB+")")
        #cur.execute("INSERT INTO public.saga_file_upload ("+InserColumnLimite+") VALUES ("+ValuesColumnLimite+")")

        logging.info("FIN ----")
        #exit()

        '''
        java -jar tabula-java.jar  -a 99.833,26.393,198.518,578.723 -p 1 "$1" 
        java -jar tabula-java.jar  -a 215.348,27.923,476.978,584.078 -p 1 "$1" 
        '''



print(parametros)
