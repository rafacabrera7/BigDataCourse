import pyspark 
import re
import time

# Tiempo de inicio
start_time = time.time()

# Creación de un contexto de Spark
sc = pyspark.SparkContext()

# Función para limpiar el dataset nasdaq
def clean_nasdaq(line):
    try:
        fields = line.split(",")
        if len(fields) != 9:
            return False
        
        int(fields[2][:4])
        return True
    
    except:
        return False
    
# Función para limpiar el dataset company
def clean_company(line):
    try:
        fields = line.split("\t")
        if len(fields) != 5:
            return False
        
        return True
    
    except:
        return False

# Importamos los datasets
data = sc.textFile("input/NASDAQsample.csv")
data2 = sc.textFile("input/companylist.tsv")
    
# Transformamos cada dataset con las funciones creadas anteriormente
nasdaq = data.filter(clean_nasdaq).map(lambda x: x.split(","))
company = data2.filter(clean_company).map(lambda x: x.split("\t"))

# Filtramos las columnas que nos interesan del dataset company
# 'simbolo', 'sector'
sectors = company.map(lambda x: (x[0], x[3]))

# Filtramos las columnas que nos interesan del dataset nasdaq,
# 'simbolo', 'fecha' pero solo el año y 'volumen de operaciones' 
num_opr = nasdaq.map(lambda x: ((x[1], x[2][:4]), int(x[7])))

# Utilizamos el simbolo como clave para poder unir los datasets
sectors2 = sectors.map(lambda x: (x[0], x[1]))
num_opr2 = num_opr.map(lambda x: (x[0][0], (x[0][1], x[1])))

# Unimos los datasets por la clave 'simbolo'
joined_data = num_opr2.join(sectors2)

# Extraer solo el año, el sector y el volumen de operaciones
# y calcular el total de volumen de operaciones por año y sector
total_volume = joined_data.map(lambda x: ((x[1][0][0], x[1][1]), x[1][0][1])).reduceByKey(lambda x, y: x + y)    

# Encontrar el sector con el mayor volumen de operaciones para cada año
max_volume_year = total_volume.map(lambda x: ((x[0][0], (x[0][1], x[1])))).reduceByKey(lambda x, y: x if x[1] > y[1] else y)

# Guardar los resultados
max_volume_year.saveAsTextFile("1_out.txt")

# Tiempo de finalización
end_time = time.time()

# Tiempo total de ejecución
print('-----------------------------------------\n')
print("Tiempo de ejecución: ", end_time - start_time)
print('\n-----------------------------------------')
