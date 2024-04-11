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
# 'simbolo', 'fecha' y 'precio apertura' y 'precio cierre' 
num_opr = nasdaq.map(lambda x: ((x[1], (x[2][:4])), (((x[2][5:7]),(x[2][8:])), (float(x[3]), float(x[6])))))

# Agrupar los datos por símbolo y año, tendremos el ('simbolo', 'año') como clave
# y una lista con los meses y días, y sus correspondientes precios de apertura y cierre como valor
grouped_data = num_opr.map(lambda x: ((x[0][0], x[0][1]), [x[1]])) \
                      .reduceByKey(lambda a, b: a + b)

#result = grouped_data.collect()
#for i in result:
#    print(i)

# Funcion para calcular el porcentaje de crecimiento para cada empresa en cada año
def calculate_growth(data):
    sorted_data = sorted(data, key=lambda x: x[0])  # Ordenar por fecha
    opening_price = sorted_data[0][1][0]            # Precio de apertura el primer día
    closing_price = sorted_data[-1][1][1]           # Precio de cierre el último día

    # Manejar el caso donde el precio de apertura es cero
    # en este caso ignoramos estas empresas
    if opening_price == 0:  
        return None 
    
    # Manejar si el precio de apertura es mayor que el de cierre
    # o si son iguales (no hay crecimiento), ignoramos estas empresas
    if opening_price >= closing_price:
        return None
    
    # Calcular el porcentaje de crecimiento
    growth_percentage = ((closing_price - opening_price) / opening_price) * 100
    
    return (sorted_data[0][0], growth_percentage)

# Aplicar la función calculate_growth a cada empresa
growth_data = grouped_data.mapValues(calculate_growth).filter(lambda x: x[1] is not None)

#reuslt2 = growth_data.collect()
#for i in reuslt2:
#    print(i)

# Utilizamos el simbolo como clave para unir los datos
# 'simbolo', ('año', 'porcentaje de crecimiento') 
growth_data2 = growth_data.map(lambda x: (x[0][0], (x[0][1], x[1][1])))  
# 'simbolo', 'sector'
sectors2 = sectors.map(lambda x: (x[0], x[1])) 

# Unir los datos por el símbolo, tendremos ('simbolo', (('año', 'porcentaje de crecimiento'), 'sector'))
joined_data = growth_data2.join(sectors2)

#result3 = joined_data.collect()
#for i in result3:
#    print(i)

# Agrupar los datos por sector y año, tendremos (('sector', 'año'), ('simbolo', 'porcentaje de crecimiento'))
grouped_sector_year = joined_data.map(lambda x: ((x[1][1], x[1][0][0]), (x[0], x[1][0][1]))).sortBy(lambda x: x[0])

#result4 = grouped_sector_year.collect()
#for i in result4:
#    print(i)

# Encontrar el porcentaje de crecimiento máximo para cada sector y empresa 
# para encontrar que empresa creció más por año
max_growth = grouped_sector_year.reduceByKey(lambda x, y: x if x[1] > y[1] else y)
max_growth = max_growth.map(lambda x: (x[0][0], x[0][1], x[1][0], str(x[1][1]) + "%")).sortBy(lambda x: x[0])

#result5 = max_growth.collect()
#for i in result5:
#    print(i)

# Guardar los resultados
max_growth.saveAsTextFile("2_out.txt")

# Tiempo de finalización
end_time = time.time()

# Tiempo total de ejecución
print('-----------------------------------------\n')
print("Tiempo de ejecución: ", end_time - start_time)
print('\n-----------------------------------------')