import os
import subprocess
from nptdms import TdmsFile
import pandas as pd
import json
import shutil
from scipy.io import savemat
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm


def decompress_zip_files(input_folder, output_folder, selected_files):
    """
    Descomprime los archivos ZIP seleccionados directamente en la carpeta de salida sin crear subcarpetas.
    Si hay conflictos de nombres, los archivos se renombran automáticamente.
    """
    # Verificar que 7z está instalado
    if shutil.which('7z') is None:
        raise EnvironmentError("El programa '7z' no está instalado o no está en el PATH.")

    os.makedirs(output_folder, exist_ok=True)

    for zip_file in selected_files:
        zip_path = os.path.join(input_folder, zip_file)
        print(f"Procesando archivo: {zip_file}")

        try:
            # Descomprimir utilizando 7zip directamente en la carpeta de salida
            subprocess.run(['7z', 'x', zip_path, f'-o{output_folder}', '-y'], check=True)

            # Resolver posibles conflictos de nombres
            for root, _, files in os.walk(output_folder):
                for file in files:
                    src_path = os.path.join(root, file)
                    dest_path = os.path.join(output_folder, file)

                    # Renombrar si hay conflictos de nombres
                    if src_path != dest_path:
                        if os.path.exists(dest_path):
                            base, ext = os.path.splitext(file)
                            counter = 1
                            while os.path.exists(dest_path):
                                dest_path = os.path.join(output_folder, f"{base}_{counter}{ext}")
                                counter += 1
                        shutil.move(src_path, dest_path)

            # Eliminar subcarpetas vacías
            for root, dirs, _ in os.walk(output_folder):
                for dir in dirs:
                    dir_path = os.path.join(root, dir)
                    if not os.listdir(dir_path):  # Solo eliminar si está vacía
                        shutil.rmtree(dir_path)

        except subprocess.CalledProcessError as e:
            print(f"Error al descomprimir {zip_file}: {e}")
        except Exception as e:
            print(f"Error procesando {zip_file}: {e}")

def convertir_tdms_a_csv(archivo_tdms, carpeta_salida):
    try:
        # Leer el archivo TDMS
        tdms_file = TdmsFile.read(archivo_tdms)

        # Obtener el único grupo (si solo hay uno)
        grupo = tdms_file.groups()[0]

        # Crear un diccionario para almacenar los datos de los canales
        data_dict = {}

        for canal in grupo.channels():
            nombre_canal = canal.name
            data_dict[nombre_canal] = canal.data

        # Crear un DataFrame con los datos
        df = pd.DataFrame(data_dict)

        # Crear el nombre del archivo CSV (el mismo nombre que el archivo TDMS, pero con extensión .csv)
        nombre_archivo_csv = os.path.splitext(os.path.basename(archivo_tdms))[0] + ".csv"
        ruta_archivo_csv = os.path.join(carpeta_salida, nombre_archivo_csv)

        # Guardar el DataFrame como CSV
        df.to_csv(ruta_archivo_csv, index=False, sep=';')


        # Verificar que el archivo CSV existe antes de eliminar los archivos TDMS
        if os.path.exists(ruta_archivo_csv):
            os.remove(archivo_tdms)

            # Eliminar el archivo .tdms_index si existe
            archivo_tdms_index = archivo_tdms + '_index'
            if os.path.exists(archivo_tdms_index):
                os.remove(archivo_tdms_index)
        else:
            print(f"Error: No se pudo crear el archivo CSV {ruta_archivo_csv}. TDMS no eliminado.")

    except Exception as e:
        print(f"Error al convertir TDMS a CSV: {e}")

def procesar_archivos_tdms(carpeta_tdms, carpeta_salida):
    # Verificar que la carpeta existe
    if not os.path.exists(carpeta_tdms):
        print(f"La carpeta {carpeta_tdms} no existe.")
        return

    # Verificar que la carpeta de salida existe, si no, crearla
    if not os.path.exists(carpeta_salida):
        os.makedirs(carpeta_salida)

    # Listar archivos TDMS en la carpeta
    archivos_tdms = [f for f in os.listdir(carpeta_tdms) if f.endswith(".tdms")]

    if not archivos_tdms:
        print(f"No se encontraron archivos TDMS en la carpeta {carpeta_tdms}.")
        return

    # Iterar sobre todos los archivos TDMS
    for archivo in archivos_tdms:
        archivo_tdms = os.path.join(carpeta_tdms, archivo)
        convertir_tdms_a_csv(archivo_tdms, carpeta_salida)

def procesar_archivos_tdms_paralelo(carpeta_tdms, num_workers=4):
    """
    Procesa los archivos TDMS en paralelo para reducir el tiempo total de ejecución.
    Muestra una barra de progreso dinámica.
    
    Parámetros:
    carpeta_tdms (str): Carpeta donde se encuentran los archivos TDMS.
    num_workers (int): Número de hilos (o procesos) a usar para el procesamiento paralelo.
    """
    # Verificar que la carpeta existe
    if not os.path.exists(carpeta_tdms):
        print(f"La carpeta {carpeta_tdms} no existe.")
        return

    # Lista de archivos TDMS en la carpeta
    archivos_tdms = [
        os.path.join(carpeta_tdms, archivo)
        for archivo in os.listdir(carpeta_tdms)
        if archivo.endswith(".tdms")
    ]

    if not archivos_tdms:
        print(f"No se encontraron archivos TDMS en la carpeta {carpeta_tdms}.")
        return

    # Crear una barra de progreso
    with tqdm(total=len(archivos_tdms), desc="Procesando archivos TDMS", unit="archivo") as barra:
        # Crear un pool de hilos para procesamiento paralelo
        with ThreadPoolExecutor(max_workers=num_workers) as executor:
            # Enviar tareas al pool
            futuros = {
                executor.submit(convertir_tdms_a_csv, archivo, carpeta_tdms): archivo
                for archivo in archivos_tdms
            }

            # Esperar a que se completen todas las tareas
            for futuro in as_completed(futuros):
                archivo = futuros[futuro]
                try:
                    futuro.result()  # Obtener el resultado de la tarea
                except Exception as e:
                    print(f"\nError procesando {archivo}: {e}")  # Mostrar errores en una nueva línea
                finally:
                    barra.update(1)  # Incrementar la barra de progreso

def ordenar_y_agrupado_por_dia(input_folder):
    """
    Procesa archivos CSV por hora, agrupa por día y maneja archivos incompletos.
    Si un archivo está incompleto, se guarda en una carpeta temporal. Luego se completa
    con los datos de la siguiente ejecución y se mueve a la carpeta de salida.
    
    Parámetros:
    input_folder (str): Carpeta donde se encuentran los archivos CSV.
    """
    temp_folder = os.path.join(input_folder, 'temp')
    
    # Verificar si la carpeta temporal existe, si no la creamos
    if not os.path.exists(temp_folder):
        os.makedirs(temp_folder)

    # Obtener la lista de todos los archivos CSV en la carpeta de entrada
    csv_files = [
        os.path.join(root, file)
        for root, _, files in os.walk(input_folder)
        for file in files if file.endswith(".csv")
    ]

    # Verificar si hay archivos CSV
    if not csv_files:
        print("No se encontraron archivos CSV en la carpeta especificada.")
        return

    # Crear un diccionario para almacenar temporalmente los datos por día
    datos_por_dia = {}

    # Procesar cada archivo CSV individualmente
    for file in csv_files:
        # Leer el archivo CSV por partes para manejar archivos grandes
        for chunk in pd.read_csv(file, delimiter=";", decimal=",", parse_dates=['Time'], chunksize=10000):
            # Ordenar los datos por fecha y hora
            chunk.sort_values(by='Time', inplace=True)

            # Extraer la fecha de la columna 'Time'
            chunk['Date'] = chunk['Time'].dt.date

            # Agrupar los datos por día y agregar al diccionario
            for date, group in chunk.groupby('Date'):
                if date not in datos_por_dia:
                    datos_por_dia[date] = []
                datos_por_dia[date].append(group)

    # Guardar los datos agrupados por día
    for date, groups in tqdm(datos_por_dia.items(), desc="Concatenando archivos por día", unit="día"):
        # Concatenar los grupos del mismo día
        daily_data = pd.concat(groups, ignore_index=True)

        # Eliminar la columna 'Date' antes de guardar
        daily_data.drop('Date', axis=1, inplace=True)

        # Crear el nombre del archivo de salida
        output_file = os.path.join(input_folder, f"{date}.csv")

        # Guardar el archivo CSV del día
        daily_data.to_csv(output_file, sep=";", decimal=",", index=False)

        # Verificar si el archivo está completo hasta las 23:59:59
        last_time = daily_data['Time'].max()
        if last_time.hour != 23 or last_time.minute != 59 or last_time.second != 59:
            # Mover el archivo incompleto a la carpeta temporal
            temp_file = os.path.join(temp_folder, f"{date}.temp.csv")
            shutil.copy(output_file, temp_file)


    # Verificar si el archivo temporal de la ejecución anterior existe
    for temp_file_name in os.listdir(temp_folder):
        temp_file_path = os.path.join(temp_folder, temp_file_name)
        if os.path.exists(temp_file_path):
            # Verificar si ya existe un archivo con el mismo nombre en la carpeta de origen
            matching_file = os.path.join(input_folder, temp_file_name)
            if os.path.exists(matching_file):
                # Concatenar los archivos temporal y de salida
                temp_data = pd.read_csv(temp_file_path, delimiter=";", decimal=",")
                output_data = pd.read_csv(matching_file, delimiter=";", decimal=",")

                # Concatenar los datos y guardar el archivo final
                combined_data = pd.concat([output_data, temp_data], ignore_index=True)
                combined_data.to_csv(matching_file, sep=";", decimal=",", index=False)

                # Eliminar el archivo temporal
                os.remove(temp_file_path)
                
                # Mover el archivo combinado a la carpeta de salida
                shutil.move(matching_file, input_folder)
                break

    # Eliminar los archivos CSV procesados
    eliminar_archivos_csv(csv_files)

def eliminar_archivos_csv(csv_files):
    for file in csv_files:
        if os.path.exists(file):
            os.remove(file)

def csv_to_mat(folder_path):
    """
    Convierte todos los archivos CSV en la carpeta especificada a formato .mat.
    La columna 'Time' se convierte a formato epoch con precisión en milisegundos.
    Después de la conversión, se eliminan los archivos CSV originales.
    
    Parámetros:
    folder_path (str): Ruta de la carpeta que contiene los archivos CSV.
    """
    # Verificar si la carpeta existe
    if not os.path.exists(folder_path):
        print(f"La carpeta {folder_path} no existe.")
        return
    
    # Obtener todos los archivos CSV en la carpeta
    csv_files = [f for f in os.listdir(folder_path) if f.endswith('.csv')]

    if not csv_files:
        print(f"No se encontraron archivos CSV en la carpeta {folder_path}.")
        return

    for csv_file in tqdm(csv_files, desc="Convirtiendo archivos", unit="archivo"):
        input_file = os.path.join(folder_path, csv_file)
        output_file = os.path.join(folder_path, f"{os.path.splitext(csv_file)[0]}.mat")
        
        # Paso 1: Leer el archivo CSV
        data = pd.read_csv(input_file, delimiter=";", decimal=".")
        
        # Paso 2: Convertir la columna 'Time' a formato epoch (segundos desde 1970-01-01) con precisión en milisegundos
        data["Time"] = pd.to_datetime(data["Time"])  # Convertir a formato datetime
        data["Time_epoch"] = (data["Time"] - pd.Timestamp("1970-01-01")) / pd.Timedelta("1s")  # Usar fracciones de segundo
        
        # Paso 3: Crear el diccionario para guardar en .mat
        mat_data = {
            "time_epoch": data["Time_epoch"].values,  # Fechas con milisegundos
            "data": data.drop(columns=["Time", "Time_epoch"]).values  # Solo los datos numéricos
        }
        
        # Paso 4: Guardar el archivo .mat
        savemat(output_file, mat_data)
        
        # Eliminar el archivo CSV original
        os.remove(input_file)

def load_config(config_path):
    """
    Carga la configuración desde el archivo JSON. Si no existe, devuelve un diccionario vacío.
    """
    if os.path.exists(config_path):
        with open(config_path, 'r') as f:
            return json.load(f)
    return {}

def save_config(config, config_path):
    """
    Guarda la configuración en un archivo JSON.
    """
    with open(config_path, 'w') as f:
        json.dump(config, f, indent=4)

def get_folders_from_user(config):
    """
    Solicita las carpetas de entrada y salida al usuario si no están definidas en la configuración.
    """
    if 'input_folder' not in config:
        config['input_folder'] = input("Ingrese la carpeta de entrada: ").strip()
    if 'output_folder' not in config:
        config['output_folder'] = input("Ingrese la carpeta de salida: ").strip()
    return config

def select_processing_option(input_folder, config):
    """
    Muestra las opciones al usuario para seleccionar cómo procesar los archivos.
    """
    print("Seleccione una opción:")
    print("1. Procesar a partir del último archivo procesado")
    print("2. Procesar un archivo específico")
    print("3. Procesar un rango de archivos")
    option = input("Ingrese el número de su elección: ").strip()

    zip_files = sorted([f for f in os.listdir(input_folder) if f.endswith('.zip')])
    last_processed_file = config.get('last_processed_file', None)

    if option == "1":
        if last_processed_file:
            start_index = zip_files.index(last_processed_file) + 1
            return zip_files[start_index:]
        else:
            print("No hay registro de un último archivo procesado.")
            return zip_files  # Procesar todos los archivos
    elif option == "2":
        print("\nArchivos disponibles:")
        for idx, f in enumerate(zip_files, start=1):
            print(f"{idx}. {f}")
        file_index = int(input("Ingrese el número del archivo a procesar: ")) - 1
        return [zip_files[file_index]]  # Procesar solo ese archivo
    elif option == "3":
        print("\nArchivos disponibles:")
        for idx, f in enumerate(zip_files, start=1):
            print(f"{idx}. {f}")
        start_index = int(input("Ingrese el número del primer archivo del rango: ")) - 1
        end_index = int(input("Ingrese el número del último archivo del rango: ")) - 1
        return zip_files[start_index:end_index + 1]
    else:
        print("Opción no válida. Intente de nuevo.")
        return select_processing_option(input_folder, config)

def main():
    config_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), "config.json")

    # Cargar configuración
    config = load_config(config_path)

    # Solicitar carpetas si no están en la configuración
    config = get_folders_from_user(config)

    # Guardar configuración actualizada
    save_config(config, config_path)

    input_folder = config['input_folder']
    output_folder = config['output_folder']
    daily_folder = os.path.join(output_folder, "diarios")  # Carpeta para los archivos diarios

    # Seleccionar archivos a procesar
    files_to_process = select_processing_option(input_folder, config)

    if not files_to_process:
        print("No hay archivos para procesar.")
        return

    # Procesar cada archivo seleccionado
    decompress_zip_files(input_folder, output_folder, files_to_process)

    # Actualizar el último archivo procesado
    config['last_processed_file'] = files_to_process[-1]  # Se actualiza con el último archivo procesado
    save_config(config, config_path)

    # Procesar archivos TDMS en paralelo
    procesar_archivos_tdms_paralelo(output_folder, num_workers=4)

    # Ejecutar la función para ordenar y agrupar por día
    ordenar_y_agrupado_por_dia(output_folder)

    #Convertir de csv a mat
    csv_to_mat(output_folder)




if __name__ == "__main__":
    main()
