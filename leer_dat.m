% Cargar el archivo .mat
data_struct = load('salida_mat\2024-10-03.mat');  % Cargar el archivo .mat como estructura

% Verificar si las variables necesarias existen en la estructura cargada
if isfield(data_struct, 'time_epoch') && isfield(data_struct, 'data')
    % Asignar las variables a nombres más fáciles de usar
    time_epoch = data_struct.time_epoch;  % Vector de tiempos
    data = data_struct.data;              % Matriz de datos numéricos
else
    error('El archivo .mat no contiene las variables esperadas.');
end

% Transponer time_epoch para que sea un vector columna
time_epoch = time_epoch(:);  % Esto asegura que time_epoch es un vector columna

% Convertir time_epoch a datetime
time_datetime = datetime(time_epoch, 'ConvertFrom', 'posixtime', 'Format', 'yyyy-MM-dd HH:mm:ss.SSSSSS');

% Crear la tabla con time_epoch (convertido a datetime) y los datos numéricos
data_table = [table(time_datetime), array2table(data)];

% Definir los nombres de las columnas
column_names = {'Time', 'Potencia', 'Paletas', 'Alabes', 'Pres_Abr_Pal', ...
                'Pres_Cerr_Pal', 'Pres_Abr_Alab', 'Pres_Cerr_Alab', 'Cont_Potencia', ...
                'Consigna_Pal', 'Consigna_Pot', 'Consigna_Alab', 'Salto_Reg', ...
                'Velocidad', 'Frecuencia', 'ModoPotCon', 'FaseDiv2'};

% Asignar nombres a las columnas de la tabla
data_table.Properties.VariableNames = column_names;

