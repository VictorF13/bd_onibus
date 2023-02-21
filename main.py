# Importing libraries:
import pandas as pd
import numpy as np
from os import listdir

# Creating dataframes:
# 1. t_bus:
path = '../sample_data/ticketing'
files = listdir(path)
array = []

for file in files:
    df = pd.read_csv(path + '/' + file, index_col=None, header=0, parse_dates=['hora'])
    array.append(df)
df_ticketing = pd.concat(array, axis=0, ignore_index=True)

distinct = df_ticketing[['vehicleid', 'prefixo_carro']].drop_duplicates()
df_bus = distinct[(distinct['prefixo_carro'].notnull()) & (distinct['vehicleid'].notnull())]
df_bus = df_bus.astype({'vehicleid': int, 'prefixo_carro': int})
df_bus = df_bus.rename(columns={'vehicleid': 'id_veiculo', 'prefixo_carro': 'id_onibus'})
df_bus = df_bus.set_index('id_onibus')

# 2. t_route:
path = '../sample_data/gtfs/routes.txt'
df_route = pd.read_csv(path)
df_route = df_route[['route_id', 'route_long_name']]
df_route = df_route.rename(columns={'route_id': 'id_linha', 'route_long_name': 'ds_linha'})
df_route = df_route.set_index('id_linha')

# 3. t_trip:
path = '../sample_data/gtfs/trips.txt'
df_trip = pd.read_csv(path)
df_trip = df_trip[["route_id", "trip_id"]]
df_trip["trip_id"] = df_trip["trip_id"].str[-1:]
df_trip = df_trip.drop_duplicates()
df_trip["trip_id"] = df_trip["trip_id"].map({"I": 0, "V": 1})
df_trip.rename(columns={"route_id": "id_linha", "trip_id": "fl_sentido"}, inplace=True)
df_trip = df_trip.reset_index()
df_trip = df_trip[["id_linha", "fl_sentido"]]
df_trip.index.name = "id_rota"

# 4. t_table:
df_table = df_ticketing[["prefixo_carro", "linha", "sentido_viagem"]]
df_table = df_table[df_table["prefixo_carro"].notnull()].drop_duplicates()
df_table.rename(columns={"prefixo_carro": "id_onibus",
                         "linha": "id_linha", "sentido_viagem": "fl_sentido"}, inplace=True)
df_table["fl_sentido"] = df_table["fl_sentido"].map({"Ida": 0, "Volta": 1})
df_table = df_table.astype({"id_onibus": int, "id_linha": int, "fl_sentido": int})
df_trip_right = df_trip.copy()
df_trip_right["id_novo"] = df_trip_right.index
df_table = df_table.merge(df_trip_right, how="left", on=["id_linha", "fl_sentido"])
df_table = df_table[["id_onibus", "id_novo"]].rename(columns={"id_novo": "id_rota"})
df_table = df_table[df_table["id_rota"].notnull()]
df_table = df_table.reset_index()
df_table = df_table[["id_onibus", "id_rota"]].astype({"id_onibus": int, "id_rota": int})
df_table.index.name = "id_tabela"

# 5. t_stops:
path = '../sample_data/gtfs/stops.txt'
df_stops = pd.read_csv(path)
df_stops = df_stops[["stop_id", "stop_name", "stop_lat", "stop_lon"]]
df_stops.rename(columns={"stop_id": "id_parada", "stop_name": "ds_parada",
                         "stop_lat": "lat", "stop_lon": "long"}, inplace=True)
df_stops.set_index("id_parada", inplace=True)

# 6. t_control
path = '../sample_data/gtfs/stop_times.txt'
df_stop_time = pd.read_csv(path)
df_stop_time = df_stop_time[["trip_id", "stop_id", "stop_sequence"]]
df_stop_time["linha"] = df_stop_time["trip_id"].str[1:4]
df_stop_time["sentido"] = df_stop_time["trip_id"].str[-1:]
df_stop_time = df_stop_time.astype({"stop_id": int, "stop_sequence": int, "linha": int})
df_stop_time = df_stop_time.rename(columns={"stop_id": "id_parada", "stop_sequence": "ordem",
                                            "linha": "id_linha", "sentido": "fl_sentido"})
df_stop_time["fl_sentido"] = df_stop_time["fl_sentido"].map({"I": 0, "V": 1})
df_stop_time = df_stop_time.merge(df_trip_right, how="left", on=["id_linha", "fl_sentido"])
df_stop_time = df_stop_time[["trip_id", "id_parada", "ordem", "id_novo"]]
df_stop_time = df_stop_time.rename(columns={"id_novo": "id_rota"})
idx = df_stop_time.groupby("id_rota")["ordem"].transform(max) == df_stop_time["ordem"]
df_trip_largest = df_stop_time[idx].groupby('id_rota').first().reset_index()[["id_rota", "trip_id"]]
df_control = df_stop_time.merge(df_trip_largest, how="inner", on=["trip_id", "id_rota"])
df_control = df_control[["id_rota", "id_parada", "ordem"]]
df_control.index.name = "id_ponto_de_controle"
df_control = df_control.rename(columns={"ordem": "id_ordem"})

# 7. t_position
path = '../sample_data/gps'
files = listdir(path)
files.remove('NumLinhas-Veiculos.csv')
files.remove('Readme.txt')
array = []
for file in files:
    df = pd.read_csv(path + '/' + file, index_col=None, header=0, parse_dates=["hora"])
    array.append(df)
df_gps = pd.concat(array, axis=0, ignore_index=True)
df_gps.rename(columns={"vehicleid": "id_veiculo", "lon": "long", "hora": "dt_registro"}, inplace=True)
df_bus_right = df_bus.copy()
df_bus_right.reset_index(inplace=True)
df_gps = df_gps.merge(df_bus_right, on="id_veiculo", how="inner")
df_gps = df_gps[["id_onibus", "dt_registro", "lat", "long"]]
df_gps = df_gps.sort_values(by="dt_registro")
df_ticket = df_ticketing.copy()
df_ticket = df_ticket[["prefixo_carro", "hora", "lat", "lon", "linha", "sentido_viagem"]]
df_ticket = df_ticket[df_ticket["prefixo_carro"].notnull()]
df_ticket = df_ticket.rename(columns={"prefixo_carro": "id_onibus", "hora": "dt_registro", "linha": "id_linha",
                                      "sentido_viagem": "fl_sentido", "lon": "long"})
df_ticket = df_ticket.astype({"id_onibus": int})
df_ticket["fl_sentido"] = df_ticket["fl_sentido"].map({"Ida": 0, "Volta": 1})
df_ticket["dt_registro"] = pd.to_datetime(df_ticket["dt_registro"]).dt.tz_convert(None)
df_ticket = df_ticket.sort_values(by="dt_registro")
df_gps['id_onibus'] = df_gps['id_onibus'].astype(np.int64)
df_ticket['id_onibus'] = df_ticket['id_onibus'].astype(np.int64)
df_gps = pd.merge_asof(df_gps, df_ticket, on="dt_registro", by="id_onibus", direction="nearest")
df_gps = df_gps[["id_onibus", "dt_registro", "lat_x", "long_x", "id_linha", "fl_sentido"]]
df_gps = df_gps.rename(columns={"lat_x": "lat", "long_x": "long"})
df_position = pd.concat([df_gps, df_ticket], ignore_index=True)
df_position = df_position[df_position["lat"].notnull() & df_position["long"].notnull()]
df_trip_right = df_trip.copy()
df_trip_right = df_trip_right.reset_index()
df_position = df_position.merge(df_trip_right, how="left", on=["id_linha", "fl_sentido"])
df_position = df_position[["id_onibus", "dt_registro", "lat", "long", "id_rota"]]
df_table_right = df_table.copy()
df_table_right = df_table_right.reset_index()
df_position = df_position.merge(df_table_right, how="left", on=["id_onibus", "id_rota"])
df_position = df_position[["id_tabela", "dt_registro", "lat", "long"]]
df_position = df_position[df_position["id_tabela"].notnull()]
df_position = df_position.drop_duplicates()
df_position = df_position.reset_index()
df_position = df_position[["id_tabela", "dt_registro", "lat", "long"]]
df_position = df_position.astype({"id_tabela": int})
df_position.index.name = "id_posicao"

# 8. t_shape
path = '../sample_data/gtfs/shapes.txt'
df_shape = pd.read_csv(path)
df_shape["ID_LINHA"] = df_shape["shape_id"].str[5:8]
df_shape["FL_SENTIDO"] = df_shape["shape_id"].str[9].map({"I": 0, "V": 1})
df_shape = df_shape.rename(columns={"shape_pt_sequence": "SEQUENCIA", "shape_pt_lat": "LAT", "shape_pt_lon": "LONG"})
df_shape = df_shape.astype({"ID_LINHA": int})
df_shape = df_shape[["ID_LINHA", "FL_SENTIDO", "SEQUENCIA", "LAT", "LONG"]]
df_shape.index.name = "ID_SHAPE"

# 9. t_ticketing
df_ticketing = df_ticketing[["id", "hora", "prefixo_carro", "tipo_cartao", "integracao"]]
df_ticketing = df_ticketing[(df_ticketing["prefixo_carro"].notnull()) & (df_ticketing["id"].notnull())]
df_ticketing = df_ticketing.rename(columns={"id": "ID_USUARIO", "hora": "DT_BILHETAGEM", "prefixo_carro": "ID_ONIBUS",
                                            "tipo_cartao": "TIPO_BILHETAGEM", "integracao": "FL_INTEGRACAO"})
df_ticketing["FL_INTEGRACAO"] = df_ticketing["FL_INTEGRACAO"].map({"S": 1, "N": 0})
df_ticketing["DT_BILHETAGEM"] = pd.to_datetime(df_ticketing["DT_BILHETAGEM"]).dt.tz_convert(None)
df_ticketing = df_ticketing.astype({"ID_USUARIO": int, "ID_ONIBUS": int})
df_ticketing.index.name = "ID_BILHETAGEM"

# Creating files for each table:
df_bus.to_csv('../output_files/t_bus.csv')
df_route.to_csv('../output_files/t_route.csv')
df_trip.to_csv('../output_files/t_trip.csv')
df_table.to_csv('../output_files/t_table.csv')
df_stops.to_csv('../output_files/t_stops.csv')
df_control.to_csv('../output_files/t_control.csv')
df_position.to_csv('../output_files/t_position.csv')
df_shape.to_csv('../output_files/t_shape.csv')
df_ticketing.to_csv('../output_files/t_ticketing.csv')
