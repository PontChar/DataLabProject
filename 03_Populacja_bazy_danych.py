#!/usr/bin/env python
# coding: utf-8

#  # Opis notatnika
#  W poprzednich krokach pobraliśmy dane oraz przygotowaliśmy bazę `Postgres` na import. Głównym celem w tym notatniku jest  odpowiednie dostosowanie struktury danych z plików źródłowych do formatu zgodnego z `Postgres`, a następnie wgranie ich na nasz serwer. Dzięki temu w późniejszych krokach możemy niezależnie użyć danych do analizy czy raportowania.
#  
#  Ponownie wcielasz się w rolę Data Engineera, którego zadaniem jest zasilenie bay danych pobranymi danymi. Bez poprawnego załadowania danych nie będziesz w stanie dokonać analizy eksploracyjnej, która jest jednym z wymagań dostarczonych przez klienta.
# 
#  Przy wykonywaniu tego notebooka przydadzą się poniższe elementy kursu oraz materiały dodatkowe:
#  * `SQL - analiza danych > Zjazd 1 - materiały dodatkowe > Export danych z DB > Python` - w celu użycia połączenia razem z `Pandas`,
#  * https://docs.sqlalchemy.org/en/14/core/engines.html - w celu uzupełnienia konfiguracji `Pandas` do `PostgerSQL`,
#  * https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.to_sql.html - eksport danych z `Pandas` na bazę danych.
# 
#  > Uwaga: Ze względu na wolumen danych zawarty w pliku `flight.csv`, wykonanie tego notatnika może zająć nawet kilkadziesiąt minut lub więcej!

# ## Połączenie z bazą danych
# Tutaj uzupełnij konfigurację połączenia tworząc zmienne takie jak:
# - `username` - nazwa użytkownika bazy,
# - `password` - hasło do bazy,
# - `host` - adres naszej bazy danych, jeśli baza jest postawiona na naszej maszynie wtedy będzie to po prostu `localhost`,
# - `database` - nazwa bazy danych np. `postgresql`
# - `port` - domyślnie `5432`
# 
# > Przetrzymywanie hasła w ten sposób nie jest bezpieczne, co było zaznaczane w trakcie kursu. Lepszym sposobem jest używanie zmiennych środowiskowych, ale na nasze potrzeby nie jest to potrzebne. Dla osób chcących zapoznać się z taką formą zalecamy ten artykuł - [klik](https://developer.vonage.com/blog/21/10/01/python-environment-variables-a-primer).

# In[1]:


username = 'postgres'
password = 'junior21'

host = 'localhost'
database = 'postgresql'
port = 5432


#  Z pomocą artykułu [click](https://docs.sqlalchemy.org/en/14/core/engines.html) stwórz zmienne `url` oraz `engine`. Zgodnie z dokumentacją `Pandas`, zmienna `engine` będzie potrzebna, by móc wyeksportować dane na serwer `SQL`.

# W tym miejscu stwórz zmienne `url` oraz `engine`
# > Wskazówka: Zmienna `url` powinna być stworzona zgodnie ze schematem jak we wcześniej podanym artykule, jednak powinna używać zmiennych zdefiniowanych wyżej.

# In[2]:


import psycopg2
from tqdm import tqdm
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.engine import URL
url = URL.create(
    "postgresql",
    username=username,
    password=password, 
    host=host,
    database=database,
)
engine = create_engine(url)


#  # Załadowanie ramek do obszaru roboczego
#  Uzupełnij implementacje funkcji `load_raw_data`, która przyjmuje jeden parametr:
#  * `file_name` - nazwa pliku do zaczytania
#  Jej zadaniem jest wczytanie surowego pliku, zmodyfikowanie nazw kolumn z `NAZWA_KOLUMNY` na `nazwa_kolumny` oraz zwrócenie tak zmodyfikowanej ramki danych
# 
#  Mogą się przydać poniższe element kursu:
#  - `Python-Analiza danych -> Dzień 5 - Pandas -> Obróbka danych - częsć 1`
#  - `Python-Analiza danych -> Przygotowanie do zjazdu 3 -> Wstęp do Pandas -> Wczytywanie danych do Pandas` - jakie kodowanie mają pliki?

# In[3]:


def load_raw_data(file_name):
    df_raw = pd.read_csv(
                    f'C:/Users/patry/OneDrive/Desktop/Koncowego_sprawdzony/data/raw/{file_name}', 
                    sep=';', 
                    decimal='.' 
                    )
    df_raw.columns = df_raw.columns.str.lower()
    return df_raw


#  # Zaczytanie poszczególnych plików do ramek
# 
#  W tym miejscu zaczytaj poszczególne pliki do ramek

# In[4]:


aircraft_df = load_raw_data('aircraft.csv')
aircraft_df


# In[5]:


airport_list_df = load_raw_data('airport_list.csv')
airport_list_df


# In[6]:


airport_weather_df = load_raw_data('airport_weather.csv')
airport_weather_df


# In[7]:


flight_df = load_raw_data('flight.csv')
flight_df


#  # Eksport danych na bazę
#  Zapoznaj się z dokumentacją metody `Pandas` - [to_sql](https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.to_sql.html), której zadaniem jest wyeksportowanie ramki na bazę danych.
#  Zwróć szczególną uwagę na poniższe parametry:
#  * `if_exists` - jak ma się zachować metoda, gdy ładuje dane na bazę,
#  * `con` - połączenie do bazy,
#  * `name` - nazwa tabeli, do której ramka ma zostać wgrana,
#  * `index` - czy dodawać index z ramki jako kolumnę,
#  * `chunksize` - maksymalna liczba wierszy wgrywana za jednym razem.
# 
#  > **Uwaga:** 
#  > Przed eksportem upewnij się, że tabela jest pusta. Zwróć uwagę na pewną subtelną różnicę pomiędzy wyglądem ramki oraz tabeli docelowej na bazie danych.
# 
# Następnie uzupełnij implementację metody `export_table_to_db`, która przyjmuje dwa argumenty:
#  * `df` - ramka danych do eksportu,
#  * `table_name` - nazwa ramki na bazie.
# 
# Zalecamy, aby dodać do metody informację, która ramka jest aktualnie ładowana np.:
#  `Loading data into {table_name}...`
#  > Ze względu na rozmiar ramki `flight_df`, proces ten może potrwać nawet kilkadziesiąt minut! Z tego względu, na potrzeby testów, zalecamy przekazanie do procedury `export_table_to_db` np. pierwszych 5 wierszy, aby sprawdzić, czy działa, a potem wgrać cały zestaw danych - pamiętając o upszednim usunięciu tamtych.

# In[8]:


def export_table_to_db(df,table_name):
    df.to_sql(table_name, con=engine, if_exists='append', index=False, chunksize=None)
    print(f'Loading data into {table_name}...')
    output = engine.execute(f'SELECT * FROM {table_name}').fetchall()
    return output


#  ## Wgrywanie danych

#  ### Wgranie `aircraft_df` do tabeli `aircraft`

# In[9]:


export_table_to_db(aircraft_df,'aircraft')


#  ### Wgranie `airport_weather_df` do tabeli `airport_weather`

# In[10]:


export_table_to_db(airport_weather_df,'airport_weather')


#  ### Wgranie `flight_df` do tabeli `flight`
#  > Wykonanie tej komórki może zająć kilka-kilknaście minut za względu na ilość danych w ramce.

# In[11]:


export_table_to_db(flight_df,'flight')


#  ### Wgranie `airport_list_df` do tabeli `airport_list`

# In[12]:


export_table_to_db(airport_list_df,'airport_list')


#  # Sprawdzenie poprawności wykonania notatnika
#  Uruchom kod poniżej, aby sprawdzić, czy ta część została poprawnie wykonana

# In[13]:


def test_data_export(table_name, expected_count, expected_schema):
    real_count = pd.read_sql(f"SELECT COUNT(*) as cnt FROM {table_name}", engine).iloc[0][0]
    
    real_schema = pd.read_sql(f"SELECT * FROM {table_name} LIMIT 0", engine)
    real_schema = set(real_schema.columns)

    expected_schema = set(expected_schema)

    diff = real_schema.symmetric_difference(expected_schema)

    assert len(diff) == 0, ('Nie zgadzają się kolumny tabel....'
    f'\tOczekiwano: {expected_schema}'
    f'\tOtrzymano: {real_schema}'
    f'\tRóżnica: {diff}')

    assert expected_count == real_count,         f'Nie zgadza się liczba wierszy, oczekiwano {expected_count}, otrzymano {real_count} - sprawdź, czy nie dane nie zostały wgrane do tabeli "{table_name}" więcej niż raz.'


#  ## Sprawdzenie tabeli `aircraft`
#  Uruchom kod poniżej, aby sprawdzić, czy ta część została poprawnie wykonana

# In[14]:


aircraft_expected_count = 7383
aircraft_expected_schema = ['id', 'manufacture_year', 'tail_num', 'number_of_seats']

test_data_export('aircraft', aircraft_expected_count, aircraft_expected_schema)


#  ## Sprawdzenie tabeli `airport_weather`
#  Uruchom kod poniżej, aby sprawdzić, czy ta część została poprawnie wykonana

# In[15]:


airport_weather_expected_count = 46226
airport_weather_expected_schema = [
       'id', 'station', 'name', 'date', 'awnd', 'prcp', 'snow', 'snwd', 'tavg', 
       'tmax', 'tmin', 'wdf2', 'wdf5', 'wsf2', 'wsf5', 'wt01', 'wt08', 'wt02',
       'wt03', 'wt04', 'wt09', 'wt06', 'wt05', 'pgtm', 'wt10', 'wesd', 'sn32',
       'sx32', 'psun', 'tsun', 'tobs', 'wt07', 'wt11', 'wt18']

test_data_export('airport_weather', airport_weather_expected_count, airport_weather_expected_schema)


#  ## Sprawdzenie tabeli `flight`
#  Uruchom kod poniżej, aby sprawdzić, czy ta część została poprawnie wykonana

# In[16]:


flight_expected_count = 1386120
flight_expected_schema = [
       'id', 'month', 'day_of_month', 'day_of_week', 'op_unique_carrier', 'tail_num',
       'op_carrier_fl_num', 'origin_airport_id', 'dest_airport_id',
       'crs_dep_time', 'dep_time', 'dep_delay_new', 'dep_time_blk',
       'crs_arr_time', 'arr_time', 'arr_delay_new', 'arr_time_blk',
       'cancelled', 'crs_elapsed_time', 'actual_elapsed_time', 'distance',
       'distance_group', 'year', 'carrier_delay', 'weather_delay', 'nas_delay',
       'security_delay', 'late_aircraft_delay']

test_data_export('flight', flight_expected_count, flight_expected_schema)


#  ## Sprawdzenie tabeli `airport_list`
#  Uruchom kod poniżej, aby sprawdzić, czy ta część została poprawnie wykonana

# In[17]:


airport_list_expected_count = 97
airport_list_expected_schema = ['id', 'origin_airport_id', 'display_airport_name', 'origin_city_name', 'name']

test_data_export('airport_list', airport_list_expected_count, airport_list_expected_schema)


# In[18]:


msg = "Wszystko wygląda OK :) Możesz przejść do kolejnego kroku."
print(msg)


#  # Podsumowanie
#  W tym notatniku załadowaliśmy pobrane wcześniej pliki na bazę danych. Dzięki temu stworzyliśmy centralne miejsce ich magazynowania, co wykorzystamy zarówno przy analizie danych, jak i przy późniejszej budowie systemu raportowego.
