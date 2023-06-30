#!/usr/bin/env python
# coding: utf-8

#  # Opis notatnika
#  Zmierzamy do końca analizy danych, które zostały nam udostępnione. Ten krok dodaje jeszcze więcej informacji do naszego wyjściowego zbioru. Tym razem sprawdzimy między innymi to, czy opóźnienia lotów zależne są od trasy czy warunków pogodowych.
# 
#  Zanim jednak do tego przejdziemy, należy, podobnie jak w poprzednich krokach, skonfigurować odpowiednio notatnik.
#  
#  W tej części warsztatu ponownie wcielasz się w rolę Analiyka Danych, którego zadaniem jest wykonanie analizy eksplotacyjnej zbioru danych - jedno z wymagań dostarczonych przez klienta.

#  Tutaj zaimportuj wymagane biblioteki

# In[ ]:





#  ## Połączenie z bazą danych
#  Tutaj uzupełnij konfigurację połączenia

# In[1]:


username = 'postgres'
password = 'junior21'

host = 'localhost'
database = 'postgresql'
port = 5432


#  Tutaj stwórz zmienną engine, która zostanie użyta do połączenia z bazą danych

# In[2]:


import psycopg2
from tqdm import tqdm
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.engine import URL
from pandas import DataFrame
import seaborn as sns
import numpy as np
import matplotlib as mpl
import matplotlib.pyplot as plt
from matplotlib.colors import ListedColormap
import plotly.express as px
import threading
import plotly.graph_objects as go
url = URL.create(
    "postgresql",
    username=username,
    password=password, 
    host=host,
    database=database,
)
engine = create_engine(url)


#  Tutaj uzupełnij implementację metody `read_sql_table`

# In[3]:


def load_sql_table(table_name):
    output = pd.read_sql_table(table_name , engine)  
    return output


#  Tutaj zaczytaj zapisaną wcześniej ramkę danych `flight_df` do zmniennej o takiej samej nazwie

# In[4]:


flight_df = pd.read_csv(
                    'flight_df_02.csv', 
                    sep=';',
                    decimal='.'
)


#  # Wzbogacenie o `airport_list`
#  Wczytaj do obszaru roboczego tabelę `airport_list` używając procedury `read_sql_table`. Wykonaj poniższe ćwiczenia:  
#  1. Sprawdź, czy klucz `origin_airport_id` jest unikalny, tj. nie ma dwóch takich samych wartości w kolumnie `origin_airport_id`.  
#  1. Jeżeli duplikaty występują, usuń je w najdogodniejszy dla Ciebie sposób.  
#  1. Jeśli duplikaty nie występują, złącz ramki `airport_list_df` wraz z aktualną `flight_df`, używając kolumny `origin_airport_id` oraz złączenia typu `LEFT JOIN`. Z ramki `airport_list_df` interesuje nas dodanie kolumny `origin_city_name`.  
#  1. Dodatkowo dokonaj jeszcze raz złączenia ramki `flight_df` z `airport_list_df`, tym razem jednak złącz kolumnę `destination_airport_id` wraz z `origin_airport_id`. Podobnie jak wcześniej, interesuje nas kolumna `origin_city_name`, jedank ona powinna zostać wyświetlona jako `destination_city_name`

#  Tutaj wczytaj ramkę `airport_list_df`

# In[5]:


airport_df_raw=load_sql_table('airport_list')


#  Tutaj sprawdż, czy występują duplikaty dla kolumny `origin_airport_id`

# In[6]:


airport_df_raw['origin_airport_id'].drop_duplicates()


#  Tutaj usuń duplikaty – jeśli występują

# In[ ]:





#  Tutaj dokonaj złączenia ramki `flight_df` oraz `airport_list_df` używając `origin_airport_id`

# In[7]:


tmp_flight_df =  pd.merge(
    left=flight_df, right=airport_df_raw[['origin_airport_id','origin_city_name']],
    how='left',
    left_on=['origin_airport_id'], right_on=['origin_airport_id'])
tmp_flight_df.info()


#  Tutaj dokonaj złączenia ramki `flight_df` oraz `airport_list_df` używając `destination_airport_id`

# In[8]:


tmp_flight_df_1 =  pd.merge(
    left=tmp_flight_df, right=airport_df_raw[['origin_airport_id','origin_city_name']],
    how='left',
    left_on=['dest_airport_id'], right_on=['origin_airport_id'])
tmp_flight_df_1 = tmp_flight_df_1.drop('origin_airport_id_y', axis = 1)
tmp_flight_df_1 = tmp_flight_df_1.rename(columns = {'origin_airport_id_x': 'origin_airport_id',
                                                    'origin_city_name_x': 'origin_city_name',
                                                     'origin_city_name_y': 'destination_city_name'})
tmp_flight_df_1['route'] = [(tmp_flight_df_1['origin_airport_id'][i], tmp_flight_df_1['dest_airport_id'][i]) 
                            for i in range(len(tmp_flight_df_1))
                           ]
flight_df = tmp_flight_df_1
flight_df.info()


# ### Sprawdzenie
# Uruchom kod poniżej, aby sprawdzić, czy ta część została poprawnie wykonana

# In[9]:


assert 'origin_city_name' in flight_df.columns, 'Brak kolumny `origin_city_name` w ramce flight_df'
assert 'destination_city_name' in flight_df.columns, 'Brak kolumny `destination_city_name` w ramce flight_df'

flight_df_expected_rows_amount = 1057391
assert flight_df.shape[0] == flight_df_expected_rows_amount, 'Ups, zmieniła się liczba wierszy...'


#  ## Analiza według lotnisk oraz tras
#  Wykonaj poniższe polecenia:  
#  1. Wyznacz lotniska, z których **odlatywało** najwięcej samolotów. Wynik zapisz do ramki `top_airports_origin_df`.
#  1. Wyznacz lotnika, na których najwięcej lotów **się kończyło**. Wynik zapisz do ramki `top_airports_destination_df`.  
#  1. Wyznacz najczęściej uczęszczaną trasę, wynik zapisz do ramki `top_route_df`.  
#  1. Przy założeniu, że reprezentatywna liczba lotów na trasie wynosi ponad 500, wyznacz dodatkowo top 10:  
#      - tras z **najmniejszym odsetkiem opóźnień**, wynik zapisz do ramki `least_route_delays_df`.  
#      - tras z **największym odsetkiem opóźnień**, wynik zapisz do ramki `top_route_delays_df`.

#  Tutaj wyznacz ramkę `top_airports_origin_df`

# In[10]:


top_airports_origin_df = flight_df[['origin_airport_id', 'id']].groupby(['origin_airport_id']).agg(['count'])
top_airports_origin_df = pd.DataFrame(top_airports_origin_df)
top_airports_origin_df = top_airports_origin_df['id']
top_airports_origin_df.reset_index(inplace = True)

top_airports_origin_df = (pd.merge(
    left=top_airports_origin_df, right=flight_df[['origin_airport_id','origin_city_name']],
    how='left',
    left_on=['origin_airport_id'], right_on=['origin_airport_id'])).drop_duplicates()

top_airports_origin_df.iloc[:,[0,2,1]]

top_airports_origin_df = top_airports_origin_df.sort_values(by = ['count'], ascending = False)
top_airports_origin_df = top_airports_origin_df.reset_index(inplace = False)
top_airports_origin_df = top_airports_origin_df.drop('index', axis = 1)
top_airports_origin_df = top_airports_origin_df.head(5)
top_airports_origin_df


#  Tutaj wyznacz ramkę `top_airports_destination_df`

# In[11]:


top_airports_destination_df = flight_df[['dest_airport_id', 'id']].groupby(['dest_airport_id']).agg(['count'])
top_airports_destination_df = pd.DataFrame(top_airports_destination_df)
top_airports_destination_df = top_airports_destination_df['id']
top_airports_destination_df.reset_index(inplace = True)

top_airports_destination_df = (pd.merge(
    left=top_airports_destination_df, right=flight_df[['dest_airport_id','destination_city_name']],
    how='left',
    left_on=['dest_airport_id'], right_on=['dest_airport_id'])).drop_duplicates()

top_airports_origin_df.iloc[:,[0,2,1]]

top_airports_destination_df = top_airports_destination_df.sort_values(by = ['count'], ascending = False)
top_airports_destination_df = top_airports_destination_df.reset_index(inplace = False)
top_airports_destination_df = top_airports_destination_df.drop('index', axis = 1)
top_airports_destination_df = top_airports_destination_df.head(5)
top_airports_destination_df


# In[12]:


def count_yes(x):
    return list(x).count('yes')

top_route_df = flight_df[['is_delayed', 'route', 'id']].groupby(['route']).agg({'id': 'count', 'is_delayed': count_yes})

top_route_df = pd.DataFrame(top_route_df)
top_route_df.reset_index(inplace = True)

top_route_df['origin_airport_id'] = [el[0] for el in top_route_df['route']]
top_route_df['dest_airport_id'] = [el[1] for el in top_route_df['route']]

top_route_df = pd.merge(
    left=top_route_df, right=flight_df[['origin_airport_id','origin_city_name']],
    how='left',
    left_on=['origin_airport_id'], right_on=['origin_airport_id']).drop_duplicates()

top_route_df = pd.merge(
    left=top_route_df, right=flight_df[['dest_airport_id','destination_city_name']],
    how='left',
    left_on=['dest_airport_id'], right_on=['dest_airport_id']).drop_duplicates()

top_route_df = top_route_df.reset_index(inplace = False)
top_route_df = top_route_df.drop('index', axis = 1)
top_route_df = top_route_df.rename(columns = {'id': 'flights_count', 'is_delayed': 'delayed_flights_count'})
top_route_df['delay_ratio'] = round(top_route_df['delayed_flights_count']/top_route_df['flights_count'],2)

top_route_df


# In[13]:


least_route_delays_df = top_route_df.loc[top_route_df['flights_count']>500].sort_values(by = 
                                                                            ['delay_ratio'], ascending = True).head(10)
top_route_delays_df = top_route_df.loc[top_route_df['flights_count']>500].sort_values(by = 
                                                                            ['flights_count'], ascending = False).head(60)
#top_route_delays_df
#least_route_delays_df


#  ### Sprawdzenie dla `top_airport_origin`

# In[14]:


top_airports_origin_head = (top_airports_origin_df['count']
                                 .sort_values(ascending=False)
                                 .head()
                                 .to_list()
                                )

top_airports_origin_head = tuple(top_airports_destination_head)
top_airports_origin_head_expected = (122945, 100333, 87776, 64602, 57686)

assert top_airports_origin_head == top_airports_destination_head_expected, f"Nie zgadza się top 5 wierszy, oczekiwano wyników: {top_airports_destination_head_expected} otrzymano: {top_airports_destination_head}"


# ### Sprawdzenie dla `top_airport_destination`

# In[15]:


top_airports_destination_head = (top_airports_destination_df
                                 .sort_values(ascending=False)
                                 .head()
                                 .to_list()
                                 )
top_airports_destination_head = tuple(top_airports_destination_head)
top_airports_destination_head_expected = (122945, 100333, 87776, 64602, 57686)

assert top_airports_destination_head == top_airports_destination_head_expected, f"Nie zgadza się top 5 wierszy, oczekiwano wyników: {top_airports_destination_head_expected} otrzymano: {top_airports_destination_head}"


#  # Wzbogacenie o dane pogodowe
#  Używając procedury `read_sql_table`, wczytaj tabelę `airport_weather` do ramki `airport_weather_df`. Następnie wykonaj następujące polecenia:  
#  1. Pozostaw w ramce tylko następujące kolumny: `['station', 'name', 'date', 'prcp', 'snow', 'snwd', 'tmax', 'awnd']`.  
#  1. Połącz ramki `airport_list_df` wraz z `airport_weather_df` po odpowiedniej kolumnie używając takiego złączenia, aby w wyniku usunąć te wiersze (lotniska), które nie posiadają danych pogodowych. Dodatkowo, upewnij się, że zostanie tylko dodana kolumna `origin_airport_id`.

#  Tutaj wczytaj ramkę `airport_weather`

# In[16]:


airport_weather_raw=load_sql_table('airport_weather')


#  Tutaj oczyść ramkę `airport_weather_df` z nadmiarowych kolumn

# In[17]:


airport_weather_df = airport_weather_raw[['station', 'name', 'date', 'prcp', 'snow', 'snwd', 'tmax', 'awnd']]
airport_weather_df


#  Tutaj połącz ramki `airport_list_df` oraz `airport_weather_df` aktualizując `airport_weather_df`

# In[18]:


airport_weather_df = pd.merge(
                left=airport_weather_df, right=airport_df_raw[['name','origin_airport_id']],
                    how='left',
                    left_on=['name'], right_on=['name']).drop_duplicates()

airport_weather_df = airport_weather_df.reset_index(inplace = False)
airport_weather_df = airport_weather_df.drop('index', axis = 1)
airport_weather_df = airport_weather_df.dropna(subset = ['origin_airport_id'], how = 'any')
airport_weather_df


#  ### Sprawdzenie
#  Uruchom kod poniżej, aby sprawdzić, czy ta część została poprawnie wykonana

# In[ ]:


airport_weather_df_expected_shape = (43394, 9)
airport_weather_df_shape = airport_weather_df.shape

assert airport_weather_df_expected_shape == airport_weather_df_shape,   f'Nieodpowiedni wymiar ramki airport_weather_df, oczekiwano (wierszy, kolumn): {airport_weather_df_expected_shape}'


#  ## Połączenie `airport_weather_df` oraz `flight_df`
#  W celu złączenia ramek `airport_weather_df` oraz `flight_df` wykonaj następujące kroki:  
#  1. w ramce `aiport_weather_df` występuje kolumna `date`, zrzutuj ją na typ `DATETIME`.  
#  1. w ramce `flight_df` należy stworzyć nową kolumnę o nazwie `date`. W tym celu:  
#  	- złącz kolumny `month`, `day_of_month` oraz `year` razem, użyj następującego formatu daty: `YYYY-MM-DD`.
#  	- zrzutuj kolumnę `date` na typ `DATETIME`.  
#  1. złącz ramki używając odpowiedniego klucza, wynik złączenia zapisz do ramki `flight_df`. Użyj złącznia typu `LEFT JOIN`.
# 
#  > Dlaczego istotne jest zachowanie typów przy złączeniu?
# 
# W trakcie pracy możesz posłużyć się następującymi artykułami z `LMS`:
#  - `Python - analiza danych > Dzień 6 - Pandas > Merge`
#  - `Python - analiza danych > Dzień 6 - Pandas > Praca z datetime`
#  - Dokumentacje metody `to_datetime`: [klik](https://pandas.pydata.org/docs/reference/api/pandas.to_datetime.html)
#  - Dostępne formaty dat: [klik](https://www.programiz.com/python-programming/datetime/strftime) - sekcja `Format Code List`

#  Tutaj zrzutuj kolumnę `date` na `DATETIME` w ramce `airport_weather_df`

# In[19]:


airport_weather_df['date'] = pd.to_datetime(airport_weather_df['date'])
airport_weather_df.info()


# In[ ]:





#  Tutaj stwórz kolumnę `date` w ramce `flight_df`. Pamiętaj, aby była ona również typu `DATETIME`.

# In[20]:


flight_df['date'] = pd.to_datetime(dict(year= flight_df['year'],month = flight_df['month'],day = flight_df['day_of_month']))
airport_weather_df


#  Tutaj złącz tabeli `airport_weather_df` oraz `flight_df`

# In[21]:


flight_df = pd.merge(
                left=flight_df, right=airport_weather_df,
                    how='left',
                    left_on=['origin_airport_id', 'date'], right_on=['origin_airport_id', 'date']).drop_duplicates()

flight_df = flight_df.reset_index(inplace = False)
flight_df = flight_df.drop('index', axis = 1)
flight_df


#  ### Sprawdzenie
#  Uruchom kod poniżej, aby sprawdzić, czy ta część została poprawnie wykonana

# In[ ]:


flight_df_expected_rows_amount = 1057391
assert flight_df.shape[0] == flight_df_expected_rows_amount, 'Ups, zmieniła się liczba wierszy...'


# # Praca samodzielna
# Używając `flight_df` zbadaj hipotezę o tym, że temperatura maksymalna wpływa na **odsetek** opóźnień lotów (kolumna `tmax`).  
# 
# Przy wykonywaniu tego zadania masz pełną dowolność, jednak powinno składać się conajmniej z następujących elementów:
# - sprawdzenie, czy zmienna posiada obserwacje odstające,
# - oczyszczenie danych o ile konieczne,
# - przedstawienie w formie tabeli czy wzrost danej zmiennej powoduje zmianę w odsetku opóźnień lotów,
# - wizualizację stworzonej wcześniej tabeli w formie wykresu,
# - krótkiego opisu wyników w komórce markdown.

#  ## Analiza dla kolumny `tmax`

# In[22]:


flight_df['tmax'].describe()


# In[23]:


bins = np.arange(flight_df['tmax'].min(), flight_df['tmax'].max()+15, 15)
flight_df['tmax_agg'] = pd.cut(x=flight_df['tmax'], bins=bins)
tmax_df = flight_df[['tmax', 'is_delayed','tmax_agg']]
tmax_df


# In[24]:



tmax_df_grouped_1 = tmax_df[['tmax', 'is_delayed']].groupby(['tmax']).agg('count')
tmax_df_grouped_2 = tmax_df[['tmax', 'is_delayed']].groupby(['tmax']).agg({'is_delayed': lambda x: list(x).count('yes')})
tmax_df_grouped_2 = tmax_df_grouped_2.reset_index(inplace = False)

tmax_df_grouped = tmax_df_grouped_1

tmax_df_grouped = tmax_df_grouped.rename(columns = {'is_delayed': 'flights_count'})
tmax_df_grouped = tmax_df_grouped.reset_index(inplace = False)
tmax_df_grouped['delays_count'] = tmax_df_grouped_2['is_delayed']
tmax_df_grouped.sort_values(by = ['flights_count'], ascending = False).head(50)
tmax_df_grouped['delay_ratio'] = round(tmax_df_grouped['delays_count']/tmax_df_grouped['flights_count'],2)
tmax_temp = tmax_df_grouped.sort_values(by = ['delay_ratio'], ascending = False)
tmax_temp


# In[25]:


tmax_df_grouped_final = tmax_df_grouped.drop('delay_ratio', axis = 1)
bins = np.arange(tmax_df_grouped_final['tmax'].min()-15, tmax_df_grouped_final['tmax'].max()+15, 15)
tmax_df_grouped_final['tmax_agg'] = pd.cut(x=tmax_df_grouped_final['tmax'], bins=bins)
tmax_df_grouped_final = tmax_df_grouped_final[['tmax_agg','flights_count', 'delays_count']].groupby(['tmax_agg']).agg(['sum'])
tmax_df_grouped_final = pd.DataFrame(tmax_df_grouped_final)
tmax_df_grouped_final


# In[26]:


tmax_df_grouped_final.reset_index(inplace = True)
tmax_df_grouped_final.columns = [col[0] for col in tmax_df_grouped_final.columns]
tmax_df_grouped_final['delay_ratio'] = round(tmax_df_grouped_final['delays_count']/ tmax_df_grouped_final['flights_count'],2)

tmax_df_grouped_final


# In[30]:


tmax_df_grouped_final['tmax_agg'] = tmax_df_grouped_final['tmax_agg'].astype(str)
tmax_df_grouped_final
fig_bar_10 = px.line(tmax_df_grouped_final, 
                   x =  tmax_df_grouped_final['tmax_agg'], y = 'delay_ratio') 

fig_bar_10.update_layout(
    font_family="Courier New",
    font_color="black",
    title_font_color="black",
    xaxis_title="Tmax Range", 
    yaxis_title="Delay Ratio",
    title={'text': '<b>Delay Ratio per Tmax Range </b>', 'font': {'size': 20}}
)

fig_bar_10.show()


# # Komentarz

# Po przeprowadzeniu analizy zależności między temperaturą maksymalną a opóźnieniem procentowym można stwierdzić że w granicach -25 do 35 stopni panuje zdecydowana przewaga opóźnień, powyżej 35 stopni poziom pozostaje bez większych zmian.

# # Podsumowanie
# W tej części warsztatu dokonaliśmy kompleksowej analizy posiadanego zbioru danych. Eksploracja
# pozwoliła nam na zapoznanie się z cechami charakterystycznymi lotów - wiemy już, które 
# zmienne mogą mieć wpływ na opóźnienia lotów, a które nie. Co warto podkreślić, skupiliśmy się na wielu
# aspektach tej analizy, co otwiera potencjalnie również inne możliwości dalszej pracy nad tą bazą.
# 
# W tym momencie przejdziemy do kolejnego kroku, w którym, na podstawie tej analizy, przygotujemy 
# system raportowy. Zanim jednak stworzymy dashboard, potrzebujemy zaktualizować naszą bazę danych.
