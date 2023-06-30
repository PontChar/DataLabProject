#!/usr/bin/env python
# coding: utf-8

#  # Opis notatnika
#  Ten notatnik jest kontunacją analizy danych o lotach i ich opóźnieniach. Od tego momentu zaczniemy łączyć posiadana przez nas zbiory danych, będąc w stanie dokonać dodatkowych analiz.
# 
#  Zanim jednak do tego przejdziemy, należy, podobnie jak w poprzednim kroku, skonfigurować odpowiednio notatnik.
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

# In[15]:


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

# In[16]:


def load_sql_table(table_name):
    output = pd.read_sql_table(table_name , engine)  
    return output


#  Tutaj zaczytaj zapisaną wcześniej ramkę danych `flight_df` do zmniennej o takiej samej nazwie

# In[18]:


flight_df_raw = load_sql_table('flight')
flight_df = flight_df_raw.loc[(flight_df_raw['cancelled'] == 0) &
                             (flight_df_raw['year'] == 2019)]
flight_df = flight_df.rename(columns = {'dep_delay_new': 'dep_delay'})
flight_df = flight_df.loc[flight_df['distance'] <= flight_df['distance'].quantile(0.95)]
flight_df.shape


# Sprawdzenie poprawności danych w ramce `flight_df` 

# In[19]:


flight_df_expected_rows_amount = 1057391
flight_df_rows_amount = flight_df.shape[0]

assert flight_df_rows_amount == flight_df_expected_rows_amount, f'Oczekiwano {flight_df_expected_rows_amount} wierszy, otrzymano {flight_df_rows_amount}'


#  # Wzbogacenie o `aircraft`
#  Używając procedury `read_sql_table` wczytaj dane z tabeli `aircraft` i zapisz jako `aircraft_df`. Następnie:  
#  1. Usuń z ramki kolumny `number_of_seats` oraz `id`. Na tej podstawie usuń nadmiarowe wiersze (duplikaty).  
#  1. Następnie jeszcze raz sprawdź, czy dla kolumny `tail_num` nie występują duplikaty. Innymi słowy należy sprawdzić, czy dla jednego `tail_num` występuje więcej niż jeden rok produkcji.  
#  1. Jeśli tak to:  
#      - do ramki `aircraft_df_duplicated` zapisz powielone zgodnie ze sprawdzeniem wiersze,  
#      - zgodnie z powyższym zmodyfikuj ramkę tak, aby w przypadku duplikatu za datę wytworzenia samolotu, uznana została najnowsza tj. jeśli dla `tail_num` są dostępne daty produkcji 1998 oraz 2001, uznajemy, że `tail_num` został wyprodukowany w `2001`.
# 
#  Wskazówki:
#  - Praca z duplikatami na LMS: `Python - analiza danych -> Dzień 5 - Pandas -> Duplikaty`
#  - Dokumentacja metody `duplicated`: [klik](https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.duplicated.html)
#  - Dokumentacja metody `drop_duplicates`: [klik](https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.drop_duplicates.html)

#  Tutaj wczytaj tabelę `aircraft` używając `read_sql_table`

# In[29]:


aircraft_df_raw = load_sql_table('aircraft')
aircraft_df_raw


#  Tutaj usuń kolumny `number_of_seats`, `id` oraz duplikaty z ramki `aircraft_df`

# In[43]:


aircraft_df = aircraft_df_raw[['manufacture_year','tail_num']].drop_duplicates()
aircraft_df


#  ### Sprawdzenie
#  Uruchom kod poniżej, aby sprawdzić, czy ta część została poprawnie wykonana

# In[42]:


aircraft_df_expected_rows = 7364
aircraft_df_expected_columns = set(['tail_num', 'manufacture_year'])

aircraft_df_rows = aircraft_df.shape[0]

diff = aircraft_df_expected_columns.symmetric_difference(set(aircraft_df.columns))
assert aircraft_df_rows == aircraft_df_expected_rows, f'Spodziewano się {aircraft_df_expected_rows} wierszy , otrzymano {aircraft_df_rows} wierszy'

assert diff == set([]), f'Spodziewano się {aircraft_df_expected_columns} kolumn, otrzymano: {aircraft_df_expected_columns} kolumn. Różnica: \n\t{diff}'


#  Tutaj sprawdź czy w ramkce `aircraft_df` występują duplikaty wewnątrz kolumny `tail_num`. Czyli czy dla danego `tail_num` występuje więcej niż jeden rok produkcji.

# In[44]:


aircraft_df_is_duplicated = aircraft_df.duplicated(subset='tail_num')
aircraft_df_duplicated = aircraft_df.loc[aircraft_df_is_duplicated]


#  ### Sprawdzenie
#  Uruchom kod poniżej, aby sprawdzić czy ta część została poprawnie wykonana

# In[45]:


aircraft_df_expected_rows = 3
aircraft_df_duplicated_rows = aircraft_df_duplicated.shape[0]
assert aircraft_df_duplicated_rows == aircraft_df_expected_rows, f"Oczekiwano {aircraft_df_expected_rows} wierszy, otrzymano {aircraft_df_duplicated_rows}"


#  ## Modyfikacja `aircraft_df`
#  Tutaj dokonaj aktualizacji tabeli `aircraft_df` - jeśli jest taka potrzeba. Zrób to tak aby, dla powielonych `tail_num`, `manufacture_year` został ustawiony jako najwyższy

# In[47]:


aircraft_df = aircraft_df.drop_duplicates(subset = 'tail_num', keep = 'last')
aircraft_df


#  ### Sprawdzenie
#  Uruchom kod poniżej, aby sprawdzić, czy ta część została poprawnie wykonana

# In[48]:


test_tail = 'N783CA'
test_value = aircraft_df.loc[aircraft_df['tail_num']
                             == test_tail]['manufacture_year']
test_value = int(test_value)

expected_value = 2000
assert test_value == expected_value, f"Dla 'tail_num' == '{test_tail}' oczekiwano {expected_value} otrzymano {test_value}"


#  ## Połączenie `aircraft_df` oraz `flight_df`
#  Połącz ramkę `aircraft_df` oraz `flight_df` zgodnie z kluczem oraz odpowiedz na następujące pytania:
#  1. Czy po połączeniu pojawiły się duplikaty? Dokonaj odpowiedniego sprawdzenia.
#  1. Wyznacz zależność roku produkcji do częstotliwości opóźnień. Wynik zapisz do tabeli `delays_by_manufacture_year_df`.
#  1. Przedstaw wyniki w tabeli za pomocą wykresu punktowego.
#  1. Dokonaj modyfikacji w taki sposób, aby wyświetlone na wykresie zostały tylko takie roczniki samolotów, które wykonały łącznie więcej niż 10000 `(>)` lotów.
# 
# > **Wskazówka:**
# > Aby nie utracić potencjalnie całej dotychczasowej pracy, zapisz wynik do tymczasowej zmiennej np. `tmp_flight_df`. Po sprawdzeniu możesz użyć metody `copy`: [link](https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.copy.html), aby nadpisać `flight_df` używając `tmp_flight_df`.

#  Tutaj dokonaj złączenia `flight_df` z `aircraft_df`, wynik zapisz do `tmp_flight_df`

# In[49]:


tmp_flight_df =  pd.merge(
    left=flight_df, right=aircraft_df,
    how='left',
    left_on=['tail_num'], right_on=['tail_num'])
tmp_flight_df


#  Tutaj dokonaj sprawdzenia, czy nie pojawiły się duplikaty

# In[50]:


tmp_flight_df.drop_duplicates()


#  Tutaj nadpisz `flight_df` używając `tmp_flight_df`

# In[51]:


flight_df = tmp_flight_df


#  ## Opóźnienia vs. rok produkcji samolotu cz. 1
#  Tutaj wyznacz zależność roku produkcji do częstotliwości opóźnień

# In[58]:


conditions = [(flight_df['dep_delay']<=15),
             (flight_df['dep_delay']>15)
             ]
values = ['no', 'yes']
flight_df['is_delayed'] = np.select(conditions, values)
delays_by_manufacture_year_df = round(flight_df[['manufacture_year', 'dep_delay']].loc[flight_df['is_delayed'] == 'yes']
                        .groupby(['manufacture_year']).agg(['count', 'mean', 'median', 'std', 'min', 'max']),2)
delays_by_manufacture_year_df = pd.DataFrame(delays_by_manufacture_year_df)
delays_by_manufacture_year_df = delays_by_manufacture_year_df['dep_delay']
delays_by_manufacture_year_df.reset_index(inplace = True)
delays_by_manufacture_year_df.sort_values(by = ['manufacture_year'], ascending = True)
delays_by_manufacture_year_df = delays_by_manufacture_year_df.rename(columns={'count': 'delays_count'})

total_number_flights = round(flight_df[['manufacture_year', 'dep_delay']].groupby(['manufacture_year']).agg(['count']),2)
total_number_flights = pd.DataFrame(total_number_flights)
total_number_flights = total_number_flights['dep_delay']
total_number_flights.reset_index(inplace = True)
total_number_flights

delays_by_manufacture_year_df['total_flights_count'] = total_number_flights['count']
delays_by_manufacture_year_df['delays as percentage_of_total_flights'] = round(delays_by_manufacture_year_df['delays_count']/delays_by_manufacture_year_df['total_flights_count'],2)
delays_by_manufacture_year_df


#  Tutaj wyrysuj ramkę `delays_by_manufacture_year_df`

# In[62]:


fig_bar_7 = px.scatter(delays_by_manufacture_year_df, x =  'manufacture_year', y = 'delays as percentage_of_total_flights') 

fig_bar_7.update_layout(
    font_family="Courier New",
    font_color="black",
    title_font_color="black",
    xaxis_title="Aircraft Production Year", 
    yaxis_title="Delays to Total Flights",
    title={'text': '<b>Delays to Total Flights per Aircraft Production Year </b>', 'font': {'size': 20}},
)

fig_bar_7.show()


#  Tutaj zmodyfikuj wykres tak, aby prezentował tylko te roczniki, które odbyły więcej niż 10000 lotów

# In[63]:


fig_bar_8 = px.bar(delays_by_manufacture_year_df.loc[delays_by_manufacture_year_df['total_flights_count']>10000], x ='manufacture_year', y = 'delays_count') 

fig_bar_8.update_layout(
    font_family="Courier New",
    font_color="black",
    title_font_color="black",
    xaxis_title="Aircraft Production Year", 
    yaxis_title="Number of Delays ",
    title={'text': '<b>Number of Delays per Production Year (over 10,000 flights) </b>', 'font': {'size': 20}},
)


#  ## Opóźnienia vs. rok produkcji samolotu cz. 2
#  Dokonaj agregacji kolumny `manufacture_year` do kolumny `manufacture_year_agg` zgodnie z poniższym:
#  1. Grupując dane co 3 lata -> Czy po grupowaniu można zauważyć zależność? Wyniki zapisz do ramki `flight_delays_by_manufacture_year_agg_df`.
#  1. Wyznacz top 5 roczników samolotu, które wykonały najwięcej lotów. Wyniki zapisz do ramki `top_manufactured_df`, do obliczeń wykorzystaj `delays_by_manufacture_year_df`.

#  Tutaj dodaj kolumnę `manufacture_year_agg` do ramki `flight_df`

# In[64]:


bins = np.arange(flight_df['manufacture_year'].min(), flight_df['manufacture_year'].max()+3, 3)
flight_df['manufacture_year_agg'] = pd.cut(x=flight_df['manufacture_year'], bins=bins)


#  Tutaj stwórz zmienną `flight_delays_by_manufacture_year_agg_df`

# In[66]:


flight_delays_by_manufacture_year_agg_df = round(flight_df[['manufacture_year_agg', 'dep_delay']].loc[flight_df['is_delayed'] == 'yes']
                        .groupby(['manufacture_year_agg']).agg(['count', 'mean', 'median', 'std', 'min', 'max']),2)
flight_delays_by_manufacture_year_agg_df = pd.DataFrame(flight_delays_by_manufacture_year_agg_df)
flight_delays_by_manufacture_year_agg_df = flight_delays_by_manufacture_year_agg_df['dep_delay']
flight_delays_by_manufacture_year_agg_df.reset_index(inplace = True)
flight_delays_by_manufacture_year_agg_df = flight_delays_by_manufacture_year_agg_df.rename(columns={'count': 'delays_count'})
flight_delays_by_manufacture_year_agg_df['manufacture_year_agg'] = flight_delays_by_manufacture_year_agg_df['manufacture_year_agg'].astype(str)
flight_delays_by_manufacture_year_agg_df


#  Tutaj stwórz wykres w oparciu o dane zawarte w `flight_delays_by_manufacture_year_agg_df`

# In[68]:


fig_bar_8 = px.bar(flight_delays_by_manufacture_year_agg_df, 
                   x =  flight_delays_by_manufacture_year_agg_df['manufacture_year_agg'], y = 'delays_count') 

fig_bar_8.update_layout(
    font_family="Courier New",
    font_color="black",
    title_font_color="black",
    xaxis_title="Aircraft Production Years (Aggregated)", 
    yaxis_title="Number of Delays",
    title={'text': '<b>Number of Delays per Aggregated Production Years </b>', 'font': {'size': 20}},
)

fig_bar_8.show()


# Tutaj wyznacz TOP 5 roczników produkcji - czyli sortując według liczby wykonanych lotów, pamiętaj o wyświetleniu również wartości opóźnienia.

# In[69]:


delays_by_manufacture_year_df.sort_values(by = ['total_flights_count'], ascending = False).head(5)


#  # Podsumowanie
#  W tym notatniku do naszej wyjściowej ramki danych `flight_df` dołączyliśmy tabelę `aircraft_df` i za jej pomocą dodaliśmy kolejny wymiar do naszej analizy. Zauważmy, ile dodatkowych wniosków mogliśmy wyciągnąć dzięki jej dodaniu.
# 
#  Zanim przejdziemy dalej, należy zapisać bieżącą postać ramki (najlepiej lokalnie), która zostanie użyta w kolejnym notatniku.
# 
#  > **Wskazówka:**  
#  > Aby uniknąć potencjalnych problemów, najlepiej zapisać ramkę z nazwą nawiązującą do tego notatnika, np. `flight_df_01`.

#  Tutaj zapisz ramkę w najdogodniejszy sposób

# In[72]:


flight_df_02 = flight_df
flight_df_02['manufacture_year_agg'] = flight_df_02['manufacture_year_agg'].astype(object)
flight_df_02.to_csv('flight_df_02.csv',
    sep=';',
    decimal='.',
    index=False)

