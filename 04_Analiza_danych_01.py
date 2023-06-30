#!/usr/bin/env python
# coding: utf-8

#  # Opis notatnika
#  Po pobraniu danych z zewnętrznego serwisu, a następnie załadowaniu ich do naszej wewnętrznej (prawie firmowej) bazy, czas na ich analizę oraz eksplorację.
#  Ponieważ zakładamy, że pracujemy z tym zbiorem pierwszy raz, warto przejrzeć go pod dość szerokim kątem, aby nabrać świadomości, jakie informacje są tam ukryte i co stanowi potencjalną wartość biznesową.
# 
#  Eksplorację zaczniemy od centralnej bazy danych `flight`, w której nastawimy się w szczególności na zmienną `dep_delay` (za dokumentacją u [źródła](https://www.kaggle.com/datasets/threnjen/2019-airline-delays-and-cancellations?resource=download&select=raw_data_documentation.txt)), która informuje o wysokości opóźnienia odlotu samolotu.
#  Wykonując kolejne kroki, najpierw odpowiednio przygotujemy nasz wyjściowy zbiór do analizy, by później zacząć go wzbogacać o dodatkowe informacje, np. pogodowe.
# 
#  Dzięki wyciągnięciu wniosków z danych, które otrzymaliśmy, będziemy mogli zaproponować system raportowania wspomagający biznes, czy zdefiniować dalsze kroki, które usprawnią działania lotnisk.
# 
#  Powodzenia!
# 
#  > Ze względu na objętość zadań w tym obszarze, ten krok podzielony został na kilka mniejszych części.
#  
#  W tej części warsztatu wcielasz się w rolę Analiyka Danych, którego zadaniem jest wykonanie analizy eksplotacyjnej zbioru danych - jedno z wymagań dostarczonych przez klienta.

#  # Konfiguracja
#  Uzupełnij implementajcę procedury `load_table_from_db`, która będzie odpowiedzialna za
#  pobieranie danych z bazy danych oraz zwrócenie ramki do dalszej pracy.
# 
#  W trakcie pracy nad jej implementacją możesz wspomóc się następującymi materiałami:
#  - `read_sql` - dokumentacja techniczna metody: [klik](https://pandas.pydata.org/docs/reference/api/pandas.read_sql.html),
#  - `SQL - analiza danych -> Zjazd 1 - materiał dodatkowe -> Export danych -> Python`
# 
#  > **Uwaga:**  
#  > Metoda powinna tylko pobierać dane z bazy, nie implementuj tutaj dodatkowej logiki.

#  Tutaj zaimportuj wymagane biblioteki

# In[ ]:





#  ## Połączenie z bazą danych
#  Tutaj uzupełnij konfigurację połączenia

# In[2]:


username = 'postgres'
password = 'junior21'

host = 'localhost'
database = 'postgresql'
port = 5432


#  Tutaj stwórz zmienną engine, która zostanie użyta do połączenia z bazą danych

# In[3]:


import psycopg2
from tqdm import tqdm
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.engine import URL
import plotly.express as px
import numpy as np
url = URL.create(
    "postgresql",
    username=username,
    password=password, 
    host=host,
    database=database,
)
engine = create_engine(url)


#  Tutaj uzupełnij implementację metody `read_sql_table`

# In[4]:


def load_sql_table(table_name):
    output = pd.read_sql_table(table_name , engine)  
    return output


#  # Wczytanie danych do obszaru roboczego
#  Używając metody `read_sql_table`, wczytaj do obszaru roboczego zawartośc tabeli `flight` i zapisz w ramce o nazwie `flight_df_raw`.
# 
#  Następnie wykonaj poniższe polecenia:  
#  1. Usuń z ramki loty, które:
#      * odbyły się w 2020 roku,
#      * zostały anulowane.  
#  2. Zmień nazwę kolumny `dep_delay_new` na `dep_delay`.  
#  3. Tak powstałą tabelę zapisz do ramki, która nazywać się będzie `flight_df` - z tej ramki będziemy korzystali do końca analizy.  
#  4. Wyznacz, ile kolumn ma tabela `flight_df`, wynik zapisz do zmiennej `flight_df_columns_amount`.  
#  5. Wyznacz, ile wierszy ma tabela `flight_df`, wynik zapisz do zmiennej `flight_df_rows_amount`.

#  Tutaj wczytaj ramkę do obszaru roboczego

# In[5]:


flight_df_raw = load_sql_table('flight')


# In[6]:


flight_df_raw[['year','cancelled','dep_delay_new']]


#  Tutaj oczyść ramkę usuwając loty z roku 2020 oraz te anulowane

# In[7]:


flight_df = flight_df_raw.loc[(flight_df_raw['cancelled'] == 0) &
                             (flight_df_raw['year'] == 2019)]
flight_df[['year','cancelled']]


#  Tutaj zmień nazwę kolumny `dep_delay_new` na `dep_delay`

# In[8]:


flight_df = flight_df.rename(columns = {'dep_delay_new': 'dep_delay'})
flight_df['dep_delay']


#  Tutaj zainicjuj zmienne `flight_df_columns_amount` oraz `flight_df_rows_amount`, które zostaną użyte do sprawdzenia poprawności wykonania tej części

# In[9]:


flight_df_columns_amount = len(flight_df.columns)
flight_df_rows_amount = len(flight_df.index)


#  ## Sprawdzenie
#  Uruchom kod poniżej, aby sprawdzić, czy ta część została poprawnie wykonana

#  ### Sprawdzenie liczby kolumn

# In[9]:


flight_df_expected_columns_amount = 28
assert flight_df_columns_amount == flight_df_expected_columns_amount, f'Oczekiwano {flight_df_expected_columns_amount} kolumn, otrzymano {flight_df_columns_amount}'


#  ### Sprawdzenie liczby wierszy

# In[10]:


flight_df_expected_rows_amount = 1095742
assert flight_df_rows_amount == flight_df_expected_rows_amount, f'Oczekiwano {flight_df_expected_rows_amount} wierszy, otrzymano {flight_df_rows_amount}'


#  ### Sprawdzenie czy nie zostały w ramce loty z 2020

# In[11]:


flight_df_year_test = flight_df.loc[flight_df['year'] == 2020].shape[0]
assert flight_df_year_test == 0, 'W ramce `flight_df` nadal znajdują się loty z 2020 roku'


#  ### Sprawdzenie czy nie zostały w ramce loty anulowane

# In[12]:


flight_df_cancelled_test = flight_df.loc[flight_df['cancelled'] != 0].shape[0]
assert flight_df_cancelled_test == 0, 'W ramce `flight_df` nadal znajdują się anulowane loty'


#  ### Sprawdzenie czy nazwa kolumny została poprawnie zmieniona

# In[13]:


assert 'dep_delay' in flight_df.columns, 'Kolumna dep_delay nie została znaleziona w ramce flight_df'


#  # Analiza kolumny `dep_delay` cz. 1
#  Wyznacz statystyki opisowe dla zmiennej `dep_delay` i zapisz do zmiennej `dep_delay_statistics_df`.
#  W ramce powinny znaleźć się następujące wiersze:
#  - średnia,
#  - mediana,
#  - odchylenie standardowe,
#  - min, max
#  - percentyle `[0.1, 0.25, 0.5, 0.75, 0.9, 0.95, 0.99]`
# 
# Wyniki zaokrągl do dwóch miejsc po przecinku.
# 
# W trakcie rozwiązywania tego zadania możesz posłużyć się następującymi materiałami:
#  - `LMS -> Python-Analiza danych -> Przygotowanie do zjazd 3 -> Podstawy statystyki opisowej`
#  - `describe` - dokumentacja techniczna metody: [klik](https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.describe.html)

#  Tutaj wyznacz statystyki opisowe dla kolumny `dep_delay` oraz zainicjuj ramkę `dep_delay_statistics_df`.

# In[14]:


dep_delay_statistics_df = round(flight_df[['dep_delay']].agg(['count', 'mean', 'std', 'min', 'max']),2)
#dep_delay_statistics_df = flight_df['dep_delay'].describe
percentile_list = [0.1, 0.25, 0.5, 0.75, 0.9, 0.95, 0.99]
for percentile in percentile_list:
    percentile_name = f'{percentile*100}%'
    value = flight_df['dep_delay'].quantile(percentile)
    dep_delay_statistics_df.loc[percentile_name] = [value]

dep_delay_statistics_df


#  ## Sprawdzenie wyników
#  Uruchom kod poniżej, aby sprawdzić, czy ta część została poprawnie wykonana

# In[15]:


expected = {'count': 1095742.0, 'mean': 14.77, 'std': 46.49, 
            'min': 0.0, '10%': 0.0, '25%': 0.0, '50%': 0.0, 
            '75%': 8.0, '90%': 42.0, '95%': 81.0, '99%': 206.0, 'max': 1959.0}
dep_delay_statistics_dict = dep_delay_statistics_df.to_dict()

assert dep_delay_statistics_dict == expected, f'Błąd. Otrzymano wartości : {dep_delay_statistics_dict}'


#  # Analiza kolumny `dep_delay` cz. 2
#  Przeanalizuj dokładniej kolumnę `dep_delay` wykonując poniższe polecenia:  
#  1. Wyznacz wykres dla _całej kolumny_ (tzn. tak jak jest).  
#  2. Wyznacz wykres z pominięciem tych wierszy, dla których `dep_delay=0`.  
#  3. Obcinając wykres do percentyla 95% oraz pomijając `dep_delay=0`.  
# 
# Dla wszystkich wykresów użyj histogramu z koszykami co 10 tj. `[0, 10)`, `[10, 20)` i tak dalej.
# 
# Możesz tutaj użyć swojego ulubionego narzędzia do tworzenia wykresów - `matplotlib` czy `dash`. Pamiętaj o odpowiednim wystylowaniu każdego z wykresów zgodnie z dobrymi praktykami.
# 
#  W trakcie pracy możesz posłużyć się następującymi artykułami:
#  - Dla `Matplotlib`:
#      - `Python - analiza danych -> Dzień 7 - Wykresy -> Zaawansowane wykresy`
#      - `hist` - dokumentacja metody: [klik](https://matplotlib.org/stable/api/_as_gen/matplotlib.pyplot.hist.html)
#  - Dla `Plotly`:
#      - `Wizualizacja danych -> Dzień 2 -> Wprowadznie do plotly`
#      - `histogram` - dokumentacja metody: [klik](https://plotly.com/python/histograms/)

#  Tutaj stwórz wykres dla całej kolumny `dep_delay`

# In[16]:


fig_1 = px.histogram(flight_df, x = 'dep_delay', title = 'Flight delays in 2019')

fig_1.update_traces(xbins=dict( 
        start=0.0,
        end=flight_df['dep_delay'].max(),
        size=10
    ))

fig_1.show()


#  Tutaj stwórz wykres dla `dep_delay` używając warunku `dep_delay > 0`

# In[17]:


fig_2 = px.histogram(flight_df, x = 'dep_delay', title = 'Flight delays in 2019')

fig_2.update_traces(xbins=dict( 
        start=1.0,
        end=flight_df['dep_delay'].max(),
        size=10
    ))
fig_2.show()


#  Tutaj stwórz wykres dla `dep_delay` używając warunków `dep_delay > 0` oraz `dep_delay < percentile 95%`

# In[18]:


fig_3 = px.histogram(flight_df, x = 'dep_delay', title = 'Flight delays distribution in 2019')

fig_3.update_traces(xbins=dict( 
        start=1.0,
        end=flight_df['dep_delay'].quantile(0.95),
        size=10
    ))

fig_3.show()


#  # Analiza opóźnień
#  Zdefiniuj w ramce `flight_df` nową kolumnę - `is_delayed` jako te opóźnienia, które wynosiły więcej niż `(>)` 15 minut.
# 
#  Zgodnie z powyższą definicją, wyznacz jaki procent wszystkich lotów był opóźniony. Wynik zapisz do zmiennej `delayed_ratio` z dokładnością do dwóch miejsc po przecinku. Postaraj się, aby wartość tej zmiennej nie była zapisana ręcznie.

#  Tutaj stwórz nową kolumnę `is_delayed` oraz odpowiednio ją uzupełnij

# In[22]:


conditions = [(flight_df['dep_delay']<=15),
             (flight_df['dep_delay']>15)
             ]
values = ['no', 'yes']
flight_df['is_delayed'] = np.select(conditions, values)
flight_df['is_delayed']


#  Tutaj zdefiniuj oraz wyznacz wartość dla zmiennej `delayed_ratio`

# In[17]:


delayed_ratio = round(flight_df.loc[flight_df['is_delayed'] == 'yes'].shape[0]/flight_df.shape[0],2)
delayed_ratio


#  ### Sprawdzenie
#  Uruchom kod poniżej, aby sprawdzić, czy ta część została poprawnie wykonana

# In[21]:


delayed_ratio_expected = 0.19
assert delayed_ratio == delayed_ratio_expected, f"Oczekiwanio {delayed_ratio_expected}, otrzymano {delayed_ratio}"


#  # Opóźnienia vs. miesiąc kalendarzowy
#  Zbadaj, jak zmienia się odsetek opóźnień w zależności od **miesiąca kalendarzowego**. Zadanie wykonaj w dwóch krokach:
#  1. stwórz zmienną `flight_delays_by_month_df` używając metody `groupby`,
#  1. na podstawie zmiennej `flight_delays_by_month_df`, wygeneruj odpowiedni wykres zgodnie z dobrymi praktykami.
# 
# W trakcie pracy nad tym zadaniem możesz posłużyć się następującymi materiałami z `LMS`:
#  - `Python - analiza danych -> Dzień 5 - Pandas -> Grupowanie`
#  - `groupby`- dokumentacja metody `Pandas`: [klik](https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.groupby.html)

#  Tutaj pogrupuj dane, a wyniki zapisz do ramki `flight_delays_by_month_df`

# In[25]:


flight_delays_by_month_df = round(flight_df[['month', 'dep_delay']].loc[flight_df['is_delayed'] == 'yes'].groupby(['month']).agg(['count', 'mean', 'median', 'std', 'min', 'max']),2).reset_index()
flight_delays_by_month_df = pd.DataFrame(flight_delays_by_month_df)
flight_delays_by_month_df = flight_delays_by_month_df['dep_delay']
flight_delays_by_month_df


#  Tutaj narysuj wykres, używając danych z ramki `flight_delays_by_month_df`

# In[23]:


months = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
fig_bar_1 = px.bar(flight_delays_by_month_df, x =  months, y = ['count']) 
fig_bar_1.update_layout(
    font_family="Courier New",
    font_color="black",
    title_font_color="black",
    xaxis_title="Month", 
    yaxis_title="Number of delayes",
    title={'text': '<b>Number of flight delays per month in 2019</b>', 'font': {'size': 20}},
)

fig_bar_1.show()


#  # Opóźnienia vs. dzień tygodnia cz. 1
#  Zbadaj, jak zmienia się odsetek opóźnień w zależności od **dnia tygodnia**. Zadanie wykonaj w dwóch krokach:
#  1. stwórz zmienną `flight_delays_by_weekday_df` używając metody `groupby`,
#  1. na podstawie zmiennej `flight_delays_by_weekday_df`, wygeneruj odpowiedni wykres zgodnie z dobrymi praktykami.

#  Tutaj pogrupuj dane

# In[24]:


flight_delays_by_weekday_df = round(flight_df[['day_of_week', 'dep_delay']].loc[flight_df['is_delayed'] == 'yes']
                        .groupby(['day_of_week']).agg(['count', 'mean', 'median', 'std', 'min', 'max']),2)
flight_delays_by_weekday_df = pd.DataFrame(flight_delays_by_weekday_df)
flight_delays_by_weekday_df = flight_delays_by_weekday_df['dep_delay']
flight_delays_by_weekday_df.reset_index(inplace = True)
flight_delays_by_weekday_df


#  Tutaj narysuj wykres

# In[25]:


day_of_week = ['Mon', 'Tue', 'Wen', 'Thu', 'Fri', 'Sat', 'Sun']
fig_bar_2 = px.bar(flight_delays_by_weekday_df, x =  day_of_week, y = ['count']) 
fig_bar_2.update_layout(
    font_family="Courier New",
    font_color="black",
    title_font_color="black",
    xaxis_title="Day of Week", 
    yaxis_title="Number of delayes",
    title={'text': '<b>Number of flight delays per weekday in 2019</b>', 'font': {'size': 20}},
)

fig_bar_2.show()


#  # Opóźnienia vs. dzień tygodnia cz. 2
#  Dokonaj agregacji kolumny `day_of_week` do nowej kolumny `is_weekend` w `flight_df`. Jako weekend przyjmij wartości 6, 7.
#  1. Używając grupowania, wyznacz odsetek opóźnień w zależności od tego, czy lot odbywał się w weekend czy nie. Wyniki zapisz do ramki `flight_delays_by_weekend_df` oraz zaokrąglij do dwóch miejsc po przecinku.
#  1. Zaprezentuj graficznie wynik analizy.
#  1. Czy Twoim zdaniem odsetek opóźnień jest zależny od tego, czy lot odbywał się w weekend? Uzasadnij.

#  Tutaj dodaj nową kolumnę `is_weekend` do `flight_df`

# In[26]:


conditions = [(flight_df['day_of_week']<6),
             (flight_df['day_of_week']>=6)
             ]
values = ['no', 'yes']
flight_df['is_weekend'] = np.select(conditions, values)


#  Tutaj dokonaj agregacji danych do ramki `flight_delays_by_weekend_df`

# In[27]:


flight_delays_by_weekend_df = round(flight_df[['is_weekend', 'dep_delay']].loc[flight_df['is_delayed'] == 'yes']
                        .groupby(['is_weekend']).agg(['count', 'mean', 'median', 'std', 'min', 'max']),2)
flight_delays_by_weekend_df = pd.DataFrame(flight_delays_by_weekend_df)
flight_delays_by_weekend_df = flight_delays_by_weekend_df['dep_delay']
flight_delays_by_weekend_df.reset_index(inplace = True)
weekday_ratio = round((flight_delays_by_weekend_df['count'].loc[flight_delays_by_weekend_df['is_weekend'] == 'no']/flight_df['is_weekend'].loc[flight_df['is_weekend'] == 'no'].count()).iloc[0],2)
weekend_ratio = round((flight_delays_by_weekend_df['count'].loc[flight_delays_by_weekend_df['is_weekend'] == 'yes']/flight_df['is_weekend'].loc[flight_df['is_weekend'] == 'yes'].count()).iloc[0],2)

delay_ratio = [weekday_ratio, weekend_ratio]
flight_delays_by_weekend_df['delay_ratio'] = delay_ratio
flight_delays_by_weekend_df


#  Tutaj narysuj wykres używając danych z ramki `flight_delays_by_weekend_df`

# In[28]:


period_of_week = ['work_day', 'weekend']
fig_bar_3 = px.bar(flight_delays_by_weekend_df, x =  period_of_week, y = 'delay_ratio') 

fig_bar_3.update_layout(
    font_family="Courier New",
    font_color="black",
    title_font_color="black",
    xaxis_title="Week part", 
    yaxis_title="Delay_Ratio",
    title={'text': '<b>Flight delayed to total flights per workday/weekend in 2019</b>', 'font': {'size': 20}},
)

fig_bar_3.show()


#  ### Sprawdzenie
#  Uruchom kod poniżej, aby sprawdzić, czy ta część została poprawnie wykonana

# In[29]:


expected_flight_df_by_weekend = {0: 0.19, 1: 0.18}
assert flight_delays_by_weekend_df.to_dict(
) == expected_flight_df_by_weekend, f'Spodziewano się wyników: {expected_flight_df_by_weekend}\n otrzymano  {flight_delays_by_weekend_df}'


#  # Opóźnienia vs. odległość lotu
#  Przeanalizuj kolumnę `distance`, wykonując poniższe polecenia:  
#  1. Podobnie jak dla zmiennej `dep_delay`, wyznacz statystyki opisowe oraz dodatkowo przedstaw percentyle `[0.1, 0.25, 0.5, 0.75, 0.9, 0.95, 0.99]`. Wynik zapisz do zmiennej `flight_distance_analysis_df` oraz zaokrąglij do dwóch miejsc po przecinku.  
#  2. Nakreśl wykres punktowy (`scatter`) używając `distance` oraz `dep_delay`. Narysuj wykres dla losowych 10 tysięcy wierszy. Czy na takim wykresie możesz coś zaobserwować?  
#  3. Usuń z ramki `flight_df`, te wiersze, dla których `distance` jest powyżej 95% percentyla.  
#  4. Używając ramki `flight_df`, dokonaj agregacji zmiennej `distance` co 100 mil do nowej kolumny `distance_agg` oraz wyznacz odsetek opóźnień w każdym koszyku. Wynik zapisz do ramki o nazwie `flight_delays_by_distance_agg_df`.  
#  5. Narysuj wykres słupkowy, używając danych zapisanych w `flight_delays_by_distance_agg_df`.  
#  6. Czy Twoim zdaniem większy dystans oznacza większe prawdopodobieństwo opóźnienia lotu? Uzasadnij.
# 
#  Wskazówka:
#  - Przy generowaniu losowych wierszy przyda sie link do dokumentacji metody `sample`: [klik](https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.sample.html)
#  - Przy generowaniu grup przyda się link do dokumentacji metody `cut`: [klik](https://pandas.pydata.org/docs/reference/api/pandas.cut.html)
# 
#  > Dla dużych zbiorów danych kreślenie wszystkich danych mija się z celem ze względu na czytelność. Mimo że zaprezentujemy pewną część zbioru, zakładamy, że danych na wykresie jest na tyle dużo, że stanowią one reprezentacyjną próbkę populacji.

#  Tutaj dokonaj agregacji danych do ramki `flight_distance_analysis_df`

# In[30]:


flight_distance_analysis_df = round(flight_df[['distance']].agg(['count', 'mean', 'median', 'std', 'min', 'max']),2)

quantile_list = [0.1, 0.25, 0.5, 0.75, 0.9, 0.95, 0.99]
for quantile in quantile_list:
    quantile_name = f'{quantile*100}%'
    value = flight_df['distance'].quantile(quantile)
    flight_distance_analysis_df.loc[quantile_name] = [value]

flight_distance_analysis_df


#  Tutaj narysuj wykres dla 10 000 losowych wierszy z tabeli `flight_df`

# In[65]:


fig_4 = px.scatter(flight_df.sample(n=10000), x = 'distance', y = 'dep_delay', title = 'Flight delay distribution in 2019 for 10000 samples')

fig_4.update_layout(
    font_family="Courier New",
    font_color="black",
    title_font_color="black",
    title_font_size=20,
)

fig_4.show()


#  Tutaj usuń z ramki `flight_df` wiersze, dla których `distance` jest powyżej `95% percentyla` (> 95%)

# In[32]:


flight_df = flight_df.loc[flight_df['distance'] <= flight_df['distance'].quantile(0.95)]
flight_df.shape[0]


# ### Sprawdzenie

# In[33]:


flight_df_expected_rows_amount = 1057391
flight_df_rows_amount = flight_df.shape[0]

assert flight_df_rows_amount == flight_df_expected_rows_amount, f'Oczekiwano {flight_df_expected_rows_amount} wierszy, otrzymano {flight_df_rows_amount}'


#  Tutaj dokonaj agregacji zmiennej `distance` oraz wyznacz odsetek opóźnień

# In[41]:


conditions = [(flight_df['distance']<=100),
             (flight_df['distance']>100) & (flight_df['distance']<=200),
             (flight_df['distance']>200) & (flight_df['distance']<=300),
             (flight_df['distance']>300) & (flight_df['distance']<=400),
             (flight_df['distance']>400) & (flight_df['distance']<=500),
             (flight_df['distance']>500) & (flight_df['distance']<=600),
             (flight_df['distance']>600) & (flight_df['distance']<=700),
             (flight_df['distance']>700) & (flight_df['distance']<=800),
             (flight_df['distance']>800) & (flight_df['distance']<=900),
             (flight_df['distance']>900) & (flight_df['distance']<=1000),
             (flight_df['distance']>1000) & (flight_df['distance']<=1100),
             (flight_df['distance']>1100) & (flight_df['distance']<=1200),
             (flight_df['distance']>1200) & (flight_df['distance']<=1300),
             (flight_df['distance']>1300) & (flight_df['distance']<=1400),
             (flight_df['distance']>1400) & (flight_df['distance']<=1500),
             (flight_df['distance']>1500) & (flight_df['distance']<=1600),
             (flight_df['distance']>1600) & (flight_df['distance']<=1700),
             (flight_df['distance']>1700) & (flight_df['distance']<=1800),
             (flight_df['distance']>1800) & (flight_df['distance']<=1900),
             (flight_df['distance']>1900) & (flight_df['distance']<=2000),
             (flight_df['distance']>2000) & (flight_df['distance']<=2100),
             (flight_df['distance']>2100) & (flight_df['distance']<=2200),
             (flight_df['distance']>2200) & (flight_df['distance']<=2300),
             (flight_df['distance']>2300) & (flight_df['distance']<=2400),
             (flight_df['distance']>2400) & (flight_df['distance']<=2500),      
             (flight_df['distance']>2500)
             ]
values = ['100', '200', '300', '400', '500', '600', '700', '800', '900', '1000','1100', '1200', '1300', '1400', '1500',
          '1600', '1700', '1800', '1900', '2000', '2100', '2200', '2300', '2400', '2500','2501']
flight_df['distance_agg'] = np.select(conditions, values)
flight_df['distance_agg'] = pd.to_numeric(flight_df['distance_agg'])
flight_df['distance_agg'].unique()

flight_delays_by_distance_agg_df = round(flight_df[['distance_agg', 'dep_delay']].loc[flight_df['is_delayed'] == 'yes']
                        .groupby(['distance_agg']).agg(['count', 'mean', 'median', 'std', 'min', 'max']),2)
flight_delays_by_distance_agg_df = pd.DataFrame(flight_delays_by_distance_agg_df)
flight_delays_by_distance_agg_df = flight_delays_by_distance_agg_df['dep_delay']
flight_delays_by_distance_agg_df.reset_index(inplace = True)
flight_delays_by_distance_agg_df.sort_values(by = ['distance_agg'], ascending = True)


distance_agg = []
for i in flight_df['distance_agg'].unique():
    distance_agg.append(i)
distance_agg.sort()
distance_agg


#  Tutaj narysuj wykres słupkowy używając danych zapisanych w `flight_delays_by_distance_agg_df`

# In[44]:


fig_bar_4 = px.bar(flight_delays_by_distance_agg_df, x =  'distance_agg', y = ['count']) 

fig_bar_4.update_layout(
    font_family="Courier New",
    font_color="black",
    title_font_color="black",
    xaxis_title="Distance Group (km)", 
    yaxis_title="Number of delayes",
    title={'text': '<b>Number of flight delays per distance group in 2019</b>', 'font': {'size': 20}},
)

fig_bar_4.show()


#  ## Sprawdzenie
#  Uruchom kod poniżej, aby sprawdzić, czy ta część została poprawnie wykonana

# In[43]:


assert 'distance_agg' in flight_df.columns, 'Nie odnaleziono kolumny distance_agg w ramce flight_df'


#  # Opóźnienia vs. grupa odległości
#  Przeanalizuj kolumnę `distance_group` dostępą w zbiorze danych oraz odpowiedz na poniższe:  
#  1. Dla jakich odcinków zostały wyznaczone poszczególne grupy? Wyznacz maksymalną oraz minimalną `distance` wartość w poszczególnych grupach. Wynik zapisz do ramki `flight_distance_by_distance_group`.  
#  2. Wyznacz prawdopodobieństwo opóźnienia przy użyciu tych grup. Wynik zapisz do ramki `flight_delays_by_distance_group_df`.  
#  3. Używając ramki `flight_delays_by_distance_group_df`, wykreśl odpowiedni wykres wizualizujący dane.  
#  4. Na ile zbieżne są wyniki tej analizy z tą wykonaną w poprzednim punkcie?
# 
# Wskazówka do punktu pierwszego:
#  - Do agregacji danych przyda się metoda `agg`: `Python - analiza danych -> Dzień 5 - Pandas -> Grupowanie`
#  - Dokumentacja metody `agg`: [klik](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.agg.html)

# Tutaj wyznacz odcinki, dla których zostały wyznaczone poszczególne grupy

# In[54]:


flight_distance_groups_agg_df = round(flight_df[['distance_agg', 'dep_delay']].loc[flight_df['is_delayed'] == 'yes']
                        .groupby(['distance_agg']).agg(['count', 'mean', 'median', 'std', 'min', 'max']),2)
flight_distance_groups_agg_df = pd.DataFrame(flight_distance_groups_agg_df)
flight_distance_groups_agg_df = flight_distance_groups_agg_df['dep_delay']
flight_distance_groups_agg_df.reset_index(inplace = True)
flight_distance_groups_agg_df.sort_values(by = ['distance_agg'], ascending = True)
flight_distance_groups_agg_df


#  Tutaj wyznacz odsetek opóźnień w każdej grupie zapisując wyniki do ramki `flight_delays_by_distance_group_df`

# In[57]:


delay_ratio_distance = []
for el in distance_group:
    a = flight_distance_groups_agg_df['count'].loc[flight_distance_groups_agg_df['distance_agg'] == el].iloc[0]
    b = flight_df['distance_agg'].loc[flight_df['distance_agg'] == el].count()
    delay_ratio = round(a/b,2)
    delay_ratio_distance.append(delay_ratio)


flight_distance_groups_agg_df['delay_ratio'] = delay_ratio_distance

flight_distance_groups_agg_df


#  Tutaj narysuj wykres przy użyciu ramki `flight_delays_by_distance_group_df`

# In[64]:


fig_bar_5 = px.bar(flight_distance_groups_agg_df, x =  'distance_agg', y = 'delay_ratio') 
     
fig_bar_5.update_layout(
    font_family="Courier New",
    font_color="black",
    title_font_color="black",
    xaxis_title="Distance Group (km)", 
    yaxis_title="Delay Ratio ",
    title={'text': '<b>Delay ratio per distance group in 2019</b>', 'font': {'size': 20}},
)

fig_bar_5.show()


#  ## Czy większy dystans oznacza większe prawdopodobieństwo opóźnenia lotu?
#  Miejsce na Twój komentarz - czy wykresy można porównać? Czy dają takie same wnioski?

# Możemy zauważyć często pojawiające się opóźnienia na krótkich dystansach, natomiast w okolicach 2000 km częstość opóźnień jest największa. Na podstawie tych danych nie możemy jednoznacznie określić czy dystans ma duży wpływ na opóźnienia lotów. Zbadanie innych czynników mogłoby pomóc, na przykład pogoda lub stan maszyny

#  # Podsumowanie
#  W tym notatniku dość dokładnie przeanalizowaliśmy ramkę `fligh_delays` bez wzbogacania jej o dodatkowe dane z innych źródeł, takich jak dane pogodowe.
# 
#  Zanim przejdziemy dalej, należy zapisać bieżącą postać ramki (najlepiej lokalnie), która zostanie użyta w kolejnym notatniku.
# 
#  > **Wskazówka:**  
#  > Aby uniknąć potencjalnych problemów, najlepiej zapisać ramkę w sposób nawiązujący do tego notatnika, np. `flight_df_01`.

#  Tutaj zapisz ramkę w najdogodniejszy sposób.

# In[67]:


flight_df_01 = flight_df

