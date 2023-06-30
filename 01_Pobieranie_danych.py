#!/usr/bin/env python
# coding: utf-8

#  # Opis notatnika
# 
#  Ten notatnik inicjuje naszą pracę nad warsztatem końcowym. Twoim zadaniem tutaj jest pobranie udostępnionych danych do obszaru roboczego, które w następnym kroku wgrasz na bazę danych. Ich obróbkę oraz analizę przeprowadzisz w specjalnie do tego celu przygotowanych kolejnych notatnikach.
#  
#  To tutaj wcielasz się w rolę Data Engineera, którego zadaniem jest poprawne dostarczenie danych Analitykowi Danych. W tym notatniku zrealizujesz pierwsze wymaganie, które otrzymaliśmy od klienta: stworzenie mechanizmu pobierającego dane z udostępnionego API. 
# 
#  Na potrzeby tego warsztatu został stworzony dedykowany serwis API, który dostępny jest pod adresem: https://api-datalab.coderslab.com/api/v2. Dodatkowo udostępniona została dokumentacja, z którą można zapoznać się tutaj: [klik](https://api-datalab.coderslab.com/docs/v2).
# 
#  > Dokumentacja jest czysto techniczna i ma na celu prezentację dostępnych endpointów wraz ze zwracanym typem. W celu przetestowania należy kliknąć przysisk `Authorize`, podać token (dostępny poniżej), a następnie `Try it out!` oraz uzupełnić wymagane pola (parametry requesta).
# 
#  Zgodnie z dokumentacją możemy stwiedzić, że udostępnione zostały nam 4 endpointy:
#  - `airport` - dane o lotnisku,
#  - `weather` - informacje o zarejestrowaniej pogodzie na lotnisku danego dnia,
#  - `aircraft` - dane o samolotach
#  - `flights` - dane o wylotach z danego lotniska per dzień.
# 
#  Wszystkie te źródła musimy pobrać, aby być w stanie wykonać całość warsztatu. W celu pobrania informacji, gdzie wymagany jest parametr `airportId`, posłużymy się listą z pliku `airports.csv`.
# 
#  Przy wykonywaniu tego zadania możesz posłużyć się tym tokenem: `WpzDMZeeCq6tbPdsTHUX8W9mecuUVwXAnmcorefr`.
# 
#  ### Uwagi
#  - Ze względów ćwiczeniowych, konstrukcja poszczególnych endpointów jest różna – w trakcie pracy dokładnie przyjrzyj się, w jaki sposób należy wykonać zapytanie, aby otrzymać odpowiedź.
#  - Pamiętaj o dodaniu `sleep` pomiędzy poszczególnymi wywołaniami endpoint.
#  - Limit wywołań API to 1000/min, zadbaj o nieprzekroczenie tego limitu – w przeciwnym wypadku będzie zwracany błąd 429.

#  # Konfiguracja notatnika

#  Tutaj zaimportuj wymagane biblioteki

# In[74]:


import requests
import pandas as pd
import json 
import time
from datetime import date
from datetime import datetime
from dateutil.relativedelta import relativedelta
from tqdm import tqdm 


#  Tutaj zdefiniuj paramatry połączenia do API

# In[25]:


APIKEY = {'authorization': 'WpzDMZeeCq6tbPdsTHUX8W9mecuUVwXAnmcorefr'}


#  Tutaj wczytaj plik `airports.csv` i dostosuj do dalszych kroków w celu pobierania z kolejnych endpointów. Lista lotnisk jest dostępna w kolumnie `origin_airport_id`.

# In[18]:


df = pd.read_csv('airports.csv' ,
                 sep = ';' ,
                 decimal = '.'
                )
print(df)


#  # Pobieranie `Airport`
#  Zapoznaj się z dokumentacją endpointu `airport`, a następnie pobierz dane dot. poszczególnych lotnisk. Wyniki tego kroku zapisz do ramki `airport_df`, a następnie zapisz do pliku `csv`.
# 
#  ### Wskazówki
#  - Nie wszystkie lotniska dostępne w pliku `airports.csv`, są dostępne w endpoint. Zadbaj o odpowiednie obsłużenie takiej sytuacji,
#  - Do skonwertowania wyników przydatna może okazać się metoda `Pandas` - [from_records](https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.from_records.html),
#  - Artykuł LMS: `Python - analiza danych > Dzień 4 - API > Uwierzytelnianie`
#  - Artykuł LMS: `Python - analiza danych > Przygotowanie do zjazdu 2`

#  Tutaj pobierz dane z endpoint'u `airport`

# In[21]:


def get_airport_data(airport_id):
    response = requests.get(f"https://api-datalab.coderslab.com/api/airport/{airport_id}", headers = APIKEY)
    data_airports = response.json()
    return data_airports

data_list = []
for id in df["origin_airport_id"]:
    if 'error' in get_airport_data(id).keys():
        continue
    else:
        data_list.append(get_airport_data(id))
    time.sleep(0.05)
airport_df = pd.DataFrame(data_list)
airport_df.info()


#  ## Sprawdzenie
#  Uruchom kod poniżej, aby sprawdzić czy ta część została poprawnie wykonana

# In[23]:


airport_df_expected_shape = (97, 4)
assert airport_df_expected_shape == airport_df.shape


#  Tutaj zapisz ramkę `airport_df` do pliku `airport_list.csv`

# In[24]:


airport_df.to_csv('airport_list.csv',
    sep=';',
    decimal='.',
    index=False)


#  # Pobieranie `Weather`
#  Zapoznaj się z dokumentacją endpotu `Weather`, następnie pobierz dane dotyczące zarejestrowanej pogody na poszczególnych lotniskach. Wyniki zapisz do ramki `weather_df`, a później do pliku `airport_weather.csv`.
# 
#  Wskazówki:
#  - Ze względu na wolumen danych, które tutaj się pobiorą, odradzamy zapisywanie danych bezpośrednio do ramki. Rekomendujemy podejście podobne do tego z warsztatu na kursie `Python - analiza danych` - `Dzień 10 - Warsztat > Warsztat > Scrapowanie danych`, czyli stworzenie listy, a następnie przekonwertowanie jej w postać ramki.
#  - Data początkowa danych to `2019-01-01`, zaś data końcowa to `2020-03-31`, czyli 15 miesięcy,
#  - Ze względu na czas, jaki ten krok będzie się wykonywał, warto dodać w pętli instrukcję (lub kilka) `print`, aby monitorować przebieg wykonywania tego kroku.
#  - Przy dodawaniu miesięcy do daty może przydać się metoda [relativedelta](https://www.geeksforgeeks.org/python-get-month-from-year-and-weekday/).

# In[28]:


def get_airport_weather(date):
    response = requests.get(f"https://api-datalab.coderslab.com/api/airportWeather?date={date}", headers = APIKEY)
    data_airport_weather = response.json()
    return data_airport_weather

data_weather_list = []
my_date = date(2019, 1, 1)
my_date.strftime('%Y-%m-%d')
max_date = date(2020, 3, 31)
max_date.strftime('%Y-%m-%d')
while my_date <= max_date:
    data_weather_list.append(get_airport_weather(my_date))
    time.sleep(0.05)
    my_date = my_date + relativedelta(months=1)
    print(f'month {my_date - relativedelta(months=1)} processed')
    
list_keys = []
for i in range(len(data_weather_list)):
    for k in range(len(data_weather_list[k])):
        list_keys_temp = []
        for key in data_weather_list[i][k]:
            list_keys_temp.append(key)
            for l in list_keys_temp:
                if l not in list_keys:
                    list_keys.append(l)
                    
airport_weather_df = pd.DataFrame(columns = list_keys)
for i in range(len(data_weather_list)):
    airport_weather_df_temp = pd.DataFrame(data_weather_list[i])
    airport_weather_df = pd.concat([airport_weather_df,  airport_weather_df_temp])
airport_weather_df.info()


#  ## Sprawdzenie
#  Uruchom kod poniżej, aby sprawdzić, czy ta część została poprawnie wykonana

# In[29]:


airport_weather_df_expected_shape = (46226, 33)
assert airport_weather_df_expected_shape == airport_weather_df.shape


#  ## Zapis do pliku
#  Tutaj zapisz ramkę `weather_df` do pliku `airport_weather.csv` w katalogu `data/raw`

# In[31]:


airport_weather_df.to_csv('airport_weather.csv',
    sep=';',
    decimal='.',
    index=False)


#  # Pobranie `Aircraft`
#  Zapoznaj się z dokumentacją endpointu `aircraft`, a następnie pobierz dane produkcyjne samolotów. Wyniki zapisz do ramki `aircraft_df`, a następnie zapisz do pliku `aircraft.csv`.
# 

# In[39]:


response= requests.get("https://api-datalab.coderslab.com/api/aircraft", headers = APIKEY)
data_aircraft = response.json()
aircraft_df = pd.DataFrame(data_aircraft)


#  ## Sprawdzenie
#  Uruchom kod poniżej, aby sprawdzić, czy ta część została poprawnie wykonana

# In[40]:


aircraft_df_expected_shape = (7383, 3)
assert aircraft_df_expected_shape == aircraft_df.shape


#  ## Zapis do pliku
#  Tutaj zapisz ramkę `aircraft_df` do pliku `aircraft.csv` w katalogu `data/raw`

# In[42]:


aircraft_df.to_csv('aircraft.csv',
    sep=';',
    decimal='.',
    index=False)


#  # Pobranie `Flight`
#  Zapoznaj się z dokumentacją endpointu `flights`, następnie pobierz dane dotyczące ruchu lotniczego. Wyniki zapisz do ramki `flight_df`, a później do pliku `flight.csv`.
# 
#  Wskazówki:
#  - Zwróć szczególną uwagę na konstrukcję endpointa,
#  - Ze względu na wolumen danych, które tutaj się pobiorą, odradzamy zapisywanie danych bezpośrednio do ramki. Rekomendujemy podejście podobne do tego, z warsztatu na kursie `Python - analiza danych` - `Dzień 10 - Warsztat > Warsztat > Scrapowanie danych`,
#  - Data początkowa danych to `2019-01-01`, zaś końcowa to `2020-03-31`, czyli 456 dni,
#  - Ze względu na czas, jaki ten krok będzie się wykonywał, warto dodać w pętli instrukcję (lub kilka) `print`, aby monitorować przebieg wykonywania tego kroku,
#  - W przypadku, gdy nie ma dostępnych danych dla danego lotniska, API zwraca kod [204](https://developer.mozilla.org/en-US/docs/Web/HTTP/Status/204), w ten sposób możesz pominąć lotniska, dla których dane nie są dostępne,
#  - Pobranie całości danych zajmuje dłuższą chwilę, zanim włączysz pętle dla wszystkich danych, sprawdź pobieranie danych dla jednego, dwóch lotnisk aby uniknąć frustracji.

# In[77]:


def get_flights_data(airport_id, date):
    response = requests.get(f"https://api-datalab.coderslab.com/api/v2/flight?airportId={airport_id}&date={date}", headers = APIKEY)
    if response.status_code == 200:
        data_flights = response.json()
        return data_flights
    else:
        return None


# In[80]:


data_list = []
my_date = date(2019, 1, 1)
my_date.strftime('%Y-%m-%d')
max_date = date(2020, 3, 31)
max_date.strftime('%Y-%m-%d')

while my_date <= max_date:
    for id in tqdm(df["origin_airport_id"]):
        if get_flights_data(id, my_date) != None:
            data_list.append(get_flights_data(id, my_date))
        else:
            continue
    time.sleep(0.05)
    my_date = my_date + relativedelta(months=1)
    print(f'month {my_date - relativedelta(months=1)} processed')


# In[82]:


list_keys = []
for i in tqdm(range(len(data_list))):
    for k in range(len(data_list[i])):
        list_keys_temp = []
        for key in data_list[i][k]:
            list_keys_temp.append(key)
            for l in list_keys_temp:
                if l not in list_keys:
                    list_keys.append(l)


# In[90]:


flight_df = pd.DataFrame(columns = list_keys)
for i in tqdm(range(len(data_list))):
    flight_df_temp = pd.DataFrame(data_list[i])
    flight_df = pd.concat([flight_df,  flight_df_temp])


# In[93]:


flight_df.shape


#  ## Sprawdzenie
#  Uruchom kod poniżej, aby sprawdzić, czy ta część została poprawnie wykonana

# In[91]:


flight_df_expected_shape = (1386120, 27)
assert flight_df_expected_shape == flight_df.shape


#  ## Zapis do pliku
#  Tutaj zapisz ramkę `flight_df` do pliku `flight.csv` w katalogu `data/raw`

# In[92]:


flight_df.to_csv('flight.csv',
    sep=';',
    decimal='.',
    index=False)


#  # Podsumowanie
#  W tym notatniku wykonaliśmy podstawowy krok w analizie danych - pozyskaliśmy je. Są gotowe do dalszej pracy, czyli możemy załadować je na bazę danych, a następnie zapoznać się z tym, jakie informacje ze sobą niosą. Kolejne notatniki będą służyły właśnie tym celom.

# In[86]:


msg = "Wszystko wygląda OK :) Możesz przejść do kolejnego kroku."
print(msg)

