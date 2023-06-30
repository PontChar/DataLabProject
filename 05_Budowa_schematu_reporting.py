#!/usr/bin/env python
# coding: utf-8

#  # Opis notatnika
#  Analiza przygotowana w poprzednim kroku została odebrana pozytywnie. W związku z tym, zostaliśmy poproszeni
#  o przygotowanie raportu na podstawie danych z roku 2020. Ma być on później  wdrożony przez zespół IT na zewnętrzny
#  serwis. Wybór padł na `Dash`.
# 
#  Zadanie wykonamy w dwóch krokach:  
#  1. Znając wymagania raportu, stworzymy na bazie danych odpowiednie komponenty, które zostaną następnie wykorzystane do wizualizacji.  
#  1. Stworzymy raport używając `Dash`.
# 
#  Ten notatnik skupia się na odpowiednim przygotowaniu bazy danych pod cele raportowe.
#  
#  W tej części projektu końcowego wcielasz się w rolę BI Engineera, który ma za zadanie stworzyć raport zgodny z wytycznymi biznesowymi dostarczonymi przez klienta.

#  # Wymagania biznesowe
#  Po prezentacji wyników analizy na niezależnym spotkaniu, zostały określone następujące obszary raportowania odsetka opóźnień lotów:  
#  1. Wyświetlanie TOP 10 (w sensie najwyższego odsetka opóźnień) lotnisk na podstawie liczby odlotów, wyświetlona ma być również informacja o liczbie przylotów - widok `top_airports_by_departure`.  
#  1. Wyświetlenie TOP 10 (w sensie najwyższego odsetka opóźnień) tras lotów. Przy czym istotna dla nas jest kolejność, przykładowo trasa (Warszawa, Paryż) jest inna niż (Paryż, Warszawa). Dodatkowym wymaganiem jest, aby minimalna liczba lotów odbytych na trasie wynosiła co najmniej 10000 przelotów - widok `top_reliability_roads`.  
#  1. Porównanie roku 2019 vs. 2020, aby śledzić wpływ COVID na realizację lotów. Interesują nas podejścia:  
#      - miesiąc do miesiąca, przykładowo odsetek opoźnień styczeń 2019 vs. odsetek opoźnień styczeń 2020, odsetek opoźnień luty 2019 vs. odsetek opoźnień luty 2020 itd. - widok `year_to_year_comparision`,  
#      - dzień do dnia, przykładowo odsetek opoźnień wtorek 2019 vs. odsetek opoźnień wtorek 2020 - widok `day_to_day_comparision`.  
#  1. Dzienny, czyli jak danego dnia, globalnie wyglądał wskaźnik opóźnień lotów samolotu, tj. odsetek opóźnień 01-01-2019, odsetek opóźnień 02-01-2019 itd.

#  # Podejście techniczne do problemu
#  Naszym celem będzie odseparowanie warstwy przygotowania danych (logika raportu) od warstwy prezentacyjnej (wizualizacja).
#  Chcemy zapewnić, aby odpowiednie procesy zajmowały się tylko swoimi zadaniami. 
#  
#  > W tym podejściu warstwa prezentacyjna (wykres/raport) nie implementuje logiki biznesowej w celu przetwarzania danych. Innymi słowy, nie chcemy aby przykładowo agregacja była wykonywana w momencie tworzenia wizualizacji.
# 
#  To podejście będzie spójne ze współczesnym sposobem projektowania aplikacji. Ma to też dodatkowy benefit - ze względu na wolumen danych na bazie, nie musimy ich najpierw pobierać - warstwa logiczna je odpowiednio zagreguje i przekaże zdecydowanie mniejszą liczbę wierszy, co przyśpieszy działanie całości.
# 
#  > Logika działania tego notebooka jest zbieżna z tą, którą robiliśmy już na przykładzie `Inicjowania bazy danych` - warto mieć go pod ręką.

#  # Przygotowanie bazy danych
#  Na bazie danych, gdzie umieszczone są już dane, wszystko zawarte jest na schemacie `public`. Ponieważ zgodnie z wymaganiami otrzymujemy nowy obszar wykorzystania danych, stworzymy sobie schemat dedykowany - `reporting`.  
#  Dalej stworzymy widoki, które odpowiedzą na zadane wcześniej pytania.
# 
#  > Stworzenie dedykowanego schematu ma więcej korzyści niż nam się wydaje, w ten sposób możemy łatwo wprowadzić zabezpieczenie na dane, które chcemy udostępniać.
# 
#  ## Stworzenie dedykowanego schematu
#  W pliku `reporting.sql` napisz kwerendę, która stworzy (o ile już nie istnieje) schemat `reporting`.
#  Ten temat nie był omawiany w trakcie trwania kursu, jednak łatwo można uzupełnić wiedzę czytając np. [ten](https://www.postgresqltutorial.com/postgresql-administration/postgresql-create-schema/) samouczek.

#  # Aktualizacja bazy danych
#  W tym miejscu odpowiednio skonfiguruj połączenie do bazy danych.

#  Tutaj zaimportuj potrzebne biblioteki

# In[1]:


import psycopg2
from psycopg2 import connect
from tqdm import tqdm
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.engine import URL
from pandas import DataFrame
import seaborn as sns
import numpy as np
import threading
import datetime


#  ## Konfiguracja połączenia
#  Tutaj uzupełnij konfigurację połączenia

# In[2]:


username = 'postgres'
password = 'junior21'

host = 'localhost'
database = 'postgresql'
port = 5432


#  Tutaj zdefiniuj zmienną `con` oraz `cursor`

# In[3]:


con = connect(user=username, password=password, host=host, database=database)
con.autocommit = True
cursor = con.cursor()


#  ## Wczytanie pliku `reporting.sql`
#  Z katalogu `sql` wczytaj plik `reporting.sql`

# In[7]:


my_text_file = open('C:/Users/patry/OneDrive/Desktop/Koncowego_sprawdzony/sql/reporting.sql', encoding = "utf8")
with open('C:/Users/patry/OneDrive/Desktop/Koncowego_sprawdzony/sql/reporting.sql', encoding = "utf8") as my_file:
    for line in my_file.readlines():
        print(line)


#  W tym miejscu odpowiednio rozdziel zawartość pliku `reporting.sql` na mniejsze kwerendy używając `;`

# In[8]:


my_text_file = open('C:/Users/patry/OneDrive/Desktop/Koncowego_sprawdzony/sql/reporting.sql', encoding = "utf8")
sentence = ''
with open('C:/Users/patry/OneDrive/Desktop/Koncowego_sprawdzony/sql/reporting.sql', encoding = "utf8") as my_file:
    for line in my_file.readlines():
        sentence = sentence + line
        list = sentence


#  W tym miejscu wykonaj każdą z kwerend, aby zainicjować strukturę bazy danych

# In[9]:


cursor.execute(list)


#  Zatwierdzenie wszystkich operacji na bazie, czyli stworzenie widoków

# In[ ]:





#  ### Sprawdzenie

# In[10]:


# Ten kod chyba wygląda znajomo....
# istnienie widoków możemy sprawdzić tak samo jak tabele
def check_if_table_exists(table_name):
    msg = f"Sprawdzam czy istnieje tabela {table_name}"
    print(msg)

    query = f"select 1 from {table_name}"
    # jeżeli tabela nie istnieje, ten krok zwróci wyjątek
    cursor.execute(query)
    print('OK!')


# In[11]:


views_to_test = [
    'reporting.flight',
    'reporting.top_reliability_roads',
    'reporting.year_to_year_comparision',
    'reporting.day_to_day_comparision',
    'reporting.day_by_day_reliability'
]


# In[12]:


for view in views_to_test:
    check_if_table_exists(view)


# In[ ]:


con.close()
msg = "Wszystko wygląda OK :) Możesz przejść do kolejnego zadania."
print(msg)


#  # Podsumownie
#  W tym notatniku stworzyliśmy nowy schemat - `reporting`, którego zadaniem jest przygotowanie naszych danych
#  do wizualizacji. Dalsza część pracy będzie polegała na wyświetleniu w wizualnie atrakcyjny sposób danych w interaktywnym raporcie stworzonym
#  z pomocą `Dash`.

# In[ ]:




