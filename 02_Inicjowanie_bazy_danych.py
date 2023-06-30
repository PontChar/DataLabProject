#!/usr/bin/env python
# coding: utf-8

#  # Opis notatnika
#  W tym notatniku skupimy się na stworzeniu dedykowanej bazy danych wraz ze strukturą tabel. Jednak w tym momencie nie będziemy jej uzupełniać danymi - tym zajmiemy się w kolejnej części.
#  
#  Ponownie wcielasz się w rolę Data Engineera, którego zadaniem jest dostarczenie klientowi bazy danych, która będzie używana do celów analityczno-raportowych. Bazy danych są podstawowym źródłem pracy dla Analityków Danych.
#  
#  Plik wraz ze strukturą bazy znajdziesz w folderze `sql/database_schema.sql`. 
# 
#  > W tym zadaniu konieczne jest posiadanie zainstalowanego `Postgres` na swoim komputrze. Jeżeli z jakichś powodów został on usunięty po kursie `Python - Analiza Danych`, należy ponownie dokonać instalacji.
# 
#  > Posiadając działającą instancję serwera, można stworzyć po prostu nową bazę np. `airlines`. Stworzenie bazy danych powinno odbyć się w notatniku poprzez stworzenie obiektu cursor i wywołaniu zapytań znajdujących się w pliku .sql.
# 
#  W trakcie rozwiązywania tego notatnika przydadzą się następujące materiały:
#  - `Python - analiza danych -> Przygotowanie do kursu -> Podstawy SQL - Praca samodzielna -> Instalacja bazy danych`
#  - `Python - analiza danych -> Moduł 1 -> Dzień 3 - PostgreSQL -> SQL i Python`
#  - `Python - analiza danych -> Prework -> Podstawy SQL - Praca samodzielna -> Instalacja bazy danych -> Przygotowanie bazy danych`
#  
# Inicjację bazy danych wykonaj w notatniku, nie bezpośrednio na Postgresie.

#  Tutaj zaimportuj potrzebne biblioteki

# In[1]:


from psycopg2 import connect
import regex as re


#  ## Połączenie z bazą danych
#  Tutaj uzupełnij konfigurację połączenia

# In[2]:


username = 'postgres'
password = 'junior21'

host = 'localhost'
database = 'postgresql'
port = None


# Tutaj zdefiniuj zmienną `con` oraz `cursor`

# In[3]:


con = connect(user=username, password=password, host=host, database=database)
con.autocommit = True
cursor = con.cursor()


#  ## Wczytanie pliku `database_schema.sql`
#  Z katalogu `sql` wczytaj plik `database_schema.sql`

# In[4]:


my_text_file = open('C:/Users/patry/OneDrive/Desktop/Koncowego_sprawdzony/sql/database_schema.sql', encoding = "utf8")
with open('C:/Users/patry/OneDrive/Desktop/Koncowego_sprawdzony/sql/database_schema.sql', encoding = "utf8") as my_file:
    for line in my_file.readlines():
        print(line)


#  W tym miejscu odpowiednio rozdziel zawartość pliku `database_schema.sql` na mniejsze kwerendy używając `;`

# In[5]:


my_text_file = open('C:/Users/patry/OneDrive/Desktop/Koncowego_sprawdzony/sql/database_schema.sql', encoding = "utf8")
sentence = ""
with open('C:/Users/patry/OneDrive/Desktop/Koncowego_sprawdzony/sql/database_schema.sql', encoding = "utf8") as my_file:
    for line in my_file.readlines():
        sentence = sentence + line
sentence = sentence.replace("\n", " ")
res = re.sub(' +', ' ', sentence)
sentence = str(res)
sentence
list = sentence.split(";")
for l in list:
    l = l + ";"

print(list)


#  W tym miejscu wykonaj każdą z kwerend, aby zainicjować strukturę bazy danych

# In[6]:


for command in list:
    cursor.execute(command)


#  Zatwierdznie wszystkich operacji na bazie, czyli stworzenie tabel

# In[ ]:





#  ### Sprawdzenie
#  Uruchom kod poniżej, aby sprawdzić, czy ta część została poprawnie wykonana

# In[7]:


def check_if_table_exists(table_name):
    msg = f"Sprawdzam czy istnieje tabela {table_name}"
    print(msg)

    query = f"select 1 from {table_name}"
    # jeżeli tabela nie istnieje, ten krok zwróci wyjątek
    cursor.execute(query)
    print('OK!')


# In[8]:


tables_to_test = [
    'aircraft',
    'airport_weather',
    'flight',
    'airport_list'
]


# In[9]:


for table in tables_to_test:
    check_if_table_exists(table)


# In[10]:


con.close()
msg = "Wszystko wygląda OK :) Możesz przejść do kolejnego zadania."
print(msg)


#  # Podsumowanie
#  Za pomocą tego notatnika została zbudowana w sposób automatyczny nasza docelowa baza danych. Dzięki temu nie musimy się już martwić o jej ręczną przebudowę - w ramach potrzeby wystarczy włączyć notatnik.
#  Wykonując kolejny notatnik, sprawimy, że w tabelach pojawią się również dane potrzebne do przeprowadzenia późniejszej analizy oraz wykorzystywane w raportowaniu.
