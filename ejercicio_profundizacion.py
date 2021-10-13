"""
EJERCICIO DE PROFUNDIZACIÓN
MERCADO LIBRE
PRODUCTOS

by @andresdeinnocentiis
"""

import os
from pandas.core.frame import DataFrame
import requests
import time
import json
import pandas as pd
import sqlite3
import sqlalchemy
from sqlalchemy import Column, Integer, String, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship


PATH = os.path.dirname(os.path.realpath(__file__))
FILE = "meli_technical_challenge_data.csv"
engine = sqlalchemy.create_engine("sqlite:///productos_MELI.db")
base = declarative_base()


def create_schema():
    base.metadata.drop_all(engine)
    base.metadata.create_all(engine)

# Creamos un dataframe desde un archivo (Al ser Pandas luego será más rápido de leer que una lista)
def get_dataframe(path:str = PATH, file:str = FILE)-> DataFrame:
    try:
        file_path = os.path.join(path,file)
    except:
        ValueError("El PATH o el nombre de archivo ingresado 'file' son erróneos. Revise los parámetros.")
    dataframe = None

    # Chequeamos que la extensión del archivo sea .csv o .xlsx:
    if file[-4:] == ".csv":
        dataframe = pd.read_csv(file_path)
    elif file[-4:] == "xlsx":
        dataframe = pd.read_excel(file_path)
    else:
        raise ValueError("Error. La extensión del archivo ingresado es inválida. Formatos permitidos: '.csv' o '.xlsx'")
        
    dataframe = clean_dataframe(dataframe) 
    
    return dataframe

def clean_dataframe(dataframe:DataFrame)->DataFrame:
    #print("TOTAL DATOS VACÍOS:\n",dataframe.isna().sum())
    clean_dframe = dataframe.dropna()
    #print("TOTAL DATOS VACÍOS DESPUES DE LIMPIAR:\n",dataframe.isna().sum())
    
    return clean_dframe

def create_url(id,site=None): #Esto es así contemplando el caso en que se le pase el id completo, o el id y el site por separado
    
    part_url = 'https://api.mercadolibre.com/items?ids='
    
    if not site and "MLA" in id:
        url = part_url + str(id)
    elif not site and not "MLA" in str(id):
        url = part_url + "MLA" + str(id)
    else:
        url = part_url + site + str(id)
    
    return url


# Función para filtrar los datos de un json obtenido x un item, y quedarnos con los datos que necesitamos, en un diccionario.
def json_to_dic_filtrado(data_json:json)->dict:
    dic_filtrado = {}
    dic_filtrado["id"] = data_json.get("id")[3:] #Removemos "MLA" que ya lo contiene "site_id", para no tener que limpiarlo despues.
    dic_filtrado["site_id"] = data_json.get("site_id")
    dic_filtrado["title"] = data_json.get("title")
    dic_filtrado["price"] = data_json.get("price")
    dic_filtrado["currency_id"] = data_json.get("currency_id")
    dic_filtrado["initial_quantity"] = data_json.get("initial_quantity")
    dic_filtrado["available_quantity"] = data_json.get("available_quantity")
    dic_filtrado["sold_quantity"] = data_json.get("sold_quantity")
    
    return dic_filtrado


class Articulo(base):
    __tablename__ = 'articulo'
    id = Column(String, primary_key=True, autoincrement=False)
    site_id = Column(String)
    title = Column(String)
    price = Column(Integer)
    currency_id = Column(String)
    initial_quantity = Column(Integer)
    available_quantity = Column(Integer)
    sold_quantity = Column(Integer)

    def __repr__(self):
        return f'Articulo\nId: {self.id}\nSite id: {self.site_id}\nTitle: {self.title}\
                 \nPrice: {self.price}\nCurrency id: {self.currency_id},\nInitial Quantity: {self.initial_quantity}\
                 \nAvailable Quantity: {self.initial_quantity}\nSold Quantity: {self.sold_quantity}'
                 
                 
def insert_to_db(data_capt:dict):
    Session = sessionmaker(bind=engine)
    session = Session()
    
    articulo = Articulo(id=data_capt.get("id"), site_id=data_capt.get('site_id'), title=data_capt.get('title'), price=data_capt.get('price'),
                            currency_id=data_capt.get('currency_id'), initial_quantity=data_capt.get('initial_quantity'),
                            available_quantity=data_capt.get('available_quantity'), sold_quantity=data_capt.get('sold_quantity'))
    
    session.add(articulo)
    session.commit()
    
    
def fetch(url:str):
    
    response = requests.get(url)
    
    data_json = response.json()
    data_json_body = data_json[0]["body"] # Esto es así porque es un único resultado, siendo un diccionario adentro de una lista, 
  
    dic = json_to_dic_filtrado(data_json_body) # Convierto a diccionario para buscar valores "None" (celdas vacías) y no incorporarlas
    
    if (not 404 in data_json_body.values()) and (None not in dic.values()):
        insert_to_db(dic) # Vamos a insertar el artículo solamente si la request es 200 y no tiene celdas vacías (de las que necesitamos)
    
    

     

def fill():
    dataframe = get_dataframe()
    for index, row in dataframe.iterrows():
        id = row["id"]
        site = row["site"]
        url = create_url(id,site)
        fetch(url)

def show_db(limit=0,id=None):
    Session = sessionmaker(bind=engine)
    session = Session()
    item = None
    query = ""
    if not id:
        query = session.query(Articulo)
        for articulo in query:
            print(articulo)
    
    elif "MLA" not in str(id):
        query = session.query(Articulo).filter(Articulo.id == id)
        item = query.first()
    else:
        id = id[3:]
        query = session.query(Articulo).filter(Articulo.id == id)
        item = query.first()
        
    if limit > 0:
            query = query.limit(limit)    
    
    if id and item is None:
        print(f"El id ingresado {id} no se corresponde a un artículo de esta base de datos.")
    elif id and item:
        print(f"I.D. requerido: {id}.\n{item}")

         

def crear_db():
    t1 = time.time()
    #Crear DB
    create_schema()
  
    # Completar la DB con el CSV
    fill()
    
    
    
    t2 = time.time()
    print("\n")
    print("Tiempo de procesamiento:", t2-t1)

def comprar():
    salir = 0
    art = ""
    lista_arts = []
    while art != salir:
        art = int(input("Ingrese el ID SIN 'MLA' del artículo que desea comprar o '0' para salir: "))
        if art != salir:
            lista_arts.append(art)

    carrito = ver_carrito(lista_arts)
    cuenta = ""
    ind = 0
    for articulo, precio in carrito.items():
        ind += 1
        cuenta += f"{ind}) {articulo} - ${precio}\n"
        
    total = sum(carrito.values())
    print("\n")
    print(f"TICKET DE COMPRA:\n---------------------------------\n{cuenta}\n---------------------------------\nTOTAL: ${total}\n---------------------------------\n")
    
    os.system("pause")


def ver_carrito(lista_arts):
    Session = sessionmaker(bind=engine)
    session = Session()
    articulo = ""
    carrito = {}
    for item in lista_arts:
        query = session.query(Articulo).filter(Articulo.id == item)   
        articulo = query.first()
        if articulo is None:
            print("\n")
            print(f"El id ingresado {item} no se corresponde a un artículo de esta base de datos.")
        else:
            carrito[articulo.title] = int(articulo.price)
        
    return carrito
        
def menu():
    salir = 0
    menu = "Bienvenido al Sistema de Artículos y Compra robado de MELI.\nQué desea hacer?\n----------------------------------------------------------\n1) Robarle toda la DD.BB. a MELI\n2) Comprar\n3) Ver DD.BB. o buscar un artículo por I.D.\n----------------------------------------------------------\n0) SALIR\n\nIngrese su opción: "
    
    opcion = ""
    while opcion != salir:
        print("\n")
        try:
            opcion = int(input(menu))
        except:
            print("Opción inválida. Solo puede marcar un número")
            opcion = int(input(menu))
        if opcion == 1:
            crear_db()
        elif opcion == 2:
            comprar()
        elif opcion == 3:
            # Leer filas (Creé una función show_db en vez de usar fetch, ya que fetch la usé para tomar datos e incorporarlos)
            id = input("Ingrese el ID del artículo que desea buscar, o presione enter para ver toda la DD.BB.: ")
            show_db(id=id)
            print("\n")
            #show_db(id='MLA845041373') # Para mostrar hay que indicar: id=(el id a buscar), o limit= (el limite deseado)
            #show_db(id='MLA693105237') # En caso de dejar vacío, se muestran todos los artículos.
        

if __name__ == "__main__":
    
    menu()