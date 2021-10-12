#!/usr/bin/env python
'''
SQL Introducción [Python]
Ejercicios de práctica
---------------------------
Autor: Inove Coding School
Version: 1.1

Descripcion:
Programa creado para poner a prueba los conocimientos
adquiridos durante la clase
'''

__author__ = "Inove Coding School"
__email__ = "alumnos@inove.com.ar"
__version__ = "1.1"

import sqlite3
import os
import sqlalchemy
from sqlalchemy import Column, Integer, String, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship
from config import config 
import csv
# Este módulo config.py se hardcodeó usando configparser, que lo que hace básicamente es configurar las settings,
# permite crear archivos de configuración de forma dinámica con secciones (el nombre de las secciones se escribe entre corchetes
# [nombre_seccion])


# Crear el motor (engine) de la base de datos
engine = sqlalchemy.create_engine("sqlite:///secundaria.db")
base = declarative_base()


# Configuramos el dataset para poder obtener los datos necesarios para llenar las tablas:
# PATH del script actual:
script_path = os.path.dirname(os.path.realpath(__file__))
# Path del archivo de configuración:
config_path = os.path.join(script_path,'config2.ini')
# Con el módulo "config" cargamos el archivo de configuracion al dataset, cuya ruta establecimos en config_path:
dataset = config("dataset",config_path) # Del archivo de configuración, lee la seccion [dataset] (la unica que hay)


class Tutor(base):
    __tablename__ = "tutor"
    id = Column(Integer, primary_key=True)
    name = Column(String)
    
    def __repr__(self):
        return f"Tutor: {self.name}"


class Estudiante(base):
    __tablename__ = "estudiante"
    id = Column(Integer, primary_key=True)
    name = Column(String)
    age = Column(Integer)
    grade = Column(Integer)
    tutor_id = Column(Integer, ForeignKey("tutor.id"))

    tutor = relationship("Tutor")

    def __repr__(self):
        return f"Estudiante: {self.name}, edad {self.age}, grado {self.grade}, tutor {self.tutor.name}"


def create_schema():
    # Borrar todos las tablas existentes en la base de datos
    # Esta linea puede comentarse sino se eliminar los datos
    base.metadata.drop_all(engine)

    # Crear las tablas
    base.metadata.create_all(engine)


def insert_tutor(nombre_tutor):
    #Crear la sesión:
    Session = sessionmaker(bind=engine)
    session = Session()
    
    # Crear tutor:
    tutor = Tutor(name = nombre_tutor) 
    
    # Agregar el tutor a la DDBB:
    session.add(tutor)
    session.commit()
    print(tutor)
    
def insert_estudiante(name, age, grade, tutorid):
    # Crear la sesión:
    Session = sessionmaker(bind=engine)
    session = Session()
    
    # Buscar el tutor:
    query = session.query(Tutor).filter(Tutor.name == tutorid)
    tutor = query.first()
    
    if tutor is None:
        raise Exception(f"Error al crear la persona '{name}'. No existe el tutor '{tutor}'.")
        
    else:   
        # Crear el alumno:
        alumno = Estudiante(name=name, age=age, grade=grade)
        alumno.tutor = tutor

        # Agregar el alumno a la DDBB:
        session.add(alumno)
        session.commit()
        print(alumno)
    
def fill():
    print('Completemos esta tablita!')
    # Llenar la tabla de la secundaria con al menos 2 tutores
    # Cada tutor tiene los campos:
    # id --> este campo es auto incremental por lo que no deberá completarlo
    # name --> El nombre del tutor (puede ser solo nombre sin apellido)
    with open(dataset['tutor']) as fi:
        data = list(csv.DictReader(fi)) # Lista de diccionarios.
        
        for row in data:
            insert_tutor(row['name']) # Inserta el nombre de cada tutor en la tabla Tutor, con el id asingado automaticamente.
            
    # Llenar la tabla de la secundaria con al menos 5 estudiantes
    # Cada estudiante tiene los posibles campos:
    # id --> este campo es auto incremental por lo que no deberá completarlo
    # name --> El nombre del estudiante (puede ser solo nombre sin apellido)
    # age --> cuantos años tiene el estudiante
    # grade --> en que año de la secundaria se encuentra (1-6)
    # tutor --> el tutor de ese estudiante (el objeto creado antes)
    with open(dataset['alumno']) as fi:
        data = list(csv.DictReader(fi))
        
        for row in data:
            insert_estudiante(row['name'], int(row['age']),int(row['grade']),row['tutor_id'])
    # No olvidarse que antes de poder crear un estudiante debe haberse
    # primero creado el tutor.


def fetch(limit=0):
    print('Comprovemos su contenido, ¿qué hay en la tabla?')
    # Crear una query para imprimir en pantalla
    # todos los objetos creaods de la tabla estudiante.
    # Imprimir en pantalla cada objeto que traiga la query
    # Realizar un bucle para imprimir de una fila a la vez
    Session = sessionmaker(bind=engine)
    session = Session()
    query = session.query(Estudiante)
    if limit > 0:
        query = query.limit(limit)
    
    for alumno in query:
        print(alumno)
    

def search_by_tutor(tutor):
    print('Operación búsqueda!')
    # Esta función recibe como parámetro el nombre de un posible tutor.
    # Crear una query para imprimir en pantalla
    # aquellos estudiantes que tengan asignado dicho tutor.

    # Para poder realizar esta query debe usar join, ya que
    # deberá crear la query para la tabla estudiante pero
    # buscar por la propiedad de tutor.name
    
    Session = sessionmaker(bind=engine)
    session = Session()
    
    # Buscar el tutor:
    query = session.query(Estudiante).join(Estudiante.tutor).filter(Tutor.name == tutor)
    estudiante = query.first()
    
    if estudiante is None:
        raise Exception(f"Error. No existe estudiante que tenga por tutor a '{tutor}'.")
        
    
    return estudiante

def modify(id, name):
    print('Modificando la tabla')
    respuesta = ""
    # Deberá actualizar el tutor de un estudiante, cambiarlo para eso debe
    # 1) buscar con una query el tutor por "tutor.name" usando name
    # pasado como parámetro y obtener el objeto del tutor
    # 2) buscar con una query el estudiante por "estudiante.id" usando
    # el id pasado como parámetro
    # 3) actualizar el objeto de tutor del estudiante con el obtenido
    # en el punto 1 y actualizar la base de datos

    # TIP: En clase se hizo lo mismo para las nacionalidades con
    # en la función update_persona_nationality
    
    Session = sessionmaker(bind=engine)
    session = Session()
    
    # Buscamos al tutor ingresado (contemplamos -muy por encima- el caso en que no exista el tutor ingresado)
    querytutor = session.query(Tutor).filter(Tutor.name == name)
    
    if querytutor.first() is None:
        rta = input("El nombre del tutor ingresado no existe. Desea agregarlo? [S/N]: ").upper()
        if rta == "S":
            insert_tutor(name)
            querytutor = session.query(Tutor).filter(Tutor.name == name)
            tutor = querytutor.first()
            
            with open("tutor.csv","a+") as fi:
                fi.write("\n")
                fi.write(tutor.name)
        else:
            raise Exception("Value Error. El nombre del tutor requerido no existe. La operación se ha interrumpido.")
    
    tutor = querytutor.first()
    
    query_est = session.query(Estudiante).filter(Estudiante.id == id)
    estudiante = query_est.first()
    
    while estudiante is None:
        try:
            id = int(input("El ID ingresado no coincide con un ID registrado en la Base de Datos, por favor ingrese un ID válido: "))
            query_est = session.query(Estudiante).filter(Estudiante.id == id)
            estudiante = query_est.first()
        except:
            ValueError("Error. Solamente puede ingresar un número de tipo INT.")
            id = int(input("Ingrese un ID válido: "))  
             
    
    estudiante.tutor = tutor
    
    respuesta = f"Se ha actualizado al alumno con I.D.: {estudiante.id}, '{estudiante.name}'. Su nuevo tutor asignado es: '{tutor.name}'."
    
    session.add(estudiante)
    session.commit()
    print(respuesta)


def count_grade(grade):
    print('Estudiante por grado')
    # Utilizar la sentencia COUNT para contar cuantos estudiantes
    # se encuentran cursando el grado "grade" pasado como parámetro
    # Imprimir en pantalla el resultado
    Session = sessionmaker(bind=engine)
    session = Session()
    
    querycount = session.query(Estudiante).filter(Estudiante.grade == grade).count()
    
    # TIP: En clase se hizo lo mismo para las nacionalidades con
    # en la función count_persona
    rta = f"Actualmente hay {querycount} estudiantes cursando el {grade}° grado."
    return rta

if __name__ == '__main__':
    print("Bienvenidos a otra clase de Inove con Python")
    create_schema()   # create and reset database (DB)
    fill()
    fetch()

    tutor = 'Albert Einstein'
    print(search_by_tutor(tutor))

    nuevo_tutor = 'Lionel Messi'
    id = 24
    modify(id, nuevo_tutor)

    grade = 9
    print(count_grade(grade))
