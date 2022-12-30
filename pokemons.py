import csv
import json
import requests
from sqlalchemy import create_engine, select
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Table, Integer, String, Boolean , ForeignKey
from sqlalchemy.orm import sessionmaker, relationship

"""REALIZAR CAMBIO SI DESEA EJECUTAR EN LOCALHOST CAMBIAR 'database'por 'localhost' """
engine = create_engine("postgresql://postgres:admin@database:5432/db_pokemons", echo=True, future=True)
Base = declarative_base()

Session = sessionmaker(bind=engine)
session = Session()

PokemonsTypes = Table(
    "pokemons_types",
    Base.metadata,
    Column("id", Integer, primary_key=True),
    Column("pokemon_id", ForeignKey("pokemons.id")),
    Column("type_id", ForeignKey("types.id")),
)

class Abilities(Base):
    __tablename__ = 'abilities'
    
    id = Column(Integer, primary_key=True)
    name= Column(String)
    url= Column(String)
    is_hidden= Column(Boolean)
    slot= Column(Integer)

    pokemons_id = Column(Integer, ForeignKey("pokemons.id"))
    pokemons = relationship("Pokemons", backref ="abilities")

    def __repr__(cls) :
        return cls.name


class Statistics(Base):

    __tablename__ = "statistics"
    
    id = Column(Integer, primary_key=True)
    weight= Column(Integer)
    height= Column(Integer)
    experience= Column(Integer)
    hp= Column(Integer)
    attack= Column(Integer)
    defense= Column(Integer)
    special_attack= Column(Integer)
    special_defense= Column(Integer)
    speed= Column(Integer)

    pokemons_id = Column(Integer, ForeignKey("pokemons.id"))
    pokemons = relationship("Pokemons", backref ="statistics")

    def __repr__(cls) :
        return cls.id


class Types(Base):
    __tablename__ = "types"
    
    id = Column(Integer, primary_key=True)
    name = Column(String)

    _pokemons = relationship('Pokemons', secondary="pokemons_types", backref='types')


class Pokemons(Base):
    __tablename__ = "pokemons"
    
    id = Column(Integer, primary_key=True)
    name= Column(String)
    image= Column(String)

    _abilities: relationship("Abilities", order_by=Abilities.id, backref ="pokemons")

    _statistics: relationship("Statistics", order_by=Statistics.id, backref ="pokemons")

    _types = relationship('Types', secondary="pokemons_types", backref='pokemons')

    def __repr__(cls) :
        return cls.name


"""
    Clase encargada de la creacion de objetos
"""
class DatabaseComunication():
    
    @classmethod
    def buildStatisticsFromStats(cls, list_stats: list, statistics: Statistics):

        for stat in list_stats:

            if stat['stat']['name'] == 'hp':
                statistics.hp = stat['base_stat']

            if stat['stat']['name'] == 'attack':
                statistics.attack = stat['base_stat']

            if stat['stat']['name'] == 'defense':
                statistics.defense = stat['base_stat']

            if stat['stat']['name'] == 'special-attack':
                statistics.special_attack = stat['base_stat']
            
            if stat['stat']['name'] == 'special-defense':
                statistics.special_defense = stat['base_stat']

            if stat['stat']['name'] == 'speed':
                statistics.speed = stat['base_stat']

    @classmethod
    def buildTypes(cls, types_list: list, pokemon: Pokemons):


        for type_item in types_list:
            name_type = type_item['type']['name']
            type_select = session.execute(f"SELECT * FROM types as t WHERE t.name = '{type_item['type']['name']}'").first()
            
            if type_select:
                session.execute(f"INSERT INTO pokemons_types (pokemon_id, type_id) VALUES ({pokemon.id} , {type_select[0]})")
                
            else:
                type = Types( name= name_type )
                session.add(type)
                session.execute(f"INSERT INTO pokemons_types (pokemon_id, type_id) VALUES ({pokemon.id} , {type.id})")
            session.commit()

    @classmethod
    def buildAbilities(cls, list_abilities: list, pokemon: Pokemons):
        list_of_abilities = []

        for ability in list_abilities:
            ability_builded = Abilities(
                    name= ability['ability']['name'],
                    url= ability['ability']['url'],
                    is_hidden= ability['is_hidden'], 
                    slot= ability['slot'],
                    pokemons = pokemon)
            list_of_abilities.append(ability_builded)

        return list_of_abilities

    @classmethod
    def buildPokemonObject(cls, pokemon_receved: dict):

        pokemon = Pokemons(
            name = pokemon_receved['name'],
            image= pokemon_receved['sprites']['other']['official-artwork']['front_default']
            )

        statistics = Statistics(
            weight= pokemon_receved['weight'], 
            height= pokemon_receved['height'], 
            experience= pokemon_receved['base_experience'],
            pokemons = pokemon
            )

        cls.buildStatisticsFromStats(pokemon_receved['stats'], statistics)

        cls.buildAbilities(pokemon_receved['abilities'], pokemon)
        session.add(pokemon)
        session.commit()
        cls.buildTypes(pokemon_receved['types'], pokemon)
        
        


"""
    Clase encargada de ejecutar las llamadas a la API de Pokeapi
"""
class RequestsToApi():

    @classmethod
    def sendRequest(cls, url:str):
        response = requests.get(url)
        
        return json.loads(response.text)

    @classmethod
    def consultAllPokemons(cls):
        url = "https://pokeapi.co/api/v2/pokemon?limit=1154&offset=0"
        result_pokemons = cls.sendRequest(url)

        for pokemon in result_pokemons['results']:
            print(f"SE ESTA CONSULTANDO Y GUARDANDO AL POKEMON: {pokemon['name']}")
            pokemon_information = cls.sendRequest(pokemon['url'] )
            DatabaseComunication.buildPokemonObject(pokemon_information)



"""
    Clase encargada de las consultas SQL para guardar la informacion en csv
"""
class SaveDataCsv():

    @classmethod
    def savePokemonsWithMoreRepeatedTypeOnCsv(cls):
        query = session.execute('WITH tipos_mas_repetidos AS ( \
                                    SELECT pokemon_id \
                                    FROM "pokemons_types" AS pt \
                                    WHERE pt.type_id = (SELECT type_id FROM "pokemons_types" GROUP BY type_id ORDER BY count(*) DESC LIMIT 1)) \
                                SELECT p.* as pokemon_name \
                                FROM tipos_mas_repetidos as tmr \
                                INNER JOIN pokemons p ON p.id = tmr.pokemon_id \
                                ORDER BY p.name;')
        list_types = query.all()


        cls.writeCsv('PokemonsWithMoreRepeatedType.csv', list_types)
    
    @classmethod
    def savePokemonsWithMoreThanTwoTypesOnCsv(cls):
        query = session.execute('WITH pokemons_with_multiple_types AS ( \
                                    SELECT pokemon_id, COUNT(*) \
                                    FROM "pokemons_types" \
                                    GROUP by pokemon_id \
                                    HAVING COUNT(*) > 2 \
                            ) SELECT p.* FROM pokemons_with_multiple_types AS t \
                            INNER JOIN pokemons p ON p.id = t.pokemon_id \
                            ORDER BY p.name;')
        list_types = query.all()

       

        if(list_types):
            cls.writeCsv('PokemonsWithMoreThanTwoTypes.csv', list_types)
        else:
            print("SE GENERARA UN NUEVO REGISTRO EN LA TABLA DE pokemons_type PARA PODER REALIZAR EL LLENADO DEL CSV CORRECTAMENTE...")
            pokemon = session.execute('SELECT p.id FROM pokemons as p LIMIT 1')
            type = session.execute('SELECT t.id FROM types as t LIMIT 1')
            session.execute(f"INSERT INTO pokemons_types (pokemon_id, type_id) VALUES ({pokemon.first()[0]} , {type.first()[0]})")
            session.commit()
            query = session.execute('WITH pokemons_with_multiple_types AS ( \
                                    SELECT pokemon_id, COUNT(*) \
                                    FROM "pokemons_types" \
                                    GROUP by pokemon_id \
                                    HAVING COUNT(*) > 2 \
                            ) SELECT p.* FROM pokemons_with_multiple_types AS t \
                            INNER JOIN pokemons p ON p.id = t.pokemon_id \
                            ORDER BY p.name;')
            list_types = query.all()
            cls.writeCsv('PokemonsWithMoreThanTwoTypes.csv', list_types)
           

    @classmethod
    def writeCsv(cls, title_csv: str, list_items: list):
        with open(title_csv, 'w', newline='') as file:
            writer = csv.writer(file)
            writer.writerow(["pokemon_name", "image" ])
            for x in list_items:
                writer.writerow([
                    x[1],
                    x[2],
                ])   
            file.close()

    @classmethod
    def createCsv(cls):
        cls.savePokemonsWithMoreRepeatedTypeOnCsv()
        cls.savePokemonsWithMoreThanTwoTypesOnCsv()


if __name__ == "__main__":

    Base.metadata.create_all(engine)
    
    """COMENTAR LINEAS PARA CORRER MAS RAPIDO EN LOCALHOST LINE:273,274,275,276"""
    print("CONSULTANDO API Y GUARDANDO EN BD...")
    request = RequestsToApi()
    request.consultAllPokemons()
    print("CONSULTA Y GUARDADO EN BD FINALIZADA...")


    print("CONSULTANDO BD Y GENERANDO CSV...")
    SaveDataCsv.createCsv()
    print("CONSULTA Y GENERACION DE CSV FINALIZADA...")


    print("PROCESO FINALIZADO...")
    
    

    

