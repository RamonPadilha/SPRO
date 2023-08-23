import pandas as pd
from pymongo import MongoClient


serverDb = MongoClient('mongodb://localhost:27017/')
database = serverDb['simulado']


table_carros = [
    {'Carro': 'Onix', 'Cor': 'Prata', 'Montadora': 'Chevrolet'},
    {'Carro': 'Polo', 'Cor': 'Branco', 'Montadora': 'Volkswagen'},
    {'Carro': 'Sandero', 'Cor': 'Prata', 'Montadora': 'Renault'},
    {'Carro': 'Fiesta', 'Cor': 'Vermelho', 'Montadora': 'Ford'},
    {'Carro': 'City', 'Cor': 'Preto', 'Montadora': 'Honda'}
]

table_montadoras = [
    {'Montadora': 'Chevrolet', 'País': 'EUA'},
    {'Montadora': 'Volkswagen', 'País': 'Alemanha'},
    {'Montadora': 'Renault', 'País': 'França'},
    {'Montadora': 'Ford', 'País': 'EUA'},
    {'Montadora': 'Honda', 'País': 'Japão'},
]


carros = pd.DataFrame(table_carros)
montadoras = pd.DataFrame(table_montadoras)

carros_convert = carros.to_dict(orient='records')
montadoras_convert = montadoras.to_dict(orient='records')

database['carros'].insert_many(carros_convert)
database['montadoras'].insert_many(montadoras_convert)

for carro in carros_convert:
    montadora_nome = carro['Montadora']
    montadora = database['montadoras'].find_one({'Montadora': montadora_nome})
    if montadora:
        carro['montadora_id'] = montadora['_id']
        database['carros'].update_one({'_id': carro['_id']}, {'$set': {'montadora_id': montadora['_id']}})

pipeline = [
    {
        '$lookup': {
            'from': 'montadoras',
            'localField': 'Montadora',
            'foreignField': 'Montadora',
            'as': 'MontadoraInfo'
        }
    },
    {
        '$unwind': {
            'path': '$MontadoraInfo',
            'preserveNullAndEmptyArrays': True
        }
    },
    {
        '$group': {
            '_id': '$MontadoraInfo.País',
            'Carros': {
                '$push': {
                    'Carro': '$Carro',
                    'Cor': '$Cor',
                    'Montadora': '$Montadora'
                }
            }
        }
    }
]

result = list(database['carros'].aggregate(pipeline))

# Insira os dados agrupados por país em uma nova coleção
for doc in result:
    database['montadoras'].insert_one({'País': doc['_id'], 'Carros': doc['Carros']})

# Feche a conexão com o MongoDB
serverDb.close()
