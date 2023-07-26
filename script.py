"""
Teste DataOps
Ana Carolina Szczepanski Oliveira
"""
# CONSTRUÇÃO DO DATAFRAME
import pandas as pd
import pymongo 

dfCarros = pd.DataFrame(
    {
        'Carro':[
            'Onix', 
            'Polo', 
            'Sandero', 
            'Fiesta', 
            'City'
        ], 
        'Cor': [
            'Prata', 
            'Branco', 
            'Prata', 
            'Vermelho', 
            'Preto'
        ],
        'Montadora': [
            'Chevrolet', 
            'Volkswagen', 
            'Renault', 
            'Ford', 
            'Honda'
        ]
    }
)

dfMontadoras = pd.DataFrame(
    {
        'Montadora' : [
            'Chevrolet',
            'Volkwsagen',
            'Renault',
            'Ford',
            'Honda'
        ],
        'País' : [
            'EUA',
            'Alemanha',
            'França',
            'EUA',
            'Japão'
        ]
    }
)
# CONEXÃO COM O MONGODB
client = pymongo.MongoClient("localhost", 27017)

db = client['TesteDataOps']

colecaoCarros = db['Carros']
colecaoMontadoras = db['Montadoras']
colecaoAgregacao = db['Agregação']

dataCarros = dfCarros.to_dict(orient='records')
lCarros = colecaoCarros.insert_many(dataCarros)

dataMontadoras = dfMontadoras.to_dict(orient='records')
lMontadoras = colecaoMontadoras.insert_many(dataMontadoras)

# AGREGAÇÃO
pipeline = [
    {
        '$lookup': {
            'from': 'colecaoMontadoras',
            'localField': 'Montadora',
            'foreignField': 'Montadora',
            'as': 'Montadora'
        }
    },
    {
        '$unwind': '$Montadora'
    },
    {
        '$group': {
            '$sum': 'País'
        }
    },
    {
        '$project': {
            'Carro': 1,
            'Cor': 1,
            'Montadora': 1,
            'País': 1
        }
    },
    {
        '$out': 'Agregação'
    }
]

listaAgreg = colecaoCarros.aggregate(pipeline)

lAgreg = colecaoAgregacao.insert_many(listaAgreg)