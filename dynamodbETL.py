import boto3

#Identificação das tabelas

tableA_name = 'tableA'
tableB_name = 'tableB'

#Cores do Print

RED   = "\033[1;31m"
GREEN = "\033[0;32m"

#Conexão com o DynamoDB

dynamoDBConnection = boto3.resource('dynamodb',   
    aws_access_key_id = 'AKIA2CU42UIW2DRLQVX4',
    aws_secret_access_key = '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',
    region_name = 'sa-east-1'

)

#Realizando um scan nas tabelas com o objetivo de fazer uma comparação final

def ExecuteComparission():
    itemsSourceTable = len(ExecuteScan(tableA_name))
    itemsDestinyTable = len (ExecuteScan(tableA_name))
    if(itemsDestinyTable == itemsSourceTable):
        return True

#Execução de Scan

def ExecuteScan(strTableName):    
    response = dynamoDBConnection.Table(tableA_name).scan()
    data = response['Items']
    items = []
    while 'LastEvaluatedKey' in response:
        response = dynamoDBConnection.Table(tableB_name).scan(ExlusiveStarKey=response['LastEvaluatedKey'])
        data.extend(response['Items'])
    items.extend(data)    
    return items

#Realizando um scan na tabela de Origem

for item in ExecuteScan(tableA_name):
    environment = item['environment']
    accountId = item['accountId']
    type = item['type']
    fullNetwork = item['network'] + "/" + item['mask']

    #Realizando o PUT dos itens na Nova Tabela
    dynamoDBConnection.Table(tableB_name).put_item(
        Item={
            'network':fullNetwork,
            'accountId':accountId,
            'evironment':environment,
            'type':type
        }
    )
    print(GREEN + "Conta: " + accountId + " e Rede: " + fullNetwork +  " inseridos na nova tabela")

if ExecuteComparission():
    print(GREEN + "Migração da Tabela realizada com sucesso!")
else:
    print(RED + "Houve divergência na quantidade dos itens nas tabelas!")
    

