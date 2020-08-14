# Importação das bibliotecas
import pgdb
import pandas as pd
import boto3
from io import StringIO
from boto3.session import Session

# Declaração de acesso ao S3 e ao Redshift
# Lembrando que em ambiente de produção não colocamos as credenciais, este é um exemplo didático
hostname = 'aquivemseuhost.us-east-1.redshift.amazonaws.com'
username = 'seuusuario'
password = 'suasenha'
database = 'seubancodedados'
aws_access_key_id = '########################################'
aws_secret_access_key = '$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$'
bucket_name_origem = 'meu-bucket-no-s3'

# Criando a conexão com o S3
session = Session(aws_access_key_id=aws_access_key_id,
                  aws_secret_access_key=aws_secret_access_key)
s3 = session.resource('s3')
bucket = s3.Bucket('meu-bucket-no-s3')
client = boto3.client('s3', aws_access_key_id=aws_access_key_id,
                      aws_secret_access_key=aws_secret_access_key)

# Laço para ler todos os arquivos dentro da pasta e colocar em um dataframe
for s3_file in bucket.objects.all():
    subdir = s3_file.key.split('/')
    if subdir[0] == 'datasets' and subdir[1] == 'mtcars':
        if "mtcars" in s3_file.key:
            print(s3_file.key)
            client = boto3.client('s3', aws_access_key_id=aws_access_key_id,
                                  aws_secret_access_key=aws_secret_access_key)
            txt_obj = client.get_object(
                Bucket=bucket_name_origem, Key=s3_file.key)
            body = txt_obj['Body']
            csv_string = body.read().decode('8859')
            df = pd.read_csv(StringIO(csv_string), sep=";")
            df.replace('[', "", inplace=True)
            df.replace('[', "", inplace=True)
            df.replace("'", "", inplace=True)

# Laço para criar a string de insert
stringinsertlinha = ""
totalinserts = 0
count = 0
for index, row in df.iterrows():
    model = str(row['model'])
    mpg = str(row['mpg'])
    cyl = str(row['cyl'])
    disp = str(row['disp'])
    hp = str(row['hp'])
    drat = str(row['drat'])
    wt = str(row['wt'])
    qsec = str(row['qsec'])
    vs = str(row['vs'])
    am = str(row['am'])
    gear = str(row['gear'])
    carb = str(row['carb'])

    stringinsertlinha = stringinsertlinha + \
        "insert into seubancodedados.mtcars(model, mpg, cyl, disp, hp, drat, wt, qsec, vs, am, gear, carb)VALUES("
    stringinsertlinha = stringinsertlinha + "'"+model+"','"+mpg+"','"+cyl+"','"+disp + \
        "','"+hp+"','"+drat+"','"+wt+"','"+qsec + \
        "','"+vs+"','"+am+"','"+gear+"','"+carb+"'"
    stringinsertlinha = stringinsertlinha + ");"
    count = count+1
    if count == 100:
        conn = pgdb.connect(host=hostname, user=username,
                            password=password, database=database, port=5439)
        dbcursor = conn.cursor()
        dbcursor.execute(stringinsertlinha)
        conn.commit()
        dbcursor.close()
        stringinsertlinha = ""
        count = 0
if count > 0:
    conn = pgdb.connect(host=hostname, user=username,
                        password=password, database=database, port=5439)
    dbcursor = conn.cursor()
    dbcursor.execute(stringinsertlinha)
    conn.commit()
    dbcursor.close()
    stringinsertlinha = ""
    count = 0

conn.close()
