# etl_python_s3_redshift
## :rocket:  Etl criado em Python para ler dados no Amazon S3 e gravar no Redshift.

Este exemplo foi feito com o dataset mtcars, muito utilizado no aprendizado de análise de dados.

Os dados foram extraídos da revista Motor Trend US de 1974 e abrangem o consumo de combustível e 10 aspectos do design e desempenho de automóveis para 32 automóveis (modelos de 1973 a 1974).

O Etl feito foi criado na necessidade de ler os dados de arquivos em Buckets S3 no cloud da AWS. Logo em seguida serão gravados em um banco de dados do Redshift.
Foi utilizado a biblioteca PGDB que cria uma conexão ao Redshift, essa biblioteca foi construída para o banco de dados PostgreSql, e como o Redshift da Amazon é uma versão modificada dele, a lib poderá ser utilizada também. Além de libs como PG8000 e psycopg2.

A biblioteca pandas foi utilizada para transformar os dados em Dataframes e fazer um tratamento inicial.

A biblioteca boto3 é utilizada para trabalhar com os dados no S3.

Mais informações: 
[Pygresql](https://pygresql.org/contents/pgdb/index.html) |
[Pandas](https://pandas.pydata.org/) | 
[Boto3](https://boto3.amazonaws.com/v1/documentation/api/latest/index.html) | 
[Amazon S3](https://aws.amazon.com/pt/s3/) | 
[Amazon Redshift](https://aws.amazon.com/pt/redshift/)

