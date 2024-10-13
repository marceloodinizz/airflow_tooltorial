from airflow.utils.edgemodifier import Label
from datetime import datetime, timedelta
from textwrap import dedent
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow import DAG
from airflow.models import Variable
import sqlite3
import pandas as pd

# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['marcelo.diniz@indicium.tech'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


## Do not change the code below this line ---------------------!!#
def export_final_answer():
    import base64

    # count.txt path
    count_path = '/home/marcelomdm/Lighthouse/Desafio7/airflow_tooltorial/data/count.txt'
    # Import count    
    with open(count_path) as f:
        count = f.readlines()[0]

    my_email = Variable.get("my_email")
    message = my_email+count
    message_bytes = message.encode('ascii')
    base64_bytes = base64.b64encode(message_bytes)
    base64_message = base64_bytes.decode('ascii')

    with open("final_output.txt","w") as f:
        f.write(base64_message)
    return None
## Do not change the code above this line-----------------------##

with DAG(
    'DesafioAirflow',
    default_args=default_args,
    description='Desafio de Airflow da Indicium',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=['example'],
) as dag:
    dag.doc_md = """
        Esse é o desafio de Airflow da Indicium.
    """

# Task 1: Extrair dados e salvar localmente:
def extract_orders_to_csv():
    # Caminho para o banco de dados SQLite
    db_path = '/home/marcelomdm/Lighthouse/Desafio7/airflow_tooltorial/data/Northwind_small.sqlite'

    # Conectar ao banco de dados SQLite
    conn = sqlite3.connect(db_path)

    # Executar a consulta SQL para ler a tabela 'Order'
    query = "SELECT * FROM `Order`"

    # Ler os dados em um DataFrame do Pandas
    orders_df = pd.read_sql_query(query, conn)

    # Fechar a conexão com o banco de dados
    conn.close()

    # Exportar os dados para um arquivo CSV
    output_path = '/home/marcelomdm/Lighthouse/Desafio7/airflow_tooltorial/data/output_orders.csv'
    orders_df.to_csv(output_path, index=False)

# Task que usará a função acima:
extract_orders_task = PythonOperator(
    task_id='extract_orders',
    python_callable=extract_orders_to_csv,
    dag=dag,
)

# Task 2: Processar dados e calcular a soma da quantidade vendida para Rio de Janeiro
def join_and_calculate_quantity():
    # Caminho para o banco de dados SQLite
    db_path = '/home/marcelomdm/Lighthouse/Desafio7/airflow_tooltorial/data/Northwind_small.sqlite'
    
    # Conectar ao banco de dados SQLite
    conn = sqlite3.connect(db_path)
    
    # Ler a tabela OrderDetail
    query_orderdetail = "SELECT * FROM 'OrderDetail'"
    orderdetail_df = pd.read_sql_query(query_orderdetail, conn)
    
    # Fechar a conexão com o banco
    conn.close()
    
    # Ler os dados do CSV gerado pela Task anterior
    output_orders_path = '/home/marcelomdm/Lighthouse/Desafio7/airflow_tooltorial/data/output_orders.csv'
    orders_df = pd.read_csv(output_orders_path)
    
    # Realizar o JOIN entre OrderDetail e output_orders.csv usando a coluna 'OrderId' da tabela OrderDetail e a coluna 'Id' da tabela Order
    merged_df = pd.merge(orderdetail_df, orders_df, left_on='OrderId', right_on='Id')
    
    # Filtrar pelos registros onde ShipCity é "Rio de Janeiro"
    filtered_df = merged_df[merged_df['ShipCity'] == 'Rio de Janeiro']
    
    # Calcular a soma da coluna 'Quantity' para os registros filtrados
    total_quantity = filtered_df['Quantity'].sum()
    
    # Escrever o resultado no arquivo count.txt
    count_output_path = '/home/marcelomdm/Lighthouse/Desafio7/airflow_tooltorial/data/count.txt'
    with open(count_output_path, 'w') as f:
        f.write(str(total_quantity))

# Definir a task no DAG
join_and_calculate_task = PythonOperator(
    task_id='join_and_calculate_quantity',
    python_callable=join_and_calculate_quantity,
    dag=dag,
)
   
export_final_output = PythonOperator(
    task_id='export_final_output',
    python_callable=export_final_answer,
    provide_context=True
)

extract_orders_task >> join_and_calculate_task >> export_final_output