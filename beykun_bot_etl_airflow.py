import pandas as pd
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

import telegram
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import io
import pandahouse as pnd
import datetime

my_token = '8437216106:AAF9hhBPhI1dygR1QoN-E335jQa0LjzCM70'
bot = telegram.Bot(token=my_token)
chat_id = 941917487 

connection = {
    'host': 'http://clickhouse.lab.karpov.courses:8123',
    'database': 'simulator_20251220', 
    'user': 'student', 
    'password': 'dpo_python_2020',
}

# Дефолтные параметры
default_args = {
    'owner': 'v_beykun',
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': datetime.timedelta(minutes=1),
    'start_date': datetime.datetime(2026, 2, 1),
}

# Интервал запуска DAG
schedule_interval = '0 8 * * *'

@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def dag_beykun_new_bot():
    
    @task()
    def extract_feed_actions():
        q = '''select
                  toDate(time) as date,
                  countIf(action = 'like') as likes,
                  countIf(action = 'view') as views,
                  likes / views as ctr,
                  count(distinct(user_id)) as dau
                from
                  simulator_20251220.feed_actions
                where
                  toDate(time) between today() - 7
                  and today() - 1
                group by date
                order by date'''
        
        df = pnd.read_clickhouse(q, connection=connection)
        df['date'] = pd.to_datetime(df['date']).dt.date
        
        return df
    
    @task()
    def send_messages(df):
        
        yesterday = (datetime.datetime.today() - datetime.timedelta(days=1)).date()
            
        df_yest = df[df['date'] == yesterday].reset_index(drop=True)
        
        sum_like = df_yest.at[0, 'likes']
        sum_view = df_yest.at[0, 'views']
        ctr = round(df_yest.at[0, 'ctr'], 2)
        dau = df_yest.at[0, 'dau']

        msg = f"""Ключевые метрики за вчерашний день:
        DAU: {dau};
        Количество лайков: {sum_like};
        Количество просмотров: {sum_view};
        CTR: {ctr}%"""
        
        plt.figure(figsize=(12, 8))

        # График CTR
        plt.subplot(2, 2, 1)
        plt.plot(df['date'], df['ctr'] * 100, marker='o', color='red')
        plt.title('CTR (%) по дням')
        plt.ylabel('CTR (%)')
        plt.grid(True, alpha=0.3)
        plt.xticks(rotation=45)

        # График DAU
        plt.subplot(2, 2, 2)
        plt.plot(df['date'], df['dau'], marker='o', color='skyblue')
        plt.title('DAU по дням')
        plt.ylabel('Уникальные пользователи')
        plt.grid(True, alpha=0.3)
        plt.xticks(rotation=45)

        # График лайков и просмотров
        plt.subplot(2, 1, 2)  # 2 строки, 2 столбца, 1-й график
        plt.plot(df['date'], df['likes'], marker='o', label='Лайки')
        plt.plot(df['date'], df['views'], marker='s', label='Просмотры')
        plt.title('Лайки и просмотры по дням')
        plt.ylabel('Количество')
        plt.legend()
        plt.grid(True, alpha=0.3)
        plt.xticks(rotation=45)

        plt.tight_layout()

        plot_object = io.BytesIO()
        plt.savefig(plot_object, format='png')
        plot_object.seek(0)
        plot_object.name = 'metrics_plot.png'
        plt.close()
        
        bot.sendMessage(chat_id=chat_id, text=msg)
        bot.sendPhoto(chat_id=chat_id, photo=plot_object)
    
    # Запуск задач
    df = extract_feed_actions()    
    # metrics_df = transform_data(df)
    send_messages(df)

# Создание DAG
dag_beykun_new_bot = dag_beykun_new_bot()