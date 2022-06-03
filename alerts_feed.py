import telegram
import matplotlib.pyplot as plt
import seaborn as sns
import io
import pandas as pd
import pandahouse as ph
from datetime import date
import sys
import os

connection = {
    'host': 'https://base%',
    'password': '%',
    'user': '%',
    'database': '%'
}

q1 = '''SELECT toStartOfFifteenMinutes(time) as ts,
               toDate(time) as date,
               formatDateTime(ts, '%R') as hm,
               count(DISTINCT user_id) AS users_feed,
               countIf(user_id, action = 'view') as views,
               countIf(user_id, action = 'like') as likes,
               round(countIf(user_id, action = 'like')/countIf(user_id, action = 'view')*100, 2) as CTR
        FROM {db}.feed_actions
        WHERE time >= today()-1 AND time < toStartOfFifteenMinutes(now())
        GROUP BY ts, date, hm
        ORDER BY ts'''

def check_anomaly(df, metric, a=4, n=5):
# Функция поиска аномалий на основе метода межквартильных размахов
    df['q25'] = df[metric].shift(1).rolling(n).quantile(0.25)
    df['q75'] = df[metric].shift(1).rolling(n).quantile(0.75)
    df['iqr'] = df['q75'] - df['q25']
    df['upper'] = df['q75'] + a*df['iqr']
    df['lower'] = df['q25'] - a*df['iqr']
    
    df['upper'] = df['upper'].rolling(n, center=True, min_periods=1).mean()
    df['lower'] = df['lower'].rolling(n, center=True, min_periods=1).mean()
    
    if df[metric].iloc[-1] < df['lower'].iloc[-1] or df[metric].iloc[-1] > df['upper'].iloc[-1]:
        is_alert = 1
    else:
        is_alert = 0
    return is_alert, df

def run_alert_feed(chat = None):
# Функция выгрузки и визуализации метрик, отправки алертов
    chat_id = chat or 483161489
    bot = telegram.Bot(token = os.environ.get('report_bot_token')) #указать в variables в gilab свой токен
    data = ph.read_clickhouse(q1, connection=connection)
    metrics = ['users_feed', 'views', 'likes', 'CTR']
    for metric in metrics:
        df = data[['ts', 'date', 'hm', metric]].copy()
        is_alert, df = check_anomaly(df, metric)
        
        if is_alert == 1:
            msg = f'''Метрика {metric}: \n Текущее значение {df[metric].iloc[-1]}\nОтклонение от предыдущего значения
            {round(1-(df[metric].iloc[-1]/df[metric].iloc[-2]), 2)}%\nСсылка на дашборд: https://superset.'''
        
            sns.set(rc = {'figure.figsize': (19, 13)})
            plt.tight_layout()
        
            ax = sns.lineplot(x=df['ts'], y=df[metric], label='metric') # строим линейный график
            ax = sns.lineplot(x=df['ts'], y=df['upper'], label='upper border')
            ax = sns.lineplot(x=df['ts'], y=df['lower'], label='lower border')
        
            for ind, label in enumerate(ax.get_xticklabels()): # этот цикл нужен чтобы разрядить подписи координат по оси Х
                if ind % 2 == 0:
                    label.set_visible(True)
                else:
                    label.set_visible(False)
                
            ax.set(xlabel='time') # задаем имя оси Х
            ax.set(ylabel=metric) # задаем имя оси У
            ax.set_title(metric)
            ax.set(ylim=(0, None))
            
            # формируем файловый объект
            plot_object = io.BytesIO()
            ax.figure.savefig(plot_object)
            plot_object.name = '{0}.png'.format(metric)
            plot_object.seek(0)
            plt.close()

             # отправляем алерт
            bot.sendMessage(chat_id = chat_id, text = msg)
            bot.sendPhoto(chat_id = chat_id, photo = plot_object)        
                
    return


run_alert_feed()
