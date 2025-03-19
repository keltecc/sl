#!/usr/bin/env python3

import asyncio
import datetime

import pandas as pd
import streamlit as st
import matplotlib.pyplot as plt

import weather
import generate
import analysis


DEFAULT_DATA_FILE = 'temperature_data.csv'


def plot_temperature(df: pd.DataFrame) -> None:
    plt.figure(figsize=(12, 6))

    plt.plot(
        df['timestamp'],
        df['temperature'],
        label = 'Температура',
        alpha = 0.5,
    )

    plt.plot(
        df['timestamp'],
        df['temperature_avg'],
        label = 'Скользящее среднее',
        color = 'black',
    )

    anomaly_df = df[df['is_anomaly'] == True]

    plt.scatter(
        anomaly_df['timestamp'], 
        anomaly_df['temperature'],
        label = 'Аномалии',
        color = 'red',
    )
    
    plt.xlabel('Дата')
    plt.ylabel('Температура')
    plt.title('График температуры с аномалиями')
    plt.legend()


def get_current_season() -> str:
    current_month = datetime.datetime.now().month
    current_season = generate.month_to_season[current_month]

    return current_season


def detect_anomaly(df: pd.DataFrame, temperature: float) -> bool:
    current_season = get_current_season()

    df = df[df['season'] == current_season].reset_index()
    season_df = df['temperature'].agg(['mean', 'std'])

    return analysis.check_is_anomaly(season_df['mean'], season_df['std'], temperature)


async def main() -> None:
    # рисуем меню настроек

    st.sidebar.header('Настройки')

    data_file = st.sidebar.file_uploader('Выберите файл с историческими данными', type = ['csv'])
    if data_file is None:
        st.sidebar.write('Если файл не выбран, то используется сгенерированный файл из задания.')
        data_file = DEFAULT_DATA_FILE

    df = analysis.load_data(data_file)
    df = analysis.process(df)

    cities = list(df['city'].unique())

    city_name = st.sidebar.selectbox('Выберите город', cities)
    if city_name is None:
        city_name = cities[0]

    api_key = st.sidebar.text_input('Введите API-ключ OpenWeatherMap', type = 'password')

    # рисуем результаты анализа

    st.title(f'Результаты анализа для {city_name}')

    df = df[df['city'] == city_name].reset_index()
    anomaly = df[df['is_anomaly'] == True].reset_index()

    season_df = df \
        .groupby('season')['temperature'] \
        .agg(['mean', 'std'])

    st.write(f'Обработано {len(df)} погодных измерений. Среди них найдено {len(anomaly)} аномалий.')

    for season in season_df.index:
        std = season_df.loc[season]['std']
        mean = season_df.loc[season]['mean']
        st.write(f'Средняя температура в {season}: {mean:.4f} градуса, отклонение: {std:.4f}')

    plot_temperature(df)
    st.pyplot(plt)

    st.write(f'Итоговый датафрейм для города {city_name}:')
    st.write(df)

    if not api_key:
        st.warning('Введите API-ключ чтобы получить текущую температуру')
    else:
        if st.button('Получить текущую температуру'):
            client = weather.Weather(api_key)

            try:
                current = client.current(city_name)
                current_season = get_current_season()

                is_anomaly = 'нормальна'

                if detect_anomaly(df, current):
                    is_anomaly = 'аномальна'

                st.success(
                    f'Текущая температура в {city_name}: {current}. '
                    f'Эта температура _{is_anomaly}_ для сезона {current_season}.'
                )
            except Exception as e:
                st.error(f'Ошибка: {str(e)}')


if __name__ == '__main__':
    asyncio.run(main())
