#!/usr/bin/env python3

import multiprocessing as mp

import pandas as pd

from utils import measure_time


def check_is_anomaly(mean: float, std: float, temperature: float) -> bool:
    lower_bound = mean - 2*std
    upper_bound = mean + 2*std

    return not (lower_bound <= temperature <= upper_bound)


def process_city(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    # 1. Вычислить скользящее среднее температуры с окном в 30 дней для сглаживания краткосрочных колебаний.

    # очевидно что скользящее среднее нужно делать с группировкой по городу,
    # эта функция _уже_ принимает датафрейм для одного конкретного города
    # так что добавляем колонку 'temperature_avg' со скользящим средним,
    # не забывая про дни когда окно ещё не готово (используем fillna)

    days = 30

    df['temperature_avg'] = df['temperature'] \
        .rolling(window = days) \
        .mean() \
        .fillna(df['temperature'])
    
    # 2. Рассчитать среднюю температуру и стандартное отклонение для каждого сезона в каждом городе.

    # снова пользуемся тем фактом что в датафрейме только один город
    # поэтому с кайфом группируем только по сезону
    # для вычисления температуры используем ориг колонку 'temperature'

    season_df = df \
        .groupby(['season'])['temperature'] \
        .agg(['mean', 'std'])
    
    # 3. Выявить аномалии, где температура выходит за пределы среднее ± 2𝜎.

    # у нас в season_df лежит mean и std для каждого сезона
    # так что просто добавим в df колонку is_anomaly,
    # которая true если not(mean-2std <= 'temperature' <= mean+2std)
    # (можно было сделать через манипуляции с колонками но мне проще через функцию)

    def calculate_anomaly(row: pd.Series) -> bool:
        season = season_df.loc[row['season']]

        return check_is_anomaly(season['mean'], season['std'], row['temperature'])

    df['is_anomaly'] = df[['season', 'temperature']] \
        .apply(calculate_anomaly, axis = 1)
    
    # 4. Теперь в df есть колоночка 'is_anomaly', возвращаем.

    return df


def load_data(filename: str) -> pd.DataFrame:
    df = pd.read_csv(filename, parse_dates = ['timestamp'])

    return df


def split_by_city(df: pd.DataFrame) -> tuple[list[str], list[pd.DataFrame]]:
    # по факту здесь написан groupby

    cities = df['city'].unique()
    groups = []

    for city in cities:
        groups.append(df[df['city'] == city])

    return list(cities), groups


def join_by_city(cities: list[str], processed: list[pd.DataFrame]) -> pd.DataFrame:
    results = {
        city: group for city, group in zip(cities, processed)
    }

    df = pd.concat(results.values())
    df = df.reset_index(drop = True)

    return df


def process(df: pd.DataFrame) -> pd.DataFrame:
    # разделяем по городам

    cities, groups = split_by_city(df)

    # процессим каждый город _последовательно_

    processed = [process_city(group) for group in groups]

    # объединяем обратно

    return join_by_city(cities, processed)


def process_parallel(df: pd.DataFrame, workers: int) -> pd.DataFrame:
    # разделяем по городам

    cities, groups = split_by_city(df)

    # процессим каждый город _параллельно_
    # так как df-чик разбит по городам,
    # и города мы обрабатываем независимо друг от друга,
    # то просто запускаем процессинг для каждого города отдельно

    with mp.Pool(workers) as pool:
        processed = pool.map(process_city, groups)

    # объединяем обратно

    return join_by_city(cities, processed)


if __name__ == '__main__':
    # загружаем датафрейм
    # прошу обратить внимание что на данном этапе мы не группируем по городам
    # группировка по городам произойдёт в самих функциях процессинга

    df = load_data('temperature_data.csv')

    # запускаем последовательное исполнение и замеряем время

    with measure_time() as measure:
        result1 = process(df)

    print(f'process time: {measure():.4f} seconds')

    # запускаем параллельное исполнение и замеряем время

    workers = 4

    with measure_time() as measure:
        result2 = process_parallel(df, workers)

    print(f'process parallel time: {measure():.4f} seconds')

    # сравниваем результаты

    print(f'result1 == result2: {result1.equals(result2)}')

    # итоги эксперимента:
    # - у меня получилось что _последовательное_ исполнение
    #   даже быстрее, чем _параллельное_
    #   (0.9 секунд против 1.1 секунды)
    # - я это связываю с тем, что для параллельного исполнения
    #   между процессами копируется много данных (весь датафрейм)
    #   и это занимает много времени
    # - чтобы избежать копирования, нужно запускать
    #   не в разных процессах, а в разных потоках (threading)
    #   но кажется что из-за наличия GIL питон пока не готов к такому
    #   (возможно для такого существуют какие-то нативные расширения pandas)
    # - по этой причине в streamlit используется последовательный анализ

    # может возникнуть вопрос:
    #   а почему вообще используется группировка по городам?
    # ответ:
    #   кажется что город это единственный ключ по которому можно распараллелить
    #   мы считаем скользящее среднее температуры внутри одного города,
    #   поэтому разбить сильнее чем по городам не получилось бы

    # вопрос:
    #   почему бы не вынести группировку по городам и замерять только процессинг?
    # ответ:
    #   я пробовал так, существенно ничего не поменялось,
    #   поскольку сама группировка всё ещё происходит в основном процессе
    #   а в дочерние процессы в любом случае передаются группы по городам
