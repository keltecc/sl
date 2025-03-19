#!/usr/bin/env python3

import aiohttp
import requests


DEFAULT_HOSTNAME = 'api.openweathermap.org'


class Weather:
    def __init__(self, api_key: str, hostname: str = None) -> None:
        self.api_key = api_key
        self.hostname = hostname or DEFAULT_HOSTNAME

    async def current_async(self, city_name: str, units: str = 'metric') -> float:
        url = f'https://{self.hostname}/data/2.5/weather'
        params = {
            'q': city_name,
            'limit': 1,
            'appid': self.api_key,
            'units': units,
        }

        async with aiohttp.ClientSession() as session:
            async with session.get(url, params = params) as request:
                request.raise_for_status()
                response = await request.json()

        return response['main']['temp']
    
    def current(self, city_name: str, units: str = 'metric') -> float:
        url = f'https://{self.hostname}/data/2.5/weather'
        params = {
            'q': city_name,
            'limit': 1,
            'appid': self.api_key,
            'units': units,
        }

        request = requests.get(url, params = params)
        request.raise_for_status()

        response = request.json()

        return response['main']['temp']


if __name__ == '__main__':
    # не знаю на самом деле даже какой эксперимент тут проводить
    # вроде очевидно что async нужен тогда когда нужна асинхронность (что? да!)
    # и синхронные, и асинхронные HTTP-запросы летят одинаково,
    # разница лишь в том блокируемся ли мы...

    # смысла использовать 'measure_time()' по аналогии с analysis.py нет
    # поскольку в этом тесте вместе с await время будет одинаковое
    # я попробую использовать разные методы в самом streamlit приложении и напишу сюда

    # UPD: я _неиронично_ попробовал использовать `current()` и `current_async()`
    #      если честно разницу не заметил, даже интерфейс в браузере не подлагал
    #      так что хз что тут лучше использовать, кажется разницы нет
    #      (если бы мы делали несколько запросов то можно было бы их распараллелить через async)
    #      в решении оставлю асинхронную версию

    pass
