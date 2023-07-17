import asyncio
from enum import IntEnum
import aiohttp
import ssl

from pprint import pprint
from datetime import datetime
import pandas as pd

import logging

from tenacity import (
    retry,
    stop_after_attempt,
    wait_random_exponential,
) 


api_url = "https://www.nadlan.gov.il/Nadlan.REST/"


class SearchLevel(IntEnum):
    CITY = 2
    NEIGHBORHOOD = 3
    STREET = 4
    GUSH_PARCEL = 6
    ADDRESS = 7


def aiohttp_client_session():
    # Workaround issue with RSA ciphers blocking, see 
    # https://stackoverflow.com/a/71007463
    ssl_ctx = ssl.create_default_context()
    ssl_ctx.set_ciphers('DEFAULT')

    return aiohttp.ClientSession(connector=aiohttp.TCPConnector(ssl=ssl_ctx))


@retry(wait=wait_random_exponential(min=1, max=60), stop=stop_after_attempt(6))
async def get_deals(params, page=0):
    params = {**params, "PageNo": page + 1}
    async with aiohttp_client_session() as session:
        async with session.post(api_url + "Main/GetAssestAndDeals", json=params) as response:
            data = await response.json()
            return data


async def get_deals_by_object_id(object_id, object_type: SearchLevel, page=0):
    return await get_deals({"ObjectID": str(object_id),
                                       # sic.
                                       "CurrentLavel": object_type,
                                       "ObjectKey": "UNIQ_ID",
                                       "ObjectIDType": "text"
                                       }, page=page)

async def get_deals_gush_parcel(gush: int, parcel: int, page=0):
    return await get_deals({"Gush": gush,
                                       "Parcel": parcel,
                                       # sic.
                                       "CurrentLavel": SearchLevel.GUSH_PARCEL
                                       }, page=page) 


async def get_deals_city(city_id: str, page=0):
    return await get_deals_by_object_id(city_id, SearchLevel.CITY)


async def get_deals_neighborhood(id: str, page=0):
    return await get_deals_by_object_id(id, SearchLevel.NEIGHBORHOOD)


async def get_city_list():
    async with aiohttp_client_session() as session:
        async with session.get(api_url + "Main/GetCitysList", params={"nb": "true", "st": "true"}) as response:
            data = await response.json()
            return data


async def get_neighborhoods_list(params):
    async with aiohttp_client_session() as session:
        async with session.get(api_url + "Main/GetNeighborhoodsListByCity", params=params) as response:
            data = await response.json()
            return data


async def get_neighborhoods():
    async with aiohttp_client_session() as session:
        async with session.get(api_url + "Main/GetNeighborhoodsListKey", params={"startWithKey": -1}) as response:
            data = await response.json()
            return data


async def get_streets_list_by_city(params):
    async with aiohttp_client_session() as session:
        async with session.get(api_url + "Main/GetStreetsListByCityAndStartsWith", params={**params, "startWithKey": -1}) as response:
            data = await response.json()
            return data


def should_fetch_more(page_num, is_last_page, max_pages):
    if is_last_page:
        return False
    
    if max_pages is None:
        return True
    
    return page_num < max_pages


def get_search_parameters(city_id, neighborhood_id):
    if neighborhood_id:
        return neighborhood_id, SearchLevel.NEIGHBORHOOD
    
    return city_id, SearchLevel.CITY


async def get_all_houses(city_id=None, neighborhood_id=None, max_pages=None):
    object_id, object_type = get_search_parameters(city_id, neighborhood_id)

    last_page = False
    page_num = 0

    while should_fetch_more(page_num=page_num, is_last_page=last_page, max_pages=max_pages):
        logging.info(f'Starting iteration {object_id=} {object_type=} {page_num=} {max_pages=}')

        response = await get_deals_by_object_id(object_id=object_id,
                                                object_type=object_type,
                                                page=page_num)
        last_page = response['IsLastPage']

        for house in enrich_houses(response):
            yield house

        page_num += 1
        await asyncio.sleep(1)


def enrich_houses(response):
    for resp_house in response['AllResults']:
        house = resp_house.copy()

        gush_parcel = house['GUSH'].split('-')
        house['block'], house['parcel'], house['lot'] = gush_parcel

        price = house['DEALAMOUNT'].replace(',', '')
        house['price'] = int(price)

        house['deal_time'] = datetime.fromisoformat(house['DEALDATETIME'])

        yield house
    

async def main():
    logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[
            logging.FileHandler("debug.log"),
            logging.StreamHandler()
        ]
    )

    # assets_and_deals = await get_deals_gush_parcel(gush=6161, parcel=504)
    # city_list = await get_city_list()
    # neighborhoods_list_by_city = await get_neighborhoods_list_by_city({"CityName": "כוכב יאיר"})
    # neighborhoods = await get_neighborhoods()
    # streets_list_by_city = await get_streets_list_by_city({"CityName": "כוכב יאיר"})

    # assets_and_deals = await get_deals_city(city_id="1224", page=0)
    # print("Assets and Deals\n============\n\n")
    # pprint(assets_and_deals)

    logging.info("Starting fetch")
    # kochav_yair = [house async for house in get_all_houses(city_id=1224, max_pages=10)]
    tzur_igal = [house async for house in get_all_houses(neighborhood_id=65210992, max_pages=10)]

    logging.info("Ending fetch")
    
    df = pd.DataFrame(tzur_igal)
    logging.info(df.head(3))

    df.to_csv('tzur_igal.csv', index=False)

asyncio.run(main())