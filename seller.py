import io
import logging.config
import os
import re
import zipfile
from environs import Env

import pandas as pd
import requests

logger = logging.getLogger(__file__)


def get_product_list(last_id, client_id, seller_token):
    """
    Retrieve a list of products from the Ozon seller's store.

    This function fetches a list of products based on the given `last_id`, using the provided
    `client_id` and `seller_token` to authenticate against the Ozon seller API.

    Args:
        last_id (str): The ID of the last product from which to begin retrieval.
        client_id (str): The client ID for API access.
        seller_token (str): The API token for the seller's store.

    Returns:
        dict: The result containing the products list and other related data.

    Raises:
        HTTPError: Raises an exception if the request to the API fails.

    Examples:
        >>> get_product_list("012345", "YOUR_CLIENT_ID", "YOUR_SELLER_TOKEN")
        {"items": [{"product_id": 123, "name": "Watch", ...}], ...}
    """
    url = "https://api-seller.ozon.ru/v2/product/list"
    headers = {
        "Client-Id": client_id,
        "Api-Key": seller_token,
    }
    payload = {
        "filter": {
            "visibility": "ALL",
        },
        "last_id": last_id,
        "limit": 1000,
    }
    response = requests.post(url, json=payload, headers=headers)
    response.raise_for_status()
    response_object = response.json()
    return response_object.get("result")


def get_offer_ids(client_id, seller_token):
    """
    Retrieves a list of offer IDs from the server.

    Args:
        client_id (str): The client's ID.
        seller_token (str): The seller's authentication token.

    Returns:
        List[str]: A list of strings, each representing an offer ID.

    Raises:
        Exception: Invalid client_id or seller_token, or a server-side error may result in retrieval failure.

    Examples:
        # Assuming an initialized server with available offer IDs.
        >>> get_offer_ids("client_123", "token_456")
        ["123", "456", "789", ...]
    """
    last_id = ""
    product_list = []
    while True:
        some_prod = get_product_list(last_id, client_id, seller_token)
        product_list.extend(some_prod.get("items"))
        total = some_prod.get("total")
        last_id = some_prod.get("last_id")
        if total == len(product_list):
            break
    offer_ids = []
    for product in product_list:
        offer_ids.append(product.get("offer_id"))
    return offer_ids


def update_price(prices: list, client_id, seller_token):
    """
    Updates prices on the server based on the provided list of prices.

    Args:
        prices (List[Dict[str, str]]): A list of dictionaries, each representing a price entry.
        client_id (str): The client's ID.
        seller_token (str): The seller's authentication token.

    Returns:
        None: This function returns nothing, but it updates the prices on the server side.

    Raises:
        Exception: Invalid client_id or seller_token, or a server-side error may result in update failure.

    Examples:
        >>> update_price([{"offer_id": "123", "price": "5990"}], "client_123", "token_456")
        # No return value, but the price for offer_id "123" has been updated to "5990" on the server.
    """
    url = "https://api-seller.ozon.ru/v1/product/import/prices"
    headers = {
        "Client-Id": client_id,
        "Api-Key": seller_token,
    }
    payload = {"prices": prices}
    response = requests.post(url, json=payload, headers=headers)
    response.raise_for_status()
    return response.json()


def update_stocks(stocks: list, client_id, seller_token):
    """
    Updates stock quantities on the server based on the provided list of stocks.

    Args:
        stocks (List[Dict[str, Union[str, int]]]): A list of dictionaries, each representing a stock entry.
        client_id (str): The client's ID.
        seller_token (str): The seller's authentication token.

    Returns:
        None: This function returns nothing, but it updates the stocks on the server side.

    Raises:
        Exception: Invalid client_id or seller_token, or a server-side error may result in update failure.

    Examples:
        >>> update_stocks([{"offer_id": "123", "stock": 5}], "client_123", "token_456")
        # No return value, but the stock for offer_id "123" has been updated to 5 on the server.
    """
    url = "https://api-seller.ozon.ru/v1/product/import/stocks"
    headers = {
        "Client-Id": client_id,
        "Api-Key": seller_token,
    }
    payload = {"stocks": stocks}
    response = requests.post(url, json=payload, headers=headers)
    response.raise_for_status()
    return response.json()


def download_stock():
    """
    Downloads a list of watch remnants (stock) from the CASIO website.

    Returns:
        List[Dict[str, Any]]: A list of dictionaries, each representing a watch remnant.

    Raises:
        Exception: Server connection issues or unavailability may result in download errors.

    Examples:
        # Assuming an initialized server with available data.
        >>> download_stock()
        [{"Код": "123", "Количество": "5", ...}, ...]
    """
    # Скачать остатки с сайта
    casio_url = "https://timeworld.ru/upload/files/ostatki.zip"
    session = requests.Session()
    response = session.get(casio_url)
    response.raise_for_status()
    with response, zipfile.ZipFile(io.BytesIO(response.content)) as archive:
        archive.extractall(".")
    # Создаем список остатков часов:
    excel_file = "ostatki.xls"
    watch_remnants = pd.read_excel(
        io=excel_file,
        na_values=None,
        keep_default_na=False,
        header=17,
    ).to_dict(orient="records")
    os.remove("./ostatki.xls")  # Удалить файл
    return watch_remnants


def create_stocks(watch_remnants, offer_ids):
    """
    Generates a list of stocks based on available watch remnants and offer IDs.

    Args:
        watch_remnants (List[Dict[str, Any]]): A list of watches with their details.
        offer_ids (List[str]): A list of available offer IDs.

    Returns:
        List[Dict[str, Union[str, int]]]: A list of dictionaries, each representing a stock entry.

    Raises:
        ValueError: Unexpected data format may yield incorrect stock entries.

    Examples:
        >>> create_stocks([{"Код": "123", "Количество": "5"}], ["123"])
        [{"offer_id": "123", "stock": 5}]

        >>> create_stocks([{"WrongKey": "123"}], ["123"])
        [{"offer_id": "123", "stock": 0}]
    """
    # Уберем то, что не загружено в seller
    stocks = []
    for watch in watch_remnants:
        if str(watch.get("Код")) in offer_ids:
            count = str(watch.get("Количество"))
            if count == ">10":
                stock = 100
            elif count == "1":
                stock = 0
            else:
                stock = int(watch.get("Количество"))
            stocks.append({"offer_id": str(watch.get("Код")), "stock": stock})
            offer_ids.remove(str(watch.get("Код")))
    # Добавим недостающее из загруженного:
    for offer_id in offer_ids:
        stocks.append({"offer_id": offer_id, "stock": 0})
    return stocks


def create_prices(watch_remnants, offer_ids):
    """
    Generates a list of prices based on available watch remnants and offer IDs.

    Args:
        watch_remnants (List[Dict[str, Any]]): A list of watches with their details.
        offer_ids (List[str]): A list of available offer IDs.

    Returns:
        List[Dict[str, Any]]: A list of dictionaries, each representing a price entry.

    Raises:
        ValueError: Unexpected data format may yield incorrect price entries.

    Examples:
        >>> create_prices([{"Код": "123", "Цена": "5'990.00 руб."}], ["123"])
        [{"auto_action_enabled": "UNKNOWN", "currency_code": "RUB", "offer_id": "123", "old_price": "0", "price": "5990"}]
    """
    prices = []
    for watch in watch_remnants:
        if str(watch.get("Код")) in offer_ids:
            price = {
                "auto_action_enabled": "UNKNOWN",
                "currency_code": "RUB",
                "offer_id": str(watch.get("Код")),
                "old_price": "0",
                "price": price_conversion(watch.get("Цена")),
            }
            prices.append(price)
    return prices


def price_conversion(price: str) -> str:
    """
    Converts a price string into a numerical format.

    This function takes a string in the format "5'990.00 руб." and transforms it into "5990",
    removing all non-digit characters and the dot with subsequent numbers.

    Args:
        price (str): A string representing the product's price.

    Returns:
        str: The price in numerical format, represented as a string.

    Raises:
        ValueError: Incorrect input string format may lead to unpredictable results.

    Examples:
        >>> price_conversion("5'990.00 руб.")
        '5990'

        >>> price_conversion("Some random text")
        'Some'
    """
    return re.sub("[^0-9]", "", price.split(".")[0])


def divide(lst: list, n: int):
    """
    Splits a list into chunks of size 'n'.

    Args:
        lst (List[Any]): A list to be divided.
        n (int): The size of each chunk.

    Returns:
        Generator[List[Any], None, None]: A generator yielding lists with a maximum size of 'n'.

    Raises:
        ValueError: If 'n' is negative or zero.

    Examples:
        >>> list(divide([1, 2, 3, 4], 2))
        [[1, 2], [3, 4]]

        >>> list(divide([1, 2, 3, 4], -1))
        ValueError: ...
    """
    for i in range(0, len(lst), n):
        yield lst[i : i + n]


async def upload_prices(watch_remnants, client_id, seller_token):
    """Asynchronously uploads prices based on available watch remnants to a server."""
    offer_ids = get_offer_ids(client_id, seller_token)
    prices = create_prices(watch_remnants, offer_ids)
    for some_price in list(divide(prices, 1000)):
        update_price(some_price, client_id, seller_token)
    return prices


async def upload_stocks(watch_remnants, client_id, seller_token):
    """Asynchronously uploads stocks based on available watch remnants to a server."""
    offer_ids = get_offer_ids(client_id, seller_token)
    stocks = create_stocks(watch_remnants, offer_ids)
    for some_stock in list(divide(stocks, 100)):
        update_stocks(some_stock, client_id, seller_token)
    not_empty = list(filter(lambda stock: (stock.get("stock") != 0), stocks))
    return not_empty, stocks


def main():
    env = Env()
    seller_token = env.str("SELLER_TOKEN")
    client_id = env.str("CLIENT_ID")
    try:
        offer_ids = get_offer_ids(client_id, seller_token)
        watch_remnants = download_stock()
        # Обновить остатки
        stocks = create_stocks(watch_remnants, offer_ids)
        for some_stock in list(divide(stocks, 100)):
            update_stocks(some_stock, client_id, seller_token)
        # Поменять цены
        prices = create_prices(watch_remnants, offer_ids)
        for some_price in list(divide(prices, 900)):
            update_price(some_price, client_id, seller_token)
    except requests.exceptions.ReadTimeout:
        print("Превышено время ожидания...")
    except requests.exceptions.ConnectionError as error:
        print(error, "Ошибка соединения")
    except Exception as error:
        print(error, "ERROR_2")


if __name__ == "__main__":
    main()
