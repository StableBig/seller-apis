import datetime
import logging.config
from environs import Env
from seller import download_stock

import requests

from seller import divide, price_conversion

logger = logging.getLogger(__file__)


def get_product_list(page, campaign_id, access_token):
    """
    Retrieves a list of products from Yandex.Market for a specific advertising campaign.

    Args:
        page (str): The current page token for pagination.
        campaign_id (str): The identifier of the advertising campaign.
        access_token (str): The access token for Yandex.Market API.

    Returns:
        dict: List of products and pagination information.

    Raises:
        requests.exceptions.RequestException: Raised when there's an issue with the request.

    Examples:
        >>> get_product_list("", "123456", "your_access_token")
        {
            'offerMappingEntries': [...],
            'paging': {...}
        }

        >>> get_product_list("invalid_page", "123456", "invalid_token")
        Traceback (most recent call last):
        ...
        requests.exceptions.RequestException
    """
    endpoint_url = "https://api.partner.market.yandex.ru/"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
        "Host": "api.partner.market.yandex.ru",
    }
    payload = {
        "page_token": page,
        "limit": 200,
    }
    url = endpoint_url + f"campaigns/{campaign_id}/offer-mapping-entries"
    response = requests.get(url, headers=headers, params=payload)
    response.raise_for_status()
    response_object = response.json()
    return response_object.get("result")


def update_stocks(stocks, campaign_id, access_token):
    """
    Updates stock availability information in Yandex.Market.

    Args:
        stocks (list): List with stock availability information.
        campaign_id (str): The identifier of the advertising campaign.
        access_token (str): The access token for Yandex.Market API.

    Returns:
        dict: API response about the update status.

    Raises:
        requests.exceptions.RequestException: Raised when there's an issue with updating stocks in the database.

    Examples:
        >>> update_stocks([...], "123456", "your_access_token")
        {
            'result': 'success',
            ...
        }

        >>> update_stocks([], "123456", "invalid_token")
        Traceback (most recent call last):
        ...
        requests.exceptions.RequestException
    """
    endpoint_url = "https://api.partner.market.yandex.ru/"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
        "Host": "api.partner.market.yandex.ru",
    }
    payload = {"skus": stocks}
    url = endpoint_url + f"campaigns/{campaign_id}/offers/stocks"
    response = requests.put(url, headers=headers, json=payload)
    response.raise_for_status()
    response_object = response.json()
    return response_object


def update_price(prices, campaign_id, access_token):
    """
    Updates product prices in Yandex.Market.

    Args:
        prices (list): List with product price information.
        campaign_id (str): The identifier of the advertising campaign.
        access_token (str): The access token for Yandex.Market API.

    Returns:
        dict: API response about the price update status.

    Raises:
        requests.exceptions.RequestException: Raised when there's an issue with updating the product prices.

    Examples:
        >>> update_price([...], "123456", "your_access_token")
        {
            'result': 'success',
            ...
        }

        >>> update_price([], "123456", "invalid_token")
        Traceback (most recent call last):
        ...
        requests.exceptions.RequestException
    """
    endpoint_url = "https://api.partner.market.yandex.ru/"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
        "Host": "api.partner.market.yandex.ru",
    }
    payload = {"offers": prices}
    url = endpoint_url + f"campaigns/{campaign_id}/offer-prices/updates"
    response = requests.post(url, headers=headers, json=payload)
    response.raise_for_status()
    response_object = response.json()
    return response_object


def get_offer_ids(campaign_id, market_token):
    """
    Retrieves product SKUs (Stock Keeping Units) or article numbers from Yandex.Market for an advertising campaign.

    Args:
        campaign_id (str): The identifier of the advertising campaign.
        market_token (str): The access token for Yandex.Market API.

    Returns:
        list: A list of SKUs (article numbers) for products.

    Raises:
        requests.exceptions.RequestException: Raised when there's an issue with the request, e.g., due to an invalid token.

    Examples:
        >>> get_offer_ids("123456", "your_market_token")
        ['SKU123', 'SKU124', ...]

        >>> get_offer_ids("123456", "invalid_token")
        Traceback (most recent call last):
        ...
        requests.exceptions.RequestException
    """
    page = ""
    product_list = []
    while True:
        some_prod = get_product_list(page, campaign_id, market_token)
        product_list.extend(some_prod.get("offerMappingEntries"))
        page = some_prod.get("paging").get("nextPageToken")
        if not page:
            break
    offer_ids = []
    for product in product_list:
        offer_ids.append(product.get("offer").get("shopSku"))
    return offer_ids


def create_stocks(watch_remnants, offer_ids, warehouse_id):
    """
    Constructs a list of stock availability based on the provided product remnants and existing offer IDs.

    Args:
        watch_remnants (list): List of product remnants from the shop's internal system.
        offer_ids (list): List of product offer IDs (SKUs) from Yandex.Market.
        warehouse_id (str): The identifier of the warehouse where products are stored.

    Returns:
        list: A list of stock availability information for each product.

    Examples:
        >>> create_stocks([{'Код': 'SKU123', 'Количество': '5'}, ...], ['SKU123'], "WH001")
        [{'sku': 'SKU123', 'warehouseId': 'WH001', 'items': [{'count': 5, ...}]}, ...]

        >>> create_stocks([], [], "WH001")
        []
    """
    # Уберем то, что не загружено в market
    stocks = list()
    date = str(datetime.datetime.utcnow().replace(microsecond=0).isoformat() + "Z")
    for watch in watch_remnants:
        if str(watch.get("Код")) in offer_ids:
            count = str(watch.get("Количество"))
            if count == ">10":
                stock = 100
            elif count == "1":
                stock = 0
            else:
                stock = int(watch.get("Количество"))
            stocks.append(
                {
                    "sku": str(watch.get("Код")),
                    "warehouseId": warehouse_id,
                    "items": [
                        {
                            "count": stock,
                            "type": "FIT",
                            "updatedAt": date,
                        }
                    ],
                }
            )
            offer_ids.remove(str(watch.get("Код")))
    # Добавим недостающее из загруженного:
    for offer_id in offer_ids:
        stocks.append(
            {
                "sku": offer_id,
                "warehouseId": warehouse_id,
                "items": [
                    {
                        "count": 0,
                        "type": "FIT",
                        "updatedAt": date,
                    }
                ],
            }
        )
    return stocks


def create_prices(watch_remnants, offer_ids):
    """
    Constructs a list of product prices based on the provided product remnants and existing offer IDs.

    Args:
        watch_remnants (list): List of product remnants from the shop's internal system.
        offer_ids (list): List of product offer IDs (SKUs) from Yandex.Market.

    Returns:
        list: A list of product price information for each product.

    Examples:
        >>> create_prices([{'Код': 'SKU123', 'Цена': '1000'}, ...], ['SKU123'])
        [{'id': 'SKU123', 'price': {'value': 1000, ...}}, ...]

        >>> create_prices([], [])
        []
    """
    prices = []
    for watch in watch_remnants:
        if str(watch.get("Код")) in offer_ids:
            price = {
                "id": str(watch.get("Код")),
                # "feed": {"id": 0},
                "price": {
                    "value": int(price_conversion(watch.get("Цена"))),
                    # "discountBase": 0,
                    "currencyId": "RUR",
                    # "vat": 0,
                },
                # "marketSku": 0,
                # "shopSku": "string",
            }
            prices.append(price)
    return prices


async def upload_prices(watch_remnants, campaign_id, market_token):
    """
    Asynchronously uploads product prices to Yandex.Market for a specific advertising campaign.
    """
    offer_ids = get_offer_ids(campaign_id, market_token)
    prices = create_prices(watch_remnants, offer_ids)
    for some_prices in list(divide(prices, 500)):
        update_price(some_prices, campaign_id, market_token)
    return prices


async def upload_stocks(watch_remnants, campaign_id, market_token, warehouse_id):
    """
    Asynchronously uploads product stock counts to Yandex.Market for a specific advertising campaign and warehouse.
    """
    offer_ids = get_offer_ids(campaign_id, market_token)
    stocks = create_stocks(watch_remnants, offer_ids, warehouse_id)
    for some_stock in list(divide(stocks, 2000)):
        update_stocks(some_stock, campaign_id, market_token)
    not_empty = list(
        filter(lambda stock: (stock.get("items")[0].get("count") != 0), stocks)
    )
    return not_empty, stocks


def main():
    env = Env()
    market_token = env.str("MARKET_TOKEN")
    campaign_fbs_id = env.str("FBS_ID")
    campaign_dbs_id = env.str("DBS_ID")
    warehouse_fbs_id = env.str("WAREHOUSE_FBS_ID")
    warehouse_dbs_id = env.str("WAREHOUSE_DBS_ID")

    watch_remnants = download_stock()
    try:
        # FBS
        offer_ids = get_offer_ids(campaign_fbs_id, market_token)
        # Обновить остатки FBS
        stocks = create_stocks(watch_remnants, offer_ids, warehouse_fbs_id)
        for some_stock in list(divide(stocks, 2000)):
            update_stocks(some_stock, campaign_fbs_id, market_token)
        # Поменять цены FBS
        upload_prices(watch_remnants, campaign_fbs_id, market_token)

        # DBS
        offer_ids = get_offer_ids(campaign_dbs_id, market_token)
        # Обновить остатки DBS
        stocks = create_stocks(watch_remnants, offer_ids, warehouse_dbs_id)
        for some_stock in list(divide(stocks, 2000)):
            update_stocks(some_stock, campaign_dbs_id, market_token)
        # Поменять цены DBS
        upload_prices(watch_remnants, campaign_dbs_id, market_token)
    except requests.exceptions.ReadTimeout:
        print("Превышено время ожидания...")
    except requests.exceptions.ConnectionError as error:
        print(error, "Ошибка соединения")
    except Exception as error:
        print(error, "ERROR_2")


if __name__ == "__main__":
    main()
