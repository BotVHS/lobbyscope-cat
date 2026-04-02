"""
Client genèric per a l'API Socrata SODA (Open Data Cataluña).

Gestiona paginació automàtica, retry amb backoff exponencial
i suport per a l'App Token opcional.

Ús bàsic:
    from ingesta.socrata import fetch_dataset

    for row in fetch_dataset("hd8k-y28e"):
        print(row)

    # Amb filtre i ordre:
    for row in fetch_dataset(
        "n6hn-rmy7",
        where_clause="data_publicacio >= '2020-01-01T00:00:00.000'",
        order_by="data_publicacio DESC",
    ):
        print(row)
"""

import logging
import os
import time
from typing import Generator, Optional

import requests
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

BASE_URL = "https://analisi.transparenciacatalunya.cat/resource/{dataset_id}.json"
DEFAULT_PAGE_SIZE = 1000
MAX_RETRIES = 3
RETRY_BACKOFF_BASE = 2  # segons; es dobla a cada intent
DEFAULT_TIMEOUT = 30    # segons; augmentar per a datasets grans (RAISC, contractes)


def fetch_dataset(
    dataset_id: str,
    where_clause: Optional[str] = None,
    order_by: Optional[str] = None,
    page_size: int = DEFAULT_PAGE_SIZE,
    read_timeout: int = DEFAULT_TIMEOUT,
) -> Generator[dict, None, None]:
    """
    Genera tots els registres d'un dataset Socrata, paginant automàticament.

    Args:
        dataset_id:   Identificador del dataset (p. ex. "hd8k-y28e").
        where_clause: Condició SOQL per filtrar (p. ex. "data >= '2020-01-01T00:00:00'").
        order_by:     Camp i direcció d'ordenació (p. ex. "data DESC").
        page_size:    Nombre de registres per pàgina (màx. 50000 per l'API).

    Yields:
        Diccionaris amb els camps de cada registre.

    Raises:
        requests.HTTPError: Si l'API retorna un error no recuperable.
    """
    url = BASE_URL.format(dataset_id=dataset_id)
    headers = _build_headers()
    offset = 0
    total_fetched = 0

    while True:
        params = _build_params(where_clause, order_by, page_size, offset)
        rows = _fetch_page_with_retry(url, params, headers, read_timeout=read_timeout)

        if not rows:
            break

        for row in rows:
            yield row

        total_fetched += len(rows)
        logger.debug(f"[{dataset_id}] {total_fetched} registres obtinguts (offset={offset})")

        if len(rows) < page_size:
            # Darrera pàgina: menys registres del previst
            break

        offset += page_size


def fetch_single(dataset_id: str, record_id: str) -> Optional[dict]:
    """
    Obté un únic registre per ID.
    Retorna None si no es troba.
    """
    url = BASE_URL.format(dataset_id=dataset_id)
    headers = _build_headers()
    params = {"$where": f"id='{record_id}'", "$limit": 1}
    rows = _fetch_page_with_retry(url, params, headers)
    return rows[0] if rows else None


def count_dataset(dataset_id: str, where_clause: Optional[str] = None) -> int:
    """Retorna el nombre total de registres (útil per a progress bars)."""
    url = BASE_URL.format(dataset_id=dataset_id)
    headers = _build_headers()
    params = {"$select": "COUNT(*) AS n"}
    if where_clause:
        params["$where"] = where_clause
    rows = _fetch_page_with_retry(url, params, headers)
    if rows:
        return int(rows[0].get("n", 0))
    return 0


def _build_headers() -> dict:
    token = os.getenv("SOCRATA_APP_TOKEN", "").strip()
    headers = {"Accept": "application/json"}
    if token:
        headers["X-App-Token"] = token
    return headers


def _build_params(
    where_clause: Optional[str],
    order_by: Optional[str],
    page_size: int,
    offset: int,
) -> dict:
    params: dict = {
        "$limit": page_size,
        "$offset": offset,
    }
    if where_clause:
        params["$where"] = where_clause
    if order_by:
        params["$order"] = order_by
    return params


def _fetch_page_with_retry(url: str, params: dict, headers: dict,
                           read_timeout: int = DEFAULT_TIMEOUT) -> list[dict]:
    """
    Fa una crida HTTP GET amb retry exponencial.
    Llança HTTPError en errors no recuperables (4xx que no siguin 429).
    """
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = requests.get(url, params=params, headers=headers, timeout=read_timeout)

            if resp.status_code == 429:
                # Rate limit — esperar i reintentar
                wait = RETRY_BACKOFF_BASE ** attempt
                logger.warning(f"Rate limit (429). Esperant {wait}s... (intent {attempt}/{MAX_RETRIES})")
                time.sleep(wait)
                continue

            resp.raise_for_status()
            return resp.json()

        except requests.exceptions.ConnectionError as e:
            if attempt == MAX_RETRIES:
                raise
            wait = RETRY_BACKOFF_BASE ** attempt
            logger.warning(f"Error de connexió: {e}. Reintentant en {wait}s...")
            time.sleep(wait)

        except requests.exceptions.Timeout:
            if attempt == MAX_RETRIES:
                raise
            wait = RETRY_BACKOFF_BASE ** attempt * 2
            logger.warning(f"Timeout. Reintentant en {wait}s... (intent {attempt}/{MAX_RETRIES})")
            time.sleep(wait)

    return []
