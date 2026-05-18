
from fastapi import FastAPI
from pydantic import BaseModel
import requests
import datetime
import json
import time
import os
from typing import Optional, Tuple

# Local module (created separately)
from selenium_token_provider import SeleniumTokenProvider, BrowserAuth

app = FastAPI(title="Bihar Tender Scraper API")

BIHAR_TENDER_URL = "https://eproc2.bihar.gov.in/EPSV2Web/openarea/getPastTenders?startpoint=0&maxRow=1000"
TENDER_DETAIL_URL = "https://eproc2.bihar.gov.in/EPSV2Web/rest/quotation/previewTenderByTenderId"
REFRESH_TOKEN_URL = "https://eproc2.bihar.gov.in/EPSV2Web/rest/general/refreshToken"

os.makedirs("tender_data", exist_ok=True)
os.makedirs("tender_ids", exist_ok=True)
os.makedirs("full_data", exist_ok=True)



class DateRange(BaseModel):
    start_date: str
    end_date: str
    # Optional: If you have a token already, you can pass it.
    # Otherwise, you can rely on Selenium auto-capture (see /fetch-details-auto).
    auth_token: Optional[str] = None
    jsessionid: Optional[str] = None


class DetailRequest(BaseModel):
    # If omitted, /fetch-details-auto will use Selenium to obtain these.
    auth_token: str
    jsessionid: str


def iso_format(dt: datetime.datetime) -> str:
    return dt.strftime("%Y-%m-%dT00:00:00.000Z")


def _extract_token_from_refresh_response(resp: requests.Response) -> Optional[str]:
    """
    The refreshToken endpoint sometimes returns a token in the response body.
    This is defensive: we try common keys and fall back to None if not present.
    """
    try:
        data = resp.json()
    except Exception:
        return None

    if isinstance(data, str):
        # sometimes endpoints return the token directly as a string
        return data.strip()

    if isinstance(data, dict):
        for key in ("authToken", "token", "accessToken", "jwt", "authorization", "bearerToken"):
            val = data.get(key)
            if isinstance(val, str) and val.strip():
                return val.strip()

    return None


def refresh_token(auth_token: str, current_jsessionid: str, session: Optional[requests.Session] = None) -> Tuple[str, str]:
    """
    Refresh session and (if present) bearer token using the front-end refresh endpoint.

    Returns: (new_auth_token, new_jsessionid)
    """
    s = session or requests.Session()

    headers = {
        "Authorization": f"Bearer {auth_token}",
        "Cookie": f"JSESSIONID={current_jsessionid}",
        "Accept": "*/*",
        "User-Agent": "Mozilla/5.0",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
        "Content-Type": "application/json",
        "X-Requested-With": "XMLHttpRequest",
    }

    # IMPORTANT: parameter spelling on the site is "...MilliSec"
    params = {"idleTimeInMiliSec": 0}

    resp = s.post(REFRESH_TOKEN_URL, headers=headers, params=params, timeout=30)

    if resp.status_code != 200:
        raise Exception(f"Token refresh failed. status={resp.status_code}, body={resp.text[:500]}")

    # Cookie rotation is common on these platforms
    new_jsessionid = current_jsessionid
    set_cookie = resp.headers.get("set-cookie") or resp.headers.get("Set-Cookie")
    if set_cookie and "JSESSIONID=" in set_cookie:
        new_jsessionid = set_cookie.split("JSESSIONID=")[1].split(";")[0]

    # Some implementations rotate the bearer token too
    new_token = _extract_token_from_refresh_response(resp)
    if new_token:
        # If server returned a full "Bearer x" string, normalize it
        if new_token.lower().startswith("bearer "):
            new_token = new_token.split(" ", 1)[1].strip()
        # Keep same type as input: "raw token"
        return new_token, new_jsessionid

    return auth_token, new_jsessionid


def _load_tender_ids(path: str = "tender_ids/tender_ids.txt"):
    if not os.path.exists(path):
        return []
    with open(path, "r", encoding="utf-8") as f:
        return [line.strip() for line in f.readlines() if line.strip()]


def _save_full_detail(tid: str, data: dict):
    with open(f"full_data/{tid}.json", "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)


@app.post("/fetch-tenders")
def fetch_tenders(date_range: DateRange):
    start_date = datetime.datetime.fromisoformat(date_range.start_date)
    end_date = datetime.datetime.fromisoformat(date_range.end_date)

    delta = datetime.timedelta(days=7)
    current = start_date
    all_tender_ids = []

    if not date_range.auth_token:
        return {"error": "auth_token is required for /fetch-tenders (or implement selenium capture similarly)."}
    auth_token = date_range.auth_token

    # Optional cookie if endpoint requires it
    jsessionid = date_range.jsessionid or ""
    s = requests.Session()

    last_refresh_time = time.time()

    while current <= end_date:
        # Refresh token periodically (tune as required)
        if jsessionid and (time.time() - last_refresh_time >= 520):
            auth_token, jsessionid = refresh_token(auth_token, jsessionid, session=s)
            last_refresh_time = time.time()

        headers = {
            "Content-Type": "application/json",
            "User-Agent": "Mozilla/5.0",
            "Authorization": f"Bearer {auth_token}",
        }
        if jsessionid:
            headers["Cookie"] = f"JSESSIONID={jsessionid}"

        window_start = iso_format(current)
        window_end = iso_format(min(current + delta - datetime.timedelta(days=1), end_date))

        payload = {
            "orgId": "538",
            "deptId": None,
            "dateParam": "1",
            "startDate": window_start,
            "endDate": window_end,
            "closeDateFrom": "",
            "closeDateTo": "",
            "procatId": "",
            "typeId": "",
            "textFilter": None,
        }

        try:
            resp = s.post(BIHAR_TENDER_URL, json=payload, headers=headers, timeout=60)
            resp.raise_for_status()

            try:
                data = resp.json()
            except json.JSONDecodeError:
                current += delta
                continue

            tenders = data if isinstance(data, list) else (data.get("resultList", []) if isinstance(data, dict) else [])
            tender_ids = [t.get("currenttenderid") for t in tenders if t.get("currenttenderid")]
            all_tender_ids.extend(tender_ids)

            with open("tender_ids/tender_ids.txt", "a", encoding="utf-8") as f:
                for tid in tender_ids:
                    f.write(f"{tid}\n")

            for tender in tenders:
                tid = tender.get("currenttenderid")
                if tid:
                    with open(f"tender_data/{tid}.json", "w", encoding="utf-8") as tf:
                        json.dump(tender, tf, indent=2, ensure_ascii=False)

        except requests.RequestException as e:
            print(f"Error during request for range {window_start} to {window_end}: {e}")

        current += delta

    return {"total_tenders_fetched": len(all_tender_ids), "tender_ids": all_tender_ids}


def _fetch_details_with_token(auth_token: str, jsessionid: str, tender_ids: list[str]) -> dict:
    """
    Core loop. Uses refreshToken proactively and does a Selenium re-capture
    if 500s persist (optional).
    """
    s = requests.Session()

    # Selenium provider is optional; only used on repeated 500s or refresh failures
    provider = SeleniumTokenProvider(headless=True)

    last_refresh_time = time.time()
    request_count = 0
    consecutive_500 = 0

    def _headers():
        return {
            "Authorization": f"Bearer {auth_token}",
            "Cookie": f"JSESSIONID={jsessionid}",
            "Accept": "*/*",
            "User-Agent": "Mozilla/5.0",
            "Accept-Encoding": "gzip, deflate, br",
            "Connection": "keep-alive",
            "Content-Type": "application/json",
            "X-Requested-With": "XMLHttpRequest",
        }

    headers = _headers()

    for idx, tid in enumerate(tender_ids, start=1):
        # Proactive refresh: every ~8 minutes OR after ~90 calls
        if time.time() - last_refresh_time >= 480 or request_count >= 90:
            try:
                auth_token, jsessionid = refresh_token(auth_token, jsessionid, session=s)
                headers = _headers()
                last_refresh_time = time.time()
                request_count = 0
                consecutive_500 = 0
            except Exception as e:
                print(f"[refreshToken] failed: {e}")

        url = f"{TENDER_DETAIL_URL}?tenderId={tid}"

        for attempt in range(3):
            try:
                resp = s.post(url, headers=headers, timeout=60)
                if resp.status_code == 500:
                    consecutive_500 += 1
                    # refresh token once, then retry
                    if attempt < 2:
                        time.sleep(5)
                        try:
                            auth_token, jsessionid = refresh_token(auth_token, jsessionid, session=s)
                            headers = _headers()
                        except Exception as e:
                            print(f"[refreshToken after 500] failed: {e}")

                        # If server keeps returning 500, reacquire the token from UI
                        if consecutive_500 >= 3:
                            try:
                                provider.start()
                                auth = provider.refresh(timeout_sec=30)
                                auth_token = auth.bearer
                                jsessionid = auth.jsessionid
                                headers = auth.headers
                                consecutive_500 = 0
                                print("[selenium] re-acquired bearer + JSESSIONID")
                            except Exception as se:
                                print(f"[selenium] token reacquire failed: {se}")
                            finally:
                                provider.stop()
                        continue

                resp.raise_for_status()
                data = resp.json()
                _save_full_detail(tid, data)
                request_count += 1
                consecutive_500 = 0
                # Gentle pacing; tune based on observation
                time.sleep(1)
                break

            except requests.RequestException as e:
                if attempt == 2:
                    print(f"Failed to fetch details for tender ID {tid}: {e}")
                time.sleep(3)

    return {"message": "Detail fetch complete", "total_fetched": len(tender_ids)}


@app.post("/fetch-details")
def fetch_tender_details(req: DetailRequest):
    tender_ids = _load_tender_ids()
    if not tender_ids:
        return {"error": "tender_ids.txt not found or empty"}
    return _fetch_details_with_token(req.auth_token, req.jsessionid, tender_ids)


@app.post("/fetch-details-auto")
def fetch_tender_details_auto():
    """
    Uses Selenium to obtain the same bearer token + JSESSIONID that the UI uses,
    then runs the detail extraction.
    """
    provider = SeleniumTokenProvider(headless=True)
    provider.start()
    auth = provider.refresh(timeout_sec=30)
    provider.stop()

    tender_ids = _load_tender_ids()
    if not tender_ids:
        return {"error": "tender_ids.txt not found or empty"}

    return _fetch_details_with_token(auth.bearer, auth.jsessionid, tender_ids)
