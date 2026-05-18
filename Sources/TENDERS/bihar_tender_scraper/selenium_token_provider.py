
"""
Selenium-based token/header extractor for https://eproc2.bihar.gov.in

Goal:
- Open tender listing page
- Click "View Tender" button (triggers previewTenderByTenderId XHR)
- Extract Authorization: Bearer <...> from Chrome performance logs
- Read JSESSIONID cookie from browser

Notes:
- Requires Google Chrome (or Chromium) + a matching chromedriver.
- Install: pip install selenium webdriver-manager
- Selenium 4 is assumed (execute_cdp_cmd is available).

This module intentionally returns only:
- bearer token (string without "Bearer ")
- jsessionid (cookie value)
- a ready-to-use headers dict for requests
"""

from __future__ import annotations

import json
import time
from dataclasses import dataclass
from typing import Optional, Tuple

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

try:
    # Optional, but makes chromedriver setup easier
    from webdriver_manager.chrome import ChromeDriverManager
    from selenium.webdriver.chrome.service import Service
    _HAS_WDM = True
except Exception:
    _HAS_WDM = False


TENDER_LISTING_URL = "https://eproc2.bihar.gov.in/EPSV2Web/openarea/tenderListingPage.action#latestTenders"
PREVIEW_API_SUBSTRING = "/EPSV2Web/rest/quotation/previewTenderByTenderId"


@dataclass
class BrowserAuth:
    bearer: str
    jsessionid: str
    headers: dict
    fetched_at: float


def _build_driver(headless: bool = True) -> webdriver.Chrome:
    opts = Options()
    if headless:
        # "new" headless mode is more compatible with modern sites
        opts.add_argument("--headless=new")

    # Stability defaults
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--window-size=1366,768")

    # Enable Chrome performance logs (network events)
    opts.set_capability("goog:loggingPrefs", {"performance": "ALL"})

    if _HAS_WDM:
        service = Service(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service, options=opts)
    else:
        # Assumes chromedriver is already on PATH
        driver = webdriver.Chrome(options=opts)

    # Enable CDP Network domain (doesn't auto-stream to python, but helps)
    try:
        driver.execute_cdp_cmd("Network.enable", {})
    except Exception:
        pass

    return driver


def _extract_bearer_from_perflog(perf_logs) -> Optional[str]:
    """
    Look for Network.requestWillBeSent events hitting previewTenderByTenderId,
    and pull Authorization header.
    """
    for entry in perf_logs:
        try:
            msg = json.loads(entry.get("message", "{}")).get("message", {})
        except Exception:
            continue

        method = msg.get("method")
        if method != "Network.requestWillBeSent":
            continue

        params = msg.get("params", {})
        req = params.get("request", {})
        url = req.get("url", "")
        if PREVIEW_API_SUBSTRING not in url:
            continue

        headers = req.get("headers", {}) or {}
        auth = headers.get("Authorization") or headers.get("authorization")
        if not auth:
            continue

        if isinstance(auth, str) and auth.lower().startswith("bearer "):
            return auth.split(" ", 1)[1].strip()

    return None


class SeleniumTokenProvider:
    """
    Keeps a single Chrome session open and can re-fetch a fresh bearer + JSESSIONID
    by re-triggering the XHR from the UI.

    Usage:
        provider = SeleniumTokenProvider()
        provider.start()
        auth = provider.refresh()
        provider.stop()
    """

    def __init__(self, headless: bool = True, view_button_xpath: str = '//*[@id="myTablebyrTl"]/tbody/tr[1]/td[8]/button'):
        self.headless = headless
        self.view_button_xpath = view_button_xpath
        self.driver: Optional[webdriver.Chrome] = None
        self.last_auth: Optional[BrowserAuth] = None

    def start(self) -> None:
        if self.driver:
            return
        self.driver = _build_driver(headless=self.headless)
        self.driver.get(TENDER_LISTING_URL)

        # Wait for table/button to exist (page uses JS to populate)
        WebDriverWait(self.driver, 30).until(
            EC.element_to_be_clickable((By.XPATH, self.view_button_xpath))
        )

    def stop(self) -> None:
        if self.driver:
            try:
                self.driver.quit()
            finally:
                self.driver = None

    def refresh(self, timeout_sec: int = 30) -> BrowserAuth:
        """
        Clicks View Tender and harvests the Authorization header and JSESSIONID cookie.
        """
        if not self.driver:
            raise RuntimeError("Token provider not started. Call start() first.")

        # Clear old performance logs so we don't pick up stale requests
        try:
            _ = self.driver.get_log("performance")
        except Exception:
            pass

        # Trigger XHR by clicking "View Tender"
        btn = WebDriverWait(self.driver, 30).until(
            EC.element_to_be_clickable((By.XPATH, self.view_button_xpath))
        )
        btn.click()

        bearer = None
        end_time = time.time() + timeout_sec
        while time.time() < end_time and not bearer:
            time.sleep(0.25)
            try:
                logs = self.driver.get_log("performance")
            except Exception:
                logs = []
            bearer = _extract_bearer_from_perflog(logs)

        if not bearer:
            raise RuntimeError("Could not extract Bearer token from performance logs. Site may have changed.")

        # JSESSIONID cookie from browser (more reliable than parsing raw headers)
        cookie = self.driver.get_cookie("JSESSIONID")
        if not cookie or not cookie.get("value"):
            raise RuntimeError("Could not read JSESSIONID cookie from the browser session.")
        jsessionid = cookie["value"]

        headers = {
            "Authorization": f"Bearer {bearer}",
            "Cookie": f"JSESSIONID={jsessionid}",
            "Accept": "application/json, text/plain, */*",
            "X-Requested-With": "XMLHttpRequest",
            "User-Agent": "Mozilla/5.0",
        }

        self.last_auth = BrowserAuth(
            bearer=bearer,
            jsessionid=jsessionid,
            headers=headers,
            fetched_at=time.time(),
        )
        return self.last_auth
