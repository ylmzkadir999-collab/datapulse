"""DataPulse v5 — Web Scraper (scripts/scraping/scraper.py)

Playwright tabanlı.
Config parametreleri:
  url        : Hedef URL
  selectors  : CSS selector listesi → {"baslik": "h1", "fiyat": ".price"}
  pagination : {"next_btn": "a.next", "max_pages": 10}
  output_fmt : "excel" | "csv" (varsayılan: excel)
"""
from __future__ import annotations

import asyncio
import logging
from typing import Callable, Optional

import pandas as pd

logger = logging.getLogger(__name__)


class WebScraper:
    """Playwright ile yapılandırılabilir web scraper."""

    def __init__(self, config: dict, plan: str = "starter") -> None:
        self.config = config
        self.plan = plan

    def run(
        self,
        file_url: Optional[str],
        user_id: str,
        job_id: str,
        progress_callback: Optional[Callable] = None,
    ) -> dict:
        """Ana çalıştırma metodu."""
        def log(msg: str, pct: int = 0) -> None:
            logger.info(f"[WebScraper] {msg}")
            if progress_callback:
                progress_callback("running", pct, msg)

        url = self.config.get("url")
        if not url:
            raise ValueError("Config'de 'url' zorunlu.")

        selectors: dict = self.config.get("selectors", {})
        pagination: dict = self.config.get("pagination", {})
        max_pages: int = min(int(pagination.get("max_pages", 5)), 50)
        next_btn: str = pagination.get("next_btn", "")

        log(f"Scraping başlıyor: {url}", 10)
        rows = asyncio.run(
            self._scrape_async(
                url=url,
                selectors=selectors,
                next_btn=next_btn,
                max_pages=max_pages,
                log_fn=log,
            )
        )

        if not rows:
            raise ValueError("Hiç veri scraplanmadı.")

        log(f"{len(rows)} satır toplandı. Excel oluşturuluyor...", 85)
        df = pd.DataFrame(rows)
        output_url = self._upload(df, user_id, job_id)

        log("Tamamlandı.", 99)
        return {"output_url": output_url, "row_count": len(df)}

    async def _scrape_async(
        self,
        url: str,
        selectors: dict,
        next_btn: str,
        max_pages: int,
        log_fn: Callable,
    ) -> list[dict]:
        """Playwright ile asenkron sayfa dolaş."""
        from playwright.async_api import async_playwright

        all_rows: list[dict] = []

        async with async_playwright() as pw:
            browser = await pw.chromium.launch(headless=True)
            page = await browser.new_page()

            current_url = url
            for page_num in range(1, max_pages + 1):
                pct = 10 + int((page_num / max_pages) * 70)
                log_fn(f"Sayfa {page_num} yükleniyor: {current_url}", pct)

                try:
                    await page.goto(current_url, timeout=30000)
                    await page.wait_for_load_state("networkidle", timeout=15000)
                except Exception as e:
                    logger.warning(f"Sayfa yükleme hatası: {e}")
                    break

                # Seçicilerle veri çek
                if selectors:
                    rows = await self._extract_with_selectors(page, selectors)
                else:
                    rows = await self._extract_table_rows(page)

                all_rows.extend(rows)
                log_fn(f"Sayfa {page_num}: {len(rows)} satır", pct)

                # Sonraki sayfaya geç
                if not next_btn:
                    break
                try:
                    next_el = await page.query_selector(next_btn)
                    if not next_el:
                        break
                    next_href = await next_el.get_attribute("href")
                    if next_href:
                        current_url = next_href if next_href.startswith("http") else url + next_href
                    else:
                        await next_el.click()
                        await page.wait_for_load_state("networkidle")
                        current_url = page.url
                except Exception as e:
                    logger.debug(f"Pagination hatası: {e}")
                    break

            await browser.close()

        return all_rows

    @staticmethod
    async def _extract_with_selectors(page, selectors: dict) -> list[dict]:
        """CSS seçicilerle veri çek."""
        results: list[dict] = []
        # Her seçicinin kaç eleman döndürdüğünü bul
        counts = {}
        for key, sel in selectors.items():
            els = await page.query_selector_all(sel)
            counts[key] = len(els)

        n = max(counts.values()) if counts else 0
        for i in range(n):
            row: dict = {}
            for key, sel in selectors.items():
                els = await page.query_selector_all(sel)
                if i < len(els):
                    row[key] = (await els[i].inner_text()).strip()
                else:
                    row[key] = ""
            results.append(row)
        return results

    @staticmethod
    async def _extract_table_rows(page) -> list[dict]:
        """Sayfadaki ilk tabloyu çek."""
        rows: list[dict] = []
        try:
            headers = [
                (await th.inner_text()).strip()
                async for th in await page.query_selector_all("table thead th, table tr:first-child th")
            ]
            if not headers:
                return rows
            trs = await page.query_selector_all("table tbody tr")
            for tr in trs:
                tds = await tr.query_selector_all("td")
                cells = [(await td.inner_text()).strip() for td in tds]
                if cells:
                    rows.append(dict(zip(headers, cells)))
        except Exception as e:
            logger.debug(f"Tablo çıkarma hatası: {e}")
        return rows

    def _upload(self, df: pd.DataFrame, user_id: str, job_id: str) -> str:
        from backend.services.supabase_service import SupabaseService
        import io

        buf = io.BytesIO()
        df.to_excel(buf, index=False)
        buf.seek(0)
        svc = SupabaseService()
        return asyncio.run(svc.upload_file(
            user_id=user_id,
            filename=f"scrape_{job_id[:8]}.xlsx",
            content=buf.read(),
            content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        ))
