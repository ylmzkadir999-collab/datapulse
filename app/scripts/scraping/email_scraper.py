"""DataPulse v5 — Email Scraper (scripts/scraping/email_scraper.py)

Web sayfalarından e-posta adreslerini bulur.
Config:
  urls       : ["https://site.com/iletisim", ...]
  depth      : Kaç seviye link takip edilsin (0=sadece verilen URL)
  domain_filter: ["orneksite.com"] — sadece bu domain'ler
"""
from __future__ import annotations

import asyncio
import logging
import re
from typing import Callable, Optional
from urllib.parse import urljoin, urlparse

import pandas as pd

logger = logging.getLogger(__name__)

EMAIL_RE = re.compile(
    r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}"
)


class EmailScraper:
    """URL listesinden email adreslerini toplar."""

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
        def log(msg: str, pct: int = 0) -> None:
            logger.info(f"[EmailScraper] {msg}")
            if progress_callback:
                progress_callback("running", pct, msg)

        urls: list[str] = self.config.get("urls", [])
        if not urls:
            raise ValueError("Config'de 'urls' listesi zorunlu.")

        depth: int = min(int(self.config.get("depth", 0)), 3)
        domain_filter: list[str] = self.config.get("domain_filter", [])

        log(f"{len(urls)} URL taranacak, depth={depth}", 10)
        emails = asyncio.run(
            self._scrape_all(urls, depth, domain_filter, log)
        )

        log(f"{len(emails)} benzersiz email bulundu.", 85)
        df = pd.DataFrame(
            [{"email": e, "source": s} for e, s in sorted(emails)]
        )

        output_url = self._upload(df, user_id, job_id)
        log("Tamamlandı.", 99)
        return {"output_url": output_url, "row_count": len(df)}

    async def _scrape_all(
        self,
        start_urls: list[str],
        depth: int,
        domain_filter: list[str],
        log_fn: Callable,
    ) -> set[tuple[str, str]]:
        """Tüm URL'leri tara ve email topla."""
        from playwright.async_api import async_playwright

        visited: set[str] = set()
        queue: list[tuple[str, int]] = [(u, 0) for u in start_urls]
        found: set[tuple[str, str]] = set()

        async with async_playwright() as pw:
            browser = await pw.chromium.launch(headless=True)
            page = await browser.new_page()

            while queue:
                url, current_depth = queue.pop(0)
                if url in visited:
                    continue
                visited.add(url)

                pct = 10 + min(int(len(visited) / max(len(queue) + len(visited), 1) * 70), 70)
                log_fn(f"Taranan: {url[:60]}", pct)

                try:
                    await page.goto(url, timeout=20000)
                    content = await page.content()
                except Exception as e:
                    logger.debug(f"Sayfa yüklenemedi {url}: {e}")
                    continue

                # Email bul
                emails_on_page = EMAIL_RE.findall(content)
                for email in emails_on_page:
                    found.add((email.lower(), url))

                # Derin tarama
                if current_depth < depth:
                    links = await page.query_selector_all("a[href]")
                    for link in links:
                        href = await link.get_attribute("href")
                        if not href:
                            continue
                        full = urljoin(url, href)
                        parsed = urlparse(full)
                        if not parsed.scheme.startswith("http"):
                            continue
                        if domain_filter and parsed.netloc not in domain_filter:
                            continue
                        if full not in visited:
                            queue.append((full, current_depth + 1))

            await browser.close()

        return found

    def _upload(self, df: pd.DataFrame, user_id: str, job_id: str) -> str:
        from backend.services.supabase_service import SupabaseService
        import io

        buf = io.BytesIO()
        df.to_excel(buf, index=False)
        buf.seek(0)
        svc = SupabaseService()
        return asyncio.run(svc.upload_file(
            user_id=user_id,
            filename=f"emails_{job_id[:8]}.xlsx",
            content=buf.read(),
            content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        ))
