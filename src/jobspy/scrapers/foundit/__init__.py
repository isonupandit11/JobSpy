"""
jobspy.scrapers.foundit
~~~~~~~~~~~~~~~~~~~

This module contains routines to scrape job listings from Foundit.com.
"""

import json
from typing import Optional, List
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from bs4 import BeautifulSoup

# Import utilities and base classes for scraping
from .. import Scraper, ScraperInput, Site

from ..utils import (
    extract_emails_from_text,
    get_enum_from_job_type,
    markdown_converter,
    logger,
    create_session
)

from ...jobs import (
    JobPost,
    Compensation,
    CompensationInterval,
    Location,
    JobResponse,
    JobType,
    DescriptionFormat,
)


class FounditScrapper(Scraper):
    def __init__(self, proxy: Optional[str] = None):
        """
        Initializes the scraper with a specific site and optional proxy.

        :param proxy: Optional proxy for making requests.
        """
        site = Site(Site.FOUNDIT)
        super().__init__(site, proxy=proxy)
        self.api_url = "https://www.foundit.in/middleware/jobsearch"
        self.jobs_per_page = 20  # Max number of jobs to fetch per page
        self.seen_urls = set()  # To avoid processing duplicates
        self.scraper_input = None  # Initialize scraper_input

    def fetch_jobs_page(self, scraper_input: ScraperInput, page_num: int) -> List[JobPost]:
        jobs = []
        self.scraper_input = scraper_input  # Set scraper_input
        try:
            payload = self.add_payload(scraper_input, page_num)
            session = create_session(self.proxy, is_tls=False, has_retry=True)
            response = session.get(
                self.api_url, headers=self.headers(), params=payload, timeout=10)
            if response.status_code != 200:
                err = f"Failed to fetch jobs from Naukri.com. Status code: {response.status_code}"
                logger.error(err)
            res_json = response.json()
            if "errors" in res_json:
                err = f"Failed to fetch jobs from Naukri.com. Error: {res_json['errors']}"
                logger.error(err)
            jobs_data = res_json.get("jobDetails", [])
            print(jobs_data)
            with ThreadPoolExecutor(max_workers=min(len(jobs_data), self.jobs_per_page)) as executor:
                futures = [executor.submit(self.process_job, job_data)
                           for job_data in jobs_data]
                for future in as_completed(futures):
                    job_post = future.result()
                    if job_post:
                        jobs.append(job_post)
        except Exception as e:
            err = f"Failed to fetch jobs from Naukri.com. Error: {e}"
            logger.error(err)
        return jobs

    @staticmethod
    def add_payload(scraper_input: ScraperInput, page_num: int) -> dict:
        params = {
            "location": scraper_input.location,
            "query": scraper_input.search_term,
            "limits": "20",
            "sort": "1",
        }

        return params

    @staticmethod
    def headers() -> dict:
        return {
            'Accept': 'application/json, text/plain, */*',
            'Referer': 'https://www.foundit.in'
        }
