"""
jobspy.scrapers.naukri
~~~~~~~~~~~~~~~~~~~

This module contains routines to scrape job listings from Naukri.com.
"""
import json
from typing import Optional, List
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
import requests

# Import utilities and base classes for scraping
from .. import Scraper, ScraperInput, Site

from ..utils import (
    extract_emails_from_text,
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


class NaukriScraper(Scraper):
    """
    A scraper for extracting job listings from Naukri.com.
    """

    def __init__(self, proxies: list[str] | str | None = None):
        """
        Initializes the scraper with a specific site and optional proxies.

        :param proxies: Optional proxies for making requests.
        """
        site = Site.NAUKRI
        super().__init__(site, proxies=proxies)
        self.api_url = "https://www.naukri.com/jobapi/v3/search"
        self.jobs_per_page = 20  # Max number of jobs to fetch per page
        self.seen_urls = set()  # To avoid processing duplicates
        self.scraper_input = None  # Initialize scraper_input
        self.proxies = proxies  # Store proxies
        self.session = create_session(
            proxies=proxies, is_tls=False, clear_cookies=True)

    def fetch_jobs_page(self, scraper_input: ScraperInput, page_num: int) -> List[JobPost]:
        """
        Fetches a page of job listings from Naukri.com.

        :param scraper_input: The input parameters for the scraper.
        :param page_num: The page number to fetch.
        :return: A list of JobPost objects.
        """
        jobs = []
        self.scraper_input = scraper_input  # Set scraper_input
        try:
            payload = self.add_payload(scraper_input, page_num)
            response = self.session.get(
                self.api_url, headers=self.headers(), params=payload
            )
            if response.status_code != 200:
                err = f"Failed to fetch jobs from Naukri.com. Status code: {response.status_code}"
                logger.error(err)
                return jobs
            res_json = response.json()

            if "errors" in res_json:
                err = f"Failed to fetch jobs from Naukri.com. Error: {res_json['errors']}"
                logger.error(err)
                return jobs

            jobs_data = res_json.get("jobDetails", [])
            with ThreadPoolExecutor(max_workers=min(len(jobs_data), self.jobs_per_page)) as executor:
                futures = [executor.submit(self.process_job, job_data)
                           for job_data in jobs_data]
                for future in as_completed(futures):
                    job_post = future.result()
                    if job_post:
                        jobs.append(job_post)
        except Exception as e:
            if "Proxy responded with" in str(e):
                logger.error(f"Naukri: Bad proxy")
            else:
                logger.error(f"Naukri: {str(e)}")
        return jobs

    def process_job(self, job_data) -> Optional[JobPost]:
        """
        Processes a single job listing and converts it into a JobPost object.

        :param job_data: The raw data for a single job listing.
        :return: A JobPost object or None if the job should be skipped.
        """
        title = job_data["title"]
        company_name = job_data["companyName"]
        job_url = f'https://www.naukri.com{job_data["jdURL"]}'
        if job_url in self.seen_urls:
            return None  # Skip processing if this job has already been seen
        self.seen_urls.add(job_url)

        date_posted = self.get_date_posted(job_data["createdDate"])
        job_type = self.get_job_type(job_data.get("tagsAndSkills", ""))

        description = self.get_description(job_data["jobDescription"])

        if self.scraper_input.description_format == DescriptionFormat.MARKDOWN:
            description = markdown_converter(description)

        # Default to INR if not specified
        currency = job_data.get('currency', 'INR')
        # Call get_compensation with currency
        compensation = self.get_compensation(
            job_data['placeholders'], currency)

        emails = extract_emails_from_text(description)

        is_remote, location = self.get_location(job_data["placeholders"])

        job_post = JobPost(
            title=title,
            company_name=company_name,
            job_url=job_url,
            date_posted=date_posted,
            job_type=job_type,
            compensation=compensation,
            location=location,
            description=description,
            emails=emails,
            is_remote=is_remote,
        )
        return job_post

    @staticmethod
    def get_date_posted(created_date: int) -> datetime:
        """
        Converts a timestamp in milliseconds to a datetime object with zero time.

        :param created_date: The creation date timestamp in milliseconds.
        :return: A datetime object with time set to midnight.
        """
        date_posted = datetime.utcfromtimestamp(created_date / 1000.0)
        date_posted = date_posted.replace(
            hour=0, minute=0, second=0, microsecond=0, tzinfo=timezone.utc)
        return date_posted

    @staticmethod
    def get_job_type(tags_and_skills: str) -> List[JobType]:
        job_types = [JobType[tag] for tag in tags_and_skills.split(
            ",") if tag in JobType.__members__]
        return job_types

    @staticmethod
    def get_compensation(placeholders: List[dict], currency: str) -> Optional[Compensation]:
        for placeholder in placeholders:
            if placeholder["type"] == "salary":
                salary_label = placeholder.get("label", "")
                if salary_label.lower() == "not disclosed":
                    return None
                salary_parts = salary_label.split('-')
                if len(salary_parts) == 2:
                    min_salary, max_salary = None, None
                    try:
                        min_salary = float(
                            salary_parts[0].split()[0].replace(',', '').strip()) * 100000  # Assuming lacs as unit
                        max_salary = float(
                            salary_parts[1].split()[0].replace(" Lacs PA", "").replace(',', '').strip()) * 100000
                    except ValueError:
                        pass  # Handle case where conversion to float fails
                    if min_salary and max_salary:
                        return Compensation(
                            interval=CompensationInterval.YEARLY,
                            min_amount=min_salary,
                            max_amount=max_salary,
                            currency=currency
                        )
                return None

    @staticmethod
    def get_location(placeholders: List[dict]) -> (bool, Optional[Location]):
        for placeholder in placeholders:
            if placeholder["type"] == "location":
                location_label = placeholder.get("label", "")
                if "Remote" in location_label:
                    return True, None  # Marking as remote
                locations = location_label.split(',')
                if locations:
                    # Splitting the first location into parts; adjust logic as needed
                    parts = locations[0].strip().split()
                    city = parts[0] if parts else None
                    state = parts[1] if len(parts) > 1 else None
                    country = parts[-1] if len(parts) > 2 else None
                    return False, Location(city=city, state=state, country=country)
        return False, None

    @staticmethod
    def get_description(job_description: str) -> str:
        return job_description

    def scrape(self, scraper_input: ScraperInput) -> JobResponse:
        self.scraper_input = scraper_input  # Set scraper_input
        scraper_input.results_wanted = min(900, scraper_input.results_wanted)
        jobs = []
        page_num = 1
        while len(jobs) < scraper_input.results_wanted:
            page_jobs = self.fetch_jobs_page(scraper_input, page_num)
            jobs.extend(page_jobs)
            page_num += 1
            if not page_jobs:
                break
        return JobResponse(jobs=jobs)

    @staticmethod
    def add_payload(scraper_input: ScraperInput, page_num: int) -> dict:
        params = {
            "noOfResults": scraper_input.results_wanted,
            "urlType": "search_by_key_loc",
            "searchType": "adv",
            "location": scraper_input.location,
            "keyword": scraper_input.search_term,
            "pageNo": page_num,
            "k": scraper_input.search_term,
            "l": scraper_input.location,
            "seoKey": f"{scraper_input.search_term.replace(' ', '-')}-jobs-in-{scraper_input.location}",
            "src": "directSearch",
            "latLong": ""
        }

        if scraper_input.hours_old:
            params["jobAge"] = max(scraper_input.hours_old // 24, 1)
            logger.info(
                f"Filtering jobs posted within the last {params['jobAge']} days")

        return params

    @staticmethod
    def headers() -> dict:
        """
        Returns the headers to be used for requests to Naukri.com.

        :return: A dictionary of request headers.
        """
        return {
            'accept': 'application/json',
            'accept-language': 'en-US,en;q=0.9',
            'appid': '109',
            'clientid': 'd3skt0p',
            'content-type': 'application/json',
            'gid': 'LOCATION,INDUSTRY,EDUCATION,FAREA_ROLE',
            'priority': 'u=1, i',
            'referer': 'https://www.naukri.com/dot-net-jobs?k=.net',
            'sec-ch-ua': '"Not/A)Brand";v="8", "Chromium";v="126", "Google Chrome";v="126"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Windows"',
            'sec-fetch-dest': 'empty',
            'sec-fetch-mode': 'cors',
            'sec-fetch-site': 'same-origin',
            'systemid': 'Naukri',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36'
        }
