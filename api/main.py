import uuid
import redis.asyncio as redis
import json
import asyncio
from jobspy import scrape_jobs
from typing import List
from datetime import datetime, date
import math
from fastapi import FastAPI, Query, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
from fuzzywuzzy import fuzz
import logging

# Configure logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    yield

app = FastAPI(lifespan=lifespan)


async def reset_job_status(job_id):
    await REDIS_CLIENT.delete(f"{job_id}_status")

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# Define global variables
REDIS_CLIENT = redis.Redis(host='localhost', port=6379, decode_responses=True)
LOCK = asyncio.Lock()
max_retries = 3


async def update_redis_status(job_id, total_jobs_scraped, results_wanted, status=None):
    """
    Update Redis with the current scraping status.
    """
    if status is None:
        if total_jobs_scraped >= results_wanted:
            status = "complete"
            status_percentage = 100  # Ensure it's exactly 100 when complete
        else:
            status = "in progress"
            status_percentage = (total_jobs_scraped / results_wanted) * 100
    else:
        status_percentage = (total_jobs_scraped / results_wanted) * \
            100 if status == "in progress" else 100

    async with LOCK:
        await REDIS_CLIENT.hset(f"{job_id}_status", mapping={
            "status_percentage": status_percentage,
            "total_jobs_scraped": total_jobs_scraped,
            "results_wanted": results_wanted,
            "status": status,
        })


def convert_to_strings(obj):
    """
    Recursively convert date, datetime, non-serializable float objects, and None values to strings in the given object.
    Replace NaN and None values with empty strings.
    """
    if isinstance(obj, dict):
        for key, value in obj.items():
            if isinstance(value, (datetime, date)):
                obj[key] = value.strftime("%Y-%m-%d %H:%M:%S")
            elif value is None or (isinstance(value, float) and (math.isinf(value) or math.isnan(value))):
                obj[key] = ""
            elif isinstance(value, (list, dict)):
                convert_to_strings(value)
            else:
                obj[key] = str(value) if value is not None else ""
    elif isinstance(obj, list):
        for index, item in enumerate(obj):
            if isinstance(item, (datetime, date)):
                obj[index] = item.strftime("%Y-%m-%d %H:%M:%S")
            elif item is None or (isinstance(item, float) and (math.isinf(item) or math.isnan(item))):
                obj[index] = ""
            elif isinstance(item, (list, dict)):
                convert_to_strings(item)
            else:
                obj[index] = str(item) if item is not None else ""
    return obj


async def scrape_jobs_task(job_id: str, site_names: List[str], results_wanted: int, hours_old: int, country_indeed: str, search_term: str, location: str):
    all_jobs = []  # Reset `all_jobs` list for each new scrape
    total_sites = len(site_names)
    # Distribute results evenly across sites
    results_per_site = math.ceil(results_wanted / total_sites)

    # Configure your proxies
    proxies = {
        'http': 'http://brd-customer-hl_50ca6c70-zone-residential_proxy1:i841o03n302x@brd.superproxy.io:22225',
        'https': 'http://brd-customer-hl_50ca6c70-zone-residential_proxy1:i841o03n302x@brd.superproxy.io:22225'
    }

    for site_name in site_names:
        offset = 0  # Reset `offset` for each site
        retries_left = max_retries
        site_jobs = []  # Collect jobs for the current site

        while len(site_jobs) < results_per_site and retries_left > 0:
            remaining_results = results_per_site - len(site_jobs)
            results_in_each_iteration = min(remaining_results, 30)

            try:
                logger.info(
                    f"Scraping {offset} to {offset + results_in_each_iteration} jobs on site: {site_name}")

                jobs = scrape_jobs(
                    site_name=site_name,
                    search_term=search_term,
                    location=location,
                    results_wanted=results_in_each_iteration,
                    country_indeed=country_indeed,
                    offset=offset,
                    proxies=proxies,
                    hours_old=hours_old
                )

                if jobs is not None and not jobs.empty:
                    job_records = jobs.to_dict('records')
                    job_records = convert_to_strings(job_records)
                    site_jobs.extend(job_records)
                    await update_redis_status(job_id, len(all_jobs) + len(site_jobs), results_wanted)
                    logger.info(
                        f"Scraped {len(site_jobs)} jobs so far from {site_name}")
                    offset += results_in_each_iteration  # Ensure offset is incremented
                    await asyncio.sleep(5)  # Polite scraping delay
                    retries_left = max_retries  # Reset retries left if successful
                else:
                    retries_left -= 1
                    logger.warning(
                        f"No jobs found on {site_name}. Retrying... Attempts left: {retries_left}")
                    await asyncio.sleep(5)  # Polite retry delay

            except Exception as e:
                logger.error(
                    f"Error while scraping for job_id {job_id} on site {site_name}: {e}")
                retries_left -= 1  # Decrease retries_left on each retry
                logger.warning(f"Retrying... Attempts left: {retries_left}")
                await asyncio.sleep(5)  # Polite retry delay

            if retries_left <= 0:
                logger.error(
                    f"Max retries reached for job_id {job_id} on site {site_name}. Moving to next site.")
                break

        all_jobs.extend(site_jobs)  # Accumulate jobs from the current site

    # Save all collected jobs to Redis
    async with LOCK:
        filtered_jobs = []
        if search_term is not None and search_term.strip() != "" and len(all_jobs) > 0:
            keywords = search_term.lower().strip().split()

            def job_matches(job_title, keywords):
                for keyword in keywords:
                    if fuzz.partial_ratio(keyword, job_title.lower()) > 70:
                        return True
                return False

            filtered_jobs = [job for job in all_jobs if job_matches(
                job['title'], keywords)]

    await REDIS_CLIENT.hset(job_id, "jobs", json.dumps(all_jobs))
    await REDIS_CLIENT.hset(job_id, "filtered_jobs", json.dumps(filtered_jobs))

    logger.info(
        f"Scraping completed for job_id {job_id}. Total jobs scraped: {len(all_jobs)} from all sites")
    await update_redis_status(job_id, len(all_jobs), results_wanted, status="complete")

    # Additional logging for final job collection
    if len(all_jobs) > 0:
        logger.info(
            f"Total jobs collected for job_id {job_id}: {len(all_jobs)}")
    else:
        logger.info(f"No jobs were collected for job_id {job_id}.")


@app.get("/start_scraping")
async def start_scraping_api(
        site_names: List[str] = Query(
            default=["naukri"], description="Job sites to scrape"),
        results_wanted: int = Query(
            default=50, description="Number of results wanted"),
        country_indeed: str = Query(default="India"),
        search_term: str = Query(default=".net dev"),
        location: str = Query(default="Noida, India"),
        hours_old: int = Query(default=24, description="Hours old")
):
    job_id = str(uuid.uuid4())
    job_count = await REDIS_CLIENT.incr("job_count")
    start_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # Save initial job metadata
    await REDIS_CLIENT.hset(f"{job_id}_status", mapping={
        "job_id": job_id,
        "order": job_count,
        "start_time": start_time,
        "search_term": search_term,
        "location": location,
        "status": "in progress",
        "results_wanted": results_wanted
    })

    logger.info(f"Data saved to Redis Database {job_id}")

    asyncio.create_task(scrape_jobs_task(
        job_id, site_names, results_wanted, hours_old, country_indeed, search_term, location))
    logger.info(f"Started scraping task with job_id {job_id}")
    return {"message": "Scraping task started", "job_id": job_id}


@app.get("/scraping_status/{job_id}")
async def get_scraping_status(job_id: str):
    try:
        status_data = await REDIS_CLIENT.hgetall(f"{job_id}_status")
        if status_data:
            return {
                "order": int(status_data.get("order", 0)),
                "start_time": status_data.get("start_time", ""),
                "status_percentage": float(status_data.get("status_percentage", 0)),
                "total_jobs_scraped": int(status_data.get("total_jobs_scraped", 0)),
                "results_wanted": int(status_data.get("results_wanted", 0)),
                "status": status_data.get("status", "")
            }
        else:
            raise HTTPException(
                status_code=404, detail="No status found for this job ID")
    except Exception as e:
        logger.error(
            f"Error fetching scraping status for job_id {job_id}: {e}")
        raise HTTPException(status_code=500, detail="Internal Server Error")


@app.get("/job/{job_id}")
async def get_job_from_redis(job_id: str):
    try:
        job_data = await REDIS_CLIENT.hget(job_id, "jobs")
        filtered_job_data = await REDIS_CLIENT.hget(job_id, "filtered_jobs")
        if job_data and filtered_job_data:
            job_data = json.loads(job_data)
            filtered_job_data = json.loads(filtered_job_data)
            return {"original_jobs": job_data, "filtered_jobs": filtered_job_data}
        else:
            # Return an empty list if no jobs have been scraped yet
            status_data = await REDIS_CLIENT.hgetall(f"{job_id}_status")
            if status_data and status_data.get("status") == "in progress":
                return {"original_jobs": [], "filtered_jobs": []}
            else:
                raise HTTPException(
                    status_code=404, detail="No data found for this job ID")
    except Exception as e:
        logger.error(f"Error fetching job data for job_id {job_id}: {e}")
        raise HTTPException(status_code=500, detail="Internal Server Error")


@app.get("/all_jobs")
async def get_all_jobs():
    try:
        job_ids = await REDIS_CLIENT.keys("*_status")
        all_jobs = []
        for job_id in job_ids:
            job_key = job_id.split("_status")[0]
            job_status = await REDIS_CLIENT.hgetall(job_id)
            all_jobs.append({
                "job_id": job_key,
                "order": int(job_status.get("order", 0)),
                "start_time": job_status.get("start_time", ""),
                "search_term": job_status.get("search_term", ""),
                "location": job_status.get("location", ""),
                "status": job_status.get("status", ""),
                "results_wanted": int(job_status.get("results_wanted", 0))
            })
        return all_jobs
    except Exception as e:
        logger.error(f"Error fetching all jobs: {e}")
        raise HTTPException(status_code=500, detail="Internal Server Error")


@app.delete("/delete_job/{job_id}")
async def delete_job(job_id: str):
    try:
        await REDIS_CLIENT.delete(f"{job_id}_status")
        await REDIS_CLIENT.delete(job_id)
        logger.info(f"Deleted job data for job_id {job_id}")
        return {"message": "Job data deleted successfully"}
    except Exception as e:
        logger.error(f"Error deleting job data for job_id {job_id}: {e}")
        raise HTTPException(status_code=500, detail="Internal Server Error")

if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="127.0.0.1", port=8000)
