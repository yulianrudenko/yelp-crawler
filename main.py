import json
import asyncio
import aiohttp
import platform

from urllib.parse import urlparse
from urllib.parse import parse_qs
from time import perf_counter
from bs4 import BeautifulSoup

import elements

BASE_URL = "https://yelp.com"
RESULTS_FILE_NAME = "yelp_businesses.json"
ELEMENTS_PER_PAGE = 10
REVIEWS_LIMIT = 5
SCRAP_PAGES_LIMIT = 2


async def soup_parse(url: str, **kwargs) -> BeautifulSoup:
    """
    Creates a BeautifulSoup object from page response
    """
    async with aiohttp.ClientSession() as session:
        async with session.get(url, params=kwargs) as response:
            response.raise_for_status()
            content = await response.read()
            return BeautifulSoup(content, "html.parser")


async def scrap_business_detail(url: str, existing_data: dict) -> dict:
    """
    Scraps specific detail business page
    """ 

    soup_business_detail = await soup_parse(url=url)

    # Read website URL
    contact_sections = soup_business_detail.find_all(*elements.BUSINESS_CONTACT_SECTION)
    business_website_redirect_url = None
    for contact_section in contact_sections:
        # Find section with redirect link to business website
        section_links = contact_section.find_all("a")
        for link in section_links:
            link_href = link.get("href") or ""
            if "/biz_redir" in link_href:
                business_website_redirect_url = link_href
                break
    business_website_url = None
    if business_website_redirect_url is not None:
        # Parse the real website URL
        parsed_business_website_url = urlparse(business_website_redirect_url)
        business_website_url = parse_qs(parsed_business_website_url.query)['url'][0]

    # Read reviews data
    reviews: list[dict] = []
    # May not work the first time, need to try "refetching" this business page
    for _ in range(10):
        reviews_div = soup_business_detail.find(*elements.BUSINESS_REVIEWS_DIV)
        if reviews_div is None:
            soup_business_detail = await soup_parse(url)
        else:
            break
    if reviews_div is None:
        raise ValueError(f"Could not find reviews div, please try again.\nBusiness URL: {url}")
    reviews_li = reviews_div.find_all(*elements.REVIEW_LI)[1:REVIEWS_LIMIT+1]  # First one is used for input, limit to 5
    for review_li in reviews_li:
        review_data = {}
        review_data["reviewer_name"] = review_li.find(*elements.REVIEWER_NAME).text
        reviewer_location = review_li.find(*elements.REVIEWER_LOCATION)
        review_data["reviewer_location"] = reviewer_location.text if reviewer_location is not None else None
        review_data["date"] = review_li.find(*elements.REVIEW_DATE).text
        reviews.append(review_data)

    existing_data.update({
        "website_url": business_website_url,
        "reviews": reviews
    })
    return existing_data


async def scrap_businesses(category: str, location: str) -> list[dict]:
    """
    Scraps businesses data from Yelp.com for given category name and location.
    """

    tasks = []

    # Set up pagination
    is_last_page = False
    loop_step = 0
    while not is_last_page:
        if loop_step >= SCRAP_PAGES_LIMIT:
            break
        loop_step += 1

        try:
            soup_business_list = await soup_parse(
                url=f"{BASE_URL}/search", 
                find_desc=category,
                find_loc=location,
                start=loop_step*ELEMENTS_PER_PAGE
            )
        except:
            raise SystemError("Please try again as Yelp rejected request")

        pagination_btn = soup_business_list.find(*elements.NEXT_PAGE_BTN)
        is_last_page = pagination_btn.has_attr("disabled")

        # Iterate through businesses divs on this page
        for div in soup_business_list.find_all(*elements.BUSINESS_LIST_DIV):
            # Name and detail-URL
            name_and_link_element = div.find(*elements.BUSINESS_NAME_AND_LINK)
            name = name_and_link_element.text
            business_yelp_url = name_and_link_element.get('href')
            business_yelp_url = f"{BASE_URL}{business_yelp_url}"  # Build full URL

            # Rating data
            rating = div.find(*elements.BUSINESS_RATING)
            if rating is not None:
                rating = rating.text.strip()
            num_of_reviews = [x for x in div.find(*elements.BUSINESS_REVIEWS_NUM).text if x.isdigit()]
            num_of_reviews = int(''.join(num_of_reviews)) if num_of_reviews else 0
            business_data =  {
                "name": name,
                "rating": rating,
                "number_of_reviews": num_of_reviews,
                "yelp_url": business_yelp_url,
            }
            tasks.append(asyncio.create_task(scrap_business_detail(url=business_yelp_url, existing_data=business_data)))

    results: list[dict] = await asyncio.gather(*tasks)
    businesses_data: list[dict] = {
        "category": category,
        "location": location,
        "count": len(results),
        "results": results
    }
    return businesses_data


async def main():
    category: str = str(input("Category: "))
    location: str = str(input("Location: "))

    start = perf_counter()
    print("Program started...")
    businesses_data = await scrap_businesses(category=category, location=location)

    # Write data to JSON file
    with open(RESULTS_FILE_NAME, "w", encoding="utf-8") as file:
        json.dump(businesses_data, file, ensure_ascii=False, indent=4)
    print(f"Results successfully saved to {RESULTS_FILE_NAME}")
    print(f"Program execution took: {perf_counter() - start} seconds.")


if __name__ == "__main__":
    os_name = platform.system()
    if os_name.lower() == "windows":
        # Ensure async code will work on Windows OS
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(main())
