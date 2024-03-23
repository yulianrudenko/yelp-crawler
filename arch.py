import json
import requests

from urllib.parse import urlparse
from urllib.parse import parse_qs
from bs4 import BeautifulSoup
from time import perf_counter

BASE_URL = "https://yelp.com"
RESULTS_FILE_NAME = "yelp_businesses.json"
ELEMENTS_PER_PAGE = 10
REVIEWS_LIMIT = 5
SCRAP_PAGES_LIMIT = 1


def soup_parse(url: str, **kwargs) -> BeautifulSoup:
    response = requests.get(url, params=kwargs)
    soup_business_detail = BeautifulSoup(response.content, "html.parser")
    return soup_business_detail


def scrap_businesses(category: str, location: str) -> list[dict]:
    """
    Scraps businesses data from Yelp.com for given category name and location.
    """

    results: list[dict] = []

    # Set up pagination
    is_last_page = False
    loop_step = 0
    while not is_last_page:
        if loop_step >= SCRAP_PAGES_LIMIT:
            break
        loop_step += 1
        soup_business_list = soup_parse(
            url=f"{BASE_URL}/search", 
            find_desc=category,
            find_loc=location,
            start=loop_step*ELEMENTS_PER_PAGE
        )
        pagination_btn = soup_business_list.find("button", class_="pagination-button__09f24__kbFYf")
        is_last_page = pagination_btn.has_attr("disabled")

        # Iterate through business divs on page
        for div in soup_business_list.find_all("div", class_="container__09f24__FeTO6"):
            # Name and detail-URL
            name_and_link_element = div.find("a", class_="css-19v1rkv")
            name = name_and_link_element.text
            business_yelp_url = name_and_link_element.get('href')
            business_yelp_url = f"{BASE_URL}{business_yelp_url}"  # Build full URL

            # Rating data
            rating = div.find("span", class_="css-gutk1c")
            if rating is not None:
                rating = rating.text
            num_of_reviews = [x for x in div.find("span", class_="css-chan6m").text if x.isdigit()]
            num_of_reviews = int(''.join(num_of_reviews)) if num_of_reviews else 0

            # Fetch detail business page 
            soup_business_detail = soup_parse(url=business_yelp_url)

            # Read website URL
            contact_sections = soup_business_detail.find_all("section", "css-2entjo")
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
                reviews_div = soup_business_detail.find("div", id="reviews")
                if reviews_div is None:
                    soup_business_detail = soup_parse(business_yelp_url)
                else:
                    break
            if reviews_div is None:
                raise ValueError(f"Could not find reviews div, please try again.\nBusiness URL: {business_yelp_url}")
            reviews_li = reviews_div.find_all("li", class_="css-1q2nwpv")[1:REVIEWS_LIMIT+1]  # First one is used for input, limit to 5
            for review_li in reviews_li:
                review_data = {}
                review_data["reviewer_name"] = review_li.find("span", class_="css-ux5mu6").text
                reviewer_location = review_li.find("span", class_="css-qgunke")
                review_data["reviewer_location"] = reviewer_location.text if reviewer_location is not None else None
                review_data["reviewer_date"] = review_li.find("span", class_="css-chan6m").text
                reviews.append(review_data)

            results.append({
                "name": name,
                "rating": rating,
                "number_of_reviews": num_of_reviews,
                "yelp_url": business_yelp_url,
                "website_url": business_website_url,
                "reviews": reviews
            })

    businesses_data: list[dict] = {
        "category": category,
        "location": location,
        "results": results
    }
    return businesses_data

def main():
    # category: str = str(input("Category name: "))
    category = "contractors"
    location = "San Francisco, CA"

    businesses_data = scrap_businesses(category=category, location=location)

    # Write data to JSON file
    with open(RESULTS_FILE_NAME, "w") as file:
        json.dump(businesses_data, file, ensure_ascii=False, indent=4)
    print(f"Results successfully saved to {RESULTS_FILE_NAME}")


if __name__ == "__main__":
    start = perf_counter()
    main()
    print(f"Program execution duration: {perf_counter() - start}")