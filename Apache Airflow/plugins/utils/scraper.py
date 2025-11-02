import requests
from bs4 import BeautifulSoup
from urllib.parse import urlparse

def scrape_download_links(page_url: str) -> list[dict]:
    """
    Scrapes the given URL for download links (parquet files) and extracts
    the base URL and path for each link.

    Args:
        page_url: The URL of the page to scrape.

    Returns:
        A list of dictionaries, where each dictionary contains 'base_url' and 'path'
        for a download link.
    """
    response = requests.get(page_url)
    response.raise_for_status()  # Raise an exception for HTTP errors

    soup = BeautifulSoup(response.text, 'html.parser')

    download_links = []
    # Find all <a> tags that contain a link to a .parquet file
    for link_tag in soup.find_all('a', href=True):
        href = link_tag['href']
        if '.parquet' in href and 'yellow' in href:
            parsed_url = urlparse(href)
            base_url = f"{parsed_url.scheme}://{parsed_url.netloc}"
            path = parsed_url.path
            download_links.append({'base_url': base_url, 'path': path})

    return download_links

if __name__ == '__main__':
    # Example usage:
    nyc_tlc_url = "https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page"
    links = scrape_download_links(nyc_tlc_url)
    for link in links:
        print(link)
