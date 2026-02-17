import requests
from bs4 import BeautifulSoup

# Change the url to check request
url = "https://tiki.vn/the-thao-da-ngoai/c1975"
headers = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) Gecko/20100101 Firefox/147.0",
    "Accept-Language": "vi-VN,vi;q=0.9,en;q=0.8",
}

r = requests.get(url, headers=headers, timeout=30)
print(r.status_code)
print(r.text[:5000])

soup = BeautifulSoup(r.text, "lxml")
print(soup.title.get_text(strip=True) if soup.title else "No title")