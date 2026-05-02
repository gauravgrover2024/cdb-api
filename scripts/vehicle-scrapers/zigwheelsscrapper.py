from playwright.sync_api import sync_playwright
from bs4 import BeautifulSoup
import re

URL = "https://www.zigwheels.com/news-features/general-news/hyundai-cars-discounts-offers-may-2026/"


def scrape():
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()

        print("Loading page...")
        page.goto(URL, timeout=60000)

        # wait for content to load
        page.wait_for_timeout(5000)

        html = page.content()

        browser.close()

    print("HTML length:", len(html))

    with open("debug.html", "w") as f:
        f.write(html)

    soup = BeautifulSoup(html, "html.parser")

    content = soup.find("article") or soup

    data = []
    current_model = None

    for tag in content.find_all(["h2", "h3", "p"]):
        text = tag.get_text(strip=True)

        if tag.name in ["h2", "h3"]:
            current_model = text
            print("\nMODEL:", current_model)
            continue

        amounts = re.findall(r"₹\s?[\d,]+", text)

        if current_model and amounts:
            values = [int(a.replace("₹", "").replace(",", "")) for a in amounts]

            print("FOUND:", current_model, text)

            data.append({
                "model": current_model,
                "text": text,
                "max_benefit": max(values),
                "total": sum(values)
            })

    print("\nFINAL DATA:\n", data)


if __name__ == "__main__":
    scrape()