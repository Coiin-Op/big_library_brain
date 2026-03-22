import requests

# Define the URL you want to search for
url = "https://www.google.com/search?q=your+search+terms"

# Send an HTTP GET request to the URL
response = requests.get(url)

# Check if the request was successful
if response.status_code == 200:
    # Parse the HTML content of the page
    from bs4 import BeautifulSoup

    soup = BeautifulSoup(response.text, "html.parser")

    # Find all links on the page
    links = soup.find_all("a")

    # Print each link
    for link in links:
        print(link.get("href"))
else:
    print("Failed to retrieve the webpage.")
