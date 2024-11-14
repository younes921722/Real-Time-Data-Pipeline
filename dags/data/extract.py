import requests

class DataExtractor:
    """Extracts user data from the randomuser.me API."""

    def __init__(self, api_url="https://randomuser.me/api"):
        self.api_url = api_url

    def extract_data(self):
        """Fetches data from the API and returns the raw JSON response."""
        try:
            response = requests.get(self.api_url)
            response.raise_for_status()  # Raise an exception for bad status codes (4xx or 5xx)
            return response.json()['results'][0]
        
        except requests.exceptions.RequestException as e:
            print(f"Error extracting data: {e}") 
            return None  
