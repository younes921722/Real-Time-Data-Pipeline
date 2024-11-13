class DataTransformer:

    def transform_data(self, raw_data):
        """Transforms the raw data into a dictionary."""
        if raw_data is None:
            return None

        try:
            location = raw_data["location"]
            transformed_data = {
                "first_name": raw_data["name"]["first"],
                "last_name": raw_data["name"]["last"],
                "gender": raw_data['gender'],
                "address": f"{location['street']['number']} {location['street']['name']}, {location['city']}, {location['state']}, {location['country']}",
                "postcode": location["postcode"],
                "email": raw_data["email"],
                "username": raw_data["login"]["username"],
                "dob": raw_data["dob"]["date"],
                "registered": raw_data["registered"]["date"],
                "phone": raw_data["phone"],
                "picture": raw_data["picture"]["medium"]
            }
            return transformed_data
        except KeyError as e:
            print(f"Error transforming data: Missing key {e}")
            return None 