"""
get_geo.py

Fetch geolocation for a mandi (market) using the Google Maps Geocoding API
and return a document ready for MongoDB insertion.

Usage:
    from get_geo import get_mandi_geo_doc
    doc = get_mandi_geo_doc("Mayabandar", "Andaman and Nicobar Islands")
"""

import os
import requests
from datetime import datetime, timezone
from dotenv import load_dotenv

load_dotenv()


def get_mandi_geo_doc(market_name: str, state: str) -> dict | None:
    """
    Geocode a mandi by market_name + state and return a MongoDB-ready document.

    Args:
        market_name: Name of the mandi / market (e.g. "Mayabandar")
        state:       State name (e.g. "Andaman and Nicobar Islands")

    Returns:
        A dict shaped for the mandi_geo collection, or None if geocoding fails.
    """
    # TODO(security): Move API key to a secrets manager for production.
    api_key = os.getenv("GOOGLE_MAPS_API_KEY")
    if not api_key:
        raise EnvironmentError(
            "GOOGLE_MAPS_API_KEY is not set. "
            "Add it to your .env file or export it as an environment variable."
        )

    address = f"{market_name},{state},India"
    url = "https://maps.googleapis.com/maps/api/geocode/json"
    params = {
        "address": address,
        "key": api_key,
    }

    resp = requests.get(url, params=params, timeout=10)
    resp.raise_for_status()
    data = resp.json()

    if data.get("status") != "OK" or not data.get("results"):
        print(f"⚠️  Geocoding failed for '{address}': {data.get('status')}")
        return None

    result = data["results"][0]
    location = result["geometry"]["location"]

    # Extract district and postcode from address components
    district = None
    postcode = None
    for component in result.get("address_components", []):
        types = component.get("types", [])
        if "administrative_area_level_3" in types:
            district = component["long_name"]
        elif "postal_code" in types:
            postcode = component["long_name"]

    now = datetime.now(timezone.utc)

    doc = {
        "state":     state,
        "name":      market_name,
        "aliases":   [],
        "district":  district,
        "location": {
            "type":        "Point",
            "coordinates": [location["lng"], location["lat"]],  # GeoJSON: [lng, lat]
        },
        "postcode":  postcode,
        "createdAt": now,
        "updatedAt": now,
    }

    return doc