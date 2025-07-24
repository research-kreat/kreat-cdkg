from pymongo import MongoClient
import re

client = MongoClient("mongodb+srv://kreat-admin:6qiv4xCjdm1ZUzKL@aikreat.rux6qx9.mongodb.net/")  # Update with your URI
db = client["KG"]
collection = db["cdkg"]

aerospace_keywords = {
    "core": [
        "aerospace", "aeronautics", "aviation", "aircraft", "space", "satellite",
        "orbit", "orbital", "spacecraft", "launch vehicle", "rocket", "rocketry",
        "missile", "missile systems", "propulsion", "flight", "flight systems",
        "airframe", "space systems", "space missions", "aerospace engineering",
        "space engineering", "aero engineering", "aero structures", "navigation",
        "aero propulsion", "inertial systems", "aerodynamics", "re-entry systems",
        "satcom", "telemetry", "guidance systems", "flight control", "air traffic",
        "radar systems", "infrared tracking", "GNSS", "space weather", "avionics",
        "supersonic", "hypersonic", "payload", "booster", "altitude", "thrust",
        "nozzle design", "cryogenics", "launchpad", "deep space", "low earth orbit"
    ],
    "related": [
        "defense", "military tech", "drones", "UAV", "UAS", "VTOL", "helicopter",
        "glider", "airport systems", "air defense", "jet engine", "combustion chamber",
        "electrical propulsion", "turbine", "wind tunnel", "fluid dynamics",
        "structural integrity", "remote sensing", "space science", "astrophysics",
        "telecommunication", "earth observation", "planetary entry", "cubesat",
        "nano satellite", "launch services", "space debris", "ISS", "Mars mission",
        "lunar landing", "autopilot", "autonomous flight", "machine vision",
        "pressure vessel", "carbon composites", "additive manufacturing",
        "thermal protection", "radiation shielding", "payload delivery", "reusable rocket",
        "trajectory optimization", "space law", "microgravity", "altimeter", "aeroelasticity",
        "space tourism", "mission design", "flight testing", "system integration",
        "electromechanical systems", "simulation systems"
    ]
}

keywords = aerospace_keywords["core"] + aerospace_keywords["related"]
regex_pattern = "|".join(re.escape(word) for word in keywords)
# Regex to match any field containing 'aerospace' (case-insensitive)
query = {
    "technology_stack": {
        "$regex": re.compile(regex_pattern, re.IGNORECASE)
    }
}


results = list(collection.find(query))
print(f"Found {len(results)} records related to aerospace.")
