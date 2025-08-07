import pandas as pd
import requests
import re
import os
# NEW: Import the fuzzy matching library
from thefuzz import process

# --- CONFIGURATION ---
# NOTE: The INPUT_FILE for this script should be the file that contains the 'ai_generated_abstract' column.
# If you are starting from scratch, it would be 'aerospace.csv', but you'd need to run the first script first.
# This script is designed to run on the OUTPUT of the first script.
# Let's assume the previous script's output was saved to "trial_with_abstracts.csv"
INPUT_FILE = "mongo_full_export.csv" # Or mongo_full_export.csv if it has the abstract
# The final output file with the new column appended
OUTPUT_FILE = "new_mongo.csv"
FAILED_FILE = "failed_append.csv"
OLLAMA_MODEL = "herald/phi3-128k:latest"
OLLAMA_API_URL = "http://localhost:11434/api/generate"

# --- List of all possible functions for the AI to classify into ---
all_functions = [
    "Vehicle Assembly Line Quality Control Systems", "Supply Chain Management", "Component Testing",
    "Lean Manufacturing", "Advanced Driver Assistance (ADAS)", "Vehicle Diagnostics",
    "Infotainment Systems", "Electric Powertrain Control", "Predictive Maintenance",
    "Aerodynamic Design", "Material Selection", "Crash Safety Design", "Thermal Management",
    "Noise Vibration Harshness", "Vehicle-to-Everything (V2X)", "Autonomous Driving AI",
    "Machine Learning for Personalization", "Computer Vision", "Edge Computing",
    "Battery Management Systems", "Regenerative Braking", "Electric Motor Control",
    "Charging Infrastructure", "Fuel Cell Systems", "Customer Experience Management",
    "Fleet Management", "Predictive Analytics", "After-Sales Service", "Mobility-as-a-Service",
    "Safety Certification & Testing", "Emissions Testing & Compliance", "Type Approval Processes",
    "Environmental Impact Assessment", "International Standards Compliance",
    "Vehicle Financing & Leasing", "Automotive Insurance Models", "Warranty Cost Prediction",
    "Residual Value Forecasting", "Total Cost of Ownership Analysis",
    "Enterprise Resource Planning (ERP)", "Product Lifecycle Management (PLM)",
    "Automotive Cybersecurity Systems", "Vehicle Data Analytics", "Software Over-the-Air Updates",
    "Union Relations & Negotiations", "Technical Skills Training", "Workplace Safety Programs",
    "Workforce Planning & Analytics", "Knowledge Management Systems", "Product Localization",
    "Cross-Cultural Team Management", "International Trade Compliance", "Currency Risk Management",
    "Global Supply Chain Coordination", "Lifecycle Assessment (LCA)",
    "End-of-Life Vehicle Management", "Carbon Footprint Tracking", "Sustainable Materials Sourcing",
    "Circular Economy Implementation", "User Trust in Autonomous Systems", "Driver Behavior Adaptation",
    "Consumer Adoption Resistance", "Risk Perception vs. Reality", "Social Proof in Technology Adoption",
    "Quantum-Enhanced Vehicle Sensors", "Bio-Inspired Material Systems", "AI-Human Collaborative Control",
    "Synthetic Biology Energy Storage", "Neuromorphic Computing Integration", "API Architecture & Integration",
    "Agile Development Methodology", "Cloud Platform Architecture", "Software Testing Automation",
    "DevOps Pipeline Management", "Chip Architecture Design", "Wafer Fabrication",
    "Electronic Design Automation", "Yield Optimization", "Machine Learning Model Development",
    "Computer Vision Systems", "Natural Language Processing", "Data Pipeline Architecture", "MLOps",
    "Embedded Systems Design", "IoT Device Architecture", "Edge Computing Optimization",
    "Hardware Abstraction Layers", "System-on-Chip Integration", "Enterprise Software Implementation",
    "Digital Strategy Consulting", "Business Process Automation", "Data Analytics Platforms",
    "Customer Experience Platforms", "Zero Trust Architecture", "Threat Detection & Response",
    "Privacy & Data Compliance", "Incident Response Management", "Security Architecture Design",
    "Software Certification", "Data Protection Compliance", "International Standards",
    "Audit & Documentation", "Risk Assessment & Management", "SaaS Revenue Models",
    "Technology Investment", "Intellectual Property Management", "Technology Valuation",
    "Partnership & Ecosystem", "Technical Talent Acquisition", "Skills Development & Training",
    "Remote Work Management", "Performance Management", "Technology Innovation Culture",
    "Global Software Deployment", "Cross-Cultural Technology Teams", "Data Sovereignty & Localization",
    "Technology Transfer & Distribution", "Global Supply Chain Management",
    "Green Computing & Energy Efficiency", "Circular Economy in Tech",
    "Sustainable Software Development", "E-waste Management", "User Interface Design",
    "Technology Adoptions Psychology", "Digital Trust & Security", "Accessibility & Inclusion",
    "Gamification & Engagement", "Quantum Computing Development", "Neuromorphic Computing",
    "Synthetic Biology in Computing", "Extended Reality (AR/VR/MR)", "Blockchain & Distributed Ledgers",
    "Platform Ecosystem Management", "API Economy & Monetization", "Customer Success Management",
    "Technology Services Delivery", "Digital Product Management", "Thermal Power Plant Operations",
    "Nuclear Power Generation", "Renewable Energy Integration", "Hydroelectric Power Systems",
    "Combined Heat & Power (CHP)", "Power Transmission Systems", "Grid Load Balancing & Dispatch",
    "Distribution Network Management", "Grid Stability & Frequency Control", "Transmission Line Monitoring",
    "Solar Photovoltaic Systems", "Wind Turbine Control Systems", "Energy Storage Integration",
    "Renewable Energy Forecasting", "Microgrid Management", "Grid-Scale Battery Storage",
    "Pumped Hydro Storage", "Demand Response Management", "Energy Arbitrage & Trading",
    "Power Quality Management", "Customer Service & Billing", "Field Service & Maintenance",
    "Outage Management & Restoration", "Meter Reading & Data Collection", "Asset Management & Planning",
    "Environmental Compliance & Emissions", "Electric Utility Rate Regulation",
    "Grid Security & Critical Infrastructure", "Nuclear Safety & Licensing", "Renewable Energy Standards",
    "Energy Project Finance", "Electricity Market Design", "Utility Investment & Asset Valuation",
    "Power Purchase Agreements (PPAs)", "Energy Service Company (ESCO) Models",
    "Smart Grid & Advanced Metering", "SCADA & Grid Control Systems", "Energy Data Analytics",
    "Grid Cybersecurity", "Distributed Energy Resource Management", "Utility Workforce Safety Training",
    "Power Plant Operations Training", "Grid Operations Center Staffing", "Technical Skills Development",
    "Utility Leadership Development", "International Energy Trade", "Cross-Border Grid Interconnections",
    "Global Renewable Energy Projects", "Energy Diplomacy & Policy", "Technology Transfer & Licensing",
    "Carbon Emissions Management", "Environmental Impact Assessment", "Waste Heat Recovery",
    "Water Resource Management", "Product Life Cycle Assessment", "Consumer Energy Behavior",
    "Demand Response Participation", "Renewable Energy Acceptance", "Energy Efficiency Motivation",
    "Grid Modernization Acceptance", "Hydrogen Production & Infrastructure", "Fusion Power Development",
    "AI-Powered Grid Optimization", "Energy Blockchain & Peer-to-Peer Trading", "Advanced Energy Storage",
    "Energy Efficiency Services", "Distributed Generation Services", "Grid Services & Ancillary Markets",
    "Energy Management & Analytics", "Power Quality & Reliability Services",
    "Target Identification & Validation", "Lead Compound Optimization", "Platform Technology Development",
    "Clinical Trial Design", "Biologics Manufacturing Bioprocessing", "Biocompatible Materials Selection",
    "Medical Device Miniaturization", "Human-Machine Interface Design", "Implantable Device Power Systems",
    "Surgical Robotics Systems", "Clinical Trial Management", "Platform Validation Across Multiple Indications",
    "Real-World Evidence Generation", "Biostatistics & Data Analysis", "Regulatory Submission Management",
    "Medical Imaging Technology", "Laboratory Diagnostics", "Molecular Diagnostics",
    "Point-of-Care Testing", "Pathology & Digital Histology",
    "Protein Engineering & Structure-Function Optimization", "Gene & Cell Therapy",
    "Minimally Invasive Surgery", "Radiation Therapy", "Immunotherapy & Biologics",
    "FDA Drug Approval Process", "Biosafety & Containment", "Good Manufacturing Practice (GMP)",
    "Medical Device Regulation", "Pharmacovigilance", "Biotech Venture Capital & Platform Investment",
    "Platform IP Strategy & Patent Portfolios", "Pharmaceutical Pricing",
    "Healthcare Investment & Venture Capital", "Academic-Industry Partnership Models",
    "Electronic Health Records (EHR)", "Bioinformatics & Computational Platforms",
    "Technology Transfer & Academic Licensing", "Healthcare Cybersecurity",
    "Clinical Decision Support Systems", "Medical Education & Training", "Healthcare Workforce Management",
    "Clinical Research Career Development", "Healthcare Quality & Safety Training",
    "Continuing Professional Medical Education", "Global Health Initiatives", "International Clinical Trials",
    "Pharmaceutical Export/Import", "Medical Mission & Humanitarian Aid", "Health Technology Transfer",
    "Pharmaceutical Waste Management", "Green Chemistry & Manufacturing",
    "Medical Device Lifecycle Management", "Carbon Footprint in Healthcare", "Sustainable Clinical Trials",
    "Patient Compliance & Medication Adherence", "Health Behavior Change", "Patient Trust in Medical AI",
    "Healthcare Decision-Making", "Chronic Disease Self-Management", "Synthetic Biology & Bioengineering",
    "CRISPR Gene Editing", "Computational Biology & AI Drug Discovery",
    "Regenerative Medicine & Tissue Engineering", "Digital Therapeutics & Biomarker Discovery",
    "Healthcare Delivery & Operations", "Pharmaceutical Sales & Marketing",
    "Medical Device Support Services", "Clinical Laboratory Services",
    "Health Information Technology Services", "Aircraft Structural Design", "Avionics Systems Architecture",
    "Aerodynamic Optimization", "Spacecraft Thermal Protection", "Mission Systems Integration",
    "Precision Aircraft Assembly", "Defense Equipment Manufacturing", "Composite Materials Processing",
    "Quality Assurance & Testing", "Supply Chain Security", "Flight Control Systems",
    "Radar & Sensor Systems", "Navigation & Guidance", "Command & Control Systems",
    "Autonomous Defense Systems", "Jet Engine Design", "Rocket Propulsion Systems",
    "Electric Aircraft Power Systems", "Fuel Systems & Management", "Hypersonic Propulsion",
    "Satellite Communications", "Military Communications", "Electronic Warfare Systems",
    "Air Traffic Control", "Intelligence & Analytics", "Aircraft Certification (FAA)",
    "Military Standards Compliance", "Export Control Compliance", "Environmental Compliance",
    "Cybersecurity Compliance", "Defense Contracting", "Government Program Management",
    "Aerospace Financing", "International Arms Sales", "Cost-Plus Contracting",
    "Mission-Critical Systems", "Classified Data Management", "Battlefield Edge Networks",
    "Defense Analytics Platforms", "Cybersecurity Operations", "Pilot Training & Certification",
    "Security Clearance Management", "Military Leadership Development", "Specialized Technical Training",
    "Workforce Security Programs", "International Defense Cooperation", "Arms Export Management",
    "Global Aerospace Operations", "Defense Diplomacy", "Space Operations Coordination",
    "Aircraft Emissions Management", "Military Environmental Impact", "Sustainable Aviation Fuels",
    "Noise Pollution Management", "Space Debris Management", "Pilot Decision-Making Under Stress",
    "Human-Machine Interface Trust", "Combat Stress & Resilience Management",
    "Team Coordination in Crisis", "Risk Perception in High-Stakes Environments",
    "Hypersonic Vehicle Technology", "Quantum Communications", "AI-Powered Warfare Systems",
    "Space-Based Manufacturing", "Directed Energy Weapons", "Aircraft Maintenance & Support",
    "Defense Logistics & Supply", "Aerospace Customer Support", "Training & Simulation Services",
    "System Integration Services", "Not Applicable"
]

def query_ollama(prompt: str) -> str:
    try:
        response = requests.post(OLLAMA_API_URL, json={
            "model": OLLAMA_MODEL,
            "prompt": prompt,
            "stream": False
        }, timeout=120)

        if response.status_code != 200:
            raise RuntimeError(f"HTTP {response.status_code}: {response.text}")

        return response.json().get("response", "")

    except Exception as e:
        print(f"❌ Ollama API error: {e}")
        return ""

# --- MAIN SCRIPT ---

# Load the already processed input file
try:
    df = pd.read_csv(INPUT_FILE)
    if df.empty:
        print(f"❌ Error: The input file '{INPUT_FILE}' is empty.")
        exit()
except FileNotFoundError:
    print(f"❌ Error: The input file '{INPUT_FILE}' was not found.")
    exit()


# Prepare for the new column and any potential failures
df['function'] = "" # Initialize the new column
failed_rows = []

# Process each row to generate the new 'function' field
for idx, row in df.iterrows():
    print(f"\n🔍 Processing row {idx + 1}/{len(df)} for function classification")

    # Use the AI-generated abstract from the previous step as input
    # Assuming the column exists from the previous script run.
    ai_abstract = row.get("ai_generated_abstract", "")

    # Fallback to the original abstract if the AI-generated one is missing
    if not isinstance(ai_abstract, str) or not ai_abstract.strip():
        ai_abstract = row.get("abstract", "")

    if not isinstance(ai_abstract, str) or not ai_abstract.strip():
        print("⚠️ Skipping due to empty or invalid 'ai_generated_abstract' and 'abstract'.")
        failed_rows.append(row)
        continue

    # Create a new, specific prompt for the classification task
    function_prompt = f"""
Analyze the following text and classify its primary technical function.
Choose the single most fitting category from the list provided below.

**Function Categories:**
{', '.join(all_functions)}

**Text to Analyze:**
{ai_abstract[:4000]}

**Instructions:**
Return *only* the name of the chosen function category. For example: Predictive Maintenance
"""

    print("📤 Sending prompt to Ollama for function classification...")
    classified_function = query_ollama(function_prompt).strip()

    # --- MODIFIED LOGIC: Implement Fuzzy Matching ---
    if classified_function:
        # process.extractOne finds the best match from the list of choices.
        # It returns a tuple: (best_match_string, similarity_score)
        best_match, score = process.extractOne(classified_function, all_functions)
        
        # Set a similarity threshold (e.g., 85%) to ensure match quality.
        # This avoids incorrect classifications if the LLM output is irrelevant.
        if score >= 50:
            df.at[idx, 'function'] = best_match
            print(f"✅ Classified function as: '{best_match}' (Similarity: {score}%)")
        else:
            df.at[idx, 'function'] = "Unclassified"
            print(f"⚠️ Best match '{best_match}' was below threshold (Similarity: {score}%). Marking as Unclassified.")
    else:
        print("❌ Failed to classify function (empty response).")
        df.at[idx, 'function'] = "Classification Failed"
        failed_rows.append(row)
    # --- END OF MODIFIED LOGIC ---


# Save the final DataFrame with the new column
df.to_csv(OUTPUT_FILE, index=False)
print(f"\n✅ Successfully processed and saved {len(df)} rows to {OUTPUT_FILE}")

# Save any rows that failed during the new step
if failed_rows:
    df_failed = pd.DataFrame(failed_rows)
    df_failed.to_csv(FAILED_FILE, index=False)
    print(f"⚠️ Saved {len(failed_rows)} failed rows to {FAILED_FILE}")
else:
    print("✅ No rows failed during the function classification step.")