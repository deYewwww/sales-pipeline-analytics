'''
Business domain constants for the SME Sales Pipeline simulator.

Separate business logic from infrastructure config (config.py).
Because infrastructure changes per environment. Business rule don't. 
'''

# -------- Pipeline Stages --------
# HumbleBear Kanban: linear with allowed regressions.
STAGES = ['Enquiry', 'Quoted', 'Won', 'Invoiced']

# -------- Transition Probabilities --------
STAGE_TRANSITIONS = {
    "Enquiry": {
        "advance": 0.60,
        "drop"   : 0.35,
        "stay"   : 0.05
    },
    "Quoted": {
        "advance": 0.50,
        "drop"   : 0.30,
        "stay"   : 0.15,
        "regress": 0.05
    },
    "Won": {
        "advance": 0.85,
        "stay"   : 0.10,
        "regress": 0.05
    },
    "Invoiced": {}
}

# -------- Malaysia SME Verticals --------
VERTICALS = {
    "cctv": {
        "min_value": 3000,
        "max_value": 25000
    },
    "office_fitout": {
        "min_value": 15000,
        "max_value": 120000
    },
    "warehouse_racking": {
        "min_value": 8000,
        "max_value": 80000
    },
    "retail_signage": {
        "min_value": 2000,
        "max_value": 15000
    }
}

# -------- Sales Reps --------
OWNERS = ["Aisyah", "Rizal", "Wen Lin", "Priya", "Hafiz"]

# -------- Deal Name Templates --------
LOCATIONS = [
    "PJ Branch", 
    "Johor Bahru HQ", 
    "Penang Warehouse",
    "Klang Valley Office", 
    "Ipoh Outlet", 
    "Melaka Branch",
    "Cyberjaya HQ", 
    "Shah Alam Factory", 
    "Kuantan Depot",
    "Kota Kinabalu Branch"
]

DEAL_TEMPLATES = {
    "cctv": [
        "CCTV installation - {location}",
        "Security Camera Upgrade - {location}",
        "CCTV Maintenance Contract - {location}" 
    ],
    "office_fitout": [
        "Office Renovation - {location}",
        "New Office Setup - {location}",
        "Workspace Redesign - {location}"
    ],
    "warehouse_racking": [
        "Pallet Racking System - {location}",
        "Warehouse Storage Setup - {location}",
        "Racking Expansion - {location}"
    ],
    "retail_signage": [
        "Shopfront Signage - {location}",
        "LED Display Board - {location}",
        "Banner & Signage Package - {location}",
    ]
}

# -------- Event Generation Tuning --------
VALUE_UPDATE_PROBABILITY = 0.15
OWNER_REASSIGN_PROBABILITY = 0.05