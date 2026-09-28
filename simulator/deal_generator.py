import uuid 
import random 
from datetime import datetime, timezone, timedelta
from simulator.constants import (
    STAGES, STAGE_TRANSITIONS, VERTICALS, OWNERS, 
    LOCATIONS, DEAL_TEMPLATES, 
    VALUE_UPDATE_PROBABILITY, OWNER_REASSIGN_PROBABILITY
)

# Malaysia timezone (UTC+8)
MYT = timezone(timedelta(hours=8))


# -------- 1. Create one single event --------
def make_event(deal_id, event_type, owner, deal_name, deal_value_rm, old_stage, new_stage, timestamp, vertical):
    '''Build one event dictionary matching Kafka schema. (Event Builder)'''
    return {
        "event_id"      : str(uuid.uuid4()),
        "deal_id"       : deal_id,
        "event_type"    : event_type,
        "owner"         : owner,
        "deal_name"     : deal_name,
        "deal_value_rm" : deal_value_rm,
        "old_stage"     : old_stage,
        "new_stage"     : new_stage,
        "timestamp"     : timestamp.isoformat(),
        "metadata"      : {
            "source"  : "simulator_v1",
            "vertical": vertical
        }
    }


# -------- 2. Picks a random price based on the vertical --------
def random_deal_value(vertical):
    '''
    Picks a random deal value based on the vertical's range. 
    Rounded to nearest 100. (Price Generator)
    '''
    v = VERTICALS[vertical]
    raw = random.uniform(v["min_value"], v["max_value"])
    return round(raw / 100) * 100       # rounded to nearest RM 100 

# -------- 3. Simulate one full deal journey and return events --------
def generate_deal_lifecycle(deal_id):
    '''
    Simulate one deal's full life story.
    Returns a list of events in time order.
    '''
    # ---- Setup ----
    vertical = random.choice(list(VERTICALS.keys()))
    owner = random.choice(OWNERS)
    location = random.choice(LOCATIONS)
    template = random.choice(DEAL_TEMPLATES[vertical])
    deal_name = template.format(location=location)
    deal_value = random_deal_value(vertical)

    # Start Time: random point in the last 30 days 
    now = datetime.now(MYT)
    start_time = now - timedelta(days = random.randint(1, 30))
    current_time = start_time 

    events = []
    current_stage = "Enquiry"

    # ---- First event: deal_created ----
    events.append(make_event(
        deal_id = deal_id,
        event_type = "deal_created",
        owner = owner,
        deal_name = deal_name,
        deal_value_rm = deal_value,
        old_stage = None,
        new_stage = "Enquiry",
        timestamp = current_time,
        vertical = vertical,
    ))

    # ---- Walk through stages ----
    while current_stage != "Invoiced":
        current_time += timedelta(hours=random.randint(4, 72))
        if current_time > now:     
            # future time is not allowed 
            break              
        # maybe the price changes (~15%) 
        if random.random() < VALUE_UPDATE_PROBABILITY:
            deal_value = round(deal_value * random.uniform(0.8, 1.2) / 100) * 100 
            events.append(make_event(
                deal_id = deal_id,
                event_type = "value_updated",
                owner = owner,
                deal_name = deal_name,
                deal_value_rm = deal_value,
                old_stage = current_stage,
                new_stage = current_stage,
                timestamp = current_time,
                vertical = vertical
            ))
        # maybe the owner changes (~5%)
        if random.random() < OWNER_REASSIGN_PROBABILITY:
            new_owner = random.choice([o for o in OWNERS if o != owner])
            events.append(make_event(
                deal_id = deal_id,
                event_type = "owner_reassigned",
                owner = new_owner,
                deal_name = deal_name,
                deal_value_rm = deal_value,
                old_stage = current_stage,
                new_stage = current_stage,
                timestamp = current_time,
                vertical = vertical
            ))
            owner = new_owner
        # Roll the transition dice
        transitions = STAGE_TRANSITIONS[current_stage]

        if transitions is None:
            break 

        roll = random.random()
        cumulative = 0 
        outcome = None

        for action, probability in transitions.items():
            cumulative += probability 
            if roll < cumulative:
                outcome = action
                break 

        if outcome == "advance": 
            stage_index = STAGES.index(current_stage)
            new_stage = STAGES[stage_index + 1]
            events.append(make_event(
                deal_id=deal_id,
                event_type="stage_changed",
                owner=owner,
                deal_name=deal_name,
                deal_value_rm=deal_value,
                old_stage=current_stage,
                new_stage=new_stage,
                timestamp=current_time,
                vertical=vertical,
            ))
            current_stage = new_stage # update stage
        elif outcome == "stay":
                    pass              # nothing change 
        elif outcome == "drop":
            events.append(make_event(
                deal_id=deal_id,
                event_type="deal_lost",
                owner=owner,
                deal_name=deal_name,
                deal_value_rm=deal_value,
                old_stage=current_stage,
                new_stage="Lost",
                timestamp=current_time,
                vertical=vertical,
            ))
            break                   # deal is dead, exit the loop
        elif outcome == "regress":
            stage_index = STAGES.index(current_stage)
            new_stage = STAGES[stage_index - 1]
            events.append(make_event(
                deal_id=deal_id,
                event_type="stage_changed",
                owner=owner,
                deal_name=deal_name,
                deal_value_rm=deal_value,
                old_stage=current_stage,
                new_stage=new_stage,
                timestamp=current_time,
                vertical=vertical,
            ))
            current_stage = new_stage   # update stage

    return events
        
        
            
            


