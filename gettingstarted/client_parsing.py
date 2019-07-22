import pandas as pd

EAST_BAY = [
    "berkeley",
    "emeryville",
    "fremont",
    "north oakland",
    "downtown oakland",
    "walnut creek/concord",
    "grand lake",
]

SOUTH_BAY = [
    "san jose",
    "palo alto",
    "mountain view",
    "san mateo",
    "south san francisco",
    "san matro",
    "san jose",
    "san jose ca",
]

NORTH_BAY = ["Marin/North Bay"]

LOC_SF = [
    "loc_financial",
    "loc_chinatown",
    "loc_unionsq",
    "loc_soma",
    "loc_marina",
    "loc_russian",
    "loc_pacific",
    "loc_hayes",
    "loc_nopa",
    "loc_mission",
    "loc_castro",
    "loc_noe",
    "loc_dogpatch",
    "loc_richmond",
    "loc_sunset",
]


def parse_neighborhood(neighborhood, therapist_locs):
    member_locs = []
    neighborhood = [x.lower() for x in neighborhood]
    for n in neighborhood:
        if n in SOUTH_BAY:
            member_locs.append("loc_south_bay")
        if n in EAST_BAY:
            member_locs.append("loc_east")
        if n in NORTH_BAY:
            member_locs.append("loc_marin")
        for loc in therapist_locs:
            if loc[4:] in n:
                member_locs.append(loc)
    member_locs = list(set(member_locs))
    return member_locs


def parse_gender_pref(client):
    pref = client.get("prefer_gender").lower()
    if pref == "women":
        return "female"
    if pref == "men":
        return "male"
    return "both"


def parse_client_stressors(client):
    if client.get("stressors") is None:
        return []
    stressor_list = []
    stressors = client.get("stressors")
    for strs in stressors:
        strs = strs.lower()
        if "family" in strs:
            stressor_list.append("strs_family")
        if "romantic" in strs:
            stressor_list.append("strs_romantic")
        if "career" in strs:
            stressor_list.append("strs_career")
        if "money" in strs:
            stressor_list.append("strs_money")
        if "self" in strs:
            stressor_list.append("strs_self")
        if "body" in strs:
            stressor_list.append("strs_body")
        if "lgbt" in strs:
            stressor_list.append("strs_lgbt")
        if "depression" in strs:
            stressor_list.append("strs_depression")
        if "anxiety" in strs:
            stressor_list.append("strs_anxiety")
        if "trauma" in strs:
            stressor_list.append("strs_specific")
        if "addiction" in strs:
            stressor_list.append("strs_addiction")
        if "friends" in strs:
            stressor_list.append("strs_friends")
        if "dating" in strs:
            stressor_list.append("strs_dating")
        if "sleep" in strs:
            stressor_list.append("strs_sleep")
        if "fertility" in strs:
            stressor_list.append("strs_fertility")
        if "parenting" in strs:
            stressor_list.append("strs_parenting")
        if "women" in strs:
            stressor_list.append("strs_womens_health")
        if "grief" in strs:
            stressor_list.append("strs_grief")
    return stressor_list


def parse_client_dict(client, options=[]):
    # options are pass through fields
    locs = parse_neighborhood(client.get("neighborhoods"), LOC_SF)
    client_dict = {loc: 1 for loc in locs}
    client_dict["gender_preference"] = parse_gender_pref(client)
    if client.get("ethnicity") is not None:
        client_dict["ethnicity"] = client.get("ethnicity").lower()
    if client.get("sexual_orientation") is not None:
        client_dict["sexual_orientation"] = client.get("sexual_orientation").lower()
    strs = parse_client_stressors(client)
    for stressor in strs:
        client_dict[stressor] = 1
    client_dict["active"] = client.get("active")
    client_dict["solution"] = client.get("solution")
    client_dict["structure"] = client.get("structure")
    #    client_dict["practical"] = client.get("practical")
    client_dict["self_disclosure"] = client.get("self_disclosure")
    client_dict["birth_year"] = client.get("birth_year")
    client_dict["peer_mentor"] = client.get("peer_mentor").lower()
    if len(options) != 0:
        for field in options:
            if client.get(field):
                client_dict[field] = client.get(field)
    return client_dict


def fetch_client(client_id, engine, table):
    sql_query = "select * from " + table + " where id = " + str(client_id)
    query_result = engine.execute(sql_query)
    df = pd.DataFrame(query_result.fetchall())
    df.columns = query_result.keys()
    client_list = df.to_dict(orient="records")
    return client_list[0]
