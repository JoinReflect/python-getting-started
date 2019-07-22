import sqlalchemy
import os
import pandas as pd
from collections import namedtuple

TherapistScore = namedtuple("TherapistScore", "tid score")
Matches = namedtuple("Matches", "scores details")

THERAPEUTIC_STYLES = ["active", "solution", "structure", "self_disclosure"]
MAX_PEER_MENTOR_OFFSET = 15


class MatchPriority:
    """ Class to construct match functions.
    TODO: Add in case sensitive/insensitive
    """

    def __init__(self, match_dict, match_type):
        self.fields = {}
        self.match_type = match_type
        for k, v in match_dict.items():
            self.fields[k] = v

    def print_fields(self):
        for k, v in self.fields.items():
            print(k, v)

    def match(self, therapist):
        if self.match_type == "bool_any":
            for k, v in self.fields.items():
                if therapist.get(k) == v:
                    return 1
            return 0
        elif self.match_type == "distance":
            for k, v in self.fields.items():
                score = v - therapist.get(k)
                return min(score, MAX_PEER_MENTOR_OFFSET)
        elif self.match_type == "bool_all":
            for k, v in self.fields.items():
                if therapist.get(k) != v:
                    return 0
            return 1
        elif self.match_type == "bool_none":
            for k, v in self.fields.items():
                print(therapist.get(k), v, k)
                if therapist.get(k) == v:
                    return 0
            return 1
        elif self.match_type == "range_all":
            for k, v in self.fields.items():
                score = therapist.get(k)
                if score < v[0] or score > v[1]:
                    return 0
            return 1
        elif self.match_type == "multiple":
            for k, v in self.fields.items():
                print(k, v, therapist.get(k))
                if v[1] == "match":
                    if therapist.get(k) in v[0]:
                        return 1
                elif v[1] == "not_match":
                    if therapist.get(k) not in v[0]:
                        return 1
            return 0
        elif self.match_type == "closest":
            for k, v in self.fields.items():
                score = abs((therapist.get(k) - v))
                return min(score, MAX_PEER_MENTOR_OFFSET)
        elif self.match_type == "minimum":
            for k, v in self.fields.items():
                if therapist.get(k) < v:
                    return 0
            return 1  # minimum threshold achieved
        elif self.match_type == "maximum":
            for k, v in self.fields.items():
                if therapist.get(k) > v:
                    return 0
            return 1  # under maximum cutoff value


class Client:
    def __init__(self, client_survey, priority_of_variables=None):
        if priority_of_variables is None:
            print("None")
        else:
            print("Not none")


def init_engine(db_key):
    """Connect to Posgtress DB"
    """
    engine = sqlalchemy.create_engine(db_key)
    # meta = sqlalchemy.MetaData(bind=engine)
    # meta.reflect()
    return engine


def table_to_df(engine, tbl):
    """Fetch table from Postgres DB
    """
    select_statement = "select * from " + str(tbl)
    query_result = engine.execute(select_statement)
    df = pd.DataFrame(query_result.fetchall())
    df.columns = query_result.keys()
    return df


def get_therapists(source):
    """Therapist data can come from file, from database, or maybe passed
    to flask app
    """
    if source[0] == "file":
        therapist_df = pd.read_csv(source[1])
    elif source[0] == "db":
        engine = init_engine(os.environ["reflect_db_key"])
        therapist_df = table_to_df(engine, "therapists")
    return therapist_df


def preprocess_client(client):
    """Parse POST message into client data (dictionary)
    status:stub
    """
    return client


def lowercase_all_fields(dictionary):
    for k, v in dictionary.items():
        if isinstance(v, str):
            dictionary[k] = v.lower()
    return dictionary


def preprocess_therapists(therapist_df, source="file"):
    """Filter and convert to a list of dictionaries
    TODO: Keep only useful data fields
    """
    # Drop all therapists that aren't taking 1:1's
    tdf = therapist_df[therapist_df.status.isin([1])]
    therapist_list = tdf.to_dict(orient="records")
    for therapist in therapist_list:
        lowercase_all_fields(therapist)
    return therapist_list


def match_loc_geo(client):
    """Figure out what geographical areas can match the client
    Areas are SF, East Bay, North Bay, South Bay
    TODO:Refactor so these are easily changed
    """
    loc_sf = [
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
    loc_east_bay = ["loc_east"]
    loc_north_bay = ["loc_marin"]
    loc_south_bay = ["loc_peninsula"]
    # south_bay = ['mountain view', 'palo alto', 'san jose', 'san mateo', san matro', south san francisco']
    areas = [loc_sf, loc_east_bay, loc_north_bay, loc_south_bay]
    therapist_areas = []
    for area in areas:
        for loc in area:
            if client.get(loc) == 1:
                therapist_areas.extend(area[:])
                break
    return therapist_areas


def match_location(therapist, locs):
    """Redo as class MatchPriority
    """
    for location in locs:
        if therapist.get(location) == 1:
            return 1
    return 0


def get_match_function(client, item):
    if item[0] == "loc_geo":
        match_dict = {x: 1 for x in match_loc_geo(client)}
        mp = MatchPriority(match_dict, "bool_any")
        return mp.match
    if item[0] == "gender":
        if client.get("gender_preference") == "male":
            return MatchPriority({"gender": "male"}, "bool_all").match
        if client.get("gender_preference") == "female":
            return MatchPriority({"gender": "female"}, "bool_all").match
        return lambda x: 1
    if item[0] == "ethnicity_exact":
        if client.get("ethnicity") == "white":
            return lambda x: 1
        else:
            return MatchPriority(
                {"ethnicity": client.get("ethnicity")}, "bool_all"
            ).match
    if item[0] == "ethnicity_nonwhite":
        if client.get("ethnicity") == "white":
            return lambda x: 1
        else:
            return MatchPriority({"ethnicity": "white"}, "bool_none").match
    if item[0] == "sexual_orientation":
        print(item)
        if client.get("sexual_orientation") == "straight":
            return lambda x: 1
        else:
            return MatchPriority({"sex_orientation": "straight"}, "bool_none").match
        # note the mismatch of sex vs sexual. also applies below to lgbt issues
    if item[0] in THERAPEUTIC_STYLES:
        lower = client.get(item[0]) - item[1]
        upper = client.get(item[0]) + item[1]
        return MatchPriority({item[0]: (lower, upper)}, "range_all").match
    if item[0] == "strs_lgbt":
        if client.get("strs_lgbt") != 1:
            return lambda x: 1
        match_dict = {
            "strs_lgbt": ([1], "match"),
            "gender": (["male", "female"], "not_match"),
            "sex_orientation": (["straight"], "not_match"),
        }  # this will match on therapists who don't provide a gender or sexual orientation.  Note the mismatch of sex vs sexual; also above
        return MatchPriority(match_dict, "multiple").match
    if item[0] == "strs_womens_health":
        if client.get("strs_womens_health") != 1:
            return lambda x: 1
        match_dict = {"strs_womens_health": 1, "gender": "female"}
        return MatchPriority(match_dict, "bool_any").match
    if item[0][:4] == "strs":
        if client.get(item[0]) != 1:
            return lambda x: 1
        return MatchPriority({item[0]: 1}, "bool_all").match
    if item[0] == "peer_mentor":
        if client.get(item[0]) == "peer":
            lower = client.get("birth_year") - item[1]
            upper = client.get("birth_year") + item[1]
            return MatchPriority({"born_year": (lower, upper)}, "range_all").match
        if client.get(item[0]) == "mentor":
            lower = client.get("birth_year") - item[1]
            return MatchPriority({"born_year": lower}, "maximum").match
        return lambda x: 1  # match all if peer/mentor not selected
    if item[0] == "peer":
        match_dict = {"born_year": client.get("birth_year")}  # really?
        match_type = "closest"
        return MatchPriority(match_dict, match_type).match
    if item[0] == "mentor":
        match_dict = {"born_year": client.get("birth_year")}  # really?
        match_type = "distance"
        return MatchPriority(match_dict, match_type).match
    if item[0] == "loc_exact":
        locs = []
        for k,v in client.items():
            if k[:3] == 'loc':
                if v == 1:
                    locs.append(k)
        if len(locs) > 0:
            match_dict = {x:1 for x in locs}
            match_type = "bool_any"
            return MatchPriority(match_dict, match_type).match
            
    print("invalid selection", item)
    return lambda x: 1


def build_match_functions(client, priority):
    match_functions = []
    for tier in priority:
        tier_list = []
        for p in tier:
            tup = (p, get_match_function(client, p))
            tier_list.append(tup)
            # tier_list.append(get_match_function(client, p))
            print(p)
        match_functions.append(tier_list)
    return match_functions


def match(therapists, client, priority_of_variables, flags=None):
    """
    Rank all therapists according to degree of match with client where
    matching on first tier supersedes all later tiers.  On each tier
    all boolean matches are equal.
    Peer/mentor is a distance match.
    """
    from math import ceil
    from math import log

    scores = {x.get("id"): 0 for x in therapists}
    details = {x.get("id"): [] for x in therapists}
    for tier in priority_of_variables:
        multiplier = 2 ** (ceil(log(len(tier))) + 1)
        for k, v in scores.items():
            scores[k] = v * multiplier
        for match_var in tier:
            for t in therapists:
                t_score = match_var[1](t)
                scores[t.get("id")] += t_score
                details[t.get("id")].append((match_var[0], t_score))
    #                scores[t.get("id")] += match_var[1](t)
    return (scores, details)


def match_handler(therapists, client, priority_of_variables, flags=None):
    """Process POST request, build list of matches.
    status: Need to decouple loading therapists from serving matches
    """
    therapist_list = preprocess_therapists(therapists)
    print(therapist_list[0])
    client_data = preprocess_client(client)
    match_functions = build_match_functions(client, priority_of_variables)
    build_ranked_list = match(therapist_list, client_data, match_functions)
    return build_ranked_list


def main(client,prior=[]):
    priority_of_variables = [
        [("loc_geo",)],
        [("gender",)],
        [("ethnicity_exact",)],
        [("ethnicity_nonwhite",)],
        [("sexual_orientation",)],
        [("active", 20)],
        [("solution", 20)],
        [("self_disclosure", 20)],
        [("structure", 20)],
        [("strs_addiction",), ("strs_lgbt",), ("strs_specific",)],
        [
            ("strs_grief",),
            ("strs_body",),
            ("strs_fertility",),
            ("strs_sleep",),
            ("strs_womens_health",),
        ],
        [("peer_mentor", 6)],
        [
            ("strs_family",),
            ("strs_romantic",),
            ("strs_career",),
            ("strs_money",),
            ("strs_self",),
            ("strs_depression",),
            ("strs_anxiety",),
            ("strs_friends",),
            ("strs_dating",),
        ],
        [("loc_exact",)],
    ]
    if len(prior) > 0:
        priority_of_variables = prior[:]
    if client.get("peer_mentor") == "peer":
        priority_of_variables.append([("peer", 6)])
    if client.get("peer_mentor") == "mentor":
        priority_of_variables.append([("mentor", 6)])

    therapist_df = get_therapists(("file", "./data/therapists.07.19.csv"))
    print(client.get("peer_mentor"))
    matches = match_handler(therapist_df, client, priority_of_variables)
    print(matches)
    #    match_list = [(k, v) for k, v in matches.items()]
    match_list = [TherapistScore(k, v) for k, v in matches[0].items()]
    match_list.sort(reverse=True, key=lambda x: x[1])
    #    print(matches[1])
    #    print(match_list)

    return Matches(match_list, matches[1])
