from flask import Flask

app = Flask(__name__)

from matching import *
from client_parsing import *
from flask import request


@app.route("/")
def hello_world():
    req = request.args.get("query")
    print(req)
    client_dict = fetch_client(req, engine, "members")
    client = parse_client_dict(client_dict)
    matches = match_handler(therapist_df, client, priority_of_variables)
    #    print(matches[1])
    match_list = [(k, v) for k, v in matches[0].items()]
    match_list.sort(reverse=True, key=lambda x: x[1])
    print(match_list)

    return matches


engine = init_engine(os.environ["reflect_db_key"])
therapist_df = get_therapists(("file", "./data/therapists.07.15.csv"))
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
    [("loc_nearby",)],
    [("mentor", 6)],
]
