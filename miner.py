from bs4 import BeautifulSoup
import pandas as pd
import os
import json
"""
This is the file that extracts the statistics from SETU data (not provided). You may run this script by uncommenting the gen_database functions.
The process to extract all SETU information can take a few minutes, so be patient! After this, they are serialized for convenient use later down the line.
Credits to Jake Vandenberg for the original script.
"""


def mine_setu_html(filename: str, season: str) -> dict:
    """
    Creates a dictionary of units with their statistics.

    :param filename: str filepath to read from.
    :param save_filename: str filepath to write to.
    :param sem: adds the semester and year in which the unit was done. e.g 2020_S1
    :output: dictionary.

    """

    with open(filename, "r") as f:
        contents = f.read()
        soup = BeautifulSoup(contents, "lxml")

    database = {}

    for article in soup.find_all("article"):
        # invited number first
        base = article.find(
            "div", attrs={"class": "CrossCategoryBlockRow TableContainer"}
        ).find("tbody")

        invited = int(
            base.find("tr", attrs={"class": "CondensedTabularOddRows"}).find("td").text
        )

        responded = int(
            base.find("tr", attrs={"class": "CondensedTabularEvenRows"}).find("td").text
        )

        if responded <= 1:
            continue

        entry = {}
        entry["Responses"] = responded
        entry["Invited"] = invited
        entry["Season"] = season
        entry["Response Rate"] = responded / invited * 100

        # Full unit code
        code = article.find("table").find_all("tr")[3].text.replace("\n", "")
        unit_name = article.find("table").find_all("tr")[4].text.replace("\n", "")

        # Filter out MALAYSIA, COMPOSITE, ALFRED, SAFRICA
        if any(
            location in code
            for location in ["MALAYSIA", "ALFRED", "SAFRICA", "FLEXIBLE"]
        ):
            continue

        entry["unit_name"] = unit_name
        entry["code"] = code
        entry["unit_code"] = code.split("_")[0][1:]
        # Do not display on datatable, used only for queries
        try:
            entry["Level"] = int(entry["unit_code"][3])
        except ValueError:
            entry["Level"] = 0
        scores = []
        # Response categories, retrieve all tables
        for item_num, divs in enumerate(
            article.find_all("div", attrs={"class": "FrequencyBlock_HalfMain"})
        ):

            score_table = divs.find_all("table")[1].tbody.find_all(
                "tr"
            )  # Split by stats and chart

            # Extract the means and medians from their td element
            mean, median = list(map(lambda x: x.find("td").text, score_table))[1:3]

            # Attempt conversion, not sure if this activates...?
            try:
                mean, median = float(mean), float(median)
                entry[f"I{item_num+1}"] = [mean, median]
                scores.append([mean, median])
            except ValueError:
                print(f"score could not be converted: {code}, {mean}, {median}")

        entry["agg_score"] = [
            round(sum(map(lambda item: item[measure], scores)) / len(scores), 2)
            for measure in range(2)
        ]

        database[code] = entry

    df = pd.DataFrame(database).T.fillna(0)
    df = df.reset_index(drop=True)

    columns_to_update = ["I9", "I10", "I11", "I12", "I13"]
    df[columns_to_update] = df[columns_to_update].applymap(
        lambda x: [0, 0] if x == 0 else x
    )

    return df


if __name__ == "__main__":
    output_dir = "output"
    os.makedirs(output_dir, exist_ok=True)

    dfs = []

    # Loop through all HTML files in the 'conversion' folder
    conversion_folder = "conversion"
    for filename in os.listdir(conversion_folder):
        if filename.endswith(".html"):
            # Extract the season (YEAR_PERIOD) from the filename
            season = filename.split("_")[0] + "_" + filename.split("_")[1]

            # Call mine_setu_html for the current file and season
            data = mine_setu_html(
                filename=os.path.join(conversion_folder, filename), season=season
            )
            json_filename = f'data_{season}.json'
            json_path = os.path.join(output_dir, json_filename)
            
            with open(json_path, 'w') as f:
                json.dump(data, f)  # Assuming data is a list directly

            # Append the DataFrame to the list
            dfs.append(data)

            print(f"Processed {filename}")

    # Concatenate all the DataFrames in the list into a single DataFrame
    final_df = pd.concat(dfs, ignore_index=True)

    # Save the concatenated DataFrame to a JSON file
    final_json_path = os.path.join(output_dir, "data.json")
    final_df.to_json(final_json_path, orient="records", lines=False)
