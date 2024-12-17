import json
import os
import re
import pandas as pd
from bs4 import BeautifulSoup

"""
This is the file that extracts the statistics from SETU data (not provided). You may run this script by uncommenting the gen_database functions.
The process to extract all SETU information can take a few minutes, so be patient! After this, they are serialized for convenient use later down the line.
Credits to Jake Vandenberg for the original script.
"""


def mine_setu_html(filename: str, season: str, **qwargs) -> dict:
    """
    Args:
    -----
        filename (str): Filepath to read from.
        season (str): The season in which the unit was done.

    Optional Args:
    --------------
        results_as_dict (bool): Flag to return the results as a dictionary.  
        Default is False indicators are given as a list.
        include_labels (bool): Flag to include the indicators (labels). Default is False.
        use_labels (bool): Flag to use the labels as the key instead of the indicator number. Default is False.
        exclude_faculty_items: Flag to exclude faculty items. Default is False.
    Returns:
    --------
        dict: DataFrame with the extracted data.
    """

    results_as_dict = qwargs.get("results_as_dict", False) 
    include_labels = qwargs.get("include_labels", False)
    use_labels = qwargs.get("use_labels", False)
    exclude_faculty_items = qwargs.get("exclude_faculty_items", False)
    # [float, float, str] seems too weird, so enforcing this. Feel free to remove
    if not results_as_dict and include_labels:
        raise ValueError("include_labels can only be set to True if results_as_dict is True.")
    
    INDICATOR_PREFIX = "INDICATOR_"
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
        code = article.find("table").find_all("tr")[3].text.replace("\n", "").strip()
        unit_name = article.find("table").find_all("tr")[4].text.replace("\n", "").strip()

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
        university_items = {}
        faculty_items = {}
        entry_num = 1
        for item_num, divs in enumerate(
            article.find_all("div", attrs={"class": "FrequencyBlock_HalfMain"})
        ):
            score_table = divs.find_all("table")[1].tbody.find_all(
                "tr"
            )  # Split by stats and chart
            
            try:
                caption = divs.find_all("table")[0].caption.text
            except AttributeError:
                caption = "Unknown"
                
            # Remove the preambles from the caption
            match = re.search(r"Table for (.+?)-\d+\.\s*(.+)", caption)
            if match:
                caption = match.group(2)
            else:
                caption = "Unknown"
            
                # entry[f"{INDICATOR_PREFIX}{item_num+1}_label"] = match.group(1)
            # Extract the means and medians from their td element
            temp = list(map(lambda x: x.find("td").text, score_table))[0:3]
            mean, median = temp[1], temp[2]
            
            # Attempt conversion, not sure if this activates...?
            try:
                mean, median = float(mean), float(median)
            except ValueError:
                print(f"score could not be converted: {code}, mean: {mean} ({type(mean)}), median: {median} ({type(median)})")
            finally:
                entry_key = caption if use_labels else f"{INDICATOR_PREFIX}{item_num+1}"
                
                if results_as_dict:
                    entry_data = {
                        "mean": mean,
                        "median": median,
                        "indicator": caption,
                    }
                    if not include_labels:
                        del entry_data["indicator"]
                    if pd.isna(mean) or pd.isna(median) or pd.isna(caption):
                        entry_data = {"mean": -1, "median": -1, "indicator": "error"}   
                    # entry[entry_key] = entry_data
                    
                else:
                    entry_data = [mean, median]
                    
                scores.append([mean, median])
                
                if entry_num <= 8:
                    university_items[entry_key] = entry_data
                else:
                    faculty_items[entry_key] = entry_data
                entry_num += 1
                if exclude_faculty_items and entry_num > 8:
                    break
                
        entry["university_items"] = university_items
        if not exclude_faculty_items:
            entry["faculty_items"] = faculty_items       
        entry["agg_score"] = [
            round(sum(map(lambda item: item[measure], scores)) / len(scores), 2)
            for measure in range(2)
        ]

        database[code] = entry

    df = pd.DataFrame(database).T.infer_objects(copy=False)
    df = df.reset_index(drop=True)

    columns_to_update = [col for col in df.columns if col.startswith(INDICATOR_PREFIX)]
    df[columns_to_update] = df[columns_to_update].map(
        lambda x: [-1, -1] if not isinstance(x, list) and pd.isna(x) else x
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
                filename=os.path.join(conversion_folder, filename), season=season, results_as_dict=True, use_labels=True, exclude_faculty_items=True
            )
            json_filename = f'data_{season}.json'
            json_path = os.path.join(output_dir, json_filename)
            # TODO: come back to this :^)
            if isinstance(data, pd.DataFrame):
                data2 = data.to_dict(orient="records")

            with open(json_path, 'w') as f:
                f.write(json.dumps(data2, indent=4))

            # Append the DataFrame to the list
            dfs.append(data)

            print(f"Processed {filename}")

    # Concatenate all the DataFrames in the list into a single DataFrame
    final_df = pd.concat(dfs, ignore_index=True)

    # Save the concatenated DataFrame to a JSON file
    final_json_path = os.path.join(output_dir, "data.json")
    final_df.to_json(final_json_path, orient="records", lines=False)
