import sys, glob, os
from pathlib import Path
from sqlalchemy import create_engine
import logging
import pandabase
import logging
import json
import time
import argparse

# sys.path.insert(
#     1, r"C:\Users\Yaelg\Google Drive\National_Library\Python\VC_Preprocessing"
# )
from VC_collections import Collection

# from ..google_drive_api.google_drive_connection import *

column_mapping = {
    "סימול": "unitid",
    "סימול אב": "rootid",
    "ברקוד": "barcode",
    "סימול מקורי": "original_id",
    "רמת תיאור": "level",
    "מספר מיכל": "container",
    "קוד תיק ארכיון": "archiv_id",
    "כותרת": "unititle",
    "כותרת אנגלית": "unititle_eng",
    "תיאור": "scopecontent",
    "תאריך חופשי": "date",
    "תאריך מנורמל מוקדם": "date_start",
    "תאריך מנורמל מאוחר": "date_end",
    "תאריך יצירת החפץ / הטקסט המקורי מוקדם": "photo_date_early",
    "תאריך יצירת החפץ / הטקסט המקורי מאוחר": "photo_date_late",
    "יוצרים": "combined_creators",
    "יוצרים אישים": "combined_creators_pers",
    "יוצרים מוסדות": "combined_creators_corps",
    "מילות מפתח - אישים": "persname",
    "מילות מפתח - מוסדות": "corpname",
    "מילות מפתח_יצירות": "works",
    "מילות מפתח_נושאים": "subject",
    "סוג חומר": "archival_material",
    "מדיה + פורמט": "medium_format",
    "קנה מידה": "scale",
    "טכניקה": "technique",
    "מדינת הפרסום/הצילום": "publication_country",
    "מילות מפתח_מקומות": "geogname",
    "מגבלות פרטיות": "accessrestrict",
    "מסלול דיגיטציה": "digitization",
    "סריקה דו-צדדית": "two_side_scan",
    "מספר קבצים מוערך": "est_files_num",
    "מספר קבצים לאחר דיגיטציה": "actual_files_num",
    "שפה": "language",
    "היקף החומר": "extent",
    "משך": "duration",
    "הערות גלוי למשתמש קצה": "notes",
    "הערות לא גלוי למשתמש": "notes_hidden",
    "שם הרושם": "cataloguer",
    "תאריך הרישום": "date_cataloging",
    "בעלים נוכחי": "current_owner",
    "היסטוריה ארכיונית": "bioghist",
    "תיאור הטיפול באוסף בפרויקט": "appraisal",
    "סוג אוסף": "collection_type",
    "אוסף פתוח": "accurals",
    "ביבליוגרפיה ומקורות מידע": "bibliography",
    "מיקום פיזי": "physloc",
    "חומרים קשורים": "related_materials",
}
branches = ["Architect", "Dance", "Design", "Theater"]


def get_all_collections(branches: dict, base_path) -> dict:
    logging.info("getting all collections in all branches")
    collections = dict()
    for br in branches:
        collections[br] = list()

    for branch in branches:
        for file in glob.glob(os.path.join(base_path, "VC-" + branch) + "/*"):
            if "." in os.path.basename(file):
                continue
            collections[branch].append(os.path.basename(file))
    return collections


def prepare_table_to_insert(df, collection_id):
    logging.info(f"Preparing {collection_id} to be inserted into DB")

    df = df.rename(columns=column_mapping).set_index("")
    df.index.name = "mms_id"
    df["collection"] = collection_id
    return df


def create_table_for_db(conf_file, client):
    print([f"Reading conf file: {conf_file}"])
    with open(conf_file, mode="r") as f:
        conf = json.load(f)
        file_id = conf["google_sheet_file_id"]

        all_dfs = Collection.create_xl_from_gspread(client, file_id)
    return all_dfs["קטלוג סופי"]


def configure_parser():
    my_parser = argparse.ArgumentParser(description="add table as bath")
    # Add the arguments
    my_parser.add_argument(
        "-batch",
        type=str,
        help="Specify whether this is a batch update of a batch of catalogs to DB, or a one",
    )
    my_parser.add_argument(
        "-start ",
        type=str,
        help="Specify whether with which collection to start the batch, optional",
    )

    return my_parser


def main():

    my_parser = configure_parser()
    args = my_parser.parse_args()

    client = Collection.connect_to_google_drive()
    count = 0
    base_path = r"C:\Users\Yaelg\Google Drive\National_Library\Python"
    collections = get_all_collections(branches, base_path)
    DATABASE = "sqlite:///VC_CATALOGS.sqlite"
    if args.batch:
        for branch in branches:

            for collection_id in collections[branch]:
                print([f"Starting with {branch} / {collection_id}"])

                conf_file = (
                    Path(base_path)
                    / ("VC-" + branch)
                    / collection_id
                    / "Data"
                    / "reports"
                    / (collection_id + "_metadata.conf")
                )

                if not os.path.exists(conf_file):
                    continue

                print([f"Creating table for {branch} / {collection_id}"])
                df = create_table_for_db(conf_file, client)

                df = prepare_table_to_insert(df, collection_id)
                print([f"Inserting table into Database"])
                try:
                    pandabase.to_sql(
                        df, table_name="records", con=DATABASE, how="upsert"
                    )
                    count += 1
                except:
                    sys.stderr.write(
                        f"Problem with {collection_id}, table was not comminted in the DB"
                    )
                    continue
                print(f"commited {branch}/{collection_id}  into DB")
                count += 1
                time.sleep(100)
                print(f"Collention number: {count}")
        print(f"total of {count} collections were updated into the database")
    else:
        collection = Collection.retrieve_collection()
        df = prepare_table_to_insert(collection, collection.collection_id)
        print([f"Inserting table into Database"])
        try:
            pandabase.to_sql(df, table_name="records", con=DATABASE, how="upsert")
            count += 1
        except:
            sys.stderr.write(
                f"Problem with {collection_id}, table was not comminted in the DB"
            )
        print(f"commited {branch}/{collection_id}  into DB")

        print(f"total of {count} collections were updated into the database")


if __name__ == "__main__":

    main()

    # while True:
    #     main()
    #     batch = input("Enter another catalog to DB? (Y/N) ")
    #     if batch.strip().lower() != "y":
    #         sys.stdout.write("Ending run!")
    #         sys.exit()
    # main()
