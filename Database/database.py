import logging
import os
import sys
import time
import sqlite3 as sl
import pandas as pd
import pandabase

from VC_collections.columns import drop_col_if_exists
from sqlalchemy import create_engine, exists
from tabulate import tabulate
from vpn import check_vpn

sys.path.insert(
    1, r"C:\Users\Yaelg\Google Drive\National_Library\Python\VC_Preprocessing"
)
from VC_collections import AuthorityFiles


##############################################
# ######### DICTIONARIES & Constants #########
##############################################

DATABASE = "sqlite:///VC_CATALOGS.sqlite"

catalog_table_field_mapper = {
    "מגבלות פרטיות": "ACCESSRESTRICT",
    "אוסף פתוח": "ACCURALS",
    "יוצרים נוספים - מוסד": "ADD_CREATOR_CORPS",
    "יוצרים נוספים - איש": "ADD_CREATOR_PERS",
    "תיאור הטיפול באוסף בפרויקט": "APPRAISAL",
    "קוד תיק ארכיון": "ARCHIV_ID",
    "סוג חומר": "ARCHIVAL_MATERIAL",
    "מידע על סידור החומר": "ARRANGEMENT",
    "ברקוד": "BARCODE",
    "ביבליוגרפיה ומקורות מידע": "BIBLIOGRAPHY",
    "היסטוריה ארכיונית": "BIOGHIST",
    "שם הרושם": "CATALOGUER",
    "סוג אוסף": "COLLECTION_TYPE",
    "יוצרים": "COMBINED_CREATORS",
    "יוצרי האוסף": "COLLECTION_CREATOR",
    "יוצרים אישים": "COMBINED_CREATORS_PERS",
    "יוצרים מוסדות": "COMBINED_CREATORS_CORPS",
    "מספר מיכל": "CONTAINER",
    "מילות מפתח - מוסדות": "CORPNAME",
    "בעלים נוכחי": "CURRENT_OWNER",
    "תאריך חופשי": "DATE",
    "תאריך הרישום": "DATE_CATALOGING",
    "תאריך מנורמל מאוחר": "DATE_END",
    "תאריך מנורמל מוקדם": "DATE_START",
    "מסלול דיגיטציה": "DIGITIZATION",
    "מספר קבצים מוערך": "EST_FILES_NUM",
    "היקף החומר": "EXTENT",
    "מילות מפתח_מקומות": "GEOGNAME",
    "יוצר ראשי - מוסד": "FIRST_CREATOR_CORP",
    "יוצר ראשי - איש": "FIRST_CREATOR_PERS",
    "שפה": "LANGUAGE",
    "רמת תיאור": "LEVEL",
    "מדיה + פורמט": "MEDIUM_FORMAT",
    "הערות גלוי למשתמש קצה": "NOTES",
    "הערות לא גלוי למשתמש": "NOTES_HIDDEN",
    "סימול מקורי": "ORIGINAL_ID",
    "מילות מפתח - אישים": "PERSNAME",
    "תאריך יצירת החפץ / הטקסט המקורי מוקדם": "PHOTO_DATE_EARLY",
    "תאריך יצירת החפץ / הטקסט המקורי מאוחר": "PHOTO_DATE_LATE",
    "מיקום פיזי": "PHYSLOC",
    "מדינת הפרסום/הצילום": "PUBLICATION_COUNTRY",
    "חומרים קשורים": "RELATED_MATERIALS",
    "סימול אב": "ROOTID",
    "קנה מידה": "SCALE",
    "תיאור": "SCOPECONTENT",
    "מילות מפתח_נושאים": "SUBJECT",
    "טכניקה": "TECHNIQUE",
    "סריקה דו-צדדית": "TWO_SIDE_SCAN",
    "סוג יוצר ראשי - מוסד": "TYPE_FIRST_CREATOR_CORP",
    "סוג יוצר ראשי - איש": "TYPE_FIRST_CREATOR_PERS",
    "סימול פרויקט": "UNITID",
    "כותרת": "UNITITLE",
    "כותרת אנגלית": "UNITITLE_ENG",
    "מילות מפתח_יצירות": "WORKS",
    "מספר קבצים לאחר דיגיטציה": "NUMBER_OF_FILES",
}

branches = {
    "1": {"name_heb": "אדריכלות", "name_eng": "Architecture"},
    "5": {
        "name_heb": "אדריכלות - מבחר מייצג",
        "name_eng": "Architecture - representative selection",
    },
    "2": {"name_heb": "מחול", "name_eng": "Dance"},
    "6": {
        "name_heb": "מחול - מבחר מייצג",
        "name_eng": "Dance - representative selection",
    },
    "3": {"name_heb": "עיצוב", "name_eng": "Design"},
    "7": {
        "name_heb": "עיצוב - מבחר מייצג",
        "name_eng": "Design - representative selection",
    },
    "4": {"name_heb": "תיאטרון", "name_eng": "Theater"},
    "8": {
        "name_heb": "תיאטרון - מבחר מייצג",
        "name_eng": "Theater - representative selection",
    },
}

branches_fk_mapper = {
    "אדריכלות": "1",
    "אדריכלות - מבחר מייצג": "5",
    "מחול": "2",
    "מחול - מבחר מייצג": "6",
    "עיצוב": "3",
    "עיצוב - מבחר מייצג": "7",
    "תיאטרון": "4",
    "תיאטרון - מבחר מייצג": "8",
}

collection_table_field_mapper = {
    "אשכול": "branch",
    "סימול הארכיון": "collection_id",
    "שם הארכיון": "name_heb",
    "שם הארכיון באנגלית": "name_eng",
    "מיקום הפקדה עבור בעלים נוכחי": "current_owner",
    "קרדיט עברית": "credit_heb",
    "קרדיט אנגלית": "credit_eng",
}


##############################################
# ########         Functions        #########
##############################################


def replace_branch_with_fk(df):
    df["branch"] = df["branch"].map(branches_fk_mapper)
    return df


def create_branch_table(con):
    branch_df = pd.DataFrame(branches, columns=branches.keys())
    branch_df = branch_df.transpose()
    branch_df.index.name = "branch_id"
    logging.info(f"Creating branch table ")
    pandabase.to_sql(branch_df, table_name="branches", con=con, how="upsert")
    logging.info(f"[Success] branch table created! ")


def create_collections_table(con):
    collections_df = AuthorityFiles.Authority_instance.df_credits
    collections_df = collections_df.rename(columns=collection_table_field_mapper)
    cols = [
        col
        for col in list(collections_df.columns)
        if col in collection_table_field_mapper.values()
    ]
    collections_df_final = collections_df[cols]
    collections_df_final.index.name = "collection_id"
    collections_df_final = replace_branch_with_fk(collections_df_final)
    logging.info(f"Creating collections table ")
    pandabase.to_sql(
        collections_df_final, table_name="collections", con=con, how="upsert"
    )
    logging.info(f"[Success] collections table created! ")


def main():

    create_branch_table(con=DATABASE)
    create_collections_table(con=DATABASE)


if __name__ == "__main__":
    main()
