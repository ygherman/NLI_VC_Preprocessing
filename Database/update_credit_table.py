import logging
import os
import sys
import time

import pandas as pd
import pandabase
from VC_collections.columns import drop_col_if_exists
from sqlalchemy import create_engine, exists

from vpn import check_vpn

sys.path.insert(
    1, r"C:\Users\Yaelg\Google Drive\National_Library\Python\VC_Preprocessing"
)
from VC_collections import AuthorityFiles

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
branches = ["Architect", "Dance", "Design", "Theater"]


collection_table_field_mapper = {
    "אשכול": "branch",
    "סימול הארכיון": "collection_id",
    "שם הארכיון": "name_heb",
    "שם הארכיון באנגלית": "name_eng",
    "מיקום הפקדה עבור בעלים נוכחי": "current_owner",
    "קרדיט עברית": "credit_heb",
    "קרדיט אנגלית": "credit_eng",
}


def replace_branch_with_fk(df):
    df["branch"] = df["branch"].map(branches_fk_mapper)
    return df


def main():
    pingstatus = check_vpn()
    engine1 = create_engine(
        r"sqlite:///\\172.0.12.30\Visual_Art\Master_Catalog\NLI_VC_DB.db", echo=True
    )

    engine = create_engine(r"sqlite:///VC_CATALOGS.sqlite", echo=True)
    df = AuthorityFiles.Authority_instance.df_credits
    df = df.rename(columns=collection_table_field_mapper)
    cols = [
        col for col in list(df.columns) if col in collection_table_field_mapper.values()
    ]
    df_final = df[cols]
    df_final.index.name = "collection_id"

    df_final = replace_branch_with_fk(df_final)

    print(df_final.columns)

    df_final.to_sql("collections ", con=engine, if_exists="replace")


if __name__ == "__main__":
    main()
