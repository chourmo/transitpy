# -*- coding: utf-8 -*-

import random
import pandas as pd


def formattimedelta(td):

    df = td.dt.components[["hours", "minutes"]]
    df["hours"] = df["hours"].map(str).str.zfill(2)
    df["minutes"] = df["minutes"].map(str).str.zfill(2)
    return df.hours.str.cat(df.minutes, ":")


def random_color():
    """
    returns a list of n random colors as html color code (GTFS spec)
    """
    return "#{:06x}".format(random.randint(0, 0xFFFFFF)).upper()


def integer_id(df, id_col, new_col, unique_col=None, keep_cols=None, dtype="Int64"):
    """
    create unique integer keys for each pair in keys/group_key, add a suffix to key names
    id_col : column to make as an id integer, dropped from resulting dataframe
    new_col : column name of the new integer id
    unique_col :None or column name is used to differentiate possible
    duplicates in id_col (e.g. agency_name)
    dtype : dtype to convert new id column to

    returns a tuple of :
        the original Dataframe with id_col replaced by new_col
        a correspondance Dataframe with new_col as index, id_col and keep_cols as values
    """

    cols = [id_col]
    dup_cols = [id_col]
    if keep_cols is not None and type(keep_cols) == list:
        cols.extend(keep_cols)
    if unique_col is not None:
        cols.append(unique_col)
        dup_cols.append(unique_col)

    u_df = df[cols].drop_duplicates(dup_cols).reset_index(drop=True)
    u_df.index = u_df.index.rename(new_col)
    u_df = u_df.reset_index().set_index(id_col)
    u_df[new_col] = u_df[new_col].astype(dtype)

    df2 = pd.merge(df, u_df[[new_col]], left_on=id_col, right_index=True, how="left")
    df2 = df2.drop(columns=id_col)

    u_df = u_df.reset_index().set_index(new_col)

    return df2, u_df


def simple_list(row, length=100):
    """
    return conatenated string of row values without duplicates,
    each row value is truncated so that the string size is
    less than max_size
    """

    strings = [str(x) for x in list(set(row.to_list()))]

    if len(strings) == 1:
        return strings[0].replace(" ", "_")

    strings = sorted(strings)
    nb = sum([len(s) for s in strings]) + len(strings) - 1

    if nb > length:
        s = int((length - len(strings) + 1) // len(strings))
        strings = [x[:s].replace(" ", "_") for x in strings]
    else:
        strings = [x.replace(" ", "_") for x in strings]
    return " ".join(strings)
