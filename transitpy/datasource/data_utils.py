# -*- coding: utf-8 -*-
import os
import zipfile

import pandas as pd


def expand_json(df, json, subset=None, unstack=False):
    """
    replace a dataframe with the expanded json values
    optionaly only keep subset of the expanded json columns and unstack columns
    """

    if subset is not None and unstack:
        raise ValueError("subset cannot be set when unstack is True")

    # reset index to make a monotically increasing index
    res = df.copy()
    res.index.name = "_ix"
    res = res.reset_index()

    expanded = pd.json_normalize(df[json])
    del res[json]

    if unstack:
        expanded = expanded.unstack().dropna()
        expanded = expanded.reset_index(level=0, drop=True).sort_index()
        expanded = expanded.to_frame(json)

    if subset is not None:
        expanded = expanded[subset]

    res = pd.merge(res, expanded, left_index=True, right_index=True, how="outer")
    res = res.set_index("_ix")
    res.index.name = df.index.name

    return res


def comparable_string(df, drop_characters=["'", "’"]):
    """make string more easily comparable"""

    res = df.str.upper()
    res = res.str.normalize(form="NFC")
    for c in drop_characters:
        res = res.str.replace(c, "")
    return res


def zip_filename(name):

    if name[:-4] != ".zip":
        return name + ".zip"
    else:
        return name