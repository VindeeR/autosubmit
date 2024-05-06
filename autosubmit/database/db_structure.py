#!/usr/bin/env python3

# Copyright 2015-2020 Earth Sciences Department, BSC-CNS

# This file is part of Autosubmit.

# Autosubmit is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.

# Autosubmit is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.

# You should have received a copy of the GNU General Public License
# along with Autosubmit.  If not, see <http://www.gnu.org/licenses/>.

import os
import traceback
from typing import Dict, List
from networkx import DiGraph
from autosubmit.database import tables
from autosubmitconfigparser.config.basicconfig import BasicConfig
from autosubmit.database.db_manager import create_db_table_manager
from log.log import Log


def get_structure(expid: str, structures_path: str) -> Dict[str, List[str]]:
    """
    Creates file of database and table of experiment structure if it does not exist. Returns current structure.

    :return: Map from experiment name source to name destination
    """
    try:
        db_structure_path = ""
        if BasicConfig.DATABASE_BACKEND == "sqlite":
            if not os.path.exists(structures_path):
                raise Exception("Structures folder not found " + str(structures_path))

            db_structure_path = os.path.join(
                structures_path, "structure_" + expid + ".db"
            )
            if not os.path.exists(db_structure_path):
                with open(db_structure_path, "w"):
                    pass

        structure_manager = create_db_table_manager(
            tables.ExperimentStructureTable, db_filepath=db_structure_path, schema=expid
        )
        with structure_manager.get_connection() as conn:
            structure_manager.create_table(conn)
            rows = structure_manager.select_all(conn)
            current_table = [(r.e_from, r.e_to) for r in rows]
            conn.commit()

        current_table_structure: Dict[str, List[str]] = dict()
        for item in current_table:
            _from, _to = item
            current_table_structure.setdefault(_from, []).append(_to)
            current_table_structure.setdefault(_to, [])
        return current_table_structure

    except Exception as exc:
        Log.printlog("Get structure error: {0}".format(str(exc)), 6014)
        Log.debug(traceback.format_exc())
        return {}


def save_structure(graph: DiGraph, expid: str, structures_path: str):
    """
    Saves structure if path is valid
    """
    try:
        # Prepare data
        nodes_edges = {u for u, v in graph.edges()}
        nodes_edges.update({v for u, v in graph.edges()})
        independent_nodes = {u for u in graph.nodes() if u not in nodes_edges}
        data = {(u, v) for u, v in graph.edges()}
        data.update({(u, u) for u in independent_nodes})

        # Ensure db file exists for SQLite
        db_structure_path = ""
        if BasicConfig.DATABASE_BACKEND == "sqlite":
            if not os.path.exists(structures_path):
                raise Exception("Structures folder not found " + str(structures_path))

            db_structure_path = os.path.join(
                structures_path, "structure_" + expid + ".db"
            )
            if not os.path.exists(db_structure_path):
                with open(db_structure_path, "w"):
                    pass

        structure_manager = create_db_table_manager(
            tables.ExperimentStructureTable, db_filepath=db_structure_path, schema=expid
        )
        with structure_manager.get_connection() as conn:
            structure_manager.create_table(conn)
            structure_manager.delete_all(conn)
            edges = [{"e_from": e[0], "e_to": e[1]} for e in data]
            structure_manager.insert_many(conn, edges)
            conn.commit()

    except Exception as exc:
        Log.printlog("Save structure error: {0}".format(str(exc)), 6014)
        Log.debug(traceback.format_exc())
