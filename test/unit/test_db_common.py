import locale
import os
import tempfile
from autosubmitconfigparser.config.basicconfig import BasicConfig
from pkg_resources import resource_string
import pytest
from autosubmit.database import db_common


class TestDbCommon:
    def test_sqlite(self, fixture_sqlite: BasicConfig, monkeypatch: pytest.MonkeyPatch):
        with tempfile.TemporaryDirectory() as tmpdirname:
            db_path = os.path.join(tmpdirname, "autosubmit.db")
            monkeypatch.setattr(BasicConfig, "DB_PATH", db_path)

            # Test creation
            qry = resource_string("autosubmit.database", "data/autosubmit.sql").decode(
                locale.getlocale()[1]
            )
            db_common.create_db(qry)
            assert os.path.exists(db_path)

            # Test last name used
            assert "empty" == db_common._last_name_used()
            assert "empty" == db_common._last_name_used(test=True)
            assert "empty" == db_common._last_name_used(operational=True)

            new_exp = {
                "name": "a000",
                "description": "Description",
                "autosubmit_version": "4.0.0",
            }

            # Experiment doesn't exist yet
            with pytest.raises(Exception):
                db_common._check_experiment_exists(new_exp["name"])

            # Test save
            db_common._save_experiment(
                new_exp["name"], new_exp["description"], new_exp["autosubmit_version"]
            )
            assert db_common._check_experiment_exists(
                new_exp["name"], error_on_inexistence=False
            )
            assert db_common._last_name_used() == new_exp["name"]

            # Get version
            assert (
                db_common._get_autosubmit_version(new_exp["name"])
                == new_exp["autosubmit_version"]
            )
            new_version = "v4.1.0"
            db_common._update_experiment_descrip_version(
                new_exp["name"], version=new_version
            )
            assert db_common._get_autosubmit_version(new_exp["name"]) == new_version

            # Update description
            assert (
                db_common.get_experiment_descrip(new_exp["name"])[0][0]
                == new_exp["description"]
            )
            new_desc = "New Description"
            db_common._update_experiment_descrip_version(
                new_exp["name"], description=new_desc
            )
            assert db_common.get_experiment_descrip(new_exp["name"])[0][0] == new_desc

            # Update back both: description and version
            db_common._update_experiment_descrip_version(
                new_exp["name"],
                description=new_exp["description"],
                version=new_exp["autosubmit_version"],
            )
            assert (
                db_common.get_experiment_descrip(new_exp["name"])[0][0]
                == new_exp["description"]
                and db_common._get_autosubmit_version(new_exp["name"])
                == new_exp["autosubmit_version"]
            )

            # Delete experiment
            assert db_common._delete_experiment(new_exp["name"])
            with pytest.raises(Exception):
                db_common._get_autosubmit_version(new_exp["name"]) == new_exp[
                    "autosubmit_version"
                ]

    def test_postgres(self, fixture_postgres: BasicConfig):
        assert db_common.create_db_pg()

        # Test last name used
        assert "empty" == db_common._last_name_used()
        assert "empty" == db_common._last_name_used(test=True)
        assert "empty" == db_common._last_name_used(operational=True)

        new_exp = {
            "name": "a700",
            "description": "Description",
            "autosubmit_version": "4.0.0",
        }

        # Experiment doesn't exist yet
        with pytest.raises(Exception):
            db_common._check_experiment_exists(new_exp["name"])

        # Test save
        db_common._save_experiment(
            new_exp["name"], new_exp["description"], new_exp["autosubmit_version"]
        )
        assert db_common._check_experiment_exists(
            new_exp["name"], error_on_inexistence=False
        )
        assert db_common._last_name_used() == new_exp["name"]

        # Get version
        assert (
            db_common._get_autosubmit_version(new_exp["name"])
            == new_exp["autosubmit_version"]
        )
        new_version = "v4.1.0"
        db_common._update_experiment_descrip_version(
            new_exp["name"], version=new_version
        )
        assert db_common._get_autosubmit_version(new_exp["name"]) == new_version

        # Update description
        assert (
            db_common.get_experiment_descrip(new_exp["name"])[0][0]
            == new_exp["description"]
        )
        new_desc = "New Description"
        db_common._update_experiment_descrip_version(
            new_exp["name"], description=new_desc
        )
        assert db_common.get_experiment_descrip(new_exp["name"])[0][0] == new_desc

        # Update back both: description and version
        db_common._update_experiment_descrip_version(
            new_exp["name"],
            description=new_exp["description"],
            version=new_exp["autosubmit_version"],
        )
        assert (
            db_common.get_experiment_descrip(new_exp["name"])[0][0]
            == new_exp["description"]
            and db_common._get_autosubmit_version(new_exp["name"])
            == new_exp["autosubmit_version"]
        )

        # Delete experiment
        assert db_common._delete_experiment(new_exp["name"])
        with pytest.raises(Exception):
            db_common._get_autosubmit_version(new_exp["name"]) == new_exp[
                "autosubmit_version"
            ]
