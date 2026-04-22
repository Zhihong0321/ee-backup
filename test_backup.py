import os
import unittest
from unittest.mock import patch

from app.backup import (
    get_config,
    get_latest_backup_name,
    get_playtest_db_info,
    get_playtest_restore_blocker,
    same_database_target,
)


class BackupSafetyTests(unittest.TestCase):
    def test_get_config_prefers_playtest_alias(self):
        with patch.dict(
            os.environ,
            {
                "PLAYTEST_DATABASE": "postgresql://alias-user:secret@db.example.com/playtest",
            },
            clear=True,
        ):
            config = get_config()

        self.assertEqual(
            config["PLAYTEST_DATABASE_URL"],
            "postgresql://alias-user:secret@db.example.com/playtest",
        )

    def test_same_database_target_normalizes_default_postgres_port(self):
        self.assertTrue(
            same_database_target(
                "postgresql://user:secret@db.example.com/app_db",
                "postgresql://other:secret@db.example.com:5432/app_db",
            )
        )

    @patch("app.backup.list_backups")
    def test_get_latest_backup_name_uses_first_sorted_backup(self, mock_list_backups):
        mock_list_backups.return_value = [
            {"filename": "backup_20260422_120000.sql"},
            {"filename": "backup_20260421_120000.sql"},
        ]

        self.assertEqual(get_latest_backup_name(), "backup_20260422_120000.sql")

    def test_get_playtest_restore_blocker_reports_same_database(self):
        config = {
            "DATABASE_URL": "postgresql://prod:secret@db.example.com:5432/app_db",
            "PLAYTEST_DATABASE_URL": "postgresql://play:secret@db.example.com/app_db",
        }

        with patch.dict(os.environ, {"ENABLE_PLAYTEST_RESTORE": "true"}, clear=False):
            blocker = get_playtest_restore_blocker(config)

        self.assertEqual(
            blocker,
            "Playtest target matches the source database. Restore is blocked for safety.",
        )

    def test_get_playtest_db_info_without_target_is_not_configured(self):
        with patch.dict(os.environ, {}, clear=True):
            info = get_playtest_db_info()

        self.assertFalse(info["configured"])
        self.assertEqual(info["status"], "Not configured")


if __name__ == "__main__":
    unittest.main()
