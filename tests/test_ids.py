from __future__ import annotations

import unittest
from unittest.mock import patch

from seccloud.ids import new_prefixed_id


class IdTests(unittest.TestCase):
    def test_new_prefixed_id_uses_uuid7_when_available(self) -> None:
        with patch("seccloud.ids.uuid.uuid7", return_value="uuid7-value", create=True):
            self.assertEqual(new_prefixed_id("evt"), "evt_uuid7-value")

    def test_new_prefixed_id_falls_back_to_uuid4_on_python_without_uuid7(self) -> None:
        with patch("seccloud.ids.uuid.uuid7", None, create=True):
            with patch("seccloud.ids.uuid.uuid4", return_value="uuid4-value"):
                self.assertEqual(new_prefixed_id("evt"), "evt_uuid4-value")
