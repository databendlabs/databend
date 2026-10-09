import itertools
import unittest
from unittest.mock import patch

import grpc
import notification_pb2
import simple_server


class AbortContext:
    def abort(self, code, message):
        raise RuntimeError(code, message)


class SendNotificationMockTest(unittest.TestCase):
    def setUp(self):
        self.patches = [
            patch.object(simple_server, "NOTIFICATION_DB", {}),
            patch.object(simple_server, "NOTIFICATION_HISTORY_DB", {}),
            patch.object(
                simple_server, "NOTIFICATION_DELIVERY_IDS", itertools.count(1)
            ),
        ]
        for mock in self.patches:
            mock.start()
            self.addCleanup(mock.stop)
        self.service = simple_server.NotificationService()
        self.context = AbortContext()

    def create_integration(self, name, enabled=True):
        simple_server.NOTIFICATION_DB[name] = notification_pb2.Notification(
            name=name, tenant_id="tenant", enabled=enabled
        )

    def enqueue(self, name, query_id):
        return self.service.EnqueueNotification(
            notification_pb2.EnqueueNotificationRequest(
                tenant_id="tenant", name=name, message="alert", source_id=query_id
            ),
            self.context,
        )

    def test_dropping_an_integration_does_not_overwrite_other_deliveries(self):
        self.create_integration("first")
        self.create_integration("second")
        first = self.enqueue("first", "query-1")
        second = self.enqueue("second", "query-2")
        self.service.DropNotification(
            notification_pb2.DropNotificationRequest(tenant_id="tenant", name="first"),
            self.context,
        )
        third = self.enqueue("second", "query-3")
        self.assertEqual(
            [first.notification_id, second.notification_id, third.notification_id],
            ["1", "2", "3"],
        )
        self.assertEqual(
            set(simple_server.NOTIFICATION_HISTORY_DB), {"delivery:2", "delivery:3"}
        )
        self.assertEqual(
            simple_server.NOTIFICATION_HISTORY_DB["delivery:2"].name, "second"
        )

    def test_missing_and_disabled_integrations_are_not_enqueued(self):
        self.create_integration("disabled", enabled=False)
        for name, code in [
            ("missing", grpc.StatusCode.NOT_FOUND),
            ("disabled", grpc.StatusCode.FAILED_PRECONDITION),
        ]:
            with self.subTest(name=name), self.assertRaises(RuntimeError) as error:
                self.enqueue(name, "query-id")
            self.assertEqual(error.exception.args[0], code)
        self.assertFalse(simple_server.NOTIFICATION_HISTORY_DB)

    def test_missing_query_id_is_rejected_without_writing_history(self):
        self.create_integration("oncall")
        with self.assertRaises(RuntimeError) as error:
            self.enqueue("oncall", "")
        self.assertEqual(error.exception.args[0], grpc.StatusCode.INVALID_ARGUMENT)
        self.assertFalse(simple_server.NOTIFICATION_HISTORY_DB)


if __name__ == "__main__":
    unittest.main()
