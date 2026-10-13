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
            patch.object(simple_server, "NOTIFICATION_HISTORY_FIXTURES", {}),
            patch.object(
                simple_server, "NOTIFICATION_DELIVERY_IDS", itertools.count(1)
            ),
        ]
        for mock in self.patches:
            mock.start()
            self.addCleanup(mock.stop)
        self.service = simple_server.NotificationService()
        self.context = AbortContext()

    def create_integration(self, name, enabled=True, tenant_id="tenant"):
        response = self.service.CreateNotification(
            notification_pb2.CreateNotificationRequest(
                tenant_id=tenant_id, name=name, enabled=enabled
            ),
            self.context,
        )
        self.assertFalse(response.HasField("error"))

    def enqueue(self, name, query_id, tenant_id="tenant"):
        return self.service.EnqueueNotification(
            notification_pb2.EnqueueNotificationRequest(
                tenant_id=tenant_id, name=name, message="alert", query_id=query_id
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
            [first.delivery_id, second.delivery_id, third.delivery_id],
            ["1", "2", "3"],
        )
        self.assertEqual(
            set(simple_server.NOTIFICATION_HISTORY_DB["tenant"]),
            {"delivery:2", "delivery:3"},
        )
        self.assertEqual(
            simple_server.NOTIFICATION_HISTORY_DB["tenant"]["delivery:2"].name,
            "second",
        )

    def test_history_preserves_and_filters_query_id(self):
        self.create_integration("oncall")
        self.enqueue("oncall", "query-1")
        self.enqueue("oncall", "query-2")
        for query_id in ["query-1", "query-2", "missing"]:
            with self.subTest(query_id=query_id):
                response = self.service.ListNotificationHistory(
                    notification_pb2.ListNotificationHistoryRequest(
                        tenant_id="tenant", query_id=query_id
                    ),
                    self.context,
                )
                if query_id == "missing":
                    self.assertFalse(response.notification_histories)
                else:
                    self.assertEqual(len(response.notification_histories), 1)
                    self.assertEqual(
                        response.notification_histories[0].query_id, query_id
                    )

    def test_generated_history_is_tenant_scoped(self):
        self.create_integration("oncall")
        self.enqueue("oncall", "query-id")
        response = self.service.ListNotificationHistory(
            notification_pb2.ListNotificationHistoryRequest(tenant_id="other-tenant"),
            self.context,
        )
        self.assertFalse(response.notification_histories)

    def test_shared_fixtures_survive_tenant_history_cleanup(self):
        fixture = notification_pb2.NotificationHistory(
            name="oncall", message_source="webhook"
        )
        simple_server.NOTIFICATION_HISTORY_FIXTURES[fixture.name] = fixture
        self.create_integration("oncall")
        self.enqueue("oncall", "query-id")
        for tenant_id, sources in [
            ("tenant", ["webhook", "USER"]),
            ("other-tenant", ["webhook"]),
        ]:
            response = self.service.ListNotificationHistory(
                notification_pb2.ListNotificationHistoryRequest(tenant_id=tenant_id),
                self.context,
            )
            self.assertEqual(
                [history.message_source for history in response.notification_histories],
                sources,
            )
        self.service.DropNotification(
            notification_pb2.DropNotificationRequest(tenant_id="tenant", name="oncall"),
            self.context,
        )
        response = self.service.ListNotificationHistory(
            notification_pb2.ListNotificationHistoryRequest(tenant_id="tenant"),
            self.context,
        )
        self.assertEqual(list(response.notification_histories), [fixture])

    def test_same_name_integrations_and_history_are_isolated_between_tenants(self):
        self.create_integration("oncall", enabled=False)
        self.create_integration("oncall", tenant_id="other-tenant")
        with self.assertRaises(RuntimeError) as error:
            self.enqueue("oncall", "query-id")
        self.assertEqual(error.exception.args[0], grpc.StatusCode.FAILED_PRECONDITION)
        self.enqueue("oncall", "query-id", tenant_id="other-tenant")
        self.service.AlterNotification(
            notification_pb2.AlterNotificationRequest(
                tenant_id="tenant", name="oncall", operation_type="SET", enabled=True
            ),
            self.context,
        )
        self.enqueue("oncall", "query-id")
        for tenant_id in ["tenant", "other-tenant"]:
            response = self.service.GetNotification(
                notification_pb2.GetNotificationRequest(
                    tenant_id=tenant_id, name="oncall"
                ),
                self.context,
            )
            self.assertEqual(response.notification.tenant_id, tenant_id)
            response = self.service.ListNotification(
                notification_pb2.ListNotificationRequest(tenant_id=tenant_id),
                self.context,
            )
            self.assertEqual([n.tenant_id for n in response.notifications], [tenant_id])
            response = self.service.ListNotificationHistory(
                notification_pb2.ListNotificationHistoryRequest(
                    tenant_id=tenant_id, query_id="query-id"
                ),
                self.context,
            )
            self.assertEqual(len(response.notification_histories), 1)
        self.service.DropNotification(
            notification_pb2.DropNotificationRequest(tenant_id="tenant", name="oncall"),
            self.context,
        )
        response = self.service.GetNotification(
            notification_pb2.GetNotificationRequest(
                tenant_id="other-tenant", name="oncall"
            ),
            self.context,
        )
        self.assertFalse(response.HasField("error"))
        response = self.service.ListNotificationHistory(
            notification_pb2.ListNotificationHistoryRequest(tenant_id="other-tenant"),
            self.context,
        )
        self.assertEqual(len(response.notification_histories), 1)

    def test_history_filters_by_integration_before_applying_limit(self):
        self.create_integration("other")
        self.create_integration("oncall")
        self.enqueue("other", "query-id")
        self.enqueue("oncall", "query-id")
        response = self.service.ListNotificationHistory(
            notification_pb2.ListNotificationHistoryRequest(
                tenant_id="tenant",
                notification_name="oncall",
                query_id="query-id",
                result_limit=1,
            ),
            self.context,
        )
        self.assertEqual(len(response.notification_histories), 1)
        self.assertEqual(response.notification_histories[0].name, "oncall")

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

    def test_query_id_is_optional_and_preserves_presence(self):
        self.create_integration("oncall")
        for query_id in [None, "", "query-id"]:
            with self.subTest(query_id=query_id):
                response = self.enqueue("oncall", query_id)
                history = simple_server.NOTIFICATION_HISTORY_DB["tenant"][
                    "delivery:" + response.delivery_id
                ]
                self.assertEqual(history.HasField("query_id"), query_id is not None)
                self.assertEqual(history.query_id, query_id or "")
        unfiltered = self.service.ListNotificationHistory(
            notification_pb2.ListNotificationHistoryRequest(tenant_id="tenant"),
            self.context,
        )
        self.assertEqual(len(unfiltered.notification_histories), 3)
        empty_id = self.service.ListNotificationHistory(
            notification_pb2.ListNotificationHistoryRequest(
                tenant_id="tenant", query_id=""
            ),
            self.context,
        )
        self.assertEqual(len(empty_id.notification_histories), 1)
        self.assertTrue(empty_id.notification_histories[0].HasField("query_id"))
        self.assertEqual(empty_id.notification_histories[0].query_id, "")


if __name__ == "__main__":
    unittest.main()
