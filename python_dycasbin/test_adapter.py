import boto3
import casbin
from moto import mock_dynamodb2
import pytest
import botocore.exceptions

table_name = "casbin_rule"
endpoint_url = "http://localhost:8000"
policy_line = "mock,policy,line"


@mock_dynamodb2
@pytest.mark.parametrize("create_table", [True, False])
def test_init(create_table):
    from python_dycasbin import adapter

    dynamodb = boto3.resource("dynamodb")

    if create_table:
        adapter.Adapter()

        table = dynamodb.Table(table_name)
        assert table.table_name == table_name
        assert table.attribute_definitions == [
            {"AttributeName": "id", "AttributeType": "S"}
        ]
        assert table.key_schema == [{"AttributeName": "id", "KeyType": "HASH"}]
    else:
        obj = adapter.Adapter(create_table=False)
        assert obj.table_name == table_name

        table = dynamodb.Table(table_name)
        with pytest.raises(
            botocore.exceptions.ClientError, match=r".*ResourceNotFoundException.*"
        ):
            assert table.table_status == "FOO"


@mock_dynamodb2
def test_load_policy(mocker, monkeypatch):
    from casbin.model.model import Model
    from python_dycasbin import adapter

    model = Model()
    mocker.patch("boto3.client")
    mocker.patch("casbin.persist.load_policy_line")
    monkeypatch.setattr(adapter.Adapter, "get_line_from_item", mock_get_line_from_item)

    boto3.client.return_value.scan.return_value = {"Items": [{}]}

    obj = adapter.Adapter()
    obj.load_policy(model)

    boto3.client.return_value.scan.assert_called_with(TableName=table_name)
    casbin.persist.load_policy_line.assert_called_with(policy_line, model)


def test_load_polic_with_LastEvaluatedKey(mocker, monkeypatch):
    from casbin.model.model import Model
    from python_dycasbin import adapter

    last_evaluated_key = "from_pytest"
    model = Model()
    mocker.patch("boto3.client")
    mocker.patch("casbin.persist.load_policy_line")
    monkeypatch.setattr(adapter.Adapter, "get_line_from_item", mock_get_line_from_item)

    boto3.client.return_value.scan.return_value = {
        "Items": [{}],
        "LastEvaluatedKey": last_evaluated_key,
    }

    obj = adapter.Adapter()
    obj.load_policy(model)

    boto3.client.return_value.scan.assert_called_with(
        TableName=table_name, ExclusiveStartKey=last_evaluated_key
    )
    casbin.persist.load_policy_line.assert_called_with(policy_line, model)


def test_get_line_from_item(mocker):
    from python_dycasbin import adapter

    mocker.patch("boto3.client")

    obj = adapter.Adapter()
    result = obj.get_line_from_item(
        {"id": "rand_id", "ptype": {"S": "p"}, "v0": {"S": "user1"}}
    )
    assert result == "p, user1"


def mock_get_line_from_item(itme, model):
    return policy_line


@mock_dynamodb2
def test_remove_policy():
    from python_dycasbin import adapter

    dynamodb = boto3.resource("dynamodb")
    dynamodb.create_table(
        TableName=table_name,
        AttributeDefinitions=[{"AttributeName": "id", "AttributeType": "S"}],
        KeySchema=[
            {"AttributeName": "id", "KeyType": "HASH"},
        ],
        ProvisionedThroughput={"ReadCapacityUnits": 10, "WriteCapacityUnits": 10},
    )

    p_md5 = "8a70746059dc24fa3b1e0d3ba0aa8839"
    table = dynamodb.Table(table_name)
    table.put_item(
        Item={
            "id": p_md5,
            "v0": "sub://cd764ed2-dad7-4705-aa4a-33d833e53c5b",
            "ptype": "g",
            "v1": "group://test grp",
        }
    )

    a = adapter.Adapter(table_name=table_name, create_table=False)

    a.remove_filtered_policy(
        "g", "g", 0, {"sub://cd764ed2-dad7-4705-aa4a-33d833e53c5b"}
    )

    res = table.get_item(Key={"id": p_md5})
    assert res is None
