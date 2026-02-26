"""Tests for shared utils._s3_client() helper."""

import os
import pytest
from moto import mock_aws
from utils import _s3_client


class TestS3Client:
    """_s3_client() returns a working (client, bucket) tuple."""

    @mock_aws
    def test_returns_two_tuple(self, monkeypatch):
        monkeypatch.setenv('AWS_ACCESS_KEY_ID', 'testing')
        monkeypatch.setenv('AWS_SECRET_ACCESS_KEY', 'testing')
        monkeypatch.setenv('S3_BUCKET', 'test-bucket')
        result = _s3_client()
        assert isinstance(result, tuple)
        assert len(result) == 2

    @mock_aws
    def test_bucket_comes_from_env_var(self, monkeypatch):
        monkeypatch.setenv('AWS_ACCESS_KEY_ID', 'testing')
        monkeypatch.setenv('AWS_SECRET_ACCESS_KEY', 'testing')
        monkeypatch.setenv('S3_BUCKET', 'my-custom-bucket')
        _, bucket = _s3_client()
        assert bucket == 'my-custom-bucket'

    @mock_aws
    def test_bucket_falls_back_to_config_when_env_not_set(self, monkeypatch):
        monkeypatch.setenv('AWS_ACCESS_KEY_ID', 'testing')
        monkeypatch.setenv('AWS_SECRET_ACCESS_KEY', 'testing')
        monkeypatch.delenv('S3_BUCKET', raising=False)
        from config import S3_BUCKET
        _, bucket = _s3_client()
        assert bucket == S3_BUCKET

    @mock_aws
    def test_default_region_is_us_east_1(self, monkeypatch):
        monkeypatch.setenv('AWS_ACCESS_KEY_ID', 'testing')
        monkeypatch.setenv('AWS_SECRET_ACCESS_KEY', 'testing')
        monkeypatch.setenv('S3_BUCKET', 'test-bucket')
        monkeypatch.delenv('AWS_DEFAULT_REGION', raising=False)
        client, _ = _s3_client()
        assert client.meta.region_name == 'us-east-1'

    @mock_aws
    def test_custom_region_is_used(self, monkeypatch):
        monkeypatch.setenv('AWS_ACCESS_KEY_ID', 'testing')
        monkeypatch.setenv('AWS_SECRET_ACCESS_KEY', 'testing')
        monkeypatch.setenv('S3_BUCKET', 'test-bucket')
        monkeypatch.setenv('AWS_DEFAULT_REGION', 'us-west-2')
        client, _ = _s3_client()
        assert client.meta.region_name == 'us-west-2'

    @mock_aws
    def test_missing_access_key_raises_key_error(self, monkeypatch):
        monkeypatch.delenv('AWS_ACCESS_KEY_ID', raising=False)
        monkeypatch.setenv('AWS_SECRET_ACCESS_KEY', 'testing')
        monkeypatch.setenv('S3_BUCKET', 'test-bucket')
        with pytest.raises(KeyError):
            _s3_client()
