"""Тесты send_media MAX: retry и re-upload при 400 video token."""

from unittest.mock import AsyncMock, MagicMock

import pytest

from app.platforms.max.adapter import MaxAdapter
from app.platforms.max.client import MaxApiClient, MaxApiClientError


@pytest.mark.asyncio
async def test_send_media_retries_with_reupload_on_missing_video_token():
    client = MagicMock(spec=MaxApiClient)
    client.send_message = AsyncMock(
        side_effect=[
            MaxApiClientError(
                "MAX API error: 400",
                status=400,
                body='{"message":"Missing `token` in video attachment"}',
            ),
            {"message": {"id": "sent-1"}},
        ]
    )
    adapter = MaxAdapter(client)
    adapter.download_file = AsyncMock(
        return_value=(
            b"fake-xlsx",
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )
    )
    adapter.upload_document_bytes = AsyncMock(return_value="new-uploaded-id")

    await adapter.send_media(
        42,
        [
            {
                "type": "video",
                "file_id": "incoming-id",
                "id_or_url": "incoming-id",
                "source_message_id": "msg-99",
            }
        ],
        caption="hi",
        reupload_untrusted_video=False,
    )

    assert client.send_message.call_count == 2
    second = client.send_message.call_args_list[1]
    assert second[0][0] == 42
    atts = second[1]["attachments"]
    assert len(atts) == 1
    assert atts[0]["type"] == "file"
    assert atts[0]["payload"] == {"file_id": "new-uploaded-id"}
    adapter.download_file.assert_awaited()
    adapter.upload_document_bytes.assert_awaited()


@pytest.mark.asyncio
async def test_send_media_reupload_flag_avoids_double_send_when_ok():
    client = MagicMock(spec=MaxApiClient)
    client.send_message = AsyncMock(return_value={"message": {"id": "1"}})
    adapter = MaxAdapter(client)
    adapter.download_file = AsyncMock(return_value=(b"x", "application/octet-stream"))
    adapter.upload_document_bytes = AsyncMock(return_value="up-1")

    await adapter.send_media(
        1,
        [{"type": "video", "file_id": "a", "id_or_url": "a", "source_message_id": "m"}],
        reupload_untrusted_video=True,
    )

    assert client.send_message.call_count == 1
    atts = client.send_message.call_args[1]["attachments"]
    assert atts[0]["type"] == "file"
    assert atts[0]["payload"]["file_id"] == "up-1"


@pytest.mark.asyncio
async def test_send_media_reupload_failure_falls_back_to_original_file_id():
    """При неудачном скачивании/загрузке не отправляем video — только file + исходный id."""
    client = MagicMock(spec=MaxApiClient)
    client.send_message = AsyncMock(return_value={"message": {"id": "1"}})
    adapter = MaxAdapter(client)
    adapter.download_file = AsyncMock(
        side_effect=MaxApiClientError("no file", status=404, body="{}")
    )
    adapter.upload_document_bytes = AsyncMock()

    await adapter.send_media(
        1,
        [
            {
                "type": "video",
                "file_id": "orig-fid",
                "id_or_url": "orig-fid",
                "source_message_id": "m1",
            }
        ],
        reupload_untrusted_video=True,
    )

    assert client.send_message.call_count == 1
    atts = client.send_message.call_args[1]["attachments"]
    assert atts[0]["type"] == "file"
    assert atts[0]["payload"] == {"file_id": "orig-fid"}
    adapter.upload_document_bytes.assert_not_called()
