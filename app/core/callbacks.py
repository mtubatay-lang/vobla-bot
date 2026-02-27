"""
Единый реестр callback_data для Telegram и MAX.
Используются одни и те же строки в обеих платформах.
"""

# --- start ---
START_AUTH = "start_auth"

# --- qa_mode ---
QA_START = "qa_start"
QA_EXIT = "qa_exit"
FB_HELPED_PREFIX = "fb_helped:"  # yes | partial | no
FB_COMP_PREFIX = "fb_comp:"     # 1..5
FB_CLARITY_PREFIX = "fb_clarity:"  # 1..5
FB_SKIP_COMMENT = "fb_skip_comment"

# --- broadcast ---
BROADCAST_START = "broadcast_start"
BROADCAST_EDIT_TEXT = "broadcast:edit_text"
BROADCAST_EDIT_MEDIA = "broadcast:edit_media"
BROADCAST_SKIP_MEDIA = "broadcast:skip_media"
BROADCAST_CANCEL = "broadcast:cancel"
BROADCAST_AUD_TEST_SELF = "broadcast:aud:test_self"
BROADCAST_VARIANT_PREFIX = "broadcast:variant:"  # original | improved
BROADCAST_SEND_PREFIX = "broadcast:send:"  # users | chats | users_chats
BROADCAST_SEND_SELECTED_CHATS = "broadcast:send:selected_chats"
BROADCAST_SEND_SELECTED_REGIONS = "broadcast:send:selected_regions"
BROADCAST_SELECT_CHATS = "broadcast:select_chats"
BROADCAST_CANCEL_SEND = "broadcast:cancel_send"
BROADCAST_SEND_NOW = "broadcast:send_now"
BROADCAST_SCHEDULE_PREFIX = "broadcast:schedule:"  # weekly | monthly
BROADCAST_WEEKDAY_PREFIX = "broadcast:weekday:"   # 0..6
BROADCAST_SEGMENTATION_PREFIX = "broadcast:segmentation:"  # regions | ip
BROADCAST_REGION_TOGGLE_PREFIX = "broadcast:region_toggle:"
BROADCAST_REGIONS_PAGE_PREFIX = "broadcast:regions_page:"
BROADCAST_CHAT_TOGGLE_PREFIX = "broadcast:chat_toggle:"
BROADCAST_CHATS_PAGE_PREFIX = "broadcast:chats_page:"
BROADCAST_SCHEDULED_DISABLE_PREFIX = "broadcast:scheduled_disable:"

# --- knowledge_base_admin ---
KB_ADD = "kb_add"

# --- group_chat_qa ---
KB_CONFIRM_ADD_PREFIX = "kb_confirm_add:"
KB_EDIT_ANSWER_PREFIX = "kb_edit_answer:"

# --- manager_reply / faq ---
MGR_REPLY_PREFIX = "mgr_reply:"
