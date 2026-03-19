from app.ui.keyboards import max_main_menu_rows


def _flatten_callbacks(rows):
    out = []
    for row in rows:
        for btn in row:
            if btn.callback_data:
                out.append(btn.callback_data)
    return out


def test_max_menu_unauthorized_has_auth_and_help_only():
    rows = max_main_menu_rows(is_authorized=False, is_admin=False)
    callbacks = _flatten_callbacks(rows)
    assert "start_auth" in callbacks
    assert "max:menu:help" in callbacks
    assert "qa_start" not in callbacks
    assert "max:menu:admin" not in callbacks


def test_max_menu_authorized_admin_has_admin_button():
    rows = max_main_menu_rows(is_authorized=True, is_admin=True)
    callbacks = _flatten_callbacks(rows)
    assert "qa_start" in callbacks
    assert "max:menu:help" in callbacks
    assert "max:menu:kilbil" in callbacks
    assert "max:menu:admin" in callbacks
